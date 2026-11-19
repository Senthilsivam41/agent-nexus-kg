"""Tests for IcebergPuppyGraphStore zero-ETL Parquet partitioning, pruning, and remote query."""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

import scale_kg.adapters.iceberg_puppygraph as iceberg_module
from scale_kg.adapters import IcebergPuppyGraphStore, InMemoryGraphStore
from scale_kg.core import (
    Domain,
    EntityNode,
    GuardrailViolationCode,
    RelationshipEdge,
    SupernodeBucketer,
    TraversalRequest,
    compute_bucket_id,
)


def _build_multi_bucket_dataset() -> tuple[list[EntityNode], list[RelationshipEdge]]:
    nodes: list[EntityNode] = [
        EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
        ),
        EntityNode(
            id="product:sdk:kinetic", domain=Domain.PRODUCT, entity_type="Product"
        ),
        EntityNode(
            id="knowledge:doc:guide",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            lakehouse_ref="s3://lakehouse/knowledge/guide.parquet",
        ),
    ]
    edges: list[RelationshipEdge] = [
        RelationshipEdge(
            source_id="technology:lang:python",
            target_id="product:sdk:kinetic",
            edge_type="POWERS",
            domain=Domain.TECHNOLOGY,
            target_domain=Domain.PRODUCT,
        ),
        RelationshipEdge(
            source_id="product:sdk:kinetic",
            target_id="knowledge:doc:guide",
            edge_type="DESCRIBED_BY",
            domain=Domain.PRODUCT,
            target_domain=Domain.KNOWLEDGE,
        ),
    ]
    for i in range(40):
        org_id = f"customer:org:{i}"
        nodes.append(
            EntityNode(id=org_id, domain=Domain.CUSTOMER, entity_type="Organization")
        )
        edges.append(
            RelationshipEdge(
                source_id=org_id,
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            )
        )
    return nodes, edges


@pytest.mark.asyncio
class TestIcebergPuppyGraphStore:
    async def test_writes_domain_bucket_parquet_partitions_and_puppygraph_schema(
        self, tmp_path: Path
    ) -> None:
        warehouse = tmp_path / "iceberg_warehouse"
        store = IcebergPuppyGraphStore(warehouse)

        assert store.schema_path.exists()
        schema = json.loads(store.schema_path.read_text(encoding="utf-8"))
        assert schema["catalogs"][0]["type"] == "iceberg"
        assert schema["graph"]["vertices"][0]["mappedTableSource"]["table"] == "nodes"
        assert schema["graph"]["edges"][0]["mappedTableSource"]["table"] == "edges"
        assert (warehouse / "metadata" / "v1.metadata.json").exists()

        nodes, edges = _build_multi_bucket_dataset()
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        expected_bucket = compute_bucket_id("customer:org:0", 16)
        expected_node_parquet = (
            warehouse
            / "nodes"
            / "domain=customer"
            / f"bucket_16={expected_bucket}"
            / "part-00000.parquet"
        )
        expected_edge_parquet = (
            warehouse
            / "edges"
            / "domain=customer"
            / f"bucket_16={expected_bucket}"
            / "part-00000.parquet"
        )
        assert expected_node_parquet.exists()
        assert expected_edge_parquet.exists()

        table = pq.read_table(expected_node_parquet)
        assert "customer:org:0" in table.column("id").to_pylist()

    async def test_partition_pruning_achieves_at_least_50_percent_io_reduction(
        self, tmp_path: Path
    ) -> None:
        store = IcebergPuppyGraphStore(tmp_path / "warehouse")
        nodes, edges = _build_multi_bucket_dataset()
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        # Single node lookup scans only matching bucket_16 partitions
        node = await store.get_node("customer:org:0")
        assert node is not None
        assert store.last_scan_stats.total_partitions >= 10
        assert store.last_scan_stats.pruning_ratio >= 0.5

        # 1-hop traversal scans only partitions for seed bucket(s)
        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:0"], max_depth=1)
        )
        assert res.metrics.aborted is False
        assert store.last_scan_stats.pruning_ratio >= 0.5
        assert store.cumulative_scan_stats.pruning_ratio >= 0.5

    async def test_local_pyarrow_matches_in_memory_parity_with_bucketed_supernodes(
        self, tmp_path: Path
    ) -> None:
        nodes, edges = _build_multi_bucket_dataset()
        bucketer = SupernodeBucketer(degree_threshold=20, num_buckets=16)
        b_nodes, b_edges, supernodes = bucketer.transform_dataset(nodes, edges)
        assert "technology:lang:python" in supernodes

        mem_store = InMemoryGraphStore()
        ice_store = IcebergPuppyGraphStore(tmp_path / "warehouse")

        await mem_store.upsert_nodes(b_nodes)
        await mem_store.upsert_edges(b_edges)
        await ice_store.upsert_nodes(b_nodes)
        await ice_store.upsert_edges(b_edges)

        req = TraversalRequest(seed_ids=["customer:org:3"], max_depth=3)
        mem_res = await mem_store.traverse_k_hop(req)
        ice_res = await ice_store.traverse_k_hop(req)

        assert {n.id for n in ice_res.nodes} == {n.id for n in mem_res.nodes}
        assert {(e.source_id, e.target_id, e.edge_type) for e in ice_res.edges} == {
            (e.source_id, e.target_id, e.edge_type) for e in mem_res.edges
        }
        assert sorted(ice_res.paths) == sorted(mem_res.paths)
        assert ice_res.metrics.depth_reached == mem_res.metrics.depth_reached == 3
        assert (
            ice_res.metrics.bucket_hops_unwrapped
            == mem_res.metrics.bucket_hops_unwrapped
        )

    @pytest.mark.parametrize("protocol", ["cypher", "gremlin"])
    async def test_delegates_to_remote_puppygraph_endpoint_when_configured(
        self, tmp_path: Path, protocol: str
    ) -> None:
        captured: list[tuple[str, dict[str, Any]]] = []

        class _MockPuppyGraphTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                if request.method == "GET" and request.url.path == "/health":
                    return httpx.Response(200, json={"status": "ok"})
                payload = json.loads(request.content.decode("utf-8"))
                captured.append((request.url.path, payload))
                return httpx.Response(
                    200,
                    json={
                        "nodes": [
                            {
                                "id": "customer:org:0",
                                "domain": "customer",
                                "entity_type": "Organization",
                            },
                            {
                                "id": "technology:lang:python",
                                "domain": "technology",
                                "entity_type": "Language",
                            },
                        ],
                        "edges": [
                            {
                                "source_id": "customer:org:0",
                                "target_id": "technology:lang:python",
                                "edge_type": "USES",
                                "domain": "customer",
                                "target_domain": "technology",
                            }
                        ],
                        "paths": [
                            [
                                "customer:org:0",
                                "technology:lang:python::bucket::3",
                                "technology:lang:python",
                            ]
                        ],
                    },
                )

        store = IcebergPuppyGraphStore(
            tmp_path / "warehouse",
            puppygraph_url="http://puppygraph.mock:8081",
            query_protocol=protocol,  # type: ignore[arg-type]
            transport=_MockPuppyGraphTransport(),
        )
        health = await store.health_check()
        assert health.healthy is True
        assert health.details["execution_mode"] == "remote_puppygraph"

        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:0"], max_depth=2)
        )
        assert res.metrics.bucket_hops_unwrapped == 1
        assert res.paths == [["customer:org:0", "technology:lang:python"]]
        assert captured[0][0] == f"/{protocol}"

    async def test_remote_puppygraph_bucketing_neighbors_pruning_and_guardrails(
        self, tmp_path: Path
    ) -> None:
        captured_queries: list[dict[str, Any]] = []

        class _BucketPuppyTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                payload = json.loads(request.content.decode("utf-8"))
                captured_queries.append(payload)
                query = payload.get("query", "")
                if "WHERE s.id IN $ids" in query:
                    return httpx.Response(
                        200,
                        json={
                            "neighbors": [
                                {
                                    "edge": {
                                        "source_id": "technology:lang:python::bucket::3",
                                        "target_id": "product:sdk:kinetic",
                                        "edge_type": "POWERS",
                                        "domain": "technology",
                                        "target_domain": "product",
                                    },
                                    "node": {
                                        "id": "product:sdk:kinetic",
                                        "domain": "product",
                                        "entity_type": "Product",
                                    },
                                },
                                {
                                    "edge": {
                                        "source_id": "technology:lang:python",
                                        "target_id": "technology:lang:python::bucket::3",
                                        "edge_type": "BUCKET_LINK",
                                        "domain": "technology",
                                        "target_domain": "technology",
                                    },
                                    "node": {
                                        "id": "technology:lang:python::bucket::3",
                                        "domain": "technology",
                                        "entity_type": "LanguageBucket",
                                        "is_bucket": True,
                                    },
                                },
                            ]
                        },
                    )
                return httpx.Response(
                    200,
                    json={
                        "nodes": [
                            {
                                "id": "customer:org:0",
                                "domain": "customer",
                                "entity_type": "Organization",
                            },
                            {
                                "id": "technology:lang:python::bucket::3",
                                "domain": "technology",
                                "entity_type": "LanguageBucket",
                                "is_bucket": True,
                            },
                            {
                                "id": "technology:lang:python",
                                "domain": "technology",
                                "entity_type": "Language",
                            },
                            {
                                "id": "product:sdk:kinetic",
                                "domain": "product",
                                "entity_type": "Product",
                            },
                        ],
                        "edges": [
                            {
                                "source_id": "customer:org:0",
                                "target_id": "technology:lang:python::bucket::3",
                                "edge_type": "USES",
                                "domain": "customer",
                                "target_domain": "technology",
                            },
                            {
                                "source_id": "technology:lang:python::bucket::3",
                                "target_id": "technology:lang:python",
                                "edge_type": "BUCKET_LINK",
                                "domain": "technology",
                                "target_domain": "technology",
                            },
                            {
                                "source_id": "technology:lang:python::bucket::7",
                                "target_id": "product:sdk:kinetic",
                                "edge_type": "POWERS",
                                "domain": "technology",
                                "target_domain": "product",
                            },
                        ],
                        "paths": [
                            [
                                "customer:org:0",
                                "technology:lang:python::bucket::3",
                                "technology:lang:python",
                            ],
                            [
                                "customer:org:0",
                                "technology:lang:python::bucket::3",
                                "technology:lang:python",
                                "technology:lang:python::bucket::7",
                                "product:sdk:kinetic",
                            ],
                        ],
                    },
                )

        store = IcebergPuppyGraphStore(
            tmp_path / "warehouse",
            puppygraph_url="http://puppygraph.mock:8081",
            query_protocol="cypher",
            transport=_BucketPuppyTransport(),
        )
        neighbors = await store.get_neighbors("technology:lang:python", direction="out")
        assert len(captured_queries[0]["parameters"]["ids"]) == 17
        assert (
            "technology:lang:python::bucket::15"
            in (captured_queries[0]["parameters"]["ids"])
        )
        assert [
            (e.source_id, e.target_id, e.edge_type, n.id) for e, n in neighbors
        ] == [
            (
                "technology:lang:python",
                "product:sdk:kinetic",
                "POWERS",
                "product:sdk:kinetic",
            )
        ]

        # max_depth=1 sends *1..3 physical hops, unwraps ::bucket::k on edges, and prunes 2-logical-hop path
        res = await store.traverse_k_hop(
            TraversalRequest(
                seed_ids=["customer:org:0"], max_depth=1, edge_types=["USES"]
            )
        )
        assert "*1..3" in captured_queries[1]["query"]
        assert "BUCKET_LINK" in captured_queries[1]["query"]
        assert res.paths == [["customer:org:0", "technology:lang:python"]]
        assert {n.id for n in res.nodes} == {"customer:org:0", "technology:lang:python"}
        assert [(e.source_id, e.target_id, e.edge_type) for e in res.edges] == [
            ("customer:org:0", "technology:lang:python", "USES")
        ]

        # RuntimeGuardrailTracker aborts remote PuppyGraph traversal when max_results is exceeded
        aborted_res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:0"], max_depth=2, max_results=2)
        )
        assert aborted_res.metrics.aborted is True
        assert aborted_res.metrics.violation is not None
        assert (
            aborted_res.metrics.violation.code
            == GuardrailViolationCode.MAX_RESULTS_EXCEEDED
        )

    async def test_local_in_neighbors_scans_each_edge_parquet_file_once_for_all_buckets(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nodes, edges = _build_multi_bucket_dataset()
        bucketer = SupernodeBucketer(degree_threshold=20, num_buckets=16)
        b_nodes, b_edges, supernodes = bucketer.transform_dataset(nodes, edges)
        assert "technology:lang:python" in supernodes

        store = IcebergPuppyGraphStore(tmp_path / "warehouse")
        await store.upsert_nodes(b_nodes)
        await store.upsert_edges(b_edges)

        read_in_calls: list[Sequence[str]] = []
        orig_read_in = store._read_in_edges_local

        def _spy_read_in(
            candidate_ids: Sequence[str],
            scanned_tracker: set[Path] | None = None,
            *,
            all_files: list[Path] | None = None,
            partition_cache: dict[Path, Any] | None = None,
        ) -> list[RelationshipEdge]:
            read_in_calls.append(candidate_ids)
            return orig_read_in(
                candidate_ids,
                scanned_tracker,
                all_files=all_files,
                partition_cache=partition_cache,
            )

        edge_files_read: list[Path] = []
        orig_read_table = pq.read_table

        def _spy_read_table(source: Any, *args: Any, **kwargs: Any) -> Any:
            path = Path(source)
            if store.edges_dir in path.parents:
                edge_files_read.append(path)
            return orig_read_table(source, *args, **kwargs)

        monkeypatch.setattr(store, "_read_in_edges_local", _spy_read_in)
        monkeypatch.setattr(iceberg_module.pq, "read_table", _spy_read_table)

        neighbors = await store.get_neighbors("technology:lang:python", direction="in")
        assert {n.id for _, n in neighbors} == {f"customer:org:{i}" for i in range(40)}
        assert all(
            e.target_id == "technology:lang:python" and e.edge_type == "USES"
            for e, _ in neighbors
        )

        # Single batch call with all 17 candidate IDs (logical hub + 16 buckets)
        assert len(read_in_calls) == 1
        assert not isinstance(read_in_calls[0], str)
        assert len(read_in_calls[0]) == 17
        assert "technology:lang:python" in read_in_calls[0]
        assert "technology:lang:python::bucket::15" in read_in_calls[0]

        # Each edge Parquet partition on disk is scanned once, not 17x
        expected_edge_files = sorted(
            store.edges_dir.glob("domain=*/bucket_16=*/*.parquet")
        )
        assert len(edge_files_read) == len(expected_edge_files)
        assert len(set(edge_files_read)) == len(expected_edge_files)
