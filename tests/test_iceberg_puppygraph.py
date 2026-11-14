"""Tests for IcebergPuppyGraphStore zero-ETL Parquet partitioning, pruning, and remote query."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from scale_kg.adapters import IcebergPuppyGraphStore, InMemoryGraphStore
from scale_kg.core import (
    Domain,
    EntityNode,
    RelationshipEdge,
    SupernodeBucketer,
    TraversalRequest,
    compute_bucket_id,
)


def _build_multi_bucket_dataset() -> tuple[list[EntityNode], list[RelationshipEdge]]:
    nodes: list[EntityNode] = [
        EntityNode(id="technology:lang:python", domain=Domain.TECHNOLOGY, entity_type="Language"),
        EntityNode(id="product:sdk:kinetic", domain=Domain.PRODUCT, entity_type="Product"),
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
        assert ice_res.metrics.bucket_hops_unwrapped == mem_res.metrics.bucket_hops_unwrapped

    @pytest.mark.parametrize("protocol", ["cypher", "gremlin"])
    async def test_delegates_to_remote_puppygraph_endpoint_when_configured(
        self, tmp_path: Path, protocol: str
    ) -> None:
        captured: list[tuple[str, dict[str, Any]]] = []

        class _MockPuppyGraphTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
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
