"""Comprehensive E2 validation suite covering SEN-62, SEN-69, SEN-70, SEN-71, and SEN-72."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from test_adapters_and_golden_queries import _HugeGraphMockTransport, _Neo4jMockDriver

from scale_kg.adapters import (
    FaultMode,
    GraphStore,
    GraphStoreError,
    GraphStoreUnavailableError,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    JanusGraphStore,
    NebulaGraphStore,
    Neo4jGraphStore,
)
from scale_kg.core import (
    Domain,
    EntityNode,
    GuardrailEnforcer,
    GuardrailViolationCode,
    RelationshipEdge,
    TraversalRequest,
)


class TestE21GraphStoreProtocolAndInMemory:
    """Validation for SEN-69 ([E2.1])."""

    def test_graph_store_abc_cannot_be_instantiated_directly(self) -> None:
        with pytest.raises(TypeError):
            GraphStore()  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_in_memory_idempotent_upserts_directions_filters_and_derived_toggle(
        self,
    ) -> None:
        store = InMemoryGraphStore()
        nodes = [
            EntityNode(
                id="customer:org:1", domain=Domain.CUSTOMER, entity_type="Organization"
            ),
            EntityNode(
                id="technology:db:pg", domain=Domain.TECHNOLOGY, entity_type="Database"
            ),
            EntityNode(
                id="product:app:crm", domain=Domain.PRODUCT, entity_type="Product"
            ),
        ]
        await store.upsert_nodes(nodes)
        # Re-upsert updates domain cleanly without duplicating counts
        moved = nodes[0].model_copy(update={"domain": Domain.PRODUCT})
        await store.upsert_nodes([moved])
        stats = await store.count_stats()
        assert stats.node_count == 3
        assert stats.nodes_by_domain[Domain.CUSTOMER.value] == 0
        assert stats.nodes_by_domain[Domain.PRODUCT.value] == 2

        # Restore original domain
        await store.upsert_nodes([nodes[0]])

        edges = [
            RelationshipEdge(
                source_id="customer:org:1",
                target_id="technology:db:pg",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="product:app:crm",
                target_id="technology:db:pg",
                edge_type="DEPENDS_ON",
                domain=Domain.PRODUCT,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="customer:org:1",
                target_id="product:app:crm",
                edge_type="SIMILAR_TO",
                domain=Domain.CUSTOMER,
                target_domain=Domain.PRODUCT,
                is_derived=True,
                materialized_hot_path=True,
            ),
        ]
        await store.upsert_edges(edges)
        await store.upsert_edges(edges)  # Idempotent re-upsert
        assert (await store.count_stats()).edge_count == 3

        # Incoming neighbors & direction="both"
        incoming = await store.get_neighbors("technology:db:pg", direction="in")
        assert {(e.edge_type, n.id) for e, n in incoming} == {
            ("USES", "customer:org:1"),
            ("DEPENDS_ON", "product:app:crm"),
        }
        filtered_in = await store.get_neighbors(
            "technology:db:pg",
            direction="in",
            edge_types=["DEPENDS_ON"],
            domains=[Domain.PRODUCT],
            limit=1,
        )
        assert [(e.edge_type, n.id) for e, n in filtered_in] == [
            ("DEPENDS_ON", "product:app:crm")
        ]

        # Derived materialized hot-path edge respected by include_derived flag
        res_no_derived = await store.traverse_k_hop(
            TraversalRequest(
                seed_ids=["customer:org:1"], max_depth=1, include_derived=False
            )
        )
        assert {n.id for n in res_no_derived.nodes} == {
            "customer:org:1",
            "technology:db:pg",
        }

        res_with_derived = await store.traverse_k_hop(
            TraversalRequest(
                seed_ids=["customer:org:1"], max_depth=1, include_derived=True
            )
        )
        assert {n.id for n in res_with_derived.nodes} == {
            "customer:org:1",
            "technology:db:pg",
            "product:app:crm",
        }

    @pytest.mark.asyncio
    async def test_fault_scoping_degraded_timeout_and_runtime_budget_aborts(
        self,
    ) -> None:
        now = [10.0]
        enforcer = GuardrailEnforcer(clock=lambda: now[0])
        store = InMemoryGraphStore(enforcer=enforcer)
        nodes = [
            EntityNode(
                id="customer:org:a", domain=Domain.CUSTOMER, entity_type="Organization"
            ),
            EntityNode(
                id="technology:lang:go",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
            EntityNode(
                id="technology:lang:rust",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="customer:org:a",
                target_id="technology:lang:go",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="technology:lang:go",
                target_id="technology:lang:rust",
                edge_type="INSPIRES",
                domain=Domain.TECHNOLOGY,
                target_domain=Domain.TECHNOLOGY,
            ),
        ]
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        # Domain-scoped fault blocks customer but allows technology reads
        await store.set_fault_mode(FaultMode.NODE_DOWN, target_domain=Domain.CUSTOMER)
        with pytest.raises(GraphStoreUnavailableError):
            await store.get_node("customer:org:a")
        assert await store.get_node("technology:lang:go") is not None

        # Replica degraded keeps health_check healthy=True with status="degraded"
        await store.set_fault_mode(FaultMode.REPLICA_DEGRADED, latency_ms=1.0)
        degraded = await store.health_check()
        assert degraded.healthy is True
        assert degraded.status == "degraded"

        # Timeout fault raises TimeoutError
        await store.set_fault_mode(FaultMode.TIMEOUT)
        with pytest.raises(TimeoutError):
            await store.get_node("technology:lang:go")

        await store.set_fault_mode(FaultMode.NONE)

        # Runtime max_expanded_nodes abort
        res_exp = await store.traverse_k_hop(
            TraversalRequest(
                seed_ids=["customer:org:a"], max_depth=3, max_expanded_nodes=1
            )
        )
        assert res_exp.metrics.aborted is True
        assert res_exp.metrics.violation is not None
        assert (
            res_exp.metrics.violation.code
            == GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED
        )

        # Runtime execution time abort via deterministic clock
        def _advancing_clock() -> float:
            now[0] += 0.2
            return now[0]

        timed_store = InMemoryGraphStore(
            enforcer=GuardrailEnforcer(clock=_advancing_clock)
        )
        await timed_store.upsert_nodes(nodes)
        await timed_store.upsert_edges(edges)
        res_time = await timed_store.traverse_k_hop(
            TraversalRequest(
                seed_ids=["customer:org:a"], max_depth=3, max_execution_seconds=0.1
            )
        )
        assert res_time.metrics.aborted is True
        assert res_time.metrics.violation is not None
        assert (
            res_time.metrics.violation.code
            == GuardrailViolationCode.MAX_EXECUTION_TIME_EXCEEDED
        )


class TestE22IcebergPuppyGraphValidation:
    """Validation for SEN-70 ([E2.2])."""

    @pytest.mark.asyncio
    async def test_persists_across_reopen_merges_idempotently_and_queries_inbound(
        self, tmp_path: Path
    ) -> None:
        warehouse = tmp_path / "iceberg"
        store1 = IcebergPuppyGraphStore(warehouse)
        nodes = [
            EntityNode(
                id="customer:org:1", domain=Domain.CUSTOMER, entity_type="Organization"
            ),
            EntityNode(
                id="product:sku:1", domain=Domain.PRODUCT, entity_type="Product"
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="customer:org:1",
                target_id="product:sku:1",
                edge_type="PURCHASED",
                domain=Domain.CUSTOMER,
                target_domain=Domain.PRODUCT,
            )
        ]
        await store1.upsert_nodes(nodes)
        await store1.upsert_edges(edges)
        await store1.upsert_nodes(nodes)
        await store1.upsert_edges(edges)

        # Reopen from disk without keeping in-memory state
        store2 = IcebergPuppyGraphStore(warehouse)
        stats = await store2.count_stats()
        assert stats.node_count == 2
        assert stats.edge_count == 1

        in_neighbors = await store2.get_neighbors("product:sku:1", direction="in")
        assert [(e.edge_type, n.id) for e, n in in_neighbors] == [
            ("PURCHASED", "customer:org:1")
        ]

        # Query builders respect guardrail ceilings
        cypher, c_params = store2.build_cypher_query(
            TraversalRequest(
                seed_ids=["customer:org:1"], max_depth=9, max_results=99999
            )
        )
        assert "*1..3" in cypher and "LIMIT 5000" in cypher
        assert c_params["max_depth"] == 3 and c_params["limit"] == 5000

        gremlin, g_params = store2.build_gremlin_query(
            TraversalRequest(
                seed_ids=["customer:org:1"], max_depth=5, max_results=99999
            )
        )
        assert ".times(3)" in gremlin and ".limit(5000)" in gremlin
        assert g_params["limit"] == 5000

    @pytest.mark.asyncio
    async def test_remote_puppygraph_error_handling(self, tmp_path: Path) -> None:
        class _FailingTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                return httpx.Response(503, json={"error": "engine busy"})

        store = IcebergPuppyGraphStore(
            tmp_path / "iceberg",
            puppygraph_url="http://puppygraph.mock:8081",
            transport=_FailingTransport(),
        )
        health = await store.health_check()
        assert health.healthy is False
        assert health.status == "unavailable"

        with pytest.raises(GraphStoreError):
            await store.traverse_k_hop(
                TraversalRequest(seed_ids=["customer:org:1"], max_depth=1)
            )


class TestE23HugeGraphValidation:
    """Validation for SEN-71 ([E2.3])."""

    @pytest.mark.asyncio
    async def test_hugegraph_404_errors_timeout_and_bucket_unwrapping(self) -> None:
        class _EdgeCaseTransport(httpx.AsyncBaseTransport):
            def __init__(self, mode: str) -> None:
                self.mode = mode

            async def handle_async_request(
                self, request: httpx.Request
            ) -> httpx.Response:
                if self.mode == "timeout":
                    raise httpx.ReadTimeout("timed out", request=request)
                if self.mode == "error":
                    return httpx.Response(500, json={"message": "HStore raft error"})
                if request.method == "GET" and "/graph/vertices/" in request.url.path:
                    return httpx.Response(404, json={"message": "missing"})
                if request.method == "POST" and request.url.path.endswith("/gremlin"):
                    return httpx.Response(
                        200,
                        json={
                            "vertices": [
                                {
                                    "id": "customer:org:1",
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
                            ],
                            "edges": [
                                {
                                    "source_id": "customer:org:1",
                                    "target_id": "technology:lang:python::bucket::3",
                                    "edge_type": "USES",
                                    "domain": "customer",
                                    "target_domain": "technology",
                                }
                            ],
                            "paths": [
                                [
                                    "customer:org:1",
                                    "technology:lang:python::bucket::3",
                                    "technology:lang:python",
                                ]
                            ],
                        },
                    )
                return httpx.Response(200, json={})

        ok_store = HugeGraphStore(
            "http://hg.mock",
            tenant_id="t1",
            transport=_EdgeCaseTransport("ok"),
        )
        assert await ok_store.get_node("missing:id") is None
        hint1 = ok_store.partition_hint("customer:org:1", Domain.CUSTOMER)
        hint2 = ok_store.partition_hint("customer:org:1", Domain.CUSTOMER)
        assert hint1 == hint2 and hint1.startswith("customer:t1:")

        res = await ok_store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:1"], max_depth=2)
        )
        assert [n.id for n in res.nodes] == ["customer:org:1", "technology:lang:python"]
        assert res.paths == [["customer:org:1", "technology:lang:python"]]
        assert res.metrics.bucket_hops_unwrapped == 1

        err_store = HugeGraphStore(
            "http://hg.mock", transport=_EdgeCaseTransport("error")
        )
        with pytest.raises(GraphStoreError):
            await err_store.get_node("customer:org:1")

        to_store = HugeGraphStore(
            "http://hg.mock", transport=_EdgeCaseTransport("timeout")
        )
        with pytest.raises(TimeoutError):
            await to_store.get_node("customer:org:1")


class TestE24Neo4jAndStubsValidation:
    """Validation for SEN-72 ([E2.4])."""

    @pytest.mark.asyncio
    async def test_neo4j_requires_driver_or_extra_and_routes_shards(self) -> None:
        store_no_driver = Neo4jGraphStore()
        assert store_no_driver.shard_for_domain(Domain.CUSTOMER) == "kg_customer"
        assert store_no_driver.shard_for_domain(None) == "composite"

        with pytest.raises(GraphStoreError):
            await store_no_driver.get_node("customer:org:1")

        proxy = Neo4jGraphStore.create_proxy_node(
            "product:sku:1",
            owning_domain=Domain.CUSTOMER,
            target_domain=Domain.PRODUCT,
        )
        assert proxy.is_proxy is True
        assert proxy.domain == Domain.CUSTOMER
        assert proxy.target_domain == Domain.PRODUCT

        cypher, params = store_no_driver.build_k_hop_cypher(
            TraversalRequest(
                seed_ids=["customer:org:1"], max_depth=2, edge_types=["USES"]
            )
        )
        assert "r:REL*1..2" in cypher and "LIMIT 5000" in cypher
        assert params["edge_types"] == ["USES"]


class TestE2EndToEndParityAcrossAllAdapters:
    """Cross-adapter parity check covering SEN-62."""

    @pytest.mark.asyncio
    async def test_all_six_adapters_return_identical_3_hop_logical_subgraph(
        self, tmp_path: Path
    ) -> None:
        nodes = [
            EntityNode(
                id="customer:org:alpha",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="technology:lang:python",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
            EntityNode(
                id="product:sdk:core", domain=Domain.PRODUCT, entity_type="Product"
            ),
            EntityNode(
                id="knowledge:doc:spec",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lakehouse/spec.parquet",
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="customer:org:alpha",
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="technology:lang:python",
                target_id="product:sdk:core",
                edge_type="POWERS",
                domain=Domain.TECHNOLOGY,
                target_domain=Domain.PRODUCT,
            ),
            RelationshipEdge(
                source_id="product:sdk:core",
                target_id="knowledge:doc:spec",
                edge_type="DESCRIBED_BY",
                domain=Domain.PRODUCT,
                target_domain=Domain.KNOWLEDGE,
            ),
        ]

        adapters: list[GraphStore] = [
            InMemoryGraphStore(),
            IcebergPuppyGraphStore(tmp_path / "warehouse"),
            HugeGraphStore("http://hg.mock", transport=_HugeGraphMockTransport()),
            Neo4jGraphStore(driver=_Neo4jMockDriver()),
            JanusGraphStore(),
            NebulaGraphStore(),
        ]

        req = TraversalRequest(seed_ids=["customer:org:alpha"], max_depth=3)
        observed_node_sets: list[set[str]] = []
        observed_edge_sets: list[set[tuple[str, str, str]]] = []
        for adapter in adapters:
            await adapter.upsert_nodes(nodes)
            await adapter.upsert_edges(edges)
            res = await adapter.traverse_k_hop(req)
            assert res.metrics.aborted is False
            assert res.metrics.depth_reached == 3
            observed_node_sets.append({n.id for n in res.nodes})
            observed_edge_sets.append(
                {(e.source_id, e.target_id, e.edge_type) for e in res.edges}
            )

        expected_nodes = {
            "customer:org:alpha",
            "technology:lang:python",
            "product:sdk:core",
            "knowledge:doc:spec",
        }
        expected_edges = {
            ("customer:org:alpha", "technology:lang:python", "USES"),
            ("technology:lang:python", "product:sdk:core", "POWERS"),
            ("product:sdk:core", "knowledge:doc:spec", "DESCRIBED_BY"),
        }
        assert all(ns == expected_nodes for ns in observed_node_sets)
        assert all(es == expected_edges for es in observed_edge_sets)
