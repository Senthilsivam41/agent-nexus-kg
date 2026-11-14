"""Shared GraphStore contract tests and adapter-specific tests for E2 (SEN-62)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import httpx
import pytest

from scale_kg.adapters import (
    FaultMode,
    GraphStore,
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
    GuardrailViolationError,
    RelationshipEdge,
    SupernodeBucketer,
    TraversalRequest,
)


def _sample_graph() -> tuple[list[EntityNode], list[RelationshipEdge]]:
    nodes = [
        EntityNode(id="customer:org:acme", domain=Domain.CUSTOMER, entity_type="Organization"),
        EntityNode(id="technology:lang:python", domain=Domain.TECHNOLOGY, entity_type="Language"),
        EntityNode(id="product:sdk:kinetic", domain=Domain.PRODUCT, entity_type="Product"),
        EntityNode(
            id="knowledge:doc:arch",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            lakehouse_ref="s3://lakehouse/knowledge/arch.parquet",
        ),
        EntityNode(id="customer:org:beta", domain=Domain.CUSTOMER, entity_type="Organization"),
    ]
    edges = [
        RelationshipEdge(
            source_id="customer:org:acme",
            target_id="technology:lang:python",
            edge_type="USES",
            domain=Domain.CUSTOMER,
            target_domain=Domain.TECHNOLOGY,
        ),
        RelationshipEdge(
            source_id="technology:lang:python",
            target_id="product:sdk:kinetic",
            edge_type="POWERS",
            domain=Domain.TECHNOLOGY,
            target_domain=Domain.PRODUCT,
        ),
        RelationshipEdge(
            source_id="product:sdk:kinetic",
            target_id="knowledge:doc:arch",
            edge_type="DESCRIBED_BY",
            domain=Domain.PRODUCT,
            target_domain=Domain.KNOWLEDGE,
        ),
        RelationshipEdge(
            source_id="customer:org:beta",
            target_id="customer:org:acme",
            edge_type="PARTNERS_WITH",
            domain=Domain.CUSTOMER,
            target_domain=Domain.CUSTOMER,
        ),
    ]
    return nodes, edges


class _HugeGraphMockTransport(httpx.AsyncBaseTransport):
    """Stateful HugeGraph REST/Gremlin/OpenCypher mock verifying PD headers."""

    def __init__(self) -> None:
        self.backing = InMemoryGraphStore()
        self.recorded_headers: list[dict[str, str]] = []
        self.recorded_paths: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.recorded_headers.append(dict(request.headers))
        self.recorded_paths.append(request.url.path)
        path = request.url.path
        method = request.method
        body = json.loads(request.content.decode("utf-8")) if request.content else None

        if method == "GET" and path == "/versions":
            return httpx.Response(200, json={"versions": {"api": "1.5.0"}})

        if method == "POST" and path.endswith("/graph/vertices/batch"):
            nodes = [HugeGraphStore._deserialize_vertex(item) for item in body]
            await self.backing.upsert_nodes(nodes)
            return httpx.Response(201, json={"count": len(nodes)})

        if method == "POST" and path.endswith("/graph/edges/batch"):
            edges = [HugeGraphStore._deserialize_edge(item) for item in body]
            await self.backing.upsert_edges(edges)
            return httpx.Response(201, json={"count": len(edges)})

        if method == "GET" and "/graph/vertices/" in path:
            node_id = unquote(path.rsplit("/graph/vertices/", 1)[1])
            node = await self.backing.get_node(node_id)
            if node is None:
                return httpx.Response(404, json={"message": "Not found"})
            return httpx.Response(200, json=node.model_dump(mode="json"))

        if method == "GET" and path.endswith("/traversers/edges"):
            q = request.url.params
            vid = q["vertex_id"]
            direction = q.get("direction", "OUT").lower()
            edge_types = q["labels"].split(",") if q.get("labels") else None
            domains = [Domain(d) for d in q["domains"].split(",")] if q.get("domains") else None
            limit = int(q.get("limit", "5000"))
            neighbors = await self.backing.get_neighbors(
                vid,
                edge_types=edge_types,
                domains=domains,
                direction=direction,  # type: ignore[arg-type]
                limit=limit,
            )
            return httpx.Response(
                200,
                json={
                    "edges": [e.model_dump(mode="json") for e, _ in neighbors],
                    "vertices": [n.model_dump(mode="json") for _, n in neighbors],
                },
            )

        if method == "POST" and (
            path.endswith("/gremlin")
            or path.endswith("/cypher")
            or path.endswith("/traversers/kneighbor")
        ):
            if path.endswith("/gremlin"):
                params = body["bindings"]
                seed_ids = params["seed_ids"]
                depth = params["max_depth"]
                limit = params["limit"]
                domains = [Domain(d) for d in params["domains"]] if params.get("domains") else None
                edge_types = None
            elif path.endswith("/cypher"):
                params = body["parameters"]
                seed_ids = params["seed_ids"]
                depth = params["max_depth"]
                limit = params["limit"]
                domains = [Domain(d) for d in params["domains"]] if params.get("domains") else None
                edge_types = None
            else:
                seed_ids = body["sources"]["ids"]
                depth = body["max_depth"]
                limit = body["limit"]
                domains = [Domain(d) for d in body["domains"]] if body.get("domains") else None
                edge_types = body["steps"]["labels"] or None

            res = await self.backing.traverse_k_hop(
                TraversalRequest(
                    seed_ids=seed_ids,
                    max_depth=depth,
                    edge_types=edge_types,
                    domains=domains,
                    max_results=limit,
                )
            )
            return httpx.Response(
                200,
                json={
                    "vertices": [n.model_dump(mode="json") for n in res.nodes],
                    "edges": [e.model_dump(mode="json") for e in res.edges],
                    "paths": res.paths,
                    "nodes_expanded": res.metrics.nodes_expanded,
                },
            )

        if method == "GET" and path.endswith("/traversers/count"):
            stats = await self.backing.count_stats()
            return httpx.Response(200, json=stats.model_dump(mode="json"))

        if method == "DELETE" and path.endswith("/clear"):
            await self.backing.clear()
            return httpx.Response(200, json={"cleared": True})

        return httpx.Response(404, json={"error": f"Unhandled mock route {method} {path}"})


class _Neo4jMockResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    async def data(self) -> list[dict[str, Any]]:
        return self._rows


class _Neo4jMockSession:
    def __init__(self, shard_store: InMemoryGraphStore, log: list[tuple[str, str]]) -> None:
        self._store = shard_store
        self._log = log

    async def __aenter__(self) -> _Neo4jMockSession:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def run(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> _Neo4jMockResult:
        params = parameters or {}
        self._log.append((query, json.dumps(params, sort_keys=True)))

        if "UNWIND $nodes AS n" in query:
            nodes = [Neo4jGraphStore._deserialize_node(n) for n in params["nodes"]]
            await self._store.upsert_nodes(nodes)
            return _Neo4jMockResult([{"upserted": len(nodes)}])

        if "UNWIND $edges AS e" in query:
            edges = [Neo4jGraphStore._deserialize_edge(e) for e in params["edges"]]
            await self._store.upsert_edges(edges)
            return _Neo4jMockResult([{"upserted": len(edges)}])

        if "MATCH (n:Entity {id: $id})" in query:
            node = await self._store.get_node(params["id"])
            if node is None or node.is_proxy:
                return _Neo4jMockResult([])
            return _Neo4jMockResult([Neo4jGraphStore._serialize_node(node)])

        if "RETURN r, t LIMIT" in query:
            direction: Any = "both"
            if "-[r:REL]->" in query:
                direction = "out"
            elif "<-[r:REL]-" in query:
                direction = "in"
            edge_types = params["edge_types"] or None
            domains = [Domain(d) for d in params["domains"]] if params["domains"] else None
            neighbors = await self._store.get_neighbors(
                params["node_id"],
                edge_types=edge_types,
                domains=domains,
                direction=direction,
            )
            return _Neo4jMockResult(
                [
                    {
                        "r": Neo4jGraphStore._serialize_edge(e),
                        "t": Neo4jGraphStore._serialize_node(n),
                    }
                    for e, n in neighbors
                ]
            )

        if "MATCH p = (s:Entity)-[r:REL*1.." in query:
            depth = int(query.split("*1..", 1)[1].split("]", 1)[0])
            edge_types = params["edge_types"] or None
            domains = [Domain(d) for d in params["domains"]] if params["domains"] else None
            res = await self._store.traverse_k_hop(
                TraversalRequest(
                    seed_ids=params["seed_ids"],
                    max_depth=depth,
                    edge_types=edge_types,
                    domains=domains,
                    max_results=params["limit"],
                    include_derived=params["include_derived"],
                )
            )
            if not res.nodes:
                return _Neo4jMockResult([])
            # Return shard-local paths including proxy nodes for cross-shard continuation
            nodes_by_id = {n.id: n for n in res.nodes}
            rows = []
            for p in res.paths:
                p_nodes = [
                    Neo4jGraphStore._serialize_node(nodes_by_id[nid])
                    for nid in p
                    if nid in nodes_by_id
                ]
                p_edges = [
                    Neo4jGraphStore._serialize_edge(e)
                    for e in res.edges
                    if e.source_id in p and e.target_id in p
                ]
                rows.append({"nodes": p_nodes, "edges": p_edges, "path": p})
            if not rows:
                rows.append(
                    {
                        "nodes": [Neo4jGraphStore._serialize_node(n) for n in res.nodes],
                        "edges": [Neo4jGraphStore._serialize_edge(e) for e in res.edges],
                        "path": [n.id for n in res.nodes],
                    }
                )
            return _Neo4jMockResult(rows)

        if "RETURN count(n) AS total" in query:
            stats = await self._store.count_stats()
            return _Neo4jMockResult(
                [
                    {
                        "total": stats.node_count,
                        "buckets": stats.bucket_node_count,
                        "proxies": stats.proxy_node_count,
                    }
                ]
            )

        if "RETURN count(r) AS total" in query:
            stats = await self._store.count_stats()
            return _Neo4jMockResult([{"total": stats.edge_count}])

        if "DETACH DELETE n" in query:
            await self._store.clear()
            return _Neo4jMockResult([])

        raise AssertionError(f"Unexpected Cypher query in mock: {query}")


class _Neo4jMockDriver:
    """Shard-isolated Neo4j mock driver (no cross-shard edges inside a single shard)."""

    def __init__(self) -> None:
        self.shards: dict[str, InMemoryGraphStore] = {
            f"kg_{d.value}": InMemoryGraphStore() for d in Domain
        }
        self.shards["composite"] = InMemoryGraphStore()
        self.query_log: list[tuple[str, str]] = []

    def session(self, *, database: str | None = None) -> _Neo4jMockSession:
        db = database or "composite"
        if db not in self.shards:
            self.shards[db] = InMemoryGraphStore()
        return _Neo4jMockSession(self.shards[db], self.query_log)

    async def verify_connectivity(self) -> None:
        return None

    async def close(self) -> None:
        return None


def _build_adapter(adapter_kind: str, tmp_path: Path) -> GraphStore:
    if adapter_kind == "in_memory":
        return InMemoryGraphStore()
    if adapter_kind == "iceberg_puppygraph":
        return IcebergPuppyGraphStore(tmp_path / "warehouse")
    if adapter_kind == "hugegraph":
        return HugeGraphStore(
            base_url="http://hugegraph.mock",
            tenant_id="acme-prod",
            transport=_HugeGraphMockTransport(),
        )
    if adapter_kind == "neo4j":
        return Neo4jGraphStore(driver=_Neo4jMockDriver())
    if adapter_kind == "janusgraph":
        return JanusGraphStore()
    if adapter_kind == "nebulagraph":
        return NebulaGraphStore()
    raise ValueError(f"Unknown adapter kind: {adapter_kind}")


ALL_ADAPTERS = [
    "in_memory",
    "iceberg_puppygraph",
    "hugegraph",
    "neo4j",
    "janusgraph",
    "nebulagraph",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter_kind", ALL_ADAPTERS)
class TestSharedGraphStoreContract:
    """Every Tier-1 and Tier-2 adapter must satisfy the shared GraphStore contract."""

    async def test_crud_neighbors_traversal_stats_health_and_clear(
        self, adapter_kind: str, tmp_path: Path
    ) -> None:
        store = _build_adapter(adapter_kind, tmp_path)
        nodes, edges = _sample_graph()

        assert await store.upsert_nodes(nodes) == len(nodes)
        assert await store.upsert_edges(edges) == len(edges)

        fetched = await store.get_node("technology:lang:python")
        assert fetched is not None
        assert fetched.domain == Domain.TECHNOLOGY
        assert await store.get_node("customer:org:does_not_exist") is None

        neighbors = await store.get_neighbors("customer:org:acme", direction="out")
        assert [(e.edge_type, n.id) for e, n in neighbors] == [
            ("USES", "technology:lang:python")
        ]

        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        )
        assert res.metrics.aborted is False
        assert res.metrics.depth_reached == 3
        assert {n.id for n in res.nodes} == {
            "customer:org:acme",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        }
        assert [
            "customer:org:acme",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        ] in res.paths

        health = await store.health_check()
        assert health.healthy is True
        assert health.status == "healthy"

        stats = await store.count_stats()
        assert stats.node_count >= len(nodes)
        assert stats.edge_count >= len(edges)

        profile = store.cost_profile()
        assert profile.engine == store.name
        assert profile.estimated_cost_per_billion_edges_usd_month >= 0.0

        await store.clear()
        cleared_stats = await store.count_stats()
        assert cleared_stats.node_count == 0
        assert cleared_stats.edge_count == 0

    async def test_enforces_lean_graph_writes_and_preflight_guardrails(
        self, adapter_kind: str, tmp_path: Path
    ) -> None:
        store = _build_adapter(adapter_kind, tmp_path)

        bad_node = EntityNode(
            id="knowledge:doc:bad",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            routing_props={"content": "forbidden inline payload"},
            lakehouse_ref="s3://bucket/bad.parquet",
        )
        with pytest.raises(GuardrailViolationError) as exc_node:
            await store.upsert_nodes([bad_node])
        assert exc_node.value.violation.code == GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD

        bad_edge = RelationshipEdge(
            source_id="customer:org:acme",
            target_id="customer:org:beta",
            edge_type="SIMILAR_TO",
            domain=Domain.CUSTOMER,
            is_derived=True,
            materialized_hot_path=False,
        )
        with pytest.raises(GuardrailViolationError) as exc_edge:
            await store.upsert_edges([bad_edge])
        assert exc_edge.value.violation.code == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE

        with pytest.raises(GuardrailViolationError) as exc_depth:
            await store.traverse_k_hop(
                TraversalRequest(seed_ids=["customer:org:acme"], max_depth=4)
            )
        assert exc_depth.value.violation.code == GuardrailViolationCode.MAX_DEPTH_EXCEEDED

    async def test_fault_mode_injection_and_recovery(
        self, adapter_kind: str, tmp_path: Path
    ) -> None:
        store = _build_adapter(adapter_kind, tmp_path)
        await store.upsert_nodes(
            [EntityNode(id="customer:org:1", domain=Domain.CUSTOMER, entity_type="Organization")]
        )

        await store.set_fault_mode(FaultMode.NODE_DOWN, target_domain=Domain.CUSTOMER)
        degraded_health = await store.health_check()
        assert degraded_health.healthy is False
        assert degraded_health.status == "unavailable"

        with pytest.raises(GraphStoreUnavailableError):
            await store.traverse_k_hop(
                TraversalRequest(
                    seed_ids=["customer:org:1"],
                    max_depth=1,
                    domains=[Domain.CUSTOMER],
                )
            )

        await store.set_fault_mode(FaultMode.NONE)
        recovered = await store.health_check()
        assert recovered.healthy is True


@pytest.mark.asyncio
class TestInMemoryAdapterSpecifics:
    async def test_transparent_supernode_bucket_collapse_in_neighbors_and_traversal(
        self,
    ) -> None:
        store = InMemoryGraphStore()
        nodes, edges = _sample_graph()
        bucketer = SupernodeBucketer(degree_threshold=100, num_buckets=16)
        b_nodes, b_edges, supernodes = bucketer.transform_dataset(nodes, edges)
        assert "technology:lang:python" in supernodes

        await store.upsert_nodes(b_nodes)
        await store.upsert_edges(b_edges)

        # get_neighbors collapses intermediate ::bucket:: vertices transparently
        out_acme = await store.get_neighbors("customer:org:acme", direction="out")
        assert [(e.edge_type, n.id) for e, n in out_acme] == [
            ("USES", "technology:lang:python")
        ]
        out_python = await store.get_neighbors("technology:lang:python", direction="out")
        assert [(e.edge_type, n.id) for e, n in out_python] == [
            ("POWERS", "product:sdk:kinetic")
        ]

        # 3-hop logical traversal succeeds despite physical bucket hops
        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        )
        assert res.metrics.aborted is False
        assert res.metrics.depth_reached == 3
        assert res.metrics.bucket_hops_unwrapped >= 1
        assert [
            "customer:org:acme",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        ] in res.paths

    async def test_transient_fault_auto_recovers_and_runtime_budget_aborts_cleanly(
        self,
    ) -> None:
        now = [0.0]
        enforcer = GuardrailEnforcer(clock=lambda: now[0])
        store = InMemoryGraphStore(enforcer=enforcer)
        nodes, edges = _sample_graph()
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        await store.set_fault_mode(FaultMode.TRANSIENT_ERROR, recover_after_calls=1)
        with pytest.raises(GraphStoreUnavailableError):
            await store.get_node("customer:org:acme")
        # Next call auto-recovers
        assert await store.get_node("customer:org:acme") is not None

        # Runtime max_results abort preserves structured violation metadata
        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3, max_results=2)
        )
        assert res.metrics.aborted is True
        assert res.metrics.violation is not None
        assert res.metrics.violation.code == GuardrailViolationCode.MAX_RESULTS_EXCEEDED


@pytest.mark.asyncio
class TestHugeGraphAdapterSpecifics:
    @pytest.mark.parametrize("query_mode", ["gremlin", "opencypher", "rest"])
    async def test_supports_gremlin_opencypher_rest_and_pd_partition_headers(
        self, query_mode: str
    ) -> None:
        transport = _HugeGraphMockTransport()
        store = HugeGraphStore(
            base_url="http://hugegraph.mock",
            tenant_id="tenant-xyz",
            pd_endpoints=["http://pd-1:8686", "http://pd-2:8686"],
            query_mode=query_mode,  # type: ignore[arg-type]
            transport=transport,
        )
        nodes, edges = _sample_graph()
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        )
        assert res.metrics.depth_reached == 3
        assert len(res.nodes) == 4
        assert any(
            h.get("x-hugegraph-tenant") == "tenant-xyz"
            and "x-hugegraph-pd-endpoints" in h
            for h in transport.recorded_headers
        )
        if query_mode == "gremlin":
            assert any(p.endswith("/gremlin") for p in transport.recorded_paths)
        elif query_mode == "opencypher":
            assert any(p.endswith("/cypher") for p in transport.recorded_paths)
        else:
            assert any(p.endswith("/traversers/kneighbor") for p in transport.recorded_paths)


@pytest.mark.asyncio
class TestNeo4jAndStubsSpecifics:
    async def test_neo4j_creates_proxy_nodes_and_traverses_across_shards(self) -> None:
        driver = _Neo4jMockDriver()
        store = Neo4jGraphStore(driver=driver)
        nodes, edges = _sample_graph()
        await store.upsert_nodes(nodes)
        await store.upsert_edges(edges)

        # Proxy node for technology:lang:python was materialized inside customer shard
        customer_stats = await driver.shards["kg_customer"].count_stats()
        assert customer_stats.proxy_node_count == 1
        assert driver.shards["kg_customer"]._nodes[Domain.CUSTOMER]["technology:lang:python"].is_proxy is True

        # Cross-shard traversal follows proxy nodes from customer -> technology -> product -> knowledge
        res = await store.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        )
        assert res.metrics.depth_reached == 3
        assert res.metrics.cross_domain_hops >= 2
        assert {n.id for n in res.nodes} >= {
            "customer:org:acme",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        }
        # All generated Cypher queries enforce bounded depth and LIMIT
        assert all("LIMIT" in q for q, _ in driver.query_log if "MATCH" in q)

    async def test_tier2_stubs_expose_cost_profiles_and_query_builders(self) -> None:
        janus = JanusGraphStore()
        nebula = NebulaGraphStore()

        assert janus.cost_profile().is_stub is True
        assert janus.cost_profile().tier == "tier2"
        assert "ScyllaDB" in janus.cost_profile().storage_backend
        gremlin, g_bindings = janus.build_gremlin_query(
            TraversalRequest(seed_ids=["customer:org:1"], max_depth=2, edge_types=["USES"])
        )
        assert ".times(2)" in gremlin and ".limit(5000)" in gremlin
        assert g_bindings["seed_ids"] == ["customer:org:1"]

        assert nebula.cost_profile().is_stub is True
        assert nebula.cost_profile().tier == "tier2"
        ngql, n_params = nebula.build_ngql_query(
            TraversalRequest(seed_ids=["customer:org:1"], max_depth=3, edge_types=["USES"])
        )
        assert "GO 1 TO 3 STEPS" in ngql and "LIMIT 5000" in ngql
        assert n_params["space"] == "scale_kg"
