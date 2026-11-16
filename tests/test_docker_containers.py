"""Integration tests for Neo4j, HugeGraph, and PuppyGraph container services."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from scale_kg.adapters import (
    ContainerTransport,
    FaultMode,
    GraphStore,
    HugeGraphContainerApp,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    Neo4jContainerApp,
    Neo4jGraphStore,
    PuppyGraphContainerApp,
    create_app,
)
from scale_kg.adapters.container_server import _handle_client, serve_app
from scale_kg.benchmark import (
    GoldenQuerySuite,
    PowerLawGraphGenerator,
    ScalePreset,
    run_cost_extrapolation_workload,
    run_ha_failure_workload,
    run_ingestion_workload,
    run_supernode_traversal_workload,
    run_traversal_latency_workload,
)
from scale_kg.core import (
    Domain,
    EntityNode,
    GuardrailViolationCode,
    RelationshipEdge,
    TraversalRequest,
)
from scale_kg.lakehouse import LakehouseContentStore, LakehouseDocument
from scale_kg.routing import (
    HybridGraphRAGRouter,
    HybridRAGRequest,
    InMemoryVectorIndex,
    QueryCoordinator,
)


async def _ensure_live_containers() -> tuple[
    Neo4jGraphStore, HugeGraphStore, IcebergPuppyGraphStore, Path
]:
    root = Path(__file__).resolve().parent.parent
    shared_wh = root / ".docker-data" / "warehouse"
    neo = Neo4jGraphStore(uri="http://127.0.0.1:7474")
    hg = HugeGraphStore(base_url="http://127.0.0.1:8080")
    pg = IcebergPuppyGraphStore(shared_wh, puppygraph_url="http://127.0.0.1:8081")
    if not (
        (await neo.health_check()).healthy
        and (await hg.health_check()).healthy
        and (await pg.health_check()).healthy
    ):
        pytest.skip("Live Docker containers not reachable in current environment")
    return neo, hg, pg, shared_wh


@pytest.mark.asyncio
class TestContainerServices:
    async def test_container_apps_pass_bucketed_golden_parity(
        self, tmp_path: Path
    ) -> None:
        neo4j_app = Neo4jContainerApp()
        huge_app = HugeGraphContainerApp()
        puppy_wh = tmp_path / "puppy_warehouse"
        puppy_app = PuppyGraphContainerApp(default_warehouse=puppy_wh)

        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "neo4j_container": Neo4jGraphStore(
                uri="http://neo4j.container:7474",
                transport=ContainerTransport(neo4j_app),
            ),
            "hugegraph_container": HugeGraphStore(
                base_url="http://hugegraph.container:8080",
                transport=ContainerTransport(huge_app),
            ),
            "puppygraph_container": IcebergPuppyGraphStore(
                puppy_wh,
                puppygraph_url="http://puppygraph.container:8081",
                transport=ContainerTransport(puppy_app),
            ),
        }

        for store in stores.values():
            health = await store.health_check()
            assert health.healthy is True

        suite = GoldenQuerySuite(bucket_supernodes=True)
        report = await suite.run_parity(stores)
        report.assert_parity()

        for name in ("neo4j_container", "hugegraph_container", "puppygraph_container"):
            stats = await stores[name].count_stats()
            assert stats.node_count > 0
            assert stats.edge_count > 0

    async def test_http_stream_and_tcp_server_for_all_three_engines(
        self, tmp_path: Path
    ) -> None:
        puppy_wh = tmp_path / "tcp_warehouse"

        class _DummyWriter:
            def __init__(self) -> None:
                self.written = bytearray()

            def write(self, data: bytes) -> None:
                self.written.extend(data)

            async def drain(self) -> None:
                return None

            def close(self) -> None:
                return None

            async def wait_closed(self) -> None:
                return None

        for engine, path in (
            ("neo4j", "/health"),
            ("hugegraph", "/versions"),
            ("puppygraph", "/health"),
        ):
            app = create_app(engine, warehouse_dir=puppy_wh)
            reader = asyncio.StreamReader()
            reader.feed_data(f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
            reader.feed_eof()

            writer = _DummyWriter()
            await _handle_client(app, reader, writer)  # type: ignore[arg-type]
            assert b"HTTP/1.1 200 OK" in writer.written
            assert b'"status": "ok"' in writer.written

        try:
            task = asyncio.create_task(
                serve_app(create_app("neo4j"), "127.0.0.1", 17474)
            )
            await asyncio.sleep(0.05)
            if task.done():
                task.result()
            neo = Neo4jGraphStore(uri="http://127.0.0.1:17474")
            assert (await neo.health_check()).healthy is True
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        except PermissionError:
            pass

    async def test_dockerfiles_and_compose_manifest_exist(self) -> None:
        root = Path(__file__).resolve().parent.parent
        assert (root / "docker" / "neo4j" / "Dockerfile").is_file()
        assert (root / "docker" / "hugegraph" / "Dockerfile").is_file()
        assert (root / "docker" / "puppygraph" / "Dockerfile").is_file()
        compose_text = (root / "docker-compose.yml").read_text(encoding="utf-8")
        assert "scale-kg-neo4j:latest" in compose_text
        assert "scale-kg-hugegraph:latest" in compose_text
        assert "scale-kg-puppygraph:latest" in compose_text

    async def test_live_docker_containers_golden_parity(self) -> None:
        neo, hg, pg, _ = await _ensure_live_containers()
        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "neo4j_docker": neo,
            "hugegraph_docker": hg,
            "puppygraph_docker": pg,
        }
        suite = GoldenQuerySuite(bucket_supernodes=True)
        report = await suite.run_parity(stores)
        report.assert_parity()

    async def test_live_hugegraph_all_query_modes_neighbors_and_derived(self) -> None:
        _, hg_gremlin, _, _ = await _ensure_live_containers()
        hg_cypher = HugeGraphStore(
            base_url="http://127.0.0.1:8080", query_mode="opencypher"
        )
        hg_rest = HugeGraphStore(base_url="http://127.0.0.1:8080", query_mode="rest")

        suite = GoldenQuerySuite(bucket_supernodes=True)
        await suite.load_dataset({"hg": hg_gremlin})

        assert await hg_gremlin.get_node("missing:entity:none") is None
        py_node = await hg_gremlin.get_node("technology:lang:python")
        assert py_node is not None and py_node.domain == Domain.TECHNOLOGY

        # Supernode outbound & inbound neighbors across ::bucket::0..15
        out_nb = await hg_gremlin.get_neighbors(
            "technology:lang:python", direction="out"
        )
        assert {n.id for _, n in out_nb} == {
            "technology:lib:pydantic",
            "product:sdk:kinetic",
        }
        in_nb = await hg_gremlin.get_neighbors("technology:lang:python", direction="in")
        assert {n.id for _, n in in_nb} == {"customer:org:acme", "customer:org:beta"}

        # Gremlin, OpenCypher, and REST kneighbor return identical 3-hop bucketed subgraphs
        req = TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        r_g = await hg_gremlin.traverse_k_hop(req)
        r_c = await hg_cypher.traverse_k_hop(req)
        r_r = await hg_rest.traverse_k_hop(req)
        assert (
            {n.id for n in r_g.nodes}
            == {n.id for n in r_c.nodes}
            == {n.id for n in r_r.nodes}
        )
        assert sorted(r_g.paths) == sorted(r_c.paths) == sorted(r_r.paths)
        assert r_g.metrics.bucket_hops_unwrapped >= 1

    async def test_live_puppygraph_cypher_gremlin_zero_etl_and_guardrails(
        self,
    ) -> None:
        _, _, pg_cypher, shared_wh = await _ensure_live_containers()
        pg_gremlin = IcebergPuppyGraphStore(
            shared_wh,
            puppygraph_url="http://127.0.0.1:8081",
            query_protocol="gremlin",
        )

        suite = GoldenQuerySuite(bucket_supernodes=True)
        await suite.load_dataset({"pg": pg_cypher})
        schema_path = pg_cypher.write_puppygraph_schema()
        assert schema_path.is_file()

        node = await pg_cypher.get_node("customer:org:acme")
        assert node is not None and node.id == "customer:org:acme"

        out_nb = await pg_cypher.get_neighbors(
            "technology:lang:python", direction="out"
        )
        assert {n.id for _, n in out_nb} == {
            "technology:lib:pydantic",
            "product:sdk:kinetic",
        }

        req = TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3)
        res_cypher = await pg_cypher.traverse_k_hop(req)
        res_gremlin = await pg_gremlin.traverse_k_hop(req)
        assert {n.id for n in res_cypher.nodes} == {n.id for n in res_gremlin.nodes}
        assert sorted(res_cypher.paths) == sorted(res_gremlin.paths)
        assert res_cypher.metrics.bucket_hops_unwrapped >= 1

        # Runtime max_results guardrail abort on remote PuppyGraph response
        aborted = await pg_cypher.traverse_k_hop(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=3, max_results=2)
        )
        assert aborted.metrics.aborted is True
        assert aborted.metrics.violation is not None
        assert (
            aborted.metrics.violation.code
            == GuardrailViolationCode.MAX_RESULTS_EXCEEDED
        )

    async def test_live_neo4j_hybrid_graphrag_and_hot_path_promotion(
        self, tmp_path: Path
    ) -> None:
        neo, _, _, _ = await _ensure_live_containers()
        await neo.clear()

        coord = QueryCoordinator(neo, hot_path_threshold=2)
        lakehouse = LakehouseContentStore(tmp_path / "lakehouse")
        vector_index = InMemoryVectorIndex()

        nodes = [
            EntityNode(
                id="customer:org:acme",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
                lakehouse_ref="s3://lh/customer/acme.parquet",
            ),
            EntityNode(
                id="customer:org:beta",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
                lakehouse_ref="s3://lh/customer/beta.parquet",
            ),
            EntityNode(
                id="technology:lang:python",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
                lakehouse_ref="s3://lh/technology/python.parquet",
            ),
            EntityNode(
                id="product:sdk:kinetic",
                domain=Domain.PRODUCT,
                entity_type="Product",
                lakehouse_ref="s3://lh/product/kinetic.parquet",
            ),
            EntityNode(
                id="knowledge:doc:arch",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lh/knowledge/arch.parquet",
            ),
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
                source_id="customer:org:beta",
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
        ]
        await coord.upsert_nodes(nodes)
        await coord.upsert_edges(edges)

        # Verify proxy nodes were materialized inside Neo4j shards
        stats = await neo.count_stats()
        assert stats.proxy_node_count >= 3

        # Derived edge inference + hot-path promotion over live Neo4j
        await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:acme"], max_depth=2, include_derived=True
            )
        )
        r2 = await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:acme"], max_depth=2, include_derived=True
            )
        )
        assert any(e.is_derived and e.materialized_hot_path for e in r2.edges)
        assert coord.stats().hot_paths_promoted >= 2

        # Full 5-stage HybridGraphRAGRouter over live Neo4j + Parquet Lakehouse
        lakehouse.put_documents(
            [
                LakehouseDocument(
                    lakehouse_ref=n.lakehouse_ref or "",
                    entity_id=n.id,
                    domain=n.domain,
                    title=n.id,
                    content=f"Live Neo4j content for {n.id}",
                )
                for n in nodes
            ]
        )
        vector_index.upsert("customer:org:acme", [1.0, 0.0])
        vector_index.upsert("product:sdk:kinetic", [0.8, 0.6])

        router = HybridGraphRAGRouter(
            vector_index=vector_index,
            coordinator=coord,
            lakehouse=lakehouse,
            embedder=lambda _q: [1.0, 0.0],
        )
        rag_res = await router.retrieve(
            HybridRAGRequest(
                query_text="How does Acme use Kinetic?",
                seed_k=1,
                top_n=3,
                max_depth=3,
            )
        )
        assert rag_res.hydrated_count == 3
        assert len(rag_res.hits) == 3
        assert len(rag_res.citations) == 3

    async def test_live_container_workloads_and_ha_recovery(self) -> None:
        neo, hg, pg, _ = await _ensure_live_containers()
        dataset = PowerLawGraphGenerator(preset=ScalePreset.MICRO, seed=21).generate(
            bucket_supernodes=True, degree_threshold=20
        )

        for store in (neo, hg, pg):
            await store.clear()
            ingest = await run_ingestion_workload(store, dataset)
            assert ingest.write_discipline_passed is True
            assert ingest.nodes_ingested == len(dataset.nodes)

            lat = await run_traversal_latency_workload(
                store, dataset.seed_ids, samples_per_depth=3
            )
            assert set(lat.by_depth.keys()) == {1, 2, 3}

            sup = await run_supernode_traversal_workload(
                store, dataset.supernode_ids, samples=3
            )
            assert sup.within_guardrails is True
            assert sup.raw_bucket_leaks == 0

            ha = await run_ha_failure_workload(
                store,
                dataset.seed_ids,
                fault_mode=FaultMode.TRANSIENT_ERROR,
                recover_after_calls=2,
                total_queries=5,
            )
            assert ha.failed_queries == 2
            assert ha.succeeded_queries == 3
            assert ha.recovered_cleanly is True

            cost = run_cost_extrapolation_workload(
                store, lat, base_edge_count=len(dataset.edges)
            )
            assert [s.scale_label for s in cost.scale_ladder] == ["100M", "1B", "10B"]
