"""Tests for FastAPI REST endpoints, gRPC servicer/bridge, auth, and guardrails."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

import httpx
import pytest

from scale_kg.adapters import FaultMode, InMemoryGraphStore
from scale_kg.api import (
    GRPC_SERVICE_NAME,
    GrpcRpcError,
    GrpcStatusCode,
    ScaleKGGrpcClient,
    create_app,
)
from scale_kg.core import Domain, EntityNode, RelationshipEdge


def _client_for(app: object) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


def _sample_ingest_payload() -> dict[str, object]:
    return {
        "nodes": [
            {
                "id": "customer:org:acme",
                "domain": "customer",
                "entity_type": "Organization",
                "routing_props": {"tier": "enterprise"},
            },
            {
                "id": "technology:lang:python",
                "domain": "technology",
                "entity_type": "Language",
                "routing_props": {"family": "dynamic"},
            },
            {
                "id": "product:sdk:scale-kg",
                "domain": "product",
                "entity_type": "Product",
                "routing_props": {"active": True},
            },
            {
                "id": "knowledge:doc:arch",
                "domain": "knowledge",
                "entity_type": "Document",
                "lakehouse_ref": "lh://knowledge/doc_arch",
                "routing_props": {"has_content": True},
            },
        ],
        "edges": [
            {
                "source_id": "customer:org:acme",
                "target_id": "technology:lang:python",
                "edge_type": "USES",
                "domain": "customer",
                "target_domain": "technology",
            },
            {
                "source_id": "technology:lang:python",
                "target_id": "product:sdk:scale-kg",
                "edge_type": "POWERS",
                "domain": "technology",
                "target_domain": "product",
            },
            {
                "source_id": "product:sdk:scale-kg",
                "target_id": "knowledge:doc:arch",
                "edge_type": "DESCRIBED_BY",
                "domain": "product",
                "target_domain": "knowledge",
            },
        ],
        "aliases": {
            "customer:org:acme": ["legacy-acme-001"],
        },
        "documents": [
            {
                "lakehouse_ref": "lh://knowledge/doc_arch",
                "entity_id": "knowledge:doc:arch",
                "domain": "knowledge",
                "title": "Scale-KG Architecture",
                "content": "4-layer lean semantic graph with 16-way supernode bucketing.",
                "source_uri": "s3://scale-kg/docs/arch.md",
            }
        ],
        "embeddings": {
            "customer:org:acme": [1.0, 0.0, 0.0],
            "product:sdk:scale-kg": [0.7, 0.7, 0.0],
            "knowledge:doc:arch": [0.4, 0.9, 0.1],
        },
    }


@pytest.mark.asyncio
class TestScaleKGAPI:
    async def test_health_and_stats_on_in_memory_and_iceberg(
        self, tmp_path: Path
    ) -> None:
        for backend in ("in_memory", "iceberg_puppygraph"):
            app = create_app(
                backend=backend,
                warehouse_dir=tmp_path / backend,
            )
            async with _client_for(app) as client:
                health_resp = await client.get("/v1/health")
                assert health_resp.status_code == 200
                health_data = health_resp.json()
                assert health_data["healthy"] is True
                assert health_data["status"] == "healthy"
                assert health_data["backend"] == backend
                assert set(health_data["shards"].keys()) == {
                    "customer",
                    "knowledge",
                    "technology",
                    "product",
                }

                stats_resp = await client.get("/v1/stats")
                assert stats_resp.status_code == 200
                stats_data = stats_resp.json()
                assert stats_data["graph"]["node_count"] == 0
                assert stats_data["lakehouse"]["document_count"] == 0

    async def test_ingest_node_and_neighbor_lookup(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            ingest_resp = await client.post("/v1/ingest", json=_sample_ingest_payload())
            assert ingest_resp.status_code == 200
            assert ingest_resp.json() == {
                "nodes_upserted": 4,
                "edges_upserted": 3,
                "documents_stored": 1,
                "embeddings_indexed": 3,
            }

            node_resp = await client.get("/v1/nodes/legacy-acme-001")
            assert node_resp.status_code == 200
            assert node_resp.json()["id"] == "customer:org:acme"

            neighbors_resp = await client.get(
                "/v1/nodes/customer:org:acme/neighbors",
                params={"direction": "out"},
            )
            assert neighbors_resp.status_code == 200
            nbr_data = neighbors_resp.json()
            assert nbr_data["node_id"] == "customer:org:acme"
            assert len(nbr_data["neighbors"]) == 1
            assert nbr_data["neighbors"][0]["node"]["id"] == "technology:lang:python"
            assert nbr_data["neighbors"][0]["edge"]["edge_type"] == "USES"

            missing_resp = await client.get("/v1/nodes/customer:org:missing")
            assert missing_resp.status_code == 404

    async def test_guardrail_violations_return_422(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            # 1. Forbidden payload property
            bad_prop = await client.post(
                "/v1/ingest",
                json={
                    "nodes": [
                        {
                            "id": "customer:org:bad",
                            "domain": "customer",
                            "entity_type": "Organization",
                            "routing_props": {"content": "heavy text disallowed"},
                        }
                    ]
                },
            )
            assert bad_prop.status_code == 422
            assert (
                bad_prop.json()["violation"]["code"] == "lean_graph_forbidden_payload"
            )

            # 2. Content-bearing entity missing lakehouse_ref
            missing_ref = await client.post(
                "/v1/ingest",
                json={
                    "nodes": [
                        {
                            "id": "knowledge:doc:missingref",
                            "domain": "knowledge",
                            "entity_type": "Document",
                        }
                    ]
                },
            )
            assert missing_ref.status_code == 422
            assert (
                missing_ref.json()["violation"]["code"]
                == "lean_graph_missing_lakehouse_ref"
            )

            # 3. Unmaterialized derived edge
            derived_edge = await client.post(
                "/v1/ingest",
                json={
                    "edges": [
                        {
                            "source_id": "customer:org:a",
                            "target_id": "customer:org:b",
                            "edge_type": "SIMILAR_TO",
                            "domain": "customer",
                        }
                    ]
                },
            )
            assert derived_edge.status_code == 422
            assert (
                derived_edge.json()["violation"]["code"]
                == "unmaterialized_derived_edge"
            )

            # 4. Depth cap violation on /v1/traverse
            bad_depth = await client.post(
                "/v1/traverse",
                json={"seed_ids": ["customer:org:acme"], "max_depth": 5},
            )
            assert bad_depth.status_code == 422
            assert bad_depth.json()["violation"]["code"] == "max_depth_exceeded"

    async def test_traverse_derived_hot_path_and_federated_shard_map(
        self, tmp_path: Path
    ) -> None:
        shards = {d: InMemoryGraphStore() for d in Domain}
        app = create_app(shard_map=shards, warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            ingest_resp = await client.post("/v1/ingest", json=_sample_ingest_payload())
            assert ingest_resp.status_code == 200

            for _ in range(3):
                trav_resp = await client.post(
                    "/v1/traverse",
                    json={
                        "seed_ids": ["customer:org:acme"],
                        "max_depth": 2,
                        "include_derived": True,
                    },
                )
                assert trav_resp.status_code == 200
                data = trav_resp.json()
                assert data["metrics"]["aborted"] is False
                assert data["metrics"]["cross_domain_hops"] >= 1
                assert any(
                    edge["edge_type"] == "INDIRECTLY_USES" for edge in data["edges"]
                )

            stats_resp = await client.get("/v1/stats")
            assert stats_resp.status_code == 200
            stats_data = stats_resp.json()
            assert stats_data["coordinator"]["queries_executed"] == 3
            assert stats_data["coordinator"]["hot_paths_promoted"] >= 1

    async def test_hybrid_retrieve_pipeline(self, tmp_path: Path) -> None:
        app = create_app(backend="iceberg_puppygraph", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            ingest_resp = await client.post("/v1/ingest", json=_sample_ingest_payload())
            assert ingest_resp.status_code == 200

            rag_resp = await client.post(
                "/v1/hybrid-retrieve",
                json={
                    "query_text": "How does Scale-KG handle supernodes?",
                    "query_embedding": [0.95, 0.1, 0.0],
                    "seed_k": 2,
                    "top_n": 3,
                    "max_depth": 3,
                },
            )
            assert rag_resp.status_code == 200
            rag = rag_resp.json()
            assert rag["hydrated_count"] == 1
            assert len(rag["citations"]) == 1
            assert rag["citations"][0]["lakehouse_ref"] == "lh://knowledge/doc_arch"
            assert any(
                hit["document"] is not None
                and "16-way supernode bucketing" in hit["document"]["content"]
                for hit in rag["hits"]
            )

    async def test_lakehouse_documents_and_slices(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            put_resp = await client.post(
                "/v1/lakehouse/documents",
                json={
                    "documents": [
                        {
                            "lakehouse_ref": "lh://product/spec_v1",
                            "entity_id": "product:spec:v1",
                            "domain": "product",
                            "title": "API Spec",
                            "content": "REST and gRPC specifications",
                            "source_uri": "s3://bucket/spec_v1.md",
                        }
                    ]
                },
            )
            assert put_resp.status_code == 200
            assert put_resp.json() == {"documents_stored": 1}

            encoded_ref = quote("lh://product/spec_v1", safe="")
            get_doc_resp = await client.get(f"/v1/lakehouse/documents/{encoded_ref}")
            assert get_doc_resp.status_code == 200
            assert get_doc_resp.json()["title"] == "API Spec"

            slice_payload = {
                "slice_id": "slice-2026-11",
                "domain": "product",
                "nodes": [
                    EntityNode(
                        id="product:sdk:scale-kg",
                        domain=Domain.PRODUCT,
                        entity_type="Product",
                    ).model_dump(mode="json")
                ],
                "edges": [
                    RelationshipEdge(
                        source_id="product:sdk:scale-kg",
                        target_id="product:spec:v1",
                        edge_type="DESCRIBED_BY",
                        domain=Domain.PRODUCT,
                    ).model_dump(mode="json")
                ],
            }
            write_slice_resp = await client.post(
                "/v1/lakehouse/slices", json=slice_payload
            )
            assert write_slice_resp.status_code == 200
            assert write_slice_resp.json()["slice_id"] == "slice-2026-11"

            list_resp = await client.get(
                "/v1/lakehouse/slices", params={"domain": "product"}
            )
            assert list_resp.status_code == 200
            assert list_resp.json()["slices"] == ["slice-2026-11"]

            get_slice_resp = await client.get("/v1/lakehouse/slices/slice-2026-11")
            assert get_slice_resp.status_code == 200
            assert len(get_slice_resp.json()["nodes"]) == 1

    async def test_directory_aliases_and_resolution(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            reg_resp = await client.post(
                "/v1/directory/aliases",
                json={
                    "entities": {"opaque-entity-99": "technology"},
                    "aliases": {"old-py": "technology:lang:python"},
                },
            )
            assert reg_resp.status_code == 200
            assert reg_resp.json()["aliases_registered"] == 1
            assert reg_resp.json()["entities_registered"] == 1

            res_canon = await client.get("/v1/directory/resolve/product:sdk:scale-kg")
            assert res_canon.status_code == 200
            assert res_canon.json()["domain"] == "product"
            assert res_canon.json()["parsed"]["slug"] == "scale-kg"

            res_alias = await client.get("/v1/directory/resolve/old-py")
            assert res_alias.status_code == 200
            assert res_alias.json()["canonical_id"] == "technology:lang:python"
            assert res_alias.json()["domain"] == "technology"

            res_opaque = await client.get("/v1/directory/resolve/opaque-entity-99")
            assert res_opaque.status_code == 200
            assert res_opaque.json()["domain"] == "technology"

            res_missing = await client.get("/v1/directory/resolve/totally-unknown")
            assert res_missing.status_code == 404

    async def test_benchmark_run_endpoint(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            resp = await client.post(
                "/v1/benchmark/run",
                json={
                    "preset": "micro",
                    "adapters": ["in_memory", "iceberg_puppygraph"],
                    "run_golden_parity": True,
                    "generate_adr": True,
                },
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["suite_result"]["golden_parity"]["passed"] is True
            assert len(data["scorecards"]) == 2
            assert data["adr_markdown"] is not None
            assert "ADR-0002" in data["adr_markdown"]

    async def test_api_key_authentication_and_unavailable_fault(
        self, tmp_path: Path
    ) -> None:
        store = InMemoryGraphStore()
        app = create_app(store=store, api_key="secret-token", warehouse_dir=tmp_path)
        async with _client_for(app) as client:
            health_resp = await client.get("/v1/health")
            assert health_resp.status_code == 200

            no_auth = await client.post(
                "/v1/traverse", json={"seed_ids": ["customer:org:acme"]}
            )
            assert no_auth.status_code == 401

            bad_auth = await client.post(
                "/v1/traverse",
                json={"seed_ids": ["customer:org:acme"]},
                headers={"X-API-Key": "wrong"},
            )
            assert bad_auth.status_code == 401

            with_key = await client.post(
                "/v1/traverse",
                json={"seed_ids": ["customer:org:acme"]},
                headers={"X-API-Key": "secret-token"},
            )
            assert with_key.status_code == 200

            with_bearer = await client.post(
                "/v1/traverse",
                json={"seed_ids": ["customer:org:acme"]},
                headers={"Authorization": "Bearer secret-token"},
            )
            assert with_bearer.status_code == 200

            await store.set_fault_mode(FaultMode.NODE_DOWN)
            degraded_health = await client.get("/v1/health")
            assert degraded_health.status_code == 200
            assert degraded_health.json()["healthy"] is False
            assert degraded_health.json()["status"] == "unavailable"

            unavailable_trav = await client.post(
                "/v1/traverse",
                json={"seed_ids": ["customer:org:acme"]},
                headers={"X-API-Key": "secret-token"},
            )
            assert unavailable_trav.status_code == 503

    async def test_grpc_servicer_and_http_json_bridge(self, tmp_path: Path) -> None:
        app = create_app(backend="in_memory", warehouse_dir=tmp_path)
        servicer = app.state.scale_kg.grpc_servicer

        direct_client = ScaleKGGrpcClient(servicer=servicer)
        health = await direct_client.check_health()
        assert health.healthy is True

        async with _client_for(app) as http_client:
            bridge_client = ScaleKGGrpcClient(http_client=http_client)
            ingest_res = await bridge_client.ingest(_sample_ingest_payload())
            assert ingest_res.nodes_upserted == 4

            trav_res = await bridge_client.traverse(
                {"seed_ids": ["customer:org:acme"], "max_depth": 2}
            )
            assert trav_res.metrics.results_returned == 3

            rag_res = await bridge_client.hybrid_retrieve(
                {
                    "query_text": "supernodes",
                    "query_embedding": [0.9, 0.2, 0.0],
                    "max_depth": 3,
                }
            )
            assert rag_res.hydrated_count == 1

            with pytest.raises(GrpcRpcError) as exc_info:
                await bridge_client.traverse(
                    {"seed_ids": ["customer:org:acme"], "max_depth": 5}
                )
            assert exc_info.value.code == GrpcStatusCode.INVALID_ARGUMENT
            assert exc_info.value.violation is not None
            assert exc_info.value.violation.code.value == "max_depth_exceeded"

            unknown_rpc = await http_client.post(
                f"/{GRPC_SERVICE_NAME}/DoesNotExist", json={}
            )
            assert unknown_rpc.status_code == 404
            assert unknown_rpc.headers["grpc-status"] == str(
                int(GrpcStatusCode.NOT_FOUND)
            )

    async def test_grpc_auth_metadata_and_contract_files(self, tmp_path: Path) -> None:
        app = create_app(
            backend="in_memory", api_key="grpc-secret", warehouse_dir=tmp_path
        )
        async with _client_for(app) as http_client:
            unauth_client = ScaleKGGrpcClient(http_client=http_client)
            with pytest.raises(GrpcRpcError) as exc_info:
                await unauth_client.traverse({"seed_ids": ["customer:org:acme"]})
            assert exc_info.value.code == GrpcStatusCode.UNAUTHENTICATED

            auth_client = ScaleKGGrpcClient(
                http_client=http_client,
                metadata={"x-api-key": "grpc-secret"},
            )
            trav = await auth_client.traverse({"seed_ids": ["customer:org:acme"]})
            assert trav.seed_ids == ["customer:org:acme"]

        root = Path(__file__).resolve().parent.parent
        proto_text = (root / "src" / "scale_kg" / "api" / "scale_kg.proto").read_text(
            encoding="utf-8"
        )
        assert "service ScaleKGService" in proto_text
        assert (root / "docker" / "api" / "Dockerfile").is_file()
        compose_text = (root / "docker-compose.yml").read_text(encoding="utf-8")
        assert "scale-kg-api:latest" in compose_text
