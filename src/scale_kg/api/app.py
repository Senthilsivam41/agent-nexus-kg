"""FastAPI REST API, JSON-gRPC HTTP bridge, and CLI entrypoint for Scale-KG."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import uvicorn
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from scale_kg.adapters.base import GraphStore, GraphStoreUnavailableError
from scale_kg.api.grpc_service import (
    GRPC_SERVICE_NAME,
    GrpcRpcError,
    GrpcStatusCode,
    ScaleKGGrpcServicer,
    create_grpc_aio_server,
    grpc_status_to_http_status,
)
from scale_kg.api.models import (
    ApiErrorResponse,
    BenchmarkRunRequest,
    BenchmarkRunResponse,
    DirectoryResolveResponse,
    GuardrailErrorResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    NeighborsResponse,
    PutDocumentsRequest,
    PutDocumentsResponse,
    RegisterAliasesRequest,
    RegisterAliasesResponse,
    SliceListResponse,
    StatsResponse,
    WriteSliceResponse,
)
from scale_kg.api.state import (
    ScaleKGApiState,
    build_graph_store,
    verify_api_key_token,
)
from scale_kg.config import ScaleKGConfig
from scale_kg.core.guardrails import GuardrailViolationError
from scale_kg.core.models import (
    Domain,
    EntityNode,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.lakehouse.store import HistoricalGraphSlice, LakehouseDocument
from scale_kg.routing.directory import EntityResolutionError
from scale_kg.routing.hybrid_rag import HybridRAGRequest, HybridRAGResult

__all__ = [
    "ScaleKGApiState",
    "build_graph_store",
    "create_app",
    "main",
]


def create_app(
    *,
    store: GraphStore | Mapping[Domain, GraphStore] | None = None,
    backend: str | None = None,
    shard_map: Mapping[Domain, GraphStore] | None = None,
    warehouse_dir: str | Path | None = None,
    api_key: str | None = None,
    config: ScaleKGConfig | None = None,
    embedder: Callable[[str], list[float]] | None = None,
) -> FastAPI:
    """Create and configure the Scale-KG FastAPI REST & gRPC-bridge application."""
    state = ScaleKGApiState(
        store=store,
        backend=backend,
        shard_map=shard_map,
        warehouse_dir=warehouse_dir,
        api_key=api_key,
        config=config,
        embedder=embedder,
    )

    app = FastAPI(
        title="Agent Nexus KG (scale-kg) API",
        version="0.1.0",
        description=(
            "Engine-agnostic Semantic Knowledge Graph REST & gRPC service "
            "enforcing Lean Graph guardrails, supernode bucketing, and Hybrid GraphRAG."
        ),
    )
    app.state.scale_kg = state

    @app.exception_handler(GuardrailViolationError)
    async def _guardrail_exception_handler(
        _request: Request, exc: Exception
    ) -> JSONResponse:
        err = cast(GuardrailViolationError, exc)
        body = GuardrailErrorResponse(violation=err.violation).model_dump(mode="json")
        return JSONResponse(status_code=422, content=body)

    @app.exception_handler(EntityResolutionError)
    async def _entity_not_found_handler(
        _request: Request, exc: Exception
    ) -> JSONResponse:
        body = ApiErrorResponse(error="entity_not_found", detail=str(exc)).model_dump(
            mode="json"
        )
        return JSONResponse(status_code=404, content=body)

    @app.exception_handler(GraphStoreUnavailableError)
    async def _store_unavailable_handler(
        _request: Request, exc: Exception
    ) -> JSONResponse:
        body = ApiErrorResponse(
            error="graph_store_unavailable", detail=str(exc)
        ).model_dump(mode="json")
        return JSONResponse(status_code=503, content=body)

    @app.exception_handler(ValueError)
    async def _value_error_handler(_request: Request, exc: Exception) -> JSONResponse:
        body = ApiErrorResponse(error="bad_request", detail=str(exc)).model_dump(
            mode="json"
        )
        return JSONResponse(status_code=400, content=body)

    async def verify_api_key(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> None:
        if not verify_api_key_token(
            state.api_key,
            x_api_key=x_api_key,
            authorization=authorization,
        ):
            raise HTTPException(
                status_code=401,
                detail="Invalid or missing API key.",
            )

    @app.get("/v1/health", response_model=HealthResponse, tags=["system"])
    async def health() -> HealthResponse:
        """Report shard health and coordinator, directory, and lakehouse counters."""
        return await state.check_health()

    protected = APIRouter(
        prefix="/v1",
        dependencies=[Depends(verify_api_key)],
    )

    @protected.post("/ingest", response_model=IngestResponse, tags=["graph"])
    async def ingest(request: IngestRequest) -> IngestResponse:
        """Validate and upsert nodes, edges, aliases, lakehouse docs, and vectors."""
        return await state.ingest(request)

    @protected.post("/traverse", response_model=TraversalResult, tags=["graph"])
    async def traverse(request: TraversalRequest) -> TraversalResult:
        """Execute a guardrail-enforced 1-to-3 hop federated traversal."""
        return await state.traverse(request)

    @protected.post(
        "/hybrid-retrieve", response_model=HybridRAGResult, tags=["retrieval"]
    )
    async def hybrid_retrieve(request: HybridRAGRequest) -> HybridRAGResult:
        """Execute the 5-stage Seed-Then-Expand Vector + KG + Lakehouse pipeline."""
        return await state.hybrid_retrieve(request)

    @protected.post(
        "/benchmark/run", response_model=BenchmarkRunResponse, tags=["benchmark"]
    )
    async def run_benchmark(request: BenchmarkRunRequest) -> BenchmarkRunResponse:
        """Run the 7-workload Gate 2 benchmark suite and generate an ADR report."""
        return await state.run_benchmark(request)

    @protected.get(
        "/nodes/{node_id:path}/neighbors",
        response_model=NeighborsResponse,
        tags=["graph"],
    )
    async def get_node_neighbors(
        node_id: str,
        edge_types: Annotated[list[str] | None, Query()] = None,
        domains: Annotated[list[Domain] | None, Query()] = None,
        direction: Annotated[Literal["out", "in", "both"], Query()] = "out",
        limit: Annotated[int, Query(ge=1, le=5000)] = 5000,
    ) -> NeighborsResponse:
        """Return logical neighbors for a node after supernode bucket collapse."""
        return await state.get_neighbors(
            node_id,
            edge_types=edge_types,
            domains=domains,
            direction=direction,
            limit=limit,
        )

    @protected.get("/nodes/{node_id:path}", response_model=EntityNode, tags=["graph"])
    async def get_node(node_id: str) -> EntityNode:
        """Fetch an EntityNode by canonical ID, legacy ID, or alias."""
        return await state.get_node(node_id)

    @protected.get("/stats", response_model=StatsResponse, tags=["system"])
    async def stats() -> StatsResponse:
        """Return graph, coordinator, entity directory, and lakehouse counters."""
        return await state.stats()

    @protected.post(
        "/lakehouse/documents",
        response_model=PutDocumentsResponse,
        tags=["lakehouse"],
    )
    async def put_lakehouse_documents(
        request: PutDocumentsRequest,
    ) -> PutDocumentsResponse:
        """Persist authoritative documents to Parquet Lakehouse partitions."""
        return state.put_documents(request)

    @protected.get(
        "/lakehouse/documents/{lakehouse_ref:path}",
        response_model=LakehouseDocument,
        tags=["lakehouse"],
    )
    async def get_lakehouse_document(lakehouse_ref: str) -> LakehouseDocument:
        """Retrieve an authoritative Lakehouse document by ``lakehouse_ref``."""
        return state.get_document(lakehouse_ref)

    @protected.post(
        "/lakehouse/slices",
        response_model=WriteSliceResponse,
        tags=["lakehouse"],
    )
    async def write_lakehouse_slice(
        graph_slice: HistoricalGraphSlice,
    ) -> WriteSliceResponse:
        """Write a historical graph slice into Lakehouse Parquet storage."""
        return state.write_slice(graph_slice)

    @protected.get(
        "/lakehouse/slices",
        response_model=SliceListResponse,
        tags=["lakehouse"],
    )
    async def list_lakehouse_slices(
        domain: Annotated[Domain | None, Query()] = None,
    ) -> SliceListResponse:
        """List stored historical graph slice IDs."""
        return state.list_slices(domain=domain)

    @protected.get(
        "/lakehouse/slices/{slice_id}",
        response_model=HistoricalGraphSlice,
        tags=["lakehouse"],
    )
    async def get_lakehouse_slice(slice_id: str) -> HistoricalGraphSlice:
        """Load a historical graph slice by ``slice_id``."""
        return state.load_slice(slice_id)

    @protected.post(
        "/directory/aliases",
        response_model=RegisterAliasesResponse,
        tags=["directory"],
    )
    async def register_directory_aliases(
        request: RegisterAliasesRequest,
    ) -> RegisterAliasesResponse:
        """Register entity domain mappings and legacy ID aliases."""
        return state.register_aliases(request)

    @protected.get(
        "/directory/resolve/{entity_id:path}",
        response_model=DirectoryResolveResponse,
        tags=["directory"],
    )
    async def resolve_directory_entity(entity_id: str) -> DirectoryResolveResponse:
        """Canonicalize an entity ID/alias and resolve its owning Domain shard."""
        return state.resolve_entity(entity_id)

    app.include_router(protected)

    def _grpc_error_response(err: GrpcRpcError) -> JSONResponse:
        http_status = grpc_status_to_http_status(
            err.code, has_violation=err.violation is not None
        )
        safe_msg = err.details.replace("\r", " ").replace("\n", " ")
        return JSONResponse(
            status_code=http_status,
            content=err.to_payload(),
            headers={
                "grpc-status": str(int(err.code)),
                "grpc-message": safe_msg,
            },
        )

    @app.post(f"/{GRPC_SERVICE_NAME}/{{rpc_method}}", tags=["grpc"])
    async def grpc_json_bridge(rpc_method: str, request: Request) -> JSONResponse:
        """Invoke ``scale_kg.v1.ScaleKGService`` unary RPCs over HTTP JSON-gRPC."""
        raw_body = await request.body()
        if raw_body:
            try:
                parsed_body = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                return _grpc_error_response(
                    GrpcRpcError(
                        GrpcStatusCode.INVALID_ARGUMENT,
                        f"Invalid JSON payload: {exc}",
                    )
                )
            if not isinstance(parsed_body, dict):
                return _grpc_error_response(
                    GrpcRpcError(
                        GrpcStatusCode.INVALID_ARGUMENT,
                        "JSON-gRPC request payload must be a JSON object.",
                    )
                )
            payload: dict[str, Any] = parsed_body
        else:
            payload = {}

        metadata = {k: v for k, v in request.headers.items()}
        servicer: ScaleKGGrpcServicer = state.grpc_servicer
        try:
            result = await servicer.dispatch_json_rpc(
                rpc_method, payload, metadata=metadata
            )
            return JSONResponse(
                status_code=200,
                content=result,
                headers={"grpc-status": str(int(GrpcStatusCode.OK))},
            )
        except GrpcRpcError as err:
            return _grpc_error_response(err)

    return app


def main() -> None:
    """Run the Scale-KG FastAPI server via Uvicorn (and optional gRPC port)."""
    parser = argparse.ArgumentParser(
        description="Run the Scale-KG FastAPI REST & gRPC API service."
    )
    parser.add_argument("--host", default=os.environ.get("API_HOST", "127.0.0.1"))
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("API_PORT", "8000"))
    )
    parser.add_argument(
        "--backend", default=os.environ.get("SCALE_KG_BACKEND", "in_memory")
    )
    parser.add_argument(
        "--warehouse-dir", default=os.environ.get("SCALE_KG_WAREHOUSE_DIR")
    )
    parser.add_argument("--grpc-port", type=int, default=None)
    args = parser.parse_args()

    app = create_app(
        backend=args.backend,
        warehouse_dir=args.warehouse_dir,
    )
    if args.grpc_port is not None:
        import asyncio

        async def _serve_both() -> None:
            scale_state: ScaleKGApiState = app.state.scale_kg
            grpc_server = create_grpc_aio_server(
                scale_state.grpc_servicer, host=args.host, port=args.grpc_port
            )
            await grpc_server.start()
            uvi_config = uvicorn.Config(app, host=args.host, port=args.port)
            uvi_server = uvicorn.Server(uvi_config)
            try:
                await uvi_server.serve()
            finally:
                await grpc_server.stop(grace=2)

        asyncio.run(_serve_both())
        return

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
