"""Typed gRPC servicer, JSON-gRPC HTTP bridge client, and optional grpcio server."""

from __future__ import annotations

import importlib
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from enum import IntEnum
from typing import TYPE_CHECKING, Any, TypeVar, cast

import httpx
from pydantic import BaseModel, ValidationError

from scale_kg.adapters.base import GraphStoreError, GraphStoreUnavailableError
from scale_kg.api.models import (
    BenchmarkRunRequest,
    BenchmarkRunResponse,
    DirectoryResolveResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    PutDocumentsRequest,
    PutDocumentsResponse,
    RegisterAliasesRequest,
    RegisterAliasesResponse,
    WriteSliceResponse,
)
from scale_kg.core.guardrails import GuardrailViolationError
from scale_kg.core.models import (
    GuardrailViolation,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.lakehouse.store import HistoricalGraphSlice, LakehouseDocument
from scale_kg.routing.directory import EntityResolutionError
from scale_kg.routing.hybrid_rag import HybridRAGRequest, HybridRAGResult

if TYPE_CHECKING:
    from scale_kg.api.state import ScaleKGApiState


GRPC_SERVICE_NAME = "scale_kg.v1.ScaleKGService"


class GrpcStatusCode(IntEnum):
    """Standard gRPC status codes used by ScaleKGService."""

    OK = 0
    INVALID_ARGUMENT = 3
    NOT_FOUND = 5
    INTERNAL = 13
    UNAVAILABLE = 14
    UNAUTHENTICATED = 16


def grpc_status_to_http_status(
    code: GrpcStatusCode, *, has_violation: bool = False
) -> int:
    """Map a GrpcStatusCode to an equivalent HTTP status code."""
    if code == GrpcStatusCode.OK:
        return 200
    if code == GrpcStatusCode.INVALID_ARGUMENT:
        return 422 if has_violation else 400
    if code == GrpcStatusCode.NOT_FOUND:
        return 404
    if code == GrpcStatusCode.UNAUTHENTICATED:
        return 401
    if code == GrpcStatusCode.UNAVAILABLE:
        return 503
    return 500


class GrpcRpcError(Exception):
    """Structured gRPC RPC failure carrying status code and optional violation."""

    def __init__(
        self,
        code: GrpcStatusCode,
        details: str,
        *,
        violation: GuardrailViolation | None = None,
    ) -> None:
        self.code = code
        self.details = details
        self.violation = violation
        super().__init__(f"[{code.name}] {details}")

    def to_payload(self) -> dict[str, Any]:
        """Serialize error details for JSON-gRPC transport responses."""
        payload: dict[str, Any] = {
            "grpc_status": int(self.code),
            "grpc_code": self.code.name,
            "details": self.details,
        }
        if self.violation is not None:
            payload["error"] = "guardrail_violation"
            payload["violation"] = self.violation.model_dump(mode="json")
        return payload


def _normalize_metadata(metadata: Mapping[str, str] | None) -> dict[str, str]:
    if not metadata:
        return {}
    return {str(k).lower(): str(v) for k, v in metadata.items()}


_ModelT = TypeVar("_ModelT", bound=BaseModel)
_ResultT = TypeVar("_ResultT")


def _coerce_model(
    value: _ModelT | Mapping[str, Any], model_cls: type[_ModelT]
) -> _ModelT:
    if isinstance(value, model_cls):
        return value
    return model_cls.model_validate(value)


def _extract_field(payload: str | Mapping[str, Any], field_name: str) -> str:
    if isinstance(payload, str):
        return payload
    raw = payload.get(field_name)
    if not isinstance(raw, str) or not raw:
        raise ValueError(f"Missing required string field '{field_name}'.")
    return raw


class ScaleKGGrpcServicer:
    """Async typed gRPC servicer for ``scale_kg.v1.ScaleKGService``."""

    def __init__(self, state: ScaleKGApiState) -> None:
        self.state = state
        self._rpc_handlers: dict[
            str,
            Callable[[dict[str, Any], Mapping[str, str] | None], Awaitable[BaseModel]],
        ] = {
            "CheckHealth": self._rpc_check_health,
            "Ingest": self._rpc_ingest,
            "Traverse": self._rpc_traverse,
            "HybridRetrieve": self._rpc_hybrid_retrieve,
            "RunBenchmark": self._rpc_run_benchmark,
            "PutDocuments": self._rpc_put_documents,
            "GetDocument": self._rpc_get_document,
            "WriteHistoricalSlice": self._rpc_write_historical_slice,
            "GetHistoricalSlice": self._rpc_get_historical_slice,
            "RegisterAliases": self._rpc_register_aliases,
            "ResolveEntity": self._rpc_resolve_entity,
        }

    @property
    def method_names(self) -> tuple[str, ...]:
        """Return supported RPC method names on ScaleKGService."""
        return tuple(self._rpc_handlers.keys())

    def _enforce_auth(self, metadata: Mapping[str, str] | None) -> None:
        from scale_kg.api.state import verify_api_key_token

        meta = _normalize_metadata(metadata)
        if not verify_api_key_token(
            self.state.api_key,
            x_api_key=meta.get("x-api-key"),
            authorization=meta.get("authorization"),
        ):
            raise GrpcRpcError(
                GrpcStatusCode.UNAUTHENTICATED,
                "Invalid or missing API key metadata.",
            )

    async def _run_guarded(
        self,
        fn: Callable[[], Awaitable[_ResultT] | _ResultT],
        *,
        metadata: Mapping[str, str] | None,
        require_auth: bool = True,
    ) -> _ResultT:
        if require_auth:
            self._enforce_auth(metadata)
        try:
            result = fn()
            if inspect.isawaitable(result):
                return cast(_ResultT, await result)
            return cast(_ResultT, result)
        except GrpcRpcError:
            raise
        except GuardrailViolationError as exc:
            raise GrpcRpcError(
                GrpcStatusCode.INVALID_ARGUMENT,
                str(exc),
                violation=exc.violation,
            ) from exc
        except EntityResolutionError as exc:
            raise GrpcRpcError(GrpcStatusCode.NOT_FOUND, str(exc)) from exc
        except GraphStoreUnavailableError as exc:
            raise GrpcRpcError(GrpcStatusCode.UNAVAILABLE, str(exc)) from exc
        except GraphStoreError as exc:
            raise GrpcRpcError(GrpcStatusCode.UNAVAILABLE, str(exc)) from exc
        except (ValidationError, ValueError) as exc:
            raise GrpcRpcError(GrpcStatusCode.INVALID_ARGUMENT, str(exc)) from exc

    async def CheckHealth(
        self,
        _request: Mapping[str, Any] | None = None,
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> HealthResponse:
        """RPC CheckHealth."""
        return await self._run_guarded(
            self.state.check_health, metadata=metadata, require_auth=False
        )

    async def Ingest(
        self,
        request: IngestRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> IngestResponse:
        """RPC Ingest."""
        return await self._run_guarded(
            lambda: self.state.ingest(_coerce_model(request, IngestRequest)),
            metadata=metadata,
        )

    async def Traverse(
        self,
        request: TraversalRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> TraversalResult:
        """RPC Traverse."""
        return await self._run_guarded(
            lambda: self.state.traverse(_coerce_model(request, TraversalRequest)),
            metadata=metadata,
        )

    async def HybridRetrieve(
        self,
        request: HybridRAGRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> HybridRAGResult:
        """RPC HybridRetrieve."""
        return await self._run_guarded(
            lambda: self.state.hybrid_retrieve(
                _coerce_model(request, HybridRAGRequest)
            ),
            metadata=metadata,
        )

    async def RunBenchmark(
        self,
        request: BenchmarkRunRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> BenchmarkRunResponse:
        """RPC RunBenchmark."""
        return await self._run_guarded(
            lambda: self.state.run_benchmark(
                _coerce_model(request, BenchmarkRunRequest)
            ),
            metadata=metadata,
        )

    async def PutDocuments(
        self,
        request: PutDocumentsRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> PutDocumentsResponse:
        """RPC PutDocuments."""
        return await self._run_guarded(
            lambda: self.state.put_documents(
                _coerce_model(request, PutDocumentsRequest)
            ),
            metadata=metadata,
        )

    async def GetDocument(
        self,
        request: str | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> LakehouseDocument:
        """RPC GetDocument."""
        return await self._run_guarded(
            lambda: self.state.get_document(_extract_field(request, "lakehouse_ref")),
            metadata=metadata,
        )

    async def WriteHistoricalSlice(
        self,
        request: HistoricalGraphSlice | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> WriteSliceResponse:
        """RPC WriteHistoricalSlice."""
        return await self._run_guarded(
            lambda: self.state.write_slice(
                _coerce_model(request, HistoricalGraphSlice)
            ),
            metadata=metadata,
        )

    async def GetHistoricalSlice(
        self,
        request: str | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> HistoricalGraphSlice:
        """RPC GetHistoricalSlice."""
        return await self._run_guarded(
            lambda: self.state.load_slice(_extract_field(request, "slice_id")),
            metadata=metadata,
        )

    async def RegisterAliases(
        self,
        request: RegisterAliasesRequest | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> RegisterAliasesResponse:
        """RPC RegisterAliases."""
        return await self._run_guarded(
            lambda: self.state.register_aliases(
                _coerce_model(request, RegisterAliasesRequest)
            ),
            metadata=metadata,
        )

    async def ResolveEntity(
        self,
        request: str | Mapping[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> DirectoryResolveResponse:
        """RPC ResolveEntity."""
        return await self._run_guarded(
            lambda: self.state.resolve_entity(_extract_field(request, "entity_id")),
            metadata=metadata,
        )

    async def _rpc_check_health(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> HealthResponse:
        return await self.CheckHealth(payload, metadata=metadata)

    async def _rpc_ingest(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> IngestResponse:
        return await self.Ingest(payload, metadata=metadata)

    async def _rpc_traverse(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> TraversalResult:
        return await self.Traverse(payload, metadata=metadata)

    async def _rpc_hybrid_retrieve(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> HybridRAGResult:
        return await self.HybridRetrieve(payload, metadata=metadata)

    async def _rpc_run_benchmark(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> BenchmarkRunResponse:
        return await self.RunBenchmark(payload, metadata=metadata)

    async def _rpc_put_documents(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> PutDocumentsResponse:
        return await self.PutDocuments(payload, metadata=metadata)

    async def _rpc_get_document(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> LakehouseDocument:
        return await self.GetDocument(payload, metadata=metadata)

    async def _rpc_write_historical_slice(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> WriteSliceResponse:
        return await self.WriteHistoricalSlice(payload, metadata=metadata)

    async def _rpc_get_historical_slice(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> HistoricalGraphSlice:
        return await self.GetHistoricalSlice(payload, metadata=metadata)

    async def _rpc_register_aliases(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> RegisterAliasesResponse:
        return await self.RegisterAliases(payload, metadata=metadata)

    async def _rpc_resolve_entity(
        self, payload: dict[str, Any], metadata: Mapping[str, str] | None
    ) -> DirectoryResolveResponse:
        return await self.ResolveEntity(payload, metadata=metadata)

    async def dispatch_json_rpc(
        self,
        method_name: str,
        payload: dict[str, Any] | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Dispatch a unary JSON-encoded gRPC RPC by method name."""
        handler = self._rpc_handlers.get(method_name)
        if handler is None:
            raise GrpcRpcError(
                GrpcStatusCode.NOT_FOUND,
                f"Unknown RPC method '{method_name}' on {GRPC_SERVICE_NAME}.",
            )
        model = await handler(dict(payload or {}), metadata)
        return model.model_dump(mode="json")


class ScaleKGGrpcClient:
    """Async client invoking ScaleKGService in-process or over HTTP JSON-gRPC."""

    def __init__(
        self,
        *,
        servicer: ScaleKGGrpcServicer | None = None,
        http_client: httpx.AsyncClient | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> None:
        if servicer is None and http_client is None:
            raise ValueError(
                "ScaleKGGrpcClient requires either 'servicer' or 'http_client'."
            )
        self.servicer = servicer
        self.http_client = http_client
        self.metadata = dict(metadata or {})

    async def call(
        self,
        method_name: str,
        payload: BaseModel | dict[str, Any] | None = None,
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        """Invoke a unary RPC and return decoded JSON response dictionary."""
        merged_meta = {**self.metadata, **(metadata or {})}
        if isinstance(payload, BaseModel):
            body = payload.model_dump(mode="json")
        else:
            body = dict(payload or {})

        if self.servicer is not None:
            return await self.servicer.dispatch_json_rpc(
                method_name, body, metadata=merged_meta
            )

        assert self.http_client is not None
        resp = await self.http_client.post(
            f"/{GRPC_SERVICE_NAME}/{method_name}",
            json=body,
            headers=merged_meta,
        )
        grpc_status_hdr = resp.headers.get("grpc-status")
        if resp.status_code >= 400 or (
            grpc_status_hdr is not None and grpc_status_hdr != "0"
        ):
            data = resp.json()
            code = GrpcStatusCode(
                int(data.get("grpc_status", grpc_status_hdr or GrpcStatusCode.INTERNAL))
            )
            violation_raw = data.get("violation")
            violation = (
                GuardrailViolation.model_validate(violation_raw)
                if violation_raw is not None
                else None
            )
            raise GrpcRpcError(
                code,
                str(data.get("details", resp.text)),
                violation=violation,
            )
        return dict(resp.json())

    async def check_health(
        self, *, metadata: Mapping[str, str] | None = None
    ) -> HealthResponse:
        return HealthResponse.model_validate(
            await self.call("CheckHealth", {}, metadata=metadata)
        )

    async def ingest(
        self,
        request: IngestRequest | dict[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> IngestResponse:
        return IngestResponse.model_validate(
            await self.call("Ingest", request, metadata=metadata)
        )

    async def traverse(
        self,
        request: TraversalRequest | dict[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> TraversalResult:
        return TraversalResult.model_validate(
            await self.call("Traverse", request, metadata=metadata)
        )

    async def hybrid_retrieve(
        self,
        request: HybridRAGRequest | dict[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> HybridRAGResult:
        return HybridRAGResult.model_validate(
            await self.call("HybridRetrieve", request, metadata=metadata)
        )

    async def run_benchmark(
        self,
        request: BenchmarkRunRequest | dict[str, Any],
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> BenchmarkRunResponse:
        return BenchmarkRunResponse.model_validate(
            await self.call("RunBenchmark", request, metadata=metadata)
        )

    async def resolve_entity(
        self,
        entity_id: str,
        *,
        metadata: Mapping[str, str] | None = None,
    ) -> DirectoryResolveResponse:
        return DirectoryResolveResponse.model_validate(
            await self.call(
                "ResolveEntity", {"entity_id": entity_id}, metadata=metadata
            )
        )


def create_grpc_aio_server(
    servicer: ScaleKGGrpcServicer,
    *,
    host: str = "0.0.0.0",
    port: int = 50051,
) -> Any:
    """Build a native ``grpc.aio.Server`` when the optional ``scale-kg[grpc]`` extra is installed."""
    try:
        grpc_mod = importlib.import_module("grpc")
    except ImportError as exc:
        raise RuntimeError(
            "grpcio is not installed; install optional extra 'scale-kg[grpc]' "
            "to run the native HTTP/2 gRPC server."
        ) from exc

    status_map = {
        GrpcStatusCode.OK: grpc_mod.StatusCode.OK,
        GrpcStatusCode.INVALID_ARGUMENT: grpc_mod.StatusCode.INVALID_ARGUMENT,
        GrpcStatusCode.NOT_FOUND: grpc_mod.StatusCode.NOT_FOUND,
        GrpcStatusCode.INTERNAL: grpc_mod.StatusCode.INTERNAL,
        GrpcStatusCode.UNAVAILABLE: grpc_mod.StatusCode.UNAVAILABLE,
        GrpcStatusCode.UNAUTHENTICATED: grpc_mod.StatusCode.UNAUTHENTICATED,
    }

    def _deserialize(raw: bytes) -> dict[str, Any]:
        if not raw:
            return {}
        return dict(json.loads(raw.decode("utf-8")))

    def _serialize(payload: dict[str, Any]) -> bytes:
        return json.dumps(payload).encode("utf-8")

    def _make_handler(method_name: str) -> Any:
        async def _unary_unary(request: dict[str, Any], context: Any) -> dict[str, Any]:
            raw_meta = context.invocation_metadata() or ()
            metadata = {str(k): str(v) for k, v in raw_meta}
            try:
                return await servicer.dispatch_json_rpc(
                    method_name, request, metadata=metadata
                )
            except GrpcRpcError as err:
                await context.abort(
                    status_map.get(err.code, grpc_mod.StatusCode.INTERNAL),
                    json.dumps(err.to_payload()),
                )
                return {}

        return grpc_mod.unary_unary_rpc_method_handler(
            _unary_unary,
            request_deserializer=_deserialize,
            response_serializer=_serialize,
        )

    rpc_method_handlers = {name: _make_handler(name) for name in servicer.method_names}
    generic_handler = grpc_mod.method_handlers_generic_handler(
        GRPC_SERVICE_NAME, rpc_method_handlers
    )
    server = grpc_mod.aio.server()
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_insecure_port(f"{host}:{port}")
    return server
