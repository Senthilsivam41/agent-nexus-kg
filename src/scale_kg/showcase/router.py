"""FastAPI router serving the zero-build showcase dashboard and live simulation API."""

from __future__ import annotations

from collections.abc import Awaitable
from pathlib import Path
from typing import Annotated, TypeVar

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from scale_kg.api.state import verify_api_key_token
from scale_kg.showcase.concurrency import BackpressureError, ShowcaseService
from scale_kg.showcase.snapshot import (
    AdapterSwitchRequest,
    AdapterSwitchResponse,
    CommitTrendEntry,
    GuardrailProbeRequest,
    GuardrailProbeResult,
    HotPathDemoResult,
    HotPathRequest,
    HybridRagDemoResult,
    RampRunRequest,
    RampRunResponse,
)

__all__ = [
    "STATIC_DIR",
    "create_showcase_router",
]

STATIC_DIR = Path(__file__).resolve().parent / "static"

_ALLOWED_ASSETS: dict[str, str] = {
    "styles.css": "text/css; charset=utf-8",
    "app.js": "application/javascript; charset=utf-8",
    "charts.js": "application/javascript; charset=utf-8",
    "views.js": "application/javascript; charset=utf-8",
}


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    """Return True if an ``If-None-Match`` header value matches ``etag``."""
    if not if_none_match:
        return False
    for raw_tag in if_none_match.split(","):
        tag = raw_tag.strip()
        if tag == "*":
            return True
        if tag.startswith("W/"):
            tag = tag[2:].strip()
        if tag == etag:
            return True
    return False


_T = TypeVar("_T")

_SIMULATION_TIMEOUT_BODY: dict[str, str] = {
    "error": "simulation_timeout",
    "detail": "Showcase simulation exceeded execution time budget.",
}


async def _run_simulation_with_timeout(coro: Awaitable[_T]) -> _T | JSONResponse:
    """Await a showcase simulation, returning HTTP 504 on ``TimeoutError``."""
    try:
        return await coro
    except TimeoutError:
        return JSONResponse(status_code=504, content=_SIMULATION_TIMEOUT_BODY)


def create_showcase_router(
    service: ShowcaseService,
    *,
    api_key: str | None = None,
    require_auth: bool = False,
) -> APIRouter:
    """Create the FastAPI router for `/showcase` UI and `/showcase/api/*` endpoints."""
    if require_auth and not (api_key and api_key.strip()):
        raise ValueError("showcase_require_auth=True requires a non-empty api_key.")

    async def _check_auth(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> None:
        if not require_auth:
            return
        if not api_key or not verify_api_key_token(
            api_key,
            x_api_key=x_api_key,
            authorization=authorization,
        ):
            raise HTTPException(status_code=401, detail="Invalid or missing API key.")

    router = APIRouter(
        prefix="/showcase",
        tags=["showcase"],
        dependencies=[Depends(_check_auth)],
    )

    @router.get("", include_in_schema=False)
    async def showcase_redirect() -> RedirectResponse:
        return RedirectResponse(url="/showcase/", status_code=307)

    @router.get("/", include_in_schema=False)
    async def showcase_index() -> FileResponse:
        static_root = STATIC_DIR.resolve()
        index_path = (static_root / "index.html").resolve()
        if not index_path.is_relative_to(static_root) or not index_path.is_file():
            raise HTTPException(status_code=404, detail="Showcase UI not found.")
        return FileResponse(
            index_path,
            media_type="text/html; charset=utf-8",
            headers={"Cache-Control": "public, max-age=300"},
        )

    @router.get("/assets/{filename}", include_in_schema=False)
    async def showcase_asset(filename: str) -> FileResponse:
        media_type = _ALLOWED_ASSETS.get(filename)
        if media_type is None:
            raise HTTPException(status_code=404, detail="Asset not found.")
        static_root = STATIC_DIR.resolve()
        target = (static_root / filename).resolve()
        if not target.is_relative_to(static_root) or not target.is_file():
            raise HTTPException(status_code=404, detail="Asset not found.")
        return FileResponse(
            target,
            media_type=media_type,
            headers={"Cache-Control": "public, max-age=300"},
        )

    @router.get("/api/snapshot")
    async def get_snapshot(
        if_none_match: Annotated[str | None, Header(alias="If-None-Match")] = None,
    ) -> Response:
        payload, etag = await service.get_snapshot_bytes()
        headers = {
            "ETag": etag,
            "Cache-Control": "public, max-age=30",
        }
        if _etag_matches(if_none_match, etag):
            return Response(status_code=304, headers=headers)
        return Response(
            content=payload,
            status_code=200,
            media_type="application/json",
            headers=headers,
        )

    @router.get("/api/history", response_model=list[CommitTrendEntry])
    async def get_history() -> list[CommitTrendEntry]:
        return await service.get_history()

    @router.post("/api/simulate/adapter-switch", response_model=AdapterSwitchResponse)
    async def simulate_adapter_switch(
        request: AdapterSwitchRequest,
    ) -> AdapterSwitchResponse | JSONResponse:
        return await _run_simulation_with_timeout(
            service.simulate_adapter_switch(request)
        )

    @router.post("/api/simulate/guardrail", response_model=GuardrailProbeResult)
    async def simulate_guardrail(
        request: GuardrailProbeRequest,
    ) -> GuardrailProbeResult | JSONResponse:
        return await _run_simulation_with_timeout(service.simulate_guardrail(request))

    @router.post("/api/simulate/hot-path", response_model=HotPathDemoResult)
    async def simulate_hot_path(
        request: HotPathRequest,
    ) -> HotPathDemoResult | JSONResponse:
        return await _run_simulation_with_timeout(service.simulate_hot_path(request))

    @router.post("/api/simulate/hybrid", response_model=HybridRagDemoResult)
    async def simulate_hybrid() -> HybridRagDemoResult | JSONResponse:
        return await _run_simulation_with_timeout(service.simulate_hybrid())

    @router.post("/api/ramp/run", response_model=RampRunResponse)
    async def run_ramp(request: RampRunRequest) -> RampRunResponse | JSONResponse:
        try:
            return await service.run_ramp(request)
        except BackpressureError as exc:
            return JSONResponse(
                status_code=429,
                content={"error": "rate_limited", "detail": str(exc)},
                headers={"Retry-After": str(exc.retry_after_seconds)},
            )

    return router
