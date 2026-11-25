"""50-concurrent-user snapshot cache, read-only playground, and single-flight runner."""

from __future__ import annotations

import asyncio
import hashlib
import json
import tempfile
import time
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Final, TypeVar, cast

from scale_kg.adapters import IcebergPuppyGraphStore, InMemoryGraphStore
from scale_kg.adapters.base import GraphStore
from scale_kg.benchmark.generator import ScalePreset
from scale_kg.benchmark.golden_queries import FROZEN_GOLDEN_QUERIES, GoldenQuerySuite
from scale_kg.config import ScaleKGConfig
from scale_kg.routing.coordinator import QueryCoordinator
from scale_kg.showcase.scenarios import (
    build_showcase_snapshot,
    run_guardrail_probe,
    run_hot_path_demo,
    run_hybrid_rag_demo,
    run_preset_benchmark,
)
from scale_kg.showcase.snapshot import (
    AdapterSwitchItem,
    AdapterSwitchRequest,
    AdapterSwitchResponse,
    CommitTrendEntry,
    GuardrailProbeRequest,
    GuardrailProbeResult,
    HotPathDemoResult,
    HotPathRequest,
    HybridRagDemoResult,
    PresetBenchmarkBundle,
    RampRunRequest,
    RampRunResponse,
    ShowcaseSnapshot,
)

T = TypeVar("T")
_MISSING: Final[object] = object()


def _consume_future_exception(fut: asyncio.Future[object]) -> None:
    """Retrieve exceptions on unawaited single-flight futures to avoid GC warnings."""
    if not fut.cancelled():
        fut.exception()


class BackpressureError(Exception):
    """Raised when heavy benchmark queue reaches bounded capacity."""

    def __init__(self, message: str, *, retry_after_seconds: int = 5) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class SingleFlightJobRunner:
    """Coalesces identical heavy jobs, enforces bounded concurrency, and caches TTL results."""

    def __init__(
        self,
        *,
        max_concurrent: int = 2,
        max_pending: int = 8,
        cache_ttl_seconds: float = 300.0,
        max_cache_entries: int = 32,
        retry_after_seconds: int = 5,
    ) -> None:
        if max_concurrent < 1:
            raise ValueError(f"max_concurrent must be >= 1, got {max_concurrent}.")
        if max_pending < 1:
            raise ValueError(f"max_pending must be >= 1, got {max_pending}.")
        if cache_ttl_seconds < 0:
            raise ValueError(
                f"cache_ttl_seconds must be >= 0, got {cache_ttl_seconds}."
            )
        if max_cache_entries < 1:
            raise ValueError(
                f"max_cache_entries must be >= 1, got {max_cache_entries}."
            )
        if retry_after_seconds < 1:
            raise ValueError(
                f"retry_after_seconds must be >= 1, got {retry_after_seconds}."
            )

        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._max_pending = max_pending
        self._cache_ttl = cache_ttl_seconds
        self._max_cache_entries = max_cache_entries
        self._retry_after = retry_after_seconds
        self._inflight: dict[str, asyncio.Future[object]] = {}
        self._cache: OrderedDict[str, tuple[float, object]] = OrderedDict()
        self._pending_count = 0

    def _get_cached(self, key: str) -> object:
        entry = self._cache.get(key)
        if entry is None:
            return _MISSING
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            self._cache.pop(key, None)
            return _MISSING
        self._cache.move_to_end(key)
        return value

    def _set_cached(self, key: str, value: object) -> None:
        if self._cache_ttl <= 0:
            return
        self._cache[key] = (time.monotonic() + self._cache_ttl, value)
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_cache_entries:
            self._cache.popitem(last=False)

    async def run(self, key: str, fn: Callable[[], T]) -> tuple[T, bool, bool]:
        """Run or coalesce ``fn`` keyed by ``key``. Returns ``(value, cache_hit, coalesced)``."""
        cached = self._get_cached(key)
        if cached is not _MISSING:
            return cast(T, cached), True, False

        existing = self._inflight.get(key)
        if existing is not None:
            result = await asyncio.shield(existing)
            return cast(T, result), False, True

        if self._pending_count >= self._max_pending:
            raise BackpressureError(
                "Showcase benchmark worker queue is at capacity; retry shortly.",
                retry_after_seconds=self._retry_after,
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[object] = loop.create_future()
        future.add_done_callback(_consume_future_exception)
        self._inflight[key] = future
        self._pending_count += 1

        try:
            async with self._semaphore:
                value = await asyncio.to_thread(fn)
            self._set_cached(key, value)
            if not future.done():
                future.set_result(value)
            return value, False, False
        except asyncio.CancelledError:
            if not future.done():
                future.cancel()
            raise
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            if self._inflight.get(key) is future:
                self._inflight.pop(key, None)
            self._pending_count = max(0, self._pending_count - 1)


def _run_benchmark_sync(req: RampRunRequest) -> PresetBenchmarkBundle:
    """Bridge async PresetBenchmarkBundle execution inside a worker thread."""
    return asyncio.run(
        run_preset_benchmark(
            ScalePreset(req.preset),
            adapters=tuple(sorted(set(req.adapters))),
            seed=req.seed,
        )
    )


class ShowcaseService:
    """Thread/async-safe showcase backend engineered for 50+ concurrent dashboard users."""

    def __init__(
        self,
        *,
        snapshot_path: Path | None = None,
        history_path: Path | None = None,
        config: ScaleKGConfig | None = None,
        light_concurrency: int = 50,
        light_timeout_seconds: float = 2.0,
        job_runner: SingleFlightJobRunner | None = None,
    ) -> None:
        self._snapshot_path = snapshot_path
        self._history_path = history_path
        self._config = config or ScaleKGConfig()
        self._light_sem = asyncio.Semaphore(light_concurrency)
        self._light_timeout = light_timeout_seconds
        self._job_runner = job_runner or SingleFlightJobRunner()

        self._init_lock = asyncio.Lock()
        self._snapshot: ShowcaseSnapshot | None = None
        self._snapshot_bytes: bytes | None = None
        self._snapshot_etag: str | None = None
        self._playground_dir: tempfile.TemporaryDirectory[str] | None = None
        self._golden_stores: dict[str, GraphStore] = {}
        self._history_cache: list[CommitTrendEntry] = []
        self._history_stamp: tuple[int, int] | None = None

    def close(self) -> None:
        """Release temporary playground resources cleanly."""
        if self._playground_dir is not None:
            self._playground_dir.cleanup()
            self._playground_dir = None
        self._golden_stores.clear()

    async def _ensure_initialized(self) -> None:
        if self._snapshot_bytes is not None and self._golden_stores:
            return
        async with self._init_lock:
            if self._snapshot_bytes is None:
                snap: ShowcaseSnapshot
                if self._snapshot_path is not None and self._snapshot_path.is_file():
                    raw = self._snapshot_path.read_bytes()
                    snap = ShowcaseSnapshot.model_validate_json(raw)
                else:
                    snap = await build_showcase_snapshot(
                        presets=(ScalePreset.MICRO,),
                        config=self._config,
                    )
                encoded = snap.model_dump_json().encode("utf-8")
                etag = f'"{hashlib.sha256(encoded).hexdigest()[:24]}"'
                self._snapshot = snap
                self._snapshot_bytes = encoded
                self._snapshot_etag = etag

            if not self._golden_stores:
                pg_dir = tempfile.TemporaryDirectory(prefix="scale_kg_showcase_pg_")
                try:
                    iceberg_dir = Path(pg_dir.name) / "iceberg"
                    iceberg_dir.mkdir(parents=True, exist_ok=True)
                    stores: dict[str, GraphStore] = {
                        "in_memory": InMemoryGraphStore(),
                        "iceberg_puppygraph": IcebergPuppyGraphStore(
                            warehouse_path=iceberg_dir
                        ),
                    }
                    await GoldenQuerySuite().load_dataset(stores)
                except BaseException:
                    pg_dir.cleanup()
                    raise
                self._playground_dir = pg_dir
                self._golden_stores = stores

    async def get_snapshot_bytes(self) -> tuple[bytes, str]:
        """Return immutable pre-serialized snapshot bytes and strong ETag."""
        await self._ensure_initialized()
        if self._snapshot_bytes is None or self._snapshot_etag is None:
            raise RuntimeError("Showcase snapshot failed to initialize.")
        return self._snapshot_bytes, self._snapshot_etag

    async def get_history(self) -> list[CommitTrendEntry]:
        """Return rolling commit trend entries from history.json if present."""
        if self._history_path is None or not self._history_path.is_file():
            self._history_cache = []
            self._history_stamp = None
            return []
        stat = self._history_path.stat()
        stamp = (stat.st_mtime_ns, stat.st_size)
        if stamp != self._history_stamp:
            data = json.loads(self._history_path.read_text(encoding="utf-8"))
            self._history_cache = [
                CommitTrendEntry.model_validate(item) for item in data
            ]
            self._history_stamp = stamp
        return list(self._history_cache)

    async def simulate_adapter_switch(
        self, req: AdapterSwitchRequest
    ) -> AdapterSwitchResponse:
        """Compare 1-2 pre-seeded read-only adapters on one frozen Golden Query."""
        await self._ensure_initialized()
        spec = next(
            (q for q in FROZEN_GOLDEN_QUERIES if q.query_id == req.query_id),
            None,
        )
        if spec is None:
            raise ValueError(f"Unknown Golden Query ID: {req.query_id!r}")

        async def _exec() -> AdapterSwitchResponse:
            items: list[AdapterSwitchItem] = []
            for name in req.adapters:
                store = self._golden_stores.get(name)
                if store is None:
                    raise ValueError(f"Unsupported adapter: {name!r}")
                started = time.perf_counter()
                if spec.requires_coordinator:
                    # Ephemeral coordinator keeps pre-seeded stores immutable across callers.
                    res = await QueryCoordinator(store).traverse(spec.request)
                else:
                    res = await store.traverse_k_hop(spec.request)
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
                node_ids = sorted({n.id for n in res.nodes})
                items.append(
                    AdapterSwitchItem(
                        adapter=name,
                        node_count=len(node_ids),
                        edge_count=len(res.edges),
                        path_count=len(res.paths),
                        bucket_hops_unwrapped=res.metrics.bucket_hops_unwrapped,
                        execution_ms=elapsed_ms,
                        node_ids=node_ids,
                    )
                )
            parity = len({tuple(i.node_ids) for i in items}) <= 1
            return AdapterSwitchResponse(
                query_id=spec.query_id,
                query_name=spec.name,
                parity_matched=parity,
                results=items,
            )

        async with self._light_sem:
            return await asyncio.wait_for(_exec(), timeout=self._light_timeout)

    async def simulate_guardrail(
        self, req: GuardrailProbeRequest
    ) -> GuardrailProbeResult:
        """Execute one allowlisted guardrail probe in an isolated ephemeral coordinator."""
        async with self._light_sem:
            return await asyncio.wait_for(
                run_guardrail_probe(req.probe_id), timeout=self._light_timeout
            )

    async def simulate_hot_path(self, req: HotPathRequest) -> HotPathDemoResult:
        """Execute the derived-edge hot-path promotion stepper cleanly."""
        async with self._light_sem:
            return await asyncio.wait_for(
                run_hot_path_demo(hits=req.hits, threshold=req.threshold),
                timeout=self._light_timeout,
            )

    async def simulate_hybrid(self) -> HybridRagDemoResult:
        """Execute the 5-stage Hybrid Vector + KG + Lakehouse trace."""
        async with self._light_sem:
            return await asyncio.wait_for(
                run_hybrid_rag_demo(), timeout=self._light_timeout
            )

    async def run_ramp(self, req: RampRunRequest) -> RampRunResponse:
        """Run a coalesced, TTL-cached live scale benchmark via SingleFlightJobRunner."""
        adapters_key = ",".join(sorted(set(req.adapters)))
        cache_key = f"{req.preset}:{adapters_key}:{req.seed}"
        bundle, cache_hit, coalesced = await self._job_runner.run(
            cache_key,
            lambda: _run_benchmark_sync(req),
        )
        return RampRunResponse(
            cache_hit=cache_hit,
            coalesced=coalesced,
            bundle=bundle,
        )
