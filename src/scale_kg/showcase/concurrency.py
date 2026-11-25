"""50-concurrent-user snapshot cache, read-only playground, and single-flight runner."""

from __future__ import annotations

import asyncio
import hashlib
import json
import stat as stat_mod
import tempfile
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Any, Final, TypeVar, cast

from scale_kg.adapters import IcebergPuppyGraphStore, InMemoryGraphStore
from scale_kg.adapters.base import GraphStore
from scale_kg.benchmark.generator import ScalePreset
from scale_kg.benchmark.golden_queries import FROZEN_GOLDEN_QUERIES, GoldenQuerySuite
from scale_kg.config import ScaleKGConfig
from scale_kg.core.models import TraversalResult
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


def _consume_future_exception(fut: asyncio.Future[Any]) -> None:
    """Retrieve exceptions on detached single-flight tasks to avoid GC warnings."""
    if not fut.cancelled():
        fut.exception()


class BackpressureError(Exception):
    """Raised when heavy benchmark queue reaches bounded capacity."""

    def __init__(self, message: str, *, retry_after_seconds: int = 5) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class _InFlightJob:
    """Detached background single-flight task with active waiter tracking."""

    __slots__ = ("released", "started", "task", "waiters")

    def __init__(self, task: asyncio.Task[object]) -> None:
        self.task = task
        self.waiters = 0
        self.started = False
        self.released = False


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
        self._inflight: dict[str, _InFlightJob] = {}
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

        job = self._inflight.get(key)
        coalesced = job is not None
        if job is None:
            if self._pending_count >= self._max_pending:
                raise BackpressureError(
                    "Showcase benchmark worker queue is at capacity; retry shortly.",
                    retry_after_seconds=self._retry_after,
                )

            job_ref: list[_InFlightJob] = []

            def _release() -> None:
                if not job_ref:
                    return
                current = job_ref[0]
                if current.released:
                    return
                current.released = True
                if self._inflight.get(key) is current:
                    self._inflight.pop(key, None)
                self._pending_count = max(0, self._pending_count - 1)

            async def _worker() -> object:
                current = job_ref[0]
                try:
                    async with self._semaphore:
                        if current.waiters == 0:
                            raise asyncio.CancelledError
                        current.started = True
                        value = await asyncio.to_thread(fn)
                    self._set_cached(key, value)
                    return value
                finally:
                    _release()

            def _on_done(done_task: asyncio.Future[object]) -> None:
                _consume_future_exception(done_task)
                _release()

            task: asyncio.Task[object] = asyncio.create_task(_worker())
            job = _InFlightJob(task)
            job_ref.append(job)
            task.add_done_callback(_on_done)
            self._inflight[key] = job
            self._pending_count += 1

        job.waiters += 1
        try:
            result = await asyncio.shield(job.task)
            return cast(T, result), False, coalesced
        finally:
            job.waiters -= 1
            if job.waiters == 0 and not job.started and not job.task.done():
                if self._inflight.get(key) is job:
                    self._inflight.pop(key, None)
                job.task.cancel()


def _run_benchmark_sync(req: RampRunRequest) -> PresetBenchmarkBundle:
    """Bridge async PresetBenchmarkBundle execution inside a worker thread."""
    return asyncio.run(
        run_preset_benchmark(
            ScalePreset(req.preset),
            adapters=tuple(sorted(set(req.adapters))),
            seed=req.seed,
        )
    )


def _load_or_build_snapshot_sync(
    snapshot_path: Path | None,
    config: ScaleKGConfig,
) -> tuple[bytes, str]:
    """Load pre-built snapshot JSON from disk or build a micro snapshot off-loop."""
    snap: ShowcaseSnapshot | None = None
    if snapshot_path is not None:
        try:
            if snapshot_path.is_file():
                raw = snapshot_path.read_bytes()
                snap = ShowcaseSnapshot.model_validate_json(raw)
        except FileNotFoundError:
            snap = None
    if snap is None:
        snap = asyncio.run(
            build_showcase_snapshot(
                presets=(ScalePreset.MICRO,),
                config=config,
            )
        )
    encoded = snap.model_dump_json().encode("utf-8")
    etag = f'"{hashlib.sha256(encoded).hexdigest()[:24]}"'
    return encoded, etag


def _init_golden_stores_sync() -> tuple[
    tempfile.TemporaryDirectory[str], dict[str, GraphStore]
]:
    """Create and seed read-only Golden Query stores off the event loop."""
    pg_dir = tempfile.TemporaryDirectory(prefix="scale_kg_showcase_pg_")
    try:
        iceberg_dir = Path(pg_dir.name) / "iceberg"
        iceberg_dir.mkdir(parents=True, exist_ok=True)
        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "iceberg_puppygraph": IcebergPuppyGraphStore(
                warehouse_path=iceberg_dir,
                cache_partitions=True,
            ),
        }
        asyncio.run(GoldenQuerySuite().load_dataset(stores))
    except BaseException:
        pg_dir.cleanup()
        raise
    return pg_dir, stores


def _read_history_sync(
    history_path: Path,
    cached_stamp: tuple[int, int] | None,
) -> tuple[bool, tuple[int, int] | None, list[CommitTrendEntry] | None]:
    """Stat and conditionally load rolling history.json off the event loop."""
    try:
        st = history_path.stat()
    except FileNotFoundError:
        return False, None, None
    if not stat_mod.S_ISREG(st.st_mode):
        return False, None, None
    stamp = (st.st_mtime_ns, st.st_size)
    if stamp == cached_stamp:
        return True, stamp, None
    try:
        raw_text = history_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return False, None, None
    data = json.loads(raw_text)
    entries = [CommitTrendEntry.model_validate(item) for item in data]
    return True, stamp, entries


class ShowcaseService:
    """Single-event-loop async-safe showcase backend engineered for 50+ concurrent dashboard users."""

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
        if light_concurrency < 1:
            raise ValueError(
                f"light_concurrency must be >= 1, got {light_concurrency}."
            )
        if light_timeout_seconds <= 0:
            raise ValueError(
                f"light_timeout_seconds must be > 0, got {light_timeout_seconds}."
            )

        self._snapshot_path = snapshot_path
        self._history_path = history_path
        self._config = config or ScaleKGConfig()
        self._light_sem = asyncio.Semaphore(light_concurrency)
        self._light_timeout = light_timeout_seconds
        self._job_runner = job_runner or SingleFlightJobRunner()

        self._init_lock = asyncio.Lock()
        self._iceberg_lock = threading.Lock()
        self._iceberg_inflight: dict[
            str, asyncio.Task[tuple[TraversalResult, float]]
        ] = {}
        self._snapshot_bytes: bytes | None = None
        self._snapshot_etag: str | None = None
        self._playground_dir: tempfile.TemporaryDirectory[str] | None = None
        self._golden_stores: dict[str, GraphStore] = {}
        self._history_cache: list[CommitTrendEntry] = []
        self._history_stamp: tuple[int, int] | None = None

    def close(self) -> None:
        """Release temporary playground resources and clear cached references cleanly."""
        if self._playground_dir is not None:
            self._playground_dir.cleanup()
            self._playground_dir = None
        self._golden_stores.clear()
        self._iceberg_inflight.clear()
        self._snapshot_bytes = None
        self._snapshot_etag = None
        self._history_cache.clear()
        self._history_stamp = None

    async def _ensure_initialized(self) -> None:
        if self._snapshot_bytes is not None and self._golden_stores:
            return
        async with self._init_lock:
            if self._snapshot_bytes is None:
                encoded, etag = await asyncio.to_thread(
                    _load_or_build_snapshot_sync,
                    self._snapshot_path,
                    self._config,
                )
                self._snapshot_bytes = encoded
                self._snapshot_etag = etag

            if not self._golden_stores:
                pg_dir, stores = await asyncio.to_thread(_init_golden_stores_sync)
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
        if self._history_path is None:
            self._history_cache = []
            self._history_stamp = None
            return []
        exists, stamp, entries = await asyncio.to_thread(
            _read_history_sync,
            self._history_path,
            self._history_stamp,
        )
        if not exists:
            self._history_cache = []
            self._history_stamp = None
            return []
        if entries is not None:
            self._history_cache = entries
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

        adapters = tuple(dict.fromkeys(req.adapters))

        def _run_iceberg_query_sync(
            iceberg_store: GraphStore,
        ) -> tuple[TraversalResult, float]:
            with self._iceberg_lock:
                started = time.perf_counter()
                if spec.requires_coordinator:
                    res_sync = asyncio.run(
                        QueryCoordinator(iceberg_store).traverse(spec.request)
                    )
                else:
                    res_sync = asyncio.run(iceberg_store.traverse_k_hop(spec.request))
                elapsed = round((time.perf_counter() - started) * 1000.0, 3)
                return res_sync, elapsed

        async def _run_iceberg_coalesced(
            iceberg_store: GraphStore,
        ) -> tuple[TraversalResult, float]:
            inflight_key = spec.query_id
            existing = self._iceberg_inflight.get(inflight_key)
            if existing is not None:
                return await asyncio.shield(existing)

            async def _task_coro() -> tuple[TraversalResult, float]:
                try:
                    return await asyncio.to_thread(
                        _run_iceberg_query_sync, iceberg_store
                    )
                finally:
                    if self._iceberg_inflight.get(inflight_key) is task:
                        self._iceberg_inflight.pop(inflight_key, None)

            task = asyncio.create_task(_task_coro())
            task.add_done_callback(_consume_future_exception)
            self._iceberg_inflight[inflight_key] = task
            return await asyncio.shield(task)

        async def _exec() -> AdapterSwitchResponse:
            items: list[AdapterSwitchItem] = []
            for name in adapters:
                store = self._golden_stores.get(name)
                if store is None:
                    raise ValueError(f"Unsupported adapter: {name!r}")
                if name == "iceberg_puppygraph" or isinstance(
                    store, IcebergPuppyGraphStore
                ):
                    res, elapsed_ms = await _run_iceberg_coalesced(store)
                else:
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
                asyncio.to_thread(lambda: asyncio.run(run_hybrid_rag_demo())),
                timeout=self._light_timeout,
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
