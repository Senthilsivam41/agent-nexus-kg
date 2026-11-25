"""Tests for 50-concurrent-user scalability, single-flight coalescing, and backpressure."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import Callable
from pathlib import Path

import pytest

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.showcase.concurrency import (
    BackpressureError,
    ShowcaseService,
    SingleFlightJobRunner,
)
from scale_kg.showcase.scenarios import run_preset_benchmark
from scale_kg.showcase.snapshot import (
    AdapterSwitchRequest,
    CommitTrendEntry,
    GuardrailProbeRequest,
    HotPathRequest,
    RampRunRequest,
)


@pytest.mark.asyncio
async def test_single_flight_coalesces_50_concurrent_identical_runs() -> None:
    calls = 0
    bundle = await run_preset_benchmark(ScalePreset.MICRO, adapters=("in_memory",))

    def slow_job() -> object:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return bundle

    runner = SingleFlightJobRunner(max_concurrent=2, max_pending=8)
    results = await asyncio.gather(
        *[runner.run("micro:in_memory:42", slow_job) for _ in range(50)]
    )

    assert calls == 1
    assert len(results) == 50
    coalesced_count = sum(1 for _, _, coalesced in results if coalesced)
    assert coalesced_count == 49
    assert not any(cache_hit for _, cache_hit, _ in results)


@pytest.mark.asyncio
async def test_single_flight_raises_backpressure_when_queue_saturated() -> None:
    bundle = await run_preset_benchmark(ScalePreset.MICRO, adapters=("in_memory",))

    def slow_job() -> object:
        time.sleep(0.15)
        return bundle

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=1)
    tasks = [
        asyncio.create_task(runner.run(f"key-{idx}", slow_job)) for idx in range(5)
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    backpressure = [o for o in outcomes if isinstance(o, BackpressureError)]
    assert len(backpressure) >= 1
    assert backpressure[0].retry_after_seconds >= 1
    assert runner._pending_count == 0
    assert not runner._inflight


@pytest.mark.asyncio
async def test_single_flight_ttl_cache_hit_and_expiry() -> None:
    calls = 0

    def job() -> int:
        nonlocal calls
        calls += 1
        return calls

    runner = SingleFlightJobRunner(
        max_concurrent=2,
        max_pending=4,
        cache_ttl_seconds=0.05,
        max_cache_entries=2,
    )
    val1, hit1, coal1 = await runner.run("k1", job)
    assert (val1, hit1, coal1) == (1, False, False)

    val2, hit2, coal2 = await runner.run("k1", job)
    assert (val2, hit2, coal2) == (1, True, False)
    assert calls == 1

    await asyncio.sleep(0.06)
    val3, hit3, coal3 = await runner.run("k1", job)
    assert (val3, hit3, coal3) == (2, False, False)
    assert calls == 2


@pytest.mark.asyncio
async def test_single_flight_follower_cancel_preserves_leader_and_cleans_state() -> (
    None
):
    started = threading.Event()
    release = threading.Event()

    def blocked_job() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "ok"

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=2)
    leader = asyncio.create_task(runner.run("shared", blocked_job))
    await asyncio.to_thread(started.wait, 2.0)

    follower1 = asyncio.create_task(runner.run("shared", blocked_job))
    follower2 = asyncio.create_task(runner.run("shared", blocked_job))
    await asyncio.sleep(0.01)

    follower1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await follower1

    release.set()
    leader_res, follower2_res = await asyncio.gather(leader, follower2)
    assert leader_res == ("ok", False, False)
    assert follower2_res == ("ok", False, True)
    assert runner._pending_count == 0
    assert not runner._inflight


@pytest.mark.asyncio
async def test_single_flight_leader_cancel_preserves_coalesced_follower() -> None:
    started = threading.Event()
    release = threading.Event()
    calls = 0

    def blocked_job() -> str:
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=2.0)
        return "done"

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=2)
    leader = asyncio.create_task(runner.run("shared-cancel", blocked_job))
    await asyncio.to_thread(started.wait, 2.0)

    follower = asyncio.create_task(runner.run("shared-cancel", blocked_job))
    await asyncio.sleep(0.01)

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader

    # Coalesced follower must still succeed and fn must execute exactly once.
    release.set()
    follower_res = await follower
    assert follower_res == ("done", False, True)
    assert calls == 1
    assert runner._pending_count == 0
    assert not runner._inflight


@pytest.mark.asyncio
async def test_single_flight_cancel_while_running_holds_capacity_until_worker_exits() -> (
    None
):
    started = threading.Event()
    release = threading.Event()

    def blocked_job() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "finished"

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=1)
    leader = asyncio.create_task(runner.run("active-key", blocked_job))
    await asyncio.to_thread(started.wait, 2.0)

    inflight_entry = runner._inflight["active-key"]
    assert inflight_entry.started is True

    leader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leader

    # Worker thread is still active: pending slot and semaphore remain held.
    assert "active-key" in runner._inflight
    assert runner._pending_count == 1
    with pytest.raises(BackpressureError):
        await runner.run("other-key", lambda: "rejected")

    # Once worker thread finishes, detached task cleans up inflight + pending cleanly.
    release.set()
    assert await asyncio.wait_for(asyncio.shield(inflight_entry.task), timeout=2.0) == (
        "finished"
    )
    assert runner._pending_count == 0
    assert not runner._inflight

    # Capacity is now restored and a new key succeeds cleanly.
    val, hit, coalesced = await runner.run("other-key", lambda: "recovered")
    assert (val, hit, coalesced) == ("recovered", False, False)
    assert runner._pending_count == 0
    assert not runner._inflight


@pytest.mark.asyncio
async def test_single_flight_cancel_while_queued_skips_worker_execution() -> None:
    started = threading.Event()
    release = threading.Event()
    queued_calls = 0

    def blocker_job() -> str:
        started.set()
        release.wait(timeout=2.0)
        return "blocker"

    def queued_job() -> str:
        nonlocal queued_calls
        queued_calls += 1
        return "should-not-run"

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=2)
    blocker = asyncio.create_task(runner.run("blocker", blocker_job))
    await asyncio.to_thread(started.wait, 2.0)

    queued = asyncio.create_task(runner.run("queued", queued_job))
    await asyncio.sleep(0.01)
    assert "queued" in runner._inflight
    assert runner._inflight["queued"].started is False

    queued.cancel()
    with pytest.raises(asyncio.CancelledError):
        await queued
    await asyncio.sleep(0.01)

    release.set()
    assert await blocker == ("blocker", False, False)
    assert queued_calls == 0
    assert runner._pending_count == 0
    assert not runner._inflight


@pytest.mark.asyncio
async def test_single_flight_lru_eviction_and_zero_ttl_bypass() -> None:
    calls: dict[str, int] = {}

    def make_job(key: str) -> Callable[[], str]:
        def _fn() -> str:
            calls[key] = calls.get(key, 0) + 1
            return f"{key}-{calls[key]}"

        return _fn

    runner = SingleFlightJobRunner(
        max_concurrent=2,
        max_pending=4,
        cache_ttl_seconds=60.0,
        max_cache_entries=2,
    )
    v1, hit1, _ = await runner.run("k1", make_job("k1"))
    v2, hit2, _ = await runner.run("k2", make_job("k2"))
    assert (v1, hit1, v2, hit2) == ("k1-1", False, "k2-1", False)

    # Touch k1 so k2 becomes the least-recently-used entry.
    v1_hit, hit1_again, _ = await runner.run("k1", make_job("k1"))
    assert (v1_hit, hit1_again) == ("k1-1", True)

    # Inserting k3 must evict k2 while keeping k1 cached.
    v3, hit3, _ = await runner.run("k3", make_job("k3"))
    assert (v3, hit3) == ("k3-1", False)

    v1_still, hit1_still, _ = await runner.run("k1", make_job("k1"))
    assert (v1_still, hit1_still) == ("k1-1", True)

    v2_recomputed, hit2_recomputed, _ = await runner.run("k2", make_job("k2"))
    assert (v2_recomputed, hit2_recomputed) == ("k2-2", False)

    # cache_ttl_seconds=0 disables caching completely.
    no_cache_runner = SingleFlightJobRunner(
        max_concurrent=2,
        max_pending=4,
        cache_ttl_seconds=0.0,
    )
    first, hit_a, _ = await no_cache_runner.run("nocache", make_job("nc"))
    second, hit_b, _ = await no_cache_runner.run("nocache", make_job("nc"))
    assert (first, hit_a) == ("nc-1", False)
    assert (second, hit_b) == ("nc-2", False)


@pytest.mark.asyncio
async def test_showcase_service_serves_50_concurrent_users_under_sla() -> None:
    service = ShowcaseService()
    try:
        # Warm once before timing read SLA
        await service.get_snapshot_bytes()

        async def user_session(idx: int) -> float:
            started = time.perf_counter()
            raw, etag = await service.get_snapshot_bytes()
            assert raw and etag
            if idx % 3 == 0:
                sw = await service.simulate_adapter_switch(
                    AdapterSwitchRequest(query_id="GQ-03")
                )
                assert sw.parity_matched
            elif idx % 3 == 1:
                gp = await service.simulate_guardrail(
                    GuardrailProbeRequest(probe_id="forbidden_payload")
                )
                assert gp.blocked
            else:
                hp = await service.simulate_hot_path(
                    HotPathRequest(hits=3, threshold=3)
                )
                assert hp.promoted_at_hit == 3
            return (time.perf_counter() - started) * 1000.0

        latencies = await asyncio.gather(*[user_session(i) for i in range(50)])
        assert len(latencies) == 50
        ordered = sorted(latencies)
        p95_ms = ordered[int(0.95 * (len(ordered) - 1))]
        assert p95_ms < 250.0
    finally:
        service.close()


@pytest.mark.asyncio
async def test_showcase_service_run_ramp_coalesces_concurrent_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = await run_preset_benchmark(ScalePreset.MICRO, adapters=("in_memory",))
    calls = 0

    def fake_run_sync(req: RampRunRequest) -> object:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return bundle

    monkeypatch.setattr(
        "scale_kg.showcase.concurrency._run_benchmark_sync", fake_run_sync
    )

    service = ShowcaseService()
    try:
        req = RampRunRequest(preset="micro", adapters=["in_memory"], seed=42)
        responses = await asyncio.gather(*[service.run_ramp(req) for _ in range(10)])

        assert calls == 1
        assert len(responses) == 10
        assert sum(1 for r in responses if r.coalesced) == 9
        assert sum(1 for r in responses if not r.coalesced and not r.cache_hit) == 1

        cached_resp = await service.run_ramp(req)
        assert cached_resp.cache_hit is True
        assert cached_resp.coalesced is False
        assert calls == 1
    finally:
        service.close()


@pytest.mark.asyncio
async def test_showcase_service_get_history_missing_loads_and_reloads(
    tmp_path: Path,
) -> None:
    # No path configured -> empty list
    no_path_service = ShowcaseService()
    assert await no_path_service.get_history() == []

    # Configured path does not exist yet -> empty list
    history_file = tmp_path / "history.json"
    service = ShowcaseService(history_path=history_file)
    try:
        assert await service.get_history() == []

        entry1 = CommitTrendEntry(
            git_sha="abc123456789",
            git_ref="main",
            built_at="2026-11-25T00:00:00Z",
            config_fingerprint="fp0001",
            preset="micro",
            hop_1_p95_ms=1.2,
            hop_2_p95_ms=2.4,
            hop_3_p95_ms=4.8,
            peak_qps=500.0,
            winner_adapter="in_memory",
            winner_score=95.0,
            golden_parity_passed=True,
        )
        history_file.write_text(
            json.dumps([entry1.model_dump(mode="json")]),
            encoding="utf-8",
        )
        loaded = await service.get_history()
        assert len(loaded) == 1
        assert loaded[0].git_sha == "abc123456789"

        # Mutating returned list must not affect internal cache
        loaded.clear()
        assert len(await service.get_history()) == 1

        # Update file with a second entry and ensure reload picks it up
        entry2 = entry1.model_copy(
            update={"git_sha": "def987654321", "peak_qps": 610.5}
        )
        history_file.write_text(
            json.dumps(
                [entry1.model_dump(mode="json"), entry2.model_dump(mode="json")]
            ),
            encoding="utf-8",
        )
        reloaded = await service.get_history()
        assert [e.git_sha for e in reloaded] == ["abc123456789", "def987654321"]
        assert reloaded[1].peak_qps == 610.5

        # Deleting file resets to empty list
        history_file.unlink()
        assert await service.get_history() == []
    finally:
        service.close()


@pytest.mark.asyncio
async def test_showcase_service_hybrid_coordinator_parity_and_close() -> None:
    service = ShowcaseService()
    try:
        hybrid = await service.simulate_hybrid()
        assert hybrid.citation_count >= 1
        assert hybrid.payload_savings_pct > 0.0

        # GQ-06 requires QueryCoordinator (derived edge); repeated calls must keep parity
        sw_results = await asyncio.gather(
            *[
                service.simulate_adapter_switch(AdapterSwitchRequest(query_id="GQ-06"))
                for _ in range(5)
            ]
        )
        assert all(sw.parity_matched for sw in sw_results)
        assert service._playground_dir is not None
        pg_path = Path(service._playground_dir.name)
        assert pg_path.exists()
    finally:
        service.close()

    assert service._playground_dir is None
    assert not pg_path.exists()
    # Idempotent close
    service.close()


@pytest.mark.asyncio
async def test_single_flight_exception_does_not_cache_or_leak() -> None:
    attempts = 0

    def flaky_job() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient failure")
        return "ok"

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=2)
    with pytest.raises(RuntimeError, match="transient failure"):
        await runner.run("flaky", flaky_job)

    assert not runner._inflight
    assert runner._pending_count == 0

    val, hit, coalesced = await runner.run("flaky", flaky_job)
    assert (val, hit, coalesced) == ("ok", False, False)
    assert attempts == 2


@pytest.mark.asyncio
async def test_showcase_service_loads_snapshot_from_path_with_stable_etag(
    tmp_path: Path,
) -> None:
    # Build once via default service, persist to disk, and verify fast file-backed load.
    builder = ShowcaseService()
    try:
        raw_built, etag_built = await builder.get_snapshot_bytes()
    finally:
        builder.close()

    snap_file = tmp_path / "snapshot.json"
    snap_file.write_bytes(raw_built)

    file_service = ShowcaseService(snapshot_path=snap_file)
    try:
        raw1, etag1 = await file_service.get_snapshot_bytes()
        raw2, etag2 = await file_service.get_snapshot_bytes()
        assert raw1 == raw2 == raw_built
        assert etag1 == etag2 == etag_built
    finally:
        file_service.close()


@pytest.mark.asyncio
async def test_showcase_service_light_validation_and_timeout_enforcement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="light_concurrency"):
        ShowcaseService(light_concurrency=0)
    with pytest.raises(ValueError, match="light_timeout_seconds"):
        ShowcaseService(light_timeout_seconds=0.0)
    with pytest.raises(ValueError, match="light_timeout_seconds"):
        ShowcaseService(light_timeout_seconds=-1.0)

    async def slow_guardrail(probe_id: str) -> object:
        await asyncio.sleep(0.2)
        return probe_id

    monkeypatch.setattr(
        "scale_kg.showcase.concurrency.run_guardrail_probe",
        slow_guardrail,
    )

    service = ShowcaseService(light_timeout_seconds=0.02)
    try:
        with pytest.raises(TimeoutError):
            await service.simulate_guardrail(
                GuardrailProbeRequest(probe_id="forbidden_payload")
            )
    finally:
        service.close()


@pytest.mark.asyncio
async def test_showcase_service_run_ramp_normalizes_adapters_and_propagates_backpressure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle = await run_preset_benchmark(ScalePreset.MICRO, adapters=("in_memory",))
    calls = 0

    def fake_run_sync(req: RampRunRequest) -> object:
        nonlocal calls
        calls += 1
        return bundle

    monkeypatch.setattr(
        "scale_kg.showcase.concurrency._run_benchmark_sync", fake_run_sync
    )

    service = ShowcaseService(
        job_runner=SingleFlightJobRunner(max_concurrent=1, max_pending=1)
    )
    try:
        first = await service.run_ramp(
            RampRunRequest(
                preset="micro",
                adapters=["in_memory", "iceberg_puppygraph"],
                seed=42,
            )
        )
        assert first.cache_hit is False
        assert first.coalesced is False
        assert calls == 1

        # Reversed adapter order must normalize to the same cache key.
        second = await service.run_ramp(
            RampRunRequest(
                preset="micro",
                adapters=["iceberg_puppygraph", "in_memory"],
                seed=42,
            )
        )
        assert second.cache_hit is True
        assert second.coalesced is False
        assert calls == 1
    finally:
        service.close()

    # BackpressureError from SingleFlightJobRunner must propagate cleanly.
    started = threading.Event()
    release = threading.Event()

    def blocked_run_sync(req: RampRunRequest) -> object:
        started.set()
        release.wait(timeout=2.0)
        return bundle

    monkeypatch.setattr(
        "scale_kg.showcase.concurrency._run_benchmark_sync", blocked_run_sync
    )

    bp_service = ShowcaseService(
        job_runner=SingleFlightJobRunner(
            max_concurrent=1, max_pending=1, retry_after_seconds=7
        )
    )
    try:
        holder = asyncio.create_task(
            bp_service.run_ramp(
                RampRunRequest(preset="micro", adapters=["in_memory"], seed=1)
            )
        )
        await asyncio.to_thread(started.wait, 2.0)

        with pytest.raises(BackpressureError) as exc_info:
            await bp_service.run_ramp(
                RampRunRequest(preset="micro", adapters=["in_memory"], seed=2)
            )
        assert exc_info.value.retry_after_seconds == 7

        release.set()
        await holder
    finally:
        bp_service.close()


@pytest.mark.asyncio
async def test_showcase_service_get_history_handles_missing_and_deleted_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_file = tmp_path / "does_not_exist.json"
    service = ShowcaseService(history_path=missing_file)
    try:
        assert await service.get_history() == []

        # Simulate file disappearing between stat() and read_text().
        vanishing_file = tmp_path / "vanishing.json"
        vanishing_file.write_text("[]", encoding="utf-8")
        service._history_path = vanishing_file

        original_read_text = Path.read_text

        def vanished_read_text(
            self: Path,
            encoding: str | None = None,
            errors: str | None = None,
            newline: str | None = None,
        ) -> str:
            if self == vanishing_file:
                raise FileNotFoundError(self.name)
            return original_read_text(
                self, encoding=encoding, errors=errors, newline=newline
            )

        monkeypatch.setattr(Path, "read_text", vanished_read_text)
        assert await service.get_history() == []
        assert service._history_stamp is None
        assert service._history_cache == []
    finally:
        service.close()


@pytest.mark.asyncio
async def test_showcase_service_adapter_switch_deduplicates_preserving_order() -> None:
    service = ShowcaseService()
    try:
        dedup_resp = await service.simulate_adapter_switch(
            AdapterSwitchRequest(
                query_id="GQ-03",
                adapters=["in_memory", "in_memory"],
            )
        )
        assert [item.adapter for item in dedup_resp.results] == ["in_memory"]
        assert dedup_resp.parity_matched is True

        ordered_resp = await service.simulate_adapter_switch(
            AdapterSwitchRequest(
                query_id="GQ-03",
                adapters=["iceberg_puppygraph", "in_memory"],
            )
        )
        assert [item.adapter for item in ordered_resp.results] == [
            "iceberg_puppygraph",
            "in_memory",
        ]
        assert ordered_resp.parity_matched is True
    finally:
        service.close()
