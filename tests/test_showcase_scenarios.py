"""Tests for executable showcase switch, load-ramp, guardrail, and RAG scenarios."""

from __future__ import annotations

import tempfile

import pytest

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.core.guardrails import GuardrailViolationCode
from scale_kg.showcase.scenarios import (
    build_showcase_snapshot,
    run_federation_demo,
    run_guardrail_probe,
    run_hot_path_demo,
    run_hybrid_rag_demo,
    run_preset_benchmark,
    run_supernode_ramp,
)


@pytest.mark.asyncio
async def test_supernode_ramp_switches_at_threshold_with_zero_leaks() -> None:
    res = await run_supernode_ramp(degrees=(8, 16, 32, 64), threshold=32)
    assert res.switch_triggered_at_degree == 32
    below = [p for p in res.points if p.degree < 32]
    above = [p for p in res.points if p.degree >= 32]
    assert all(not p.bucketed for p in below)
    assert all(p.bucketed and p.bucket_hops_unwrapped > 0 for p in above)
    assert all(p.raw_bucket_leaks == 0 for p in res.points)


@pytest.mark.asyncio
async def test_hot_path_demo_promotes_on_threshold_hit() -> None:
    res = await run_hot_path_demo(hits=4, threshold=3)
    assert res.promoted_at_hit == 3
    assert [s.materialized for s in res.steps] == [False, False, True, True]


@pytest.mark.asyncio
async def test_federation_demo_tracks_hop0_and_cross_domain_locality() -> None:
    res = await run_federation_demo(shares=(0.10, 0.40))
    assert res.parsed_domain == "customer"
    assert len(res.points) == 2
    assert res.points[1].ghost_proxies_created >= res.points[0].ghost_proxies_created
    assert all(0.0 <= p.single_domain_locality_ratio <= 1.0 for p in res.points)


@pytest.mark.asyncio
async def test_all_four_guardrail_probes_return_exact_codes() -> None:
    expected = {
        "forbidden_payload": (
            422,
            GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD,
            False,
        ),
        "missing_lakehouse_ref": (
            422,
            GuardrailViolationCode.LEAN_GRAPH_MISSING_LAKEHOUSE_REF,
            False,
        ),
        "preflight_depth_cap": (
            422,
            GuardrailViolationCode.MAX_DEPTH_EXCEEDED,
            False,
        ),
        "runtime_expansion_abort": (
            200,
            GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED,
            True,
        ),
    }
    for probe_id, (status, code, aborted) in expected.items():
        res = await run_guardrail_probe(probe_id)  # type: ignore[arg-type]
        assert res.http_status == status
        assert res.aborted is aborted
        assert res.violation is not None
        assert res.violation.code == code


@pytest.mark.asyncio
async def test_hybrid_rag_demo_measures_selective_hydration_savings() -> None:
    res = await run_hybrid_rag_demo()
    assert res.hydrated_documents < res.total_lakehouse_documents
    assert res.hydrated_bytes < res.naive_full_graph_bytes
    assert res.payload_savings_pct > 0.0
    assert len(res.stage_timings_ms) == 5
    assert res.citation_count >= 1


@pytest.mark.asyncio
async def test_build_showcase_snapshot_micro_succeeds() -> None:
    snap = await build_showcase_snapshot(presets=(ScalePreset.MICRO,))
    assert "micro" in snap.benchmarks
    assert snap.benchmarks["micro"].winner_adapter
    assert len(snap.scenarios.guardrail_probes) == 4


@pytest.mark.asyncio
async def test_showcase_snapshot_contains_no_host_paths_or_secrets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test-secret-do-not-leak-999")
    snap = await build_showcase_snapshot(presets=(ScalePreset.MICRO,))
    payload = snap.model_dump_json()
    assert "sk-test-secret-do-not-leak-999" not in payload
    assert tempfile.gettempdir() not in payload
    assert "/Users/" not in payload
    assert "/home/" not in payload
    assert "/var/folders/" not in payload
    assert "scale_kg_bench_" not in payload
    assert "scale_kg_showcase_lh_" not in payload


@pytest.mark.asyncio
async def test_run_preset_benchmark_returns_scorecards_and_adr() -> None:
    bundle = await run_preset_benchmark(ScalePreset.MICRO)
    assert bundle.preset == ScalePreset.MICRO
    assert len(bundle.scorecards) == 2
    assert bundle.winner_adapter in {"in_memory", "iceberg_puppygraph"}
    assert "ADR-" in bundle.adr_markdown
