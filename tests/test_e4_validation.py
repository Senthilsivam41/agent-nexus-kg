"""Comprehensive E4 validation suite covering SEN-64, SEN-76, SEN-77, and SEN-78."""

from __future__ import annotations

from pathlib import Path

import pytest
from test_adapters_and_golden_queries import (
    _HugeGraphMockTransport,
    _Neo4jMockDriver,
)

from scale_kg.adapters import (
    GraphStore,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    Neo4jGraphStore,
)
from scale_kg.benchmark import (
    BenchmarkSuite,
    GoldenQuerySuite,
    PowerLawGraphGenerator,
    ScalePreset,
    generate_gate2_adr,
    rank_engines,
)
from scale_kg.core.guardrails import GuardrailViolationCode, GuardrailViolationError


class TestE41GeneratorAndPresets:
    """Acceptance validation for SEN-76 ([E4.1])."""

    def test_presets_power_law_supernodes_and_write_discipline(self) -> None:
        for preset in (ScalePreset.MICRO, ScalePreset.LOCAL):
            gen = PowerLawGraphGenerator(
                preset=preset,
                cross_domain_edge_share=0.15,
                seed=99,
            )
            ds = gen.generate()
            assert "technology:lang:python" in ds.supernode_ids
            assert "customer:region:india" in ds.supernode_ids
            assert ds.stats.max_degree >= gen.spec.supernode_fanout
            assert ds.stats.invalid_derived_edge_count >= 2


@pytest.mark.asyncio
class TestE42GoldenRegressionSuite:
    """Acceptance validation for SEN-77 ([E4.2])."""

    async def test_multi_engine_golden_parity_across_tier1_adapters(
        self, tmp_path: Path
    ) -> None:
        bucketed_stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "iceberg_puppygraph": IcebergPuppyGraphStore(tmp_path / "e4_iceberg"),
            "hugegraph": HugeGraphStore(
                base_url="http://hugegraph.mock",
                query_mode="rest",
                transport=_HugeGraphMockTransport(),
            ),
        }
        bucketed_report = await GoldenQuerySuite(bucket_supernodes=True).run_parity(
            bucketed_stores
        )
        bucketed_report.assert_parity()
        assert len(bucketed_report.adapters_tested) == 3
        assert bucketed_report.queries_run == 7

        all_tier1_stores: dict[str, GraphStore] = {
            **bucketed_stores,
            "neo4j": Neo4jGraphStore(driver=_Neo4jMockDriver()),
        }
        tier1_report = await GoldenQuerySuite(bucket_supernodes=False).run_parity(
            all_tier1_stores
        )
        tier1_report.assert_parity()
        assert len(tier1_report.adapters_tested) == 4
        assert tier1_report.queries_run == 7


@pytest.mark.asyncio
class TestE43SevenWorkloadBenchmarkAndGate2ADR:
    """Acceptance validation for SEN-64 and SEN-78 ([E4.3])."""

    async def test_runs_7_workloads_and_selects_gate2_winner(
        self, tmp_path: Path
    ) -> None:
        idx = 0

        def _make_iceberg() -> GraphStore:
            nonlocal idx
            idx += 1
            return IcebergPuppyGraphStore(tmp_path / f"e4_bench_iceberg_{idx}")

        def _make_hugegraph() -> GraphStore:
            return HugeGraphStore(
                base_url="http://hugegraph.mock",
                query_mode="rest",
                transport=_HugeGraphMockTransport(),
            )

        suite = BenchmarkSuite(preset=ScalePreset.MICRO, seed=42)
        res = await suite.run(
            {
                "in_memory": InMemoryGraphStore,
                "iceberg_puppygraph": _make_iceberg,
                "hugegraph": _make_hugegraph,
            }
        )

        # Verify write discipline rejection across real engines
        ds = PowerLawGraphGenerator(preset=ScalePreset.MICRO).generate()
        store = _make_hugegraph()
        with pytest.raises(GuardrailViolationError) as exc:
            await store.upsert_edges(ds.invalid_derived_edges)
        assert (
            exc.value.violation.code
            == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE
        )

        cards = rank_engines(res)
        eligible = [c for c in cards if c.eligible_for_production]
        assert eligible
        assert cards[0].adapter_name == "hugegraph"
        iceberg_card = next(c for c in cards if c.adapter_name == "iceberg_puppygraph")
        assert iceberg_card.tier == "tier1"
        assert iceberg_card.is_stub is False
        adr = generate_gate2_adr(res)
        assert f"Primary Selected Engine:** `{cards[0].adapter_name}`" in adr
        assert "100M" in adr and "1B" in adr and "10B" in adr
