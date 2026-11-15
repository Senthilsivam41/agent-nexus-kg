"""Unit and integration tests for E4 generator, golden queries, workloads, and Gate 2 ADR."""

from __future__ import annotations

from pathlib import Path

import pytest

from scale_kg.adapters import (
    FaultMode,
    GraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    JanusGraphStore,
)
from scale_kg.benchmark import (
    FROZEN_GOLDEN_QUERIES,
    GATE2_WEIGHTS,
    SCALE_PRESETS,
    BenchmarkSuite,
    GoldenQuerySuite,
    PowerLawGraphGenerator,
    ScalePreset,
    compute_percentile,
    generate_gate2_adr,
    rank_engines,
    run_concurrent_ramp_workload,
    run_cost_extrapolation_workload,
    run_cross_partition_workload,
    run_ha_failure_workload,
    run_ingestion_workload,
    run_supernode_traversal_workload,
    run_traversal_latency_workload,
)
from scale_kg.config import LatencyBudgetConfig
from scale_kg.core.guardrails import (
    GuardrailEnforcer,
    GuardrailViolationCode,
    GuardrailViolationError,
)
from scale_kg.core.models import Domain, EntityNode


class TestPowerLawGraphGenerator:
    """Tests for synthetic power-law graph generation and scale presets."""

    def test_presets_cover_micro_local_cluster_and_scale_targets(self) -> None:
        assert SCALE_PRESETS[ScalePreset.MICRO].scale_label == "100M"
        assert SCALE_PRESETS[ScalePreset.LOCAL].scale_label == "1B"
        assert SCALE_PRESETS[ScalePreset.CLUSTER].scale_label == "10B"
        assert (
            SCALE_PRESETS[ScalePreset.MICRO].node_count
            < SCALE_PRESETS[ScalePreset.LOCAL].node_count
            < SCALE_PRESETS[ScalePreset.CLUSTER].node_count
        )

    def test_reproducible_power_law_with_deliberate_supernodes(self) -> None:
        d1 = PowerLawGraphGenerator(preset=ScalePreset.MICRO, seed=7).generate()
        d2 = PowerLawGraphGenerator(preset=ScalePreset.MICRO, seed=7).generate()

        assert d1.nodes == d2.nodes
        assert d1.edges == d2.edges
        assert "technology:lang:python" in d1.supernode_ids
        assert "customer:region:india" in d1.supernode_ids
        assert d1.stats.supernode_degrees["technology:lang:python"] >= 20
        assert d1.stats.supernode_degrees["customer:region:india"] >= 20
        assert d1.stats.max_degree > d1.stats.p95_degree
        assert set(d1.stats.nodes_by_domain.keys()) == {d.value for d in Domain}

    @pytest.mark.parametrize("share", [0.05, 0.15, 0.30])
    def test_cross_domain_edge_share_tracks_target(self, share: float) -> None:
        dataset = PowerLawGraphGenerator(
            preset=ScalePreset.MICRO,
            cross_domain_edge_share=share,
            seed=42,
        ).generate()
        assert abs(dataset.stats.realized_cross_domain_share - share) <= 0.08

    def test_generates_valid_core_edges_and_rejectable_derived_payloads(self) -> None:
        dataset = PowerLawGraphGenerator(preset=ScalePreset.MICRO).generate()
        enforcer = GuardrailEnforcer()

        for node in dataset.nodes:
            enforcer.validate_node_write(node)
        for edge in dataset.edges:
            enforcer.validate_edge_write(edge)

        assert len(dataset.invalid_derived_edges) >= 2
        for bad_edge in dataset.invalid_derived_edges:
            with pytest.raises(GuardrailViolationError) as exc:
                enforcer.validate_edge_write(bad_edge)
            assert (
                exc.value.violation.code
                == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE
            )

    def test_bucket_supernodes_option_materializes_16_buckets(self) -> None:
        dataset = PowerLawGraphGenerator(preset=ScalePreset.MICRO).generate(
            bucket_supernodes=True, degree_threshold=20
        )
        bucket_nodes = [n for n in dataset.nodes if n.is_bucket]
        assert len(bucket_nodes) >= 32  # At least 2 supernodes x 16 buckets


@pytest.mark.asyncio
class TestGoldenQuerySuite:
    """Tests for frozen Golden Query regression parity across adapters."""

    async def test_100_percent_parity_between_in_memory_and_iceberg(
        self, tmp_path: Path
    ) -> None:
        stores: dict[str, GraphStore] = {
            "in_memory": InMemoryGraphStore(),
            "iceberg_puppygraph": IcebergPuppyGraphStore(tmp_path / "golden_wh"),
        }
        suite = GoldenQuerySuite()
        assert len(FROZEN_GOLDEN_QUERIES) >= 7
        report = await suite.run_parity(stores)

        report.assert_parity()
        assert report.passed is True
        assert report.queries_run == len(FROZEN_GOLDEN_QUERIES)
        assert report.failed_queries == 0
        assert any(c.category == "derived_edge" for c in report.comparisons)
        assert any(
            c.adapter_results["in_memory"].bucket_hops_unwrapped > 0
            for c in report.comparisons
        )

    async def test_detects_parity_mismatch_when_adapter_diverges(
        self, tmp_path: Path
    ) -> None:
        s1 = InMemoryGraphStore()
        s2 = IcebergPuppyGraphStore(tmp_path / "divergent_wh")
        suite = GoldenQuerySuite()
        await suite.load_dataset({"s1": s1, "s2": s2})

        # Inject an extra entity + edge only into s1 to simulate divergence
        await s1.upsert_nodes(
            [
                EntityNode(
                    id="customer:org:rogue",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                )
            ]
        )
        from scale_kg.core.models import RelationshipEdge

        await s1.upsert_edges(
            [
                RelationshipEdge(
                    source_id="customer:org:acme",
                    target_id="customer:org:rogue",
                    edge_type="OWNS",
                    domain=Domain.CUSTOMER,
                    target_domain=Domain.CUSTOMER,
                )
            ]
        )
        report = await suite.run_parity({"s1": s1, "s2": s2}, load_data=False)
        assert report.passed is False
        assert report.failed_queries >= 1
        with pytest.raises(AssertionError):
            report.assert_parity()


@pytest.mark.asyncio
class TestSevenWorkloadsAndGate2ADR:
    """Tests for all 7 PoC workloads, weighted scoring, and ADR generation."""

    async def test_individual_workloads_and_percentile_helper(self) -> None:
        assert compute_percentile([10.0, 20.0, 30.0, 40.0], 50.0) == 25.0
        assert compute_percentile([], 95.0) == 0.0

        dataset = PowerLawGraphGenerator(preset=ScalePreset.MICRO, seed=11).generate(
            bucket_supernodes=True, degree_threshold=20
        )
        store = InMemoryGraphStore()

        ingest = await run_ingestion_workload(store, dataset)
        assert ingest.nodes_ingested == len(dataset.nodes)
        assert ingest.bulk_edges_ingested + ingest.stream_edges_ingested == len(
            dataset.edges
        )
        assert ingest.write_discipline_passed is True
        assert ingest.derived_to_core_ratio == 0.0

        lat = await run_traversal_latency_workload(
            store, dataset.seed_ids, samples_per_depth=6
        )
        assert set(lat.by_depth.keys()) == {1, 2, 3}
        assert lat.all_within_budget is True

        sup = await run_supernode_traversal_workload(
            store, dataset.supernode_ids, samples=6
        )
        assert sup.within_guardrails is True
        assert sup.raw_bucket_leaks == 0
        assert sup.total_bucket_hops_unwrapped > 0

        ramp = await run_concurrent_ramp_workload(
            store, dataset.seed_ids, concurrency_levels=[2, 4]
        )
        assert ramp.peak_qps > 0.0
        assert ramp.max_concurrency_within_budget == 4

        cross = await run_cross_partition_workload(
            InMemoryGraphStore,
            preset=ScalePreset.MICRO,
            shares=(0.05, 0.30),
            samples_per_share=4,
        )
        assert len(cross.by_share) == 2
        assert (
            cross.by_share[1].avg_cross_domain_hops
            >= cross.by_share[0].avg_cross_domain_hops
        )

        ha = await run_ha_failure_workload(
            store,
            dataset.seed_ids,
            fault_mode=FaultMode.TRANSIENT_ERROR,
            recover_after_calls=2,
            total_queries=6,
        )
        assert ha.failed_queries == 2
        assert ha.succeeded_queries == 4
        assert ha.recovered_cleanly is True

        cost = run_cost_extrapolation_workload(
            store, lat, base_edge_count=len(dataset.edges)
        )
        assert [s.scale_label for s in cost.scale_ladder] == ["100M", "1B", "10B"]
        assert cost.degradation_1b_to_10b_ratio >= 1.0

    async def test_full_benchmark_suite_scoring_and_gate2_adr(
        self, tmp_path: Path
    ) -> None:
        assert abs(sum(GATE2_WEIGHTS.values()) - 1.0) < 1e-9

        wh_counter = 0

        def _make_iceberg() -> GraphStore:
            nonlocal wh_counter
            wh_counter += 1
            return IcebergPuppyGraphStore(tmp_path / f"iceberg_bench_{wh_counter}")

        # Use local PyArrow-compatible SLA budget to verify Tier-1 production ranking
        # vs non-persistent reference (in_memory) and Tier-2 stub (janusgraph).
        local_budgets = LatencyBudgetConfig(
            hop_1_p95_ms=150.0,
            hop_2_p95_ms=600.0,
            hop_3_p95_ms=1500.0,
        )
        suite = BenchmarkSuite(preset=ScalePreset.MICRO, seed=42, budgets=local_budgets)
        result = await suite.run(
            {
                "in_memory": InMemoryGraphStore,
                "iceberg_puppygraph": _make_iceberg,
                "janusgraph": JanusGraphStore,
            }
        )

        assert result.golden_parity is not None
        result.golden_parity.assert_parity()
        assert set(result.adapters.keys()) == {
            "in_memory",
            "iceberg_puppygraph",
            "janusgraph",
        }

        cards = rank_engines(result, budgets=local_budgets)
        assert cards[0].adapter_name == "iceberg_puppygraph"
        assert cards[0].eligible_for_production is True
        in_mem_card = next(c for c in cards if c.adapter_name == "in_memory")
        janus_card = next(c for c in cards if c.adapter_name == "janusgraph")
        assert in_mem_card.eligible_for_production is False
        assert janus_card.eligible_for_production is False

        adr_path = tmp_path / "docs" / "decisions" / "ADR-0002-gate2.md"
        adr_md = generate_gate2_adr(result, output_path=adr_path, budgets=local_budgets)
        assert adr_path.exists()
        assert "ADR-0002" in adr_md
        assert "Performance | 35%" in adr_md
        assert "Scalability & Resilience | 25%" in adr_md
        assert "Cost per Billion Edges | 15%" in adr_md
        assert "Operability | 15%" in adr_md
        assert "GraphRAG Fit | 10%" in adr_md
        assert "`iceberg_puppygraph`" in adr_md
        assert "100M" in adr_md and "1B" in adr_md and "10B" in adr_md
