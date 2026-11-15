"""Gate 2 weighted scoring matrix and automated Architecture Decision Record (ADR) generator."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pydantic import BaseModel, Field

from scale_kg.benchmark.workloads import AdapterBenchmarkResult, BenchmarkSuiteResult
from scale_kg.config import LatencyBudgetConfig

GATE2_WEIGHTS: dict[str, float] = {
    "performance": 0.35,
    "scalability_resilience": 0.25,
    "cost_per_billion_edges": 0.15,
    "operability": 0.15,
    "graphrag_fit": 0.10,
}


class EngineScorecard(BaseModel):
    """Normalized 0-100 Gate 2 weighted scorecard for one GraphStore engine."""

    engine: str
    adapter_name: str
    tier: str
    is_stub: bool
    performance_score: float
    scalability_resilience_score: float
    cost_score: float
    operability_score: float
    graphrag_fit_score: float
    weighted_total: float
    meets_latency_gate: bool
    eligible_for_production: bool
    disqualified_reasons: list[str] = Field(default_factory=list)


def _clamp_score(val: float) -> float:
    return round(max(0.0, min(100.0, val)), 2)


def score_adapter(
    result: AdapterBenchmarkResult,
    *,
    budgets: LatencyBudgetConfig | None = None,
) -> EngineScorecard:
    """Compute Gate 2 weighted scorecard (0-100) from empirical 7-workload metrics."""
    sla = budgets or LatencyBudgetConfig()
    profile = result.cost_extrapolation.cost_profile

    # 1. Performance (35%): 1/2/3-hop P95 budget margin + supernode P99 + peak QPS.
    depth_ratios: list[float] = []
    for depth in (1, 2, 3):
        stat = result.traversal_latency.by_depth.get(depth)
        if stat is not None:
            budget = sla.budget_for_depth(depth)
            ratio = stat.p95_ms / max(0.001, budget)
            depth_ratios.append(max(0.0, 100.0 - ratio * 45.0))
    avg_latency_score = sum(depth_ratios) / max(1, len(depth_ratios))
    supernode_bonus = 10.0 if result.supernode.within_guardrails else -25.0
    qps_bonus = min(10.0, result.concurrent_ramp.peak_qps / 50.0)
    performance_score = _clamp_score(avg_latency_score + supernode_bonus + qps_bonus)

    # 2. Scalability & Resilience (25%): cross-shard edges, HA recovery, 1B->10B degradation.
    resilience = 60.0
    if profile.supports_cross_shard_edges:
        resilience += 15.0
    if result.ha_failure.recovered_cleanly:
        resilience += 15.0
    else:
        resilience -= 30.0
    if result.cost_extrapolation.degradation_1b_to_10b_ratio <= 1.25:
        resilience += 10.0
    elif result.cost_extrapolation.degradation_1b_to_10b_ratio > 1.8:
        resilience -= 15.0
    if result.cross_partition.worst_latency_ratio > 3.0:
        resilience -= 10.0
    scalability_score = _clamp_score(resilience)

    # 3. Cost per Billion Edges (15%): lower monthly USD/1B edges scores higher.
    cost_usd = result.cost_extrapolation.cost_per_billion_edges_usd_month
    if cost_usd <= 0.0:
        cost_score = 100.0
    else:
        # Scale cleanly across $400/mo (~90) to $2500/mo (~37)
        cost_score = _clamp_score(100.0 - (cost_usd / 40.0))

    # 4. Operability (15%): operational complexity + zero-ETL simplicity.
    op_base = {"low": 90.0, "medium": 72.0, "high": 45.0}[
        profile.operational_complexity
    ]
    if profile.supports_zero_etl:
        op_base += 8.0
    operability_score = _clamp_score(op_base)

    # 5. GraphRAG Fit (10%): Golden parity + write discipline + query languages + zero-ETL.
    fit = 55.0
    if result.golden_parity_passed:
        fit += 20.0
    if result.ingestion.write_discipline_passed:
        fit += 10.0
    if any(
        lang in {"cypher", "opencypher", "gremlin"} for lang in profile.query_languages
    ):
        fit += 10.0
    if profile.supports_zero_etl:
        fit += 5.0
    graphrag_fit_score = _clamp_score(fit)

    weighted_total = round(
        performance_score * GATE2_WEIGHTS["performance"]
        + scalability_score * GATE2_WEIGHTS["scalability_resilience"]
        + cost_score * GATE2_WEIGHTS["cost_per_billion_edges"]
        + operability_score * GATE2_WEIGHTS["operability"]
        + graphrag_fit_score * GATE2_WEIGHTS["graphrag_fit"],
        2,
    )

    reasons: list[str] = []
    if profile.tier == "reference":
        reasons.append("In-memory reference baseline is non-persistent.")
    if profile.is_stub:
        reasons.append("Tier-2 contract stub (requires live cluster deployment).")
    if not result.traversal_latency.all_within_budget:
        reasons.append("Missed 1/2/3-hop Gate 2 P95 latency budget.")
    if not result.ha_failure.recovered_cleanly:
        reasons.append("Failed HA fault-recovery validation.")
    if not result.ingestion.write_discipline_passed:
        reasons.append("Failed Lean Graph unmaterialized derived-edge rejection check.")

    eligible = profile.tier != "reference" and not profile.is_stub and not reasons

    return EngineScorecard(
        engine=profile.engine,
        adapter_name=result.adapter_name,
        tier=profile.tier,
        is_stub=profile.is_stub,
        performance_score=performance_score,
        scalability_resilience_score=scalability_score,
        cost_score=cost_score,
        operability_score=operability_score,
        graphrag_fit_score=graphrag_fit_score,
        weighted_total=weighted_total,
        meets_latency_gate=result.traversal_latency.all_within_budget,
        eligible_for_production=eligible,
        disqualified_reasons=reasons,
    )


def rank_engines(
    suite_result: BenchmarkSuiteResult,
    *,
    budgets: LatencyBudgetConfig | None = None,
) -> list[EngineScorecard]:
    """Score and rank all benchmarked adapters (production-eligible first, then score)."""
    cards = [
        score_adapter(res, budgets=budgets) for res in suite_result.adapters.values()
    ]
    cards.sort(
        key=lambda c: (
            not c.eligible_for_production,
            -c.weighted_total,
            c.adapter_name,
        )
    )
    return cards


def generate_gate2_adr(
    suite_result: BenchmarkSuiteResult,
    *,
    output_path: str | Path | None = None,
    adr_number: int = 2,
    decision_date: str | None = None,
    budgets: LatencyBudgetConfig | None = None,
) -> str:
    """Render a complete Gate 2 Markdown ADR with empirical tables and recommendation."""
    sla = budgets or LatencyBudgetConfig()
    scorecards = rank_engines(suite_result, budgets=sla)
    date_str = decision_date or datetime.now(UTC).strftime("%Y-%m-%d")

    eligible_cards = [c for c in scorecards if c.eligible_for_production]
    winner = eligible_cards[0] if eligible_cards else scorecards[0]
    fallback = (
        eligible_cards[1]
        if len(eligible_cards) > 1
        else (scorecards[1] if len(scorecards) > 1 else None)
    )

    parity_summary = "Not run"
    if suite_result.golden_parity is not None:
        gp = suite_result.golden_parity
        parity_status = "PASS" if gp.passed else "FAIL"
        parity_summary = (
            f"{parity_status} ({gp.passed_queries}/{gp.queries_run} queries matched across "
            f"{', '.join(gp.adapters_tested)})"
        )

    lines: list[str] = [
        f"# ADR-{adr_number:04d}: Gate 2 Engine Selection for 10B+ Edge Semantic Knowledge Graph",
        "",
        f"**Date:** {date_str}  ",
        "**Status:** Proposed (Evidence-Based Gate 2 Recommendation)  ",
        f"**Benchmark Preset:** `{suite_result.preset.value}`  ",
        f"**Golden Query Parity:** {parity_summary}",
        "",
        "---",
        "",
        "## 1. Context",
        "",
        (
            "Agent Nexus KG (`scale_kg`) requires an engine-agnostic `GraphStore` backend capable of scaling from 100M to 10B+ relationships while preserving 1-to-3 hop GraphRAG latency budgets (1-hop P95 <= "
            f"{sla.hop_1_p95_ms:.0f} ms, 2-hop P95 <= {sla.hop_2_p95_ms:.0f} ms, 3-hop P95 <= {sla.hop_3_p95_ms:.0f} ms) and strict query guardrails."
        ),
        "",
        "## 2. Gate 2 Weighted Decision Criteria",
        "",
        "| Criterion | Weight | Measured Signals |",
        "| --- | ---: | --- |",
        "| Performance | 35% | 1/2/3-hop P95 latency vs SLA, supernode P99, peak concurrent QPS |",
        "| Scalability & Resilience | 25% | Cross-shard support, HA recovery time, 1B->10B latency ratio |",
        "| Cost per Billion Edges | 15% | Monthly infrastructure cost per 1B edges (USD) |",
        "| Operability | 15% | Operational complexity, cluster footprint, Zero-ETL capability |",
        "| GraphRAG Fit | 10% | Golden Query parity, Lean Graph write discipline, Cypher/Gremlin support |",
        "",
        "## 3. Empirical 7-Workload Benchmark Comparison",
        "",
        "| Adapter | 1-Hop P95 (ms) | 2-Hop P95 (ms) | 3-Hop P95 (ms) | Supernode P99 (ms) | Peak QPS | Bulk Ingest (edges/s) | Cross-Part Ratio | HA Recovery (ms) | Cost / 1B (USD/mo) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for name, res in suite_result.adapters.items():
        d1 = res.traversal_latency.by_depth[1].p95_ms
        d2 = res.traversal_latency.by_depth[2].p95_ms
        d3 = res.traversal_latency.by_depth[3].p95_ms
        cost_1b = res.cost_extrapolation.cost_per_billion_edges_usd_month
        lines.append(
            f"| `{name}` | {d1:.2f} | {d2:.2f} | {d3:.2f} | "
            f"{res.supernode.p99_ms:.2f} | {res.concurrent_ramp.peak_qps:.1f} | "
            f"{res.ingestion.bulk_edges_per_sec:.0f} | {res.cross_partition.worst_latency_ratio:.2f}x | "
            f"{res.ha_failure.recovery_time_ms:.2f} | ${cost_1b:,.0f} |"
        )

    lines.extend(
        [
            "",
            "## 4. Weighted Scoring Matrix",
            "",
            "| Rank | Adapter | Tier | Perf (35%) | Scale/HA (25%) | Cost (15%) | Ops (15%) | GraphRAG (10%) | Weighted Total | Eligible |",
            "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for idx, card in enumerate(scorecards, start=1):
        tier_label = f"{card.tier}{' (stub)' if card.is_stub else ''}"
        elig = "Yes" if card.eligible_for_production else "No"
        lines.append(
            f"| {idx} | `{card.adapter_name}` | {tier_label} | "
            f"{card.performance_score:.1f} | {card.scalability_resilience_score:.1f} | "
            f"{card.cost_score:.1f} | {card.operability_score:.1f} | "
            f"{card.graphrag_fit_score:.1f} | **{card.weighted_total:.2f}** | {elig} |"
        )

    lines.extend(
        [
            "",
            "## 5. Three-Step Scale Extrapolation (100M -> 1B -> 10B Edges)",
            "",
            "| Adapter | 100M 2-Hop P95 (ms) | 1B 2-Hop P95 (ms) | 10B 2-Hop P95 (ms) | 1B->10B Ratio | 10B Monthly Cost (USD) |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for name, res in suite_result.adapters.items():
        ladder = {
            step.scale_label: step for step in res.cost_extrapolation.scale_ladder
        }
        s100m = ladder["100M"]
        s1b = ladder["1B"]
        s10b = ladder["10B"]
        lines.append(
            f"| `{name}` | {s100m.projected_2hop_p95_ms:.2f} | "
            f"{s1b.projected_2hop_p95_ms:.2f} | {s10b.projected_2hop_p95_ms:.2f} | "
            f"{res.cost_extrapolation.degradation_1b_to_10b_ratio:.2f}x | "
            f"${s10b.projected_monthly_cost_usd:,.0f} |"
        )

    lines.extend(
        [
            "",
            "## 6. Decision",
            "",
            f"- **Primary Selected Engine:** `{winner.adapter_name}` (weighted score **{winner.weighted_total:.2f}/100**, tier `{winner.tier}`).",
        ]
    )
    if fallback is not None:
        lines.append(
            f"- **Secondary / Fallback Engine:** `{fallback.adapter_name}` (weighted score **{fallback.weighted_total:.2f}/100**) retained behind the `GraphStore` abstraction."
        )
    lines.extend(
        [
            "- **Engine Independence:** The reasoning and Hybrid GraphRAG layers remain insulated behind `GraphStore` and verified by the frozen `GoldenQuerySuite` CI gate.",
            "",
            "## 7. Consequences & Mitigations",
            "",
            "- All production workloads must enforce `GuardrailConfig` (`max_depth=3`, `max_results=5000`, `max_execution_seconds=2.0`, `max_expanded_nodes=50000`).",
            "- High-cardinality hubs (`technology:lang:python`, `customer:region:india`) remain deterministically partitioned via `SupernodeBucketer` (`bucket(node_id, 16)`).",
            "- Non-eligible baselines/stubs remain in the harness for contract regression and cluster-level evaluation before Gate 3.",
        ]
    )

    content = "\n".join(lines) + "\n"
    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return content
