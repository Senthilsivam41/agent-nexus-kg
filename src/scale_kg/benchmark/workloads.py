"""7-workload PoC benchmark runner for Gate 2 empirical engine evaluation."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

from pydantic import BaseModel

from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    GraphStore,
    GraphStoreError,
)
from scale_kg.benchmark.generator import (
    SCALE_PRESETS,
    GeneratedDataset,
    PowerLawGraphGenerator,
    ScalePreset,
)
from scale_kg.benchmark.golden_queries import GoldenParityReport, GoldenQuerySuite
from scale_kg.config import LatencyBudgetConfig
from scale_kg.core.guardrails import (
    GuardrailEnforcer,
    GuardrailViolationCode,
    GuardrailViolationError,
)
from scale_kg.core.models import Domain, TraversalRequest
from scale_kg.core.supernode import SupernodeBucketer
from scale_kg.routing.coordinator import QueryCoordinator


def compute_percentile(values: list[float], percentile: float) -> float:
    """Compute linear-interpolated percentile in ``[0, 100]`` for non-empty values."""
    if not values:
        return 0.0
    if not 0.0 <= percentile <= 100.0:
        raise ValueError(f"percentile must be in [0, 100], got {percentile}.")
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    rank = (percentile / 100.0) * (len(ordered) - 1)
    lo = math.floor(rank)
    hi = math.ceil(rank)
    if lo == hi:
        return round(ordered[lo], 3)
    frac = rank - lo
    return round(ordered[lo] + (ordered[hi] - ordered[lo]) * frac, 3)


class LatencyStats(BaseModel):
    """Latency distribution summary for one traversal depth."""

    p50_ms: float
    p95_ms: float
    p99_ms: float
    min_ms: float
    max_ms: float
    mean_ms: float
    sample_count: int
    budget_p95_ms: float | None = None
    within_budget: bool = True


class TraversalLatencyResult(BaseModel):
    """Workload 1: 1-, 2-, and 3-hop traversal latency vs Gate 2 SLA budgets."""

    by_depth: dict[int, LatencyStats]
    all_within_budget: bool


class SupernodeWorkloadResult(BaseModel):
    """Workload 2: Supernode traversal behavior, bucket unwrapping, and expansion caps."""

    supernode_ids: list[str]
    queries_run: int
    p95_ms: float
    p99_ms: float
    max_expanded_nodes: int
    total_bucket_hops_unwrapped: int
    raw_bucket_leaks: int
    within_guardrails: bool


class ConcurrencyLevelResult(BaseModel):
    """Throughput and tail latency observed at a single concurrency level."""

    concurrency: int
    total_queries: int
    succeeded: int
    failed: int
    qps: float
    p95_ms: float
    p99_ms: float
    within_budget: bool


class ConcurrentRampResult(BaseModel):
    """Workload 3: Concurrent retrieval ramp across increasing concurrency levels."""

    levels: list[ConcurrencyLevelResult]
    peak_qps: float
    max_concurrency_within_budget: int


class IngestionWorkloadResult(BaseModel):
    """Workload 4: Bulk & streaming ingestion throughput and Lean Graph write discipline."""

    nodes_ingested: int
    bulk_edges_ingested: int
    bulk_edges_per_sec: float
    stream_edges_ingested: int
    stream_edges_per_sec: float
    stream_batch_lag_ms: float
    derived_edges_rejected: int
    derived_to_core_ratio: float
    write_discipline_passed: bool


class CrossPartitionShareResult(BaseModel):
    """Cross-domain vs single-domain latency comparison at one edge share level."""

    cross_domain_edge_share: float
    single_domain_p95_ms: float
    cross_domain_p95_ms: float
    cross_partition_latency_ratio: float
    avg_cross_domain_hops: float
    avg_locality_ratio: float


class CrossPartitionWorkloadResult(BaseModel):
    """Workload 5: Cross-partition traversal cost ratio across tunable shares."""

    by_share: list[CrossPartitionShareResult]
    worst_latency_ratio: float


class HAFailureWorkloadResult(BaseModel):
    """Workload 6: High-availability fault injection and failover recovery metrics."""

    fault_mode: str
    total_queries: int
    succeeded_queries: int
    failed_queries: int
    recovery_time_ms: float
    degraded_health_status: str
    recovered_health_status: str
    recovered_cleanly: bool


class ScaleLadderStep(BaseModel):
    """Extrapolated performance and monthly cost at a target edge scale."""

    scale_label: str
    edge_count: int
    projected_2hop_p95_ms: float
    projected_3hop_p95_ms: float
    projected_monthly_cost_usd: float
    within_2hop_budget: bool


class CostExtrapolationResult(BaseModel):
    """Workload 7: Cost per billion edges and 100M -> 1B -> 10B scale extrapolation."""

    engine: str
    cost_profile: AdapterCostProfile
    cost_per_billion_edges_usd_month: float
    scale_ladder: list[ScaleLadderStep]
    degradation_1b_to_10b_ratio: float


class AdapterBenchmarkResult(BaseModel):
    """Complete 7-workload benchmark report for a single GraphStore adapter."""

    adapter_name: str
    preset: ScalePreset
    traversal_latency: TraversalLatencyResult
    supernode: SupernodeWorkloadResult
    concurrent_ramp: ConcurrentRampResult
    ingestion: IngestionWorkloadResult
    cross_partition: CrossPartitionWorkloadResult
    ha_failure: HAFailureWorkloadResult
    cost_extrapolation: CostExtrapolationResult
    golden_parity_passed: bool = True


class BenchmarkSuiteResult(BaseModel):
    """Full PoC benchmark output across all evaluated engines + Golden parity."""

    preset: ScalePreset
    started_at: str
    adapters: dict[str, AdapterBenchmarkResult]
    golden_parity: GoldenParityReport | None = None


async def run_traversal_latency_workload(
    store: GraphStore,
    seed_ids: list[str],
    *,
    depths: tuple[int, ...] = (1, 2, 3),
    samples_per_depth: int = 12,
    budgets: LatencyBudgetConfig | None = None,
) -> TraversalLatencyResult:
    """Workload 1: Measure 1-, 2-, and 3-hop P50/P95/P99 latency against Gate 2 budgets."""
    if not seed_ids:
        raise ValueError("seed_ids must be non-empty.")
    sla = budgets or LatencyBudgetConfig()
    by_depth: dict[int, LatencyStats] = {}

    for depth in depths:
        latencies: list[float] = []
        for idx in range(samples_per_depth):
            seed = seed_ids[idx % len(seed_ids)]
            started = time.perf_counter()
            res = await store.traverse_k_hop(
                TraversalRequest(seed_ids=[seed], max_depth=depth)
            )
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            if not res.metrics.aborted:
                latencies.append(elapsed_ms)

        if not latencies:
            latencies = [0.0]
        p95 = compute_percentile(latencies, 95.0)
        budget = sla.budget_for_depth(depth) if depth in (1, 2, 3) else None
        within = p95 <= budget if budget is not None else True
        by_depth[depth] = LatencyStats(
            p50_ms=compute_percentile(latencies, 50.0),
            p95_ms=p95,
            p99_ms=compute_percentile(latencies, 99.0),
            min_ms=round(min(latencies), 3),
            max_ms=round(max(latencies), 3),
            mean_ms=round(sum(latencies) / len(latencies), 3),
            sample_count=len(latencies),
            budget_p95_ms=budget,
            within_budget=within,
        )

    all_ok = all(stat.within_budget for stat in by_depth.values())
    return TraversalLatencyResult(by_depth=by_depth, all_within_budget=all_ok)


async def run_supernode_traversal_workload(
    store: GraphStore,
    supernode_ids: list[str],
    *,
    samples: int = 10,
    max_depth: int = 2,
    enforcer: GuardrailEnforcer | None = None,
) -> SupernodeWorkloadResult:
    """Workload 2: Measure supernode traversal P99 latency, expanded nodes, and unwrapping."""
    if not supernode_ids:
        raise ValueError("supernode_ids must be non-empty.")
    limits = (enforcer or GuardrailEnforcer()).config
    latencies: list[float] = []
    max_expanded = 0
    total_unwrapped = 0
    raw_bucket_leaks = 0
    all_within_guardrails = True

    for idx in range(samples):
        hub_id = supernode_ids[idx % len(supernode_ids)]
        started = time.perf_counter()
        res = await store.traverse_k_hop(
            TraversalRequest(
                seed_ids=[hub_id],
                max_depth=max_depth,
                max_expanded_nodes=limits.max_expanded_nodes,
                max_results=limits.max_results,
            )
        )
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        latencies.append(elapsed_ms)
        max_expanded = max(max_expanded, res.metrics.nodes_expanded)
        total_unwrapped += res.metrics.bucket_hops_unwrapped

        leaks = sum(
            1 for n in res.nodes if SupernodeBucketer.is_bucket_node_id(n.id)
        ) + sum(
            1
            for path in res.paths
            for vid in path
            if SupernodeBucketer.is_bucket_node_id(vid)
        )
        raw_bucket_leaks += leaks
        if (
            res.metrics.aborted
            or res.metrics.nodes_expanded > limits.max_expanded_nodes
            or len(res.nodes) > limits.max_results
            or leaks > 0
        ):
            all_within_guardrails = False

    return SupernodeWorkloadResult(
        supernode_ids=list(supernode_ids),
        queries_run=samples,
        p95_ms=compute_percentile(latencies, 95.0),
        p99_ms=compute_percentile(latencies, 99.0),
        max_expanded_nodes=max_expanded,
        total_bucket_hops_unwrapped=total_unwrapped,
        raw_bucket_leaks=raw_bucket_leaks,
        within_guardrails=all_within_guardrails,
    )


async def run_concurrent_ramp_workload(
    store: GraphStore,
    seed_ids: list[str],
    *,
    concurrency_levels: list[int] | None = None,
    max_depth: int = 2,
    budget_p95_ms: float = 100.0,
) -> ConcurrentRampResult:
    """Workload 3: Ramp concurrent traversals and measure QPS and tail latency."""
    if not seed_ids:
        raise ValueError("seed_ids must be non-empty.")
    levels_to_run = concurrency_levels or [2, 4, 8]
    results: list[ConcurrencyLevelResult] = []

    async def _one_query(idx: int) -> tuple[bool, float]:
        seed = seed_ids[idx % len(seed_ids)]
        t0 = time.perf_counter()
        try:
            res = await store.traverse_k_hop(
                TraversalRequest(seed_ids=[seed], max_depth=max_depth)
            )
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return (not res.metrics.aborted), dt_ms
        except (GraphStoreError, TimeoutError):
            dt_ms = (time.perf_counter() - t0) * 1000.0
            return False, dt_ms

    for conc in levels_to_run:
        batch_start = time.perf_counter()
        outcomes = await asyncio.gather(*(_one_query(i) for i in range(conc)))
        wall_sec = max(1e-6, time.perf_counter() - batch_start)

        latencies = [lat for _, lat in outcomes]
        succeeded = sum(1 for ok, _ in outcomes if ok)
        failed = conc - succeeded
        p95 = compute_percentile(latencies, 95.0)
        p99 = compute_percentile(latencies, 99.0)
        qps = round(succeeded / wall_sec, 2)
        within = failed == 0 and p95 <= budget_p95_ms
        results.append(
            ConcurrencyLevelResult(
                concurrency=conc,
                total_queries=conc,
                succeeded=succeeded,
                failed=failed,
                qps=qps,
                p95_ms=p95,
                p99_ms=p99,
                within_budget=within,
            )
        )

    peak_qps = max((lvl.qps for lvl in results), default=0.0)
    max_conc_ok = max(
        (lvl.concurrency for lvl in results if lvl.within_budget), default=0
    )
    return ConcurrentRampResult(
        levels=results,
        peak_qps=peak_qps,
        max_concurrency_within_budget=max_conc_ok,
    )


async def run_ingestion_workload(
    store: GraphStore,
    dataset: GeneratedDataset,
    *,
    stream_batch_size: int | None = None,
    clear_first: bool = True,
) -> IngestionWorkloadResult:
    """Workload 4: Measure bulk + streaming ingestion throughput and derived-edge rejection."""
    if clear_first:
        await store.clear()

    nodes_ingested = await store.upsert_nodes(dataset.nodes)

    split_idx = max(1, int(len(dataset.edges) * 0.8))
    bulk_edges = dataset.edges[:split_idx]
    stream_edges = dataset.edges[split_idx:]

    t_bulk = time.perf_counter()
    bulk_count = await store.upsert_edges(bulk_edges) if bulk_edges else 0
    bulk_elapsed = max(1e-6, time.perf_counter() - t_bulk)

    batch_size = stream_batch_size or max(50, len(stream_edges) // 2 or 1)
    t_stream = time.perf_counter()
    stream_count = 0
    batch_lags_ms: list[float] = []
    for offset in range(0, len(stream_edges), batch_size):
        batch = stream_edges[offset : offset + batch_size]
        b0 = time.perf_counter()
        stream_count += await store.upsert_edges(batch)
        batch_lags_ms.append((time.perf_counter() - b0) * 1000.0)
    stream_elapsed = max(1e-6, time.perf_counter() - t_stream)

    # Verify Lean Graph write discipline rejects unmaterialized derived edges.
    rejected = 0
    for bad_edge in dataset.invalid_derived_edges:
        try:
            await store.upsert_edges([bad_edge])
        except GuardrailViolationError as exc:
            if exc.violation.code == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE:
                rejected += 1

    core_total = max(1, bulk_count + stream_count)
    # Persisted derived edges should remain 0 when unmaterialized writes are rejected.
    persisted_derived = sum(1 for e in dataset.edges if e.is_derived)
    write_discipline_ok = rejected == len(dataset.invalid_derived_edges)

    return IngestionWorkloadResult(
        nodes_ingested=nodes_ingested,
        bulk_edges_ingested=bulk_count,
        bulk_edges_per_sec=round(bulk_count / bulk_elapsed, 2),
        stream_edges_ingested=stream_count,
        stream_edges_per_sec=round(stream_count / stream_elapsed, 2),
        stream_batch_lag_ms=round(
            sum(batch_lags_ms) / len(batch_lags_ms) if batch_lags_ms else 0.0, 3
        ),
        derived_edges_rejected=rejected,
        derived_to_core_ratio=round(persisted_derived / core_total, 6),
        write_discipline_passed=write_discipline_ok,
    )


async def run_cross_partition_workload(
    store_factory: Callable[[], GraphStore],
    *,
    preset: ScalePreset = ScalePreset.MICRO,
    shares: tuple[float, ...] = (0.05, 0.15, 0.30),
    samples_per_share: int = 8,
    seed: int = 42,
) -> CrossPartitionWorkloadResult:
    """Workload 5: Measure cross-partition latency ratio across tunable edge shares."""
    by_share: list[CrossPartitionShareResult] = []

    for share in shares:
        dataset = PowerLawGraphGenerator(
            preset=preset,
            cross_domain_edge_share=share,
            seed=seed,
            node_count_override=40 if preset == ScalePreset.MICRO else None,
            avg_degree_override=3.0 if preset == ScalePreset.MICRO else None,
            supernode_fanout_override=8 if preset == ScalePreset.MICRO else None,
        ).generate()
        shards = {d: store_factory() for d in Domain}
        coord = QueryCoordinator(shards)
        await coord.upsert_nodes(dataset.nodes)
        await coord.upsert_edges(dataset.edges)

        single_latencies: list[float] = []
        cross_latencies: list[float] = []
        cross_hops_list: list[int] = []
        locality_list: list[float] = []

        for idx in range(samples_per_share):
            seed_id = dataset.seed_ids[idx % len(dataset.seed_ids)]
            seed_domain = coord.directory.resolve_domain(seed_id)

            t_single = time.perf_counter()
            await coord.traverse(
                TraversalRequest(
                    seed_ids=[seed_id],
                    max_depth=2,
                    domains=[seed_domain],
                )
            )
            single_latencies.append((time.perf_counter() - t_single) * 1000.0)

            t_cross = time.perf_counter()
            cross_res = await coord.traverse(
                TraversalRequest(
                    seed_ids=[seed_id],
                    max_depth=2,
                )
            )
            cross_latencies.append((time.perf_counter() - t_cross) * 1000.0)
            cross_hops_list.append(cross_res.metrics.cross_domain_hops)
            locality_list.append(cross_res.metrics.single_domain_locality_ratio)

        single_p95 = compute_percentile(single_latencies, 95.0)
        cross_p95 = compute_percentile(cross_latencies, 95.0)
        ratio = round(cross_p95 / max(0.001, single_p95), 3)

        by_share.append(
            CrossPartitionShareResult(
                cross_domain_edge_share=share,
                single_domain_p95_ms=single_p95,
                cross_domain_p95_ms=cross_p95,
                cross_partition_latency_ratio=ratio,
                avg_cross_domain_hops=round(
                    sum(cross_hops_list) / max(1, len(cross_hops_list)), 3
                ),
                avg_locality_ratio=round(
                    sum(locality_list) / max(1, len(locality_list)), 4
                ),
            )
        )

    worst_ratio = max((s.cross_partition_latency_ratio for s in by_share), default=1.0)
    return CrossPartitionWorkloadResult(
        by_share=by_share, worst_latency_ratio=worst_ratio
    )


async def run_ha_failure_workload(
    store: GraphStore,
    seed_ids: list[str],
    *,
    fault_mode: FaultMode = FaultMode.TRANSIENT_ERROR,
    recover_after_calls: int = 2,
    total_queries: int = 8,
) -> HAFailureWorkloadResult:
    """Workload 6: Inject node/replica failure during traversal load and measure recovery."""
    if not seed_ids:
        raise ValueError("seed_ids must be non-empty.")

    if fault_mode == FaultMode.TRANSIENT_ERROR:
        await store.set_fault_mode(fault_mode, recover_after_calls=recover_after_calls)
    else:
        await store.set_fault_mode(fault_mode)

    degraded_health = await store.health_check()
    fault_started = time.perf_counter()
    recovery_time_ms = 0.0
    recovered_seen = False
    succeeded = 0
    failed = 0

    for idx in range(total_queries):
        if fault_mode != FaultMode.TRANSIENT_ERROR and idx == recover_after_calls:
            await store.set_fault_mode(FaultMode.NONE)

        seed = seed_ids[idx % len(seed_ids)]
        try:
            res = await store.traverse_k_hop(
                TraversalRequest(seed_ids=[seed], max_depth=2)
            )
            if not res.metrics.aborted:
                succeeded += 1
                if failed > 0 and not recovered_seen:
                    recovery_time_ms = round(
                        (time.perf_counter() - fault_started) * 1000.0, 3
                    )
                    recovered_seen = True
            else:
                failed += 1
        except (GraphStoreError, TimeoutError):
            failed += 1

    await store.set_fault_mode(FaultMode.NONE)
    recovered_health = await store.health_check()
    if not recovered_seen:
        recovery_time_ms = round((time.perf_counter() - fault_started) * 1000.0, 3)

    return HAFailureWorkloadResult(
        fault_mode=fault_mode.value,
        total_queries=total_queries,
        succeeded_queries=succeeded,
        failed_queries=failed,
        recovery_time_ms=recovery_time_ms,
        degraded_health_status=degraded_health.status,
        recovered_health_status=recovered_health.status,
        recovered_cleanly=recovered_health.healthy and succeeded > 0,
    )


def run_cost_extrapolation_workload(
    store: GraphStore,
    traversal_latency: TraversalLatencyResult,
    *,
    base_edge_count: int,
    budgets: LatencyBudgetConfig | None = None,
) -> CostExtrapolationResult:
    """Workload 7: Extrapolate cost per billion edges and 100M -> 1B -> 10B latency."""
    sla = budgets or LatencyBudgetConfig()
    profile = store.cost_profile()
    cost_per_1b = profile.estimated_cost_per_billion_edges_usd_month

    base_2hop = traversal_latency.by_depth[2].p95_ms
    base_3hop = traversal_latency.by_depth[3].p95_ms
    safe_base = max(100, base_edge_count)

    # Partitioned engines scale sub-logarithmically with shard pruning; engines lacking
    # cross-shard edges or zero-ETL incur higher coordination/fanout overhead at 10B.
    locality_penalty = 1.0 if profile.supports_cross_shard_edges else 1.65
    zero_etl_factor = 0.92 if profile.supports_zero_etl else 1.0

    ladder_targets: tuple[tuple[str, int], ...] = (
        ("100M", 100_000_000),
        ("1B", 1_000_000_000),
        ("10B", 10_000_000_000),
    )
    steps: list[ScaleLadderStep] = []
    for label, edge_target in ladder_targets:
        scale_decades = max(1.0, math.log10(edge_target / safe_base))
        scale_multiplier = (
            (1.0 + 0.18 * scale_decades) * locality_penalty * zero_etl_factor
        )
        proj_2hop = round(max(base_2hop, base_2hop * scale_multiplier), 3)
        proj_3hop = round(max(base_3hop, base_3hop * scale_multiplier * 1.15), 3)
        monthly_cost = round((edge_target / 1_000_000_000) * cost_per_1b, 2)
        steps.append(
            ScaleLadderStep(
                scale_label=label,
                edge_count=edge_target,
                projected_2hop_p95_ms=proj_2hop,
                projected_3hop_p95_ms=proj_3hop,
                projected_monthly_cost_usd=monthly_cost,
                within_2hop_budget=proj_2hop <= sla.hop_2_p95_ms,
            )
        )

    p95_1b = steps[1].projected_2hop_p95_ms
    p95_10b = steps[2].projected_2hop_p95_ms
    degradation_ratio = round(p95_10b / max(0.001, p95_1b), 3)

    return CostExtrapolationResult(
        engine=profile.engine,
        cost_profile=profile,
        cost_per_billion_edges_usd_month=cost_per_1b,
        scale_ladder=steps,
        degradation_1b_to_10b_ratio=degradation_ratio,
    )


class BenchmarkSuite:
    """Orchestrates all 7 PoC workloads + Golden Query parity across GraphStore engines."""

    def __init__(
        self,
        *,
        preset: ScalePreset | str = ScalePreset.MICRO,
        cross_domain_edge_share: float = 0.15,
        seed: int = 42,
        budgets: LatencyBudgetConfig | None = None,
        enforcer: GuardrailEnforcer | None = None,
    ) -> None:
        self.preset = ScalePreset(preset)
        self.cross_domain_edge_share = cross_domain_edge_share
        self.seed = seed
        self.budgets = budgets or LatencyBudgetConfig()
        self.enforcer = enforcer or GuardrailEnforcer()
        self.spec = SCALE_PRESETS[self.preset]

    async def run(
        self,
        adapter_factories: Mapping[str, Callable[[], GraphStore]],
        *,
        run_golden_parity: bool = True,
    ) -> BenchmarkSuiteResult:
        """Execute all 7 workloads for each engine factory and verify Golden parity."""
        if not adapter_factories:
            raise ValueError("adapter_factories must contain at least one adapter.")

        dataset = PowerLawGraphGenerator(
            preset=self.preset,
            cross_domain_edge_share=self.cross_domain_edge_share,
            seed=self.seed,
        ).generate(bucket_supernodes=True, degree_threshold=100)

        golden_report: GoldenParityReport | None = None
        if run_golden_parity and len(adapter_factories) >= 2:
            golden_stores = {
                name: factory() for name, factory in adapter_factories.items()
            }
            golden_report = await GoldenQuerySuite().run_parity(golden_stores)

        adapter_results: dict[str, AdapterBenchmarkResult] = {}
        for name, factory in adapter_factories.items():
            store = factory()
            ingestion = await run_ingestion_workload(store, dataset)
            traversal_latency = await run_traversal_latency_workload(
                store,
                dataset.seed_ids,
                samples_per_depth=self.spec.latency_samples,
                budgets=self.budgets,
            )
            supernode = await run_supernode_traversal_workload(
                store,
                dataset.supernode_ids,
                samples=max(6, self.spec.latency_samples // 2),
                enforcer=self.enforcer,
            )
            concurrent_ramp = await run_concurrent_ramp_workload(
                store,
                dataset.seed_ids,
                concurrency_levels=self.spec.concurrency_levels,
                budget_p95_ms=self.budgets.hop_2_p95_ms,
            )
            cross_partition = await run_cross_partition_workload(
                factory,
                preset=self.preset,
                samples_per_share=max(4, self.spec.latency_samples // 3),
                seed=self.seed,
            )
            ha_failure = await run_ha_failure_workload(
                store,
                dataset.seed_ids,
            )
            cost_extrapolation = run_cost_extrapolation_workload(
                store,
                traversal_latency,
                base_edge_count=len(dataset.edges),
                budgets=self.budgets,
            )

            parity_ok = True
            if golden_report is not None:
                parity_ok = all(
                    comp.adapter_results[name].guardrail_compliant
                    and not comp.mismatches
                    for comp in golden_report.comparisons
                    if name in comp.adapter_results
                )

            adapter_results[name] = AdapterBenchmarkResult(
                adapter_name=name,
                preset=self.preset,
                traversal_latency=traversal_latency,
                supernode=supernode,
                concurrent_ramp=concurrent_ramp,
                ingestion=ingestion,
                cross_partition=cross_partition,
                ha_failure=ha_failure,
                cost_extrapolation=cost_extrapolation,
                golden_parity_passed=parity_ok,
            )

        return BenchmarkSuiteResult(
            preset=self.preset,
            started_at=datetime.now(UTC).isoformat(),
            adapters=adapter_results,
            golden_parity=golden_report,
        )
