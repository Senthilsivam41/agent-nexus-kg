"""Pydantic contracts for the Scale-KG showcase snapshot, scenarios, and live API."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from scale_kg.adapters.base import AdapterCostProfile
from scale_kg.api.models import BackendType
from scale_kg.benchmark.generator import ScalePreset
from scale_kg.benchmark.report import EngineScorecard
from scale_kg.benchmark.workloads import BenchmarkSuiteResult
from scale_kg.core.models import GuardrailViolation

AdapterAvailability = Literal[
    "verified_local",
    "live_remote",
    "offline_remote",
    "contract_stub",
]

GuardrailProbeId = Literal[
    "forbidden_payload",
    "missing_lakehouse_ref",
    "preflight_depth_cap",
    "runtime_expansion_abort",
]


class GuardrailLimitInfo(BaseModel):
    """Introspected guardrail parameter and active bound."""

    name: str
    value: int | float
    unit: str
    description: str


class LatencyBudgetInfo(BaseModel):
    """Introspected Gate 2 latency SLA parameter."""

    name: str
    value_ms: float
    description: str


class SwitchRuleCard(BaseModel):
    """Self-explanatory runtime switch rule displayed in the System Map tab."""

    rule_id: str
    title: str
    layer: str
    module: str
    threshold: str
    from_behavior: str
    to_behavior: str


class AdapterCatalogEntry(BaseModel):
    """Introspected metadata and availability status for one GraphStore adapter."""

    key: BackendType
    class_name: str
    availability: AdapterAvailability
    cost_profile: AdapterCostProfile


class ScalePresetSummary(BaseModel):
    """Summary of one PowerLawGraphGenerator scale preset."""

    preset: str
    node_count: int
    avg_degree: float
    supernode_fanout: int
    reference_scale_edges: int
    scale_label: str
    concurrency_levels: list[int]


class GoldenQuerySummary(BaseModel):
    """Summary of one frozen Golden Query regression test."""

    query_id: str
    name: str
    category: str
    max_depth: int
    include_derived: bool


class SystemIntrospection(BaseModel):
    """Live code metadata proving zero drift between implementation and UI."""

    package_version: str
    python_version: str
    git_sha: str
    git_ref: str
    built_at: str
    config_fingerprint: str
    supernode_degree_threshold: int
    num_buckets: int
    hot_path_threshold: int
    domains: list[str]
    guardrails: list[GuardrailLimitInfo]
    latency_budgets: list[LatencyBudgetInfo]
    switch_rules: list[SwitchRuleCard]
    adapters: list[AdapterCatalogEntry]
    presets: list[ScalePresetSummary]
    golden_queries: list[GoldenQuerySummary]
    api_routes: list[str]


class SupernodeRampPoint(BaseModel):
    """Single degree measurement along the supernode escalation curve."""

    degree: int
    bucketed: bool
    bucket_count: int
    traversal_ms: float
    expanded_nodes: int
    bucket_hops_unwrapped: int
    raw_bucket_leaks: int
    logical_neighbors_returned: int


class SupernodeRampResult(BaseModel):
    """Supernode bucketing switch demonstration across increasing hub degrees."""

    hub_id: str
    threshold: int
    num_buckets: int
    switch_triggered_at_degree: int | None
    points: list[SupernodeRampPoint]


class HotPathStep(BaseModel):
    """Single hit step in the derived-edge hot-path promotion stepper."""

    hit_number: int
    derived_edges_returned: int
    promoted_hot_paths: int
    materialized: bool
    execution_ms: float


class HotPathDemoResult(BaseModel):
    """Demonstrates query-time derived inference switching to materialized hot path."""

    rule_name: str
    derived_edge_type: str
    hot_path_threshold: int
    promoted_at_hit: int | None
    steps: list[HotPathStep]


class FederationSharePoint(BaseModel):
    """Cross-domain locality telemetry for one cross-domain edge share."""

    cross_domain_share: float
    ghost_proxies_created: int
    cross_domain_hops: int
    single_domain_locality_ratio: float
    traversal_ms: float


class FederationDemoResult(BaseModel):
    """Demonstrates Hop-0 O(1) prefix routing and Ghost/Proxy shard federation."""

    sample_canonical_id: str
    parsed_domain: str
    parsed_entity_type: str
    parsed_slug: str
    points: list[FederationSharePoint]


class GuardrailProbeResult(BaseModel):
    """Result of executing one real Lean-Graph or runtime guardrail stress probe."""

    probe_id: GuardrailProbeId
    title: str
    http_status: int
    blocked: bool
    aborted: bool
    violation: GuardrailViolation | None = None
    nodes_returned: int = 0
    expanded_nodes: int = 0
    summary: str


class HybridRagDemoResult(BaseModel):
    """5-stage Hybrid Vector + KG + Lakehouse RRF trace and hydration savings."""

    query_text: str
    seed_count: int
    graph_candidates: int
    hydrated_documents: int
    total_lakehouse_documents: int
    hydrated_bytes: int
    naive_full_graph_bytes: int
    payload_savings_pct: float
    stage_timings_ms: dict[str, float]
    top_hit_ids: list[str]
    citation_count: int


class ScenarioBundle(BaseModel):
    """All deterministic self-explanatory mechanism demos bundled in a snapshot."""

    supernode_ramp: SupernodeRampResult
    hot_path: HotPathDemoResult
    federation: FederationDemoResult
    guardrail_probes: dict[GuardrailProbeId, GuardrailProbeResult]
    hybrid_rag: HybridRagDemoResult


class PresetBenchmarkBundle(BaseModel):
    """Complete 7-workload benchmark, Gate 2 scorecards, and ADR for one preset."""

    preset: ScalePreset
    suite_result: BenchmarkSuiteResult
    scorecards: list[EngineScorecard]
    winner_adapter: str
    adr_markdown: str


class ShowcaseSnapshot(BaseModel):
    """Canonical payload shared by live FastAPI API and static Netlify builds."""

    schema_version: str = "1.0.0"
    introspection: SystemIntrospection
    scenarios: ScenarioBundle
    benchmarks: dict[str, PresetBenchmarkBundle]
    default_preset: str = "micro"


class CommitTrendEntry(BaseModel):
    """Compact historical metrics record stored in rolling history.json."""

    git_sha: str
    git_ref: str
    built_at: str
    config_fingerprint: str
    preset: str
    hop_1_p95_ms: float
    hop_2_p95_ms: float
    hop_3_p95_ms: float
    peak_qps: float
    winner_adapter: str
    winner_score: float
    golden_parity_passed: bool


def _default_switch_adapters() -> list[Literal["in_memory", "iceberg_puppygraph"]]:
    return ["in_memory", "iceberg_puppygraph"]


class AdapterSwitchRequest(BaseModel):
    """Live simulation request comparing 1-2 adapters on a Golden Query."""

    query_id: Literal["GQ-01", "GQ-02", "GQ-03", "GQ-04", "GQ-05", "GQ-06", "GQ-07"] = (
        "GQ-03"
    )
    adapters: list[Literal["in_memory", "iceberg_puppygraph"]] = Field(
        default_factory=_default_switch_adapters,
        min_length=1,
        max_length=2,
    )


class AdapterSwitchItem(BaseModel):
    """Execution output of one adapter during live adapter-switch comparison."""

    adapter: str
    node_count: int
    edge_count: int
    path_count: int
    bucket_hops_unwrapped: int
    execution_ms: float
    node_ids: list[str]


class AdapterSwitchResponse(BaseModel):
    """Side-by-side adapter comparison proving identical logical GraphRAG results."""

    query_id: str
    query_name: str
    parity_matched: bool
    results: list[AdapterSwitchItem]


class GuardrailProbeRequest(BaseModel):
    """Request to run one allowlisted live guardrail probe."""

    probe_id: GuardrailProbeId = "forbidden_payload"


class HotPathRequest(BaseModel):
    """Request to run the live derived-edge hot-path promotion stepper."""

    hits: int = Field(default=4, ge=1, le=6)
    threshold: int = Field(default=3, ge=1, le=5)


class RampRunRequest(BaseModel):
    """Request to execute a coalesced live benchmark ramp (publicly bounded)."""

    preset: Literal["micro", "local"] = "micro"
    adapters: list[Literal["in_memory", "iceberg_puppygraph"]] = Field(
        default_factory=_default_switch_adapters,
        min_length=1,
        max_length=2,
    )
    seed: int = Field(default=42, ge=1, le=10000)


class RampRunResponse(BaseModel):
    """Response from SingleFlightJobRunner live benchmark execution."""

    cache_hit: bool
    coalesced: bool
    bundle: PresetBenchmarkBundle
