# Scale-KG Interactive Showcase & Auto-Updating Benchmark Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a self-explanatory, auto-updating interactive showcase dashboard (`scale_kg.showcase`) that proves how `scale-kg` handles scale/load and switches engines/runtime modes, sustains 50 concurrent live users via FastAPI, and auto-deploys static snapshots with commit history trends to Netlify at `https://scale-kg.labbys-aoai.in`.

**Architecture:** A cohesive Python package `src/scale_kg/showcase/` introspects live `scale_kg` configurations, adapters, presets, golden queries, and routes; executes deterministic switch/load scenarios and 7-workload benchmarks; serves an immutable pre-warmed `ShowcaseSnapshot` and single-flight coalesced live playground endpoints under `/showcase` for up to 50 concurrent users; and exports the identical zero-build HTML/CSS/ES-module UI + JSON artifacts to `dist/showcase/` for automated Netlify deployment via GitHub Actions.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2, PyArrow, Uvicorn, vanilla HTML5/CSS3/ES-modules + inline SVG charts (zero Node build step), Netlify (`netlify.toml` + CLI deploy in GitHub Actions), Pytest + `httpx.ASGITransport`.

## Global Constraints

- Python `>=3.11`; zero new PyPI runtime dependencies (uses existing `pydantic`, `fastapi`, `uvicorn`, `httpx`, `pyarrow`).
- Zero Node.js/npm build toolchain and zero external runtime CDN URLs in dashboard assets.
- Zero breaking changes to existing `/v1/*` REST, gRPC, `GraphStore`, `QueryCoordinator`, or `BenchmarkSuite` contracts.
- Live FastAPI server must handle 50 concurrent users cleanly: O(1) pre-serialized snapshot reads with `ETag`, `Semaphore(50)` + `2.0s` timeout on light interactive simulations, and `SingleFlightJobRunner` deduplication + `Semaphore(2)` + `429 Retry-After` backpressure on heavy `/ramp/run` jobs.
- Public static site targets Netlify at `https://scale-kg.labbys-aoai.in` (Hostinger `CNAME` `scale-kg` → `<site>.netlify.app`) with `.netlify/` ignored in `.gitignore`.
- Never expose environment secrets, API keys, or absolute host filesystem paths in snapshots, HTML, or error payloads.
- All code must pass `make check` (`uv run ruff format --check .`, `uv run ruff check .`, `uv run mypy src`, `uv run pytest`, `uv build`).
- Work strictly on branch `feature/showcase-dashboard` and preserve any unrelated dirty/untracked files (`AGENTS.md`, `.coverage`, `graphify-out/`, `tasks/lessons.md`).

---

## File Structure

| File | Responsibility |
| :--- | :--- |
| `src/scale_kg/showcase/__init__.py` | Package public exports (`ShowcaseSnapshot`, `ShowcaseService`, `create_showcase_router`, `build_showcase_site`) |
| `src/scale_kg/showcase/snapshot.py` | Pydantic models for `ShowcaseSnapshot`, `SystemIntrospection`, scenarios, live API requests/responses, and `CommitTrendEntry` |
| `src/scale_kg/showcase/introspect.py` | Zero-drift dynamic introspection of `ScaleKGConfig`, `Domain`, `GraphStore` adapters, `SCALE_PRESETS`, `FROZEN_GOLDEN_QUERIES`, and routes |
| `src/scale_kg/showcase/scenarios.py` | Deterministic live-code demos (supernode degree ramp, hot-path 3-hit stepper, cross-domain federation, 4 guardrail probes, 5-stage Hybrid RAG) and `build_showcase_snapshot()` |
| `src/scale_kg/showcase/concurrency.py` | `SingleFlightJobRunner` (dedup + TTL LRU cache + semaphore + backpressure) and `ShowcaseService` (pre-warmed immutable snapshot + 50-user read-only playground) |
| `src/scale_kg/showcase/router.py` | FastAPI `APIRouter` for `/showcase`, `/showcase/assets/{filename}`, and `/showcase/api/*` |
| `src/scale_kg/showcase/build.py` | Static exporter CLI (`python -m scale_kg.showcase.build`), rolling 100-entry `history.json` merger, and PR Markdown delta renderer |
| `src/scale_kg/showcase/static/index.html` | Zero-build single-page shell compatible with live and static modes |
| `src/scale_kg/showcase/static/styles.css` | Accessible responsive dashboard styles (zero external fonts/CDNs) |
| `src/scale_kg/showcase/static/charts.js` | Pure inline SVG chart renderers (latency bars with SLA lines, ramp lines, waterfall, commit sparklines) |
| `src/scale_kg/showcase/static/views.js` | Renderers for all 5 self-explanatory tabs (System Map, Load Escalator, Adapter Switchboard & Gate 2 ADR, Hybrid RAG Trace, Commit Trends) |
| `src/scale_kg/showcase/static/app.js` | Live/Static mode controller, interactive button/slider handlers, state management |
| `src/scale_kg/api/app.py` | Mount showcase router in `create_app()` with `enable_showcase`, `showcase_snapshot_path`, `showcase_require_auth` |
| `netlify.toml` | Netlify publish directory (`dist/showcase`), CSP/security headers, cache rules |
| `.github/workflows/showcase.yml` | CI auto-update workflow: builds `dist/showcase`, merges live `history.json`, deploys Netlify prod on `main` & previews on PRs |
| `.gitignore`, `Makefile`, `portfolio.yaml`, `README.md`, `CHANGELOG.md` | Ignore `.netlify/`, add `showcase-build`/`showcase` targets, document showcase interface |
| `tests/test_showcase_introspect.py` | Zero-drift introspection & config fingerprint tests |
| `tests/test_showcase_scenarios.py` | All 5 switch/load scenario & snapshot builder tests |
| `tests/test_showcase_concurrency.py` | 50-concurrent-user ASGI load, single-flight coalescing, `ETag`/`304`, and `429` backpressure tests |
| `tests/test_showcase_build.py` | Static export, zero-CDN/zero-path-leak, history cap/dedup, and PR delta tests |

---

### Task 1: Showcase Schemas & Zero-Drift Code Introspection

**Files:**
- Create: `src/scale_kg/showcase/snapshot.py`
- Create: `src/scale_kg/showcase/introspect.py`
- Create: `src/scale_kg/showcase/__init__.py`
- Test: `tests/test_showcase_introspect.py`

**Interfaces:**
- Consumes: `scale_kg.config.ScaleKGConfig`, `GuardrailConfig`, `LatencyBudgetConfig`; `scale_kg.core.models.Domain`; `scale_kg.adapters` concrete `GraphStore` subclasses + `AdapterCostProfile`; `scale_kg.benchmark.generator.SCALE_PRESETS`; `scale_kg.benchmark.golden_queries.FROZEN_GOLDEN_QUERIES`; `scale_kg.benchmark.workloads.BenchmarkSuiteResult`; `scale_kg.benchmark.report.EngineScorecard`.
- Produces:
  - `scale_kg.showcase.snapshot`: `GuardrailLimitInfo`, `LatencyBudgetInfo`, `AdapterCatalogEntry`, `SwitchRuleCard`, `ScalePresetSummary`, `GoldenQuerySummary`, `SystemIntrospection`, `SupernodeRampPoint`, `SupernodeRampResult`, `HotPathStep`, `HotPathDemoResult`, `FederationSharePoint`, `FederationDemoResult`, `GuardrailProbeResult`, `HybridRagDemoResult`, `ScenarioBundle`, `PresetBenchmarkBundle`, `ShowcaseSnapshot`, `CommitTrendEntry`, `AdapterSwitchRequest`, `AdapterSwitchResponse`, `GuardrailProbeRequest`, `HotPathRequest`, `RampRunRequest`, `RampRunResponse`.
  - `scale_kg.showcase.introspect`: `introspect_system(*, config: ScaleKGConfig | None = None, routes: Sequence[str] | None = None, probe_remote: bool = False, git_sha: str | None = None, git_ref: str | None = None) -> SystemIntrospection`.

- [ ] **Step 1: Write the failing introspection tests**

Create `tests/test_showcase_introspect.py`:

```python
"""Tests for zero-drift runtime introspection of scale_kg components."""

from __future__ import annotations

from scale_kg.adapters import (
    GraphStore,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    JanusGraphStore,
    NebulaGraphStore,
    Neo4jGraphStore,
)
from scale_kg.benchmark.generator import SCALE_PRESETS
from scale_kg.benchmark.golden_queries import FROZEN_GOLDEN_QUERIES
from scale_kg.config import GuardrailConfig, LatencyBudgetConfig, ScaleKGConfig
from scale_kg.core.models import Domain
from scale_kg.showcase.introspect import introspect_system


def test_introspect_captures_all_live_configs_domains_adapters_and_presets() -> None:
    info = introspect_system(probe_remote=False)

    guardrail_names = {item.name for item in info.guardrails}
    assert guardrail_names == set(GuardrailConfig.model_fields.keys())

    budget_names = {item.name for item in info.latency_budgets}
    assert budget_names == set(LatencyBudgetConfig.model_fields.keys())

    assert info.domains == [d.value for d in Domain]
    assert {p.preset for p in info.presets} == {p.value for p in SCALE_PRESETS}
    assert {q.query_id for q in info.golden_queries} == {
        q.query_id for q in FROZEN_GOLDEN_QUERIES
    }

    expected_adapters = {
        InMemoryGraphStore,
        IcebergPuppyGraphStore,
        HugeGraphStore,
        Neo4jGraphStore,
        JanusGraphStore,
        NebulaGraphStore,
    }
    discovered_classes = {entry.class_name for entry in info.adapters}
    assert discovered_classes == {cls.__name__ for cls in expected_adapters}
    assert all(issubclass(cls, GraphStore) for cls in expected_adapters)
    assert len(info.switch_rules) >= 4
    assert len(info.config_fingerprint) == 16


def test_introspect_fingerprint_updates_when_config_changes() -> None:
    base = introspect_system(config=ScaleKGConfig(), probe_remote=False)
    modified = introspect_system(
        config=ScaleKGConfig(supernode_degree_threshold=250, num_buckets=8),
        probe_remote=False,
    )

    assert base.config_fingerprint != modified.config_fingerprint
    assert modified.supernode_degree_threshold == 250
    assert modified.num_buckets == 8
    assert any("250" in rule.threshold for rule in modified.switch_rules)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_showcase_introspect.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scale_kg.showcase'`

- [ ] **Step 3: Implement `snapshot.py`, `introspect.py`, and initial `__init__.py`**

Create `src/scale_kg/showcase/snapshot.py`:

```python
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


class AdapterSwitchRequest(BaseModel):
    """Live simulation request comparing 1-2 adapters on a Golden Query."""

    query_id: Literal["GQ-01", "GQ-02", "GQ-03", "GQ-04", "GQ-05", "GQ-06", "GQ-07"] = (
        "GQ-03"
    )
    adapters: list[Literal["in_memory", "iceberg_puppygraph"]] = Field(
        default_factory=lambda: ["in_memory", "iceberg_puppygraph"],
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
        default_factory=lambda: ["in_memory", "iceberg_puppygraph"],
        min_length=1,
        max_length=2,
    )
    seed: int = Field(default=42, ge=1, le=10000)


class RampRunResponse(BaseModel):
    """Response from SingleFlightJobRunner live benchmark execution."""

    cache_hit: bool
    coalesced: bool
    bundle: PresetBenchmarkBundle
```

Create `src/scale_kg/showcase/introspect.py`:

```python
"""Zero-drift runtime introspection of scale_kg configs, adapters, presets, and routes."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version as pkg_version

import httpx

from scale_kg.adapters import (
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    JanusGraphStore,
    NebulaGraphStore,
    Neo4jGraphStore,
)
from scale_kg.adapters.base import AdapterCostProfile, GraphStore
from scale_kg.api.models import BackendType
from scale_kg.benchmark.generator import SCALE_PRESETS
from scale_kg.benchmark.golden_queries import FROZEN_GOLDEN_QUERIES
from scale_kg.config import ScaleKGConfig
from scale_kg.core.models import Domain
from scale_kg.showcase.snapshot import (
    AdapterAvailability,
    AdapterCatalogEntry,
    GoldenQuerySummary,
    GuardrailLimitInfo,
    LatencyBudgetInfo,
    ScalePresetSummary,
    SwitchRuleCard,
    SystemIntrospection,
)

_GUARDRAIL_META: dict[str, tuple[str, str]] = {
    "max_depth": ("hops", "Maximum logical traversal depth per query"),
    "max_results": ("nodes", "Maximum logical vertices returned per traversal"),
    "max_execution_seconds": ("seconds", "Hard runtime execution timeout per query"),
    "max_expanded_nodes": ("nodes", "Maximum frontier nodes expanded before abort"),
}

_BUDGET_META: dict[str, str] = {
    "hop_1_p95_ms": "Gate 2 1-hop P95 latency SLA",
    "hop_2_p95_ms": "Gate 2 2-hop P95 latency SLA",
    "hop_3_p95_ms": "Gate 2 3-hop P95 latency SLA",
    "hard_timeout_ms": "Gate 2 absolute hard execution timeout",
}

_DEFAULT_PUBLIC_ROUTES: tuple[str, ...] = (
    "/v1/health",
    "/v1/ingest",
    "/v1/traverse",
    "/v1/hybrid-retrieve",
    "/v1/benchmark/run",
    "/v1/nodes/{node_id}",
    "/v1/nodes/{node_id}/neighbors",
    "/v1/stats",
    "/v1/lakehouse/documents",
    "/v1/lakehouse/slices",
    "/v1/directory/aliases",
    "/v1/directory/resolve/{entity_id}",
    "/scale_kg.v1.ScaleKGService/{rpc_method}",
)


def _detect_git_metadata() -> tuple[str, str]:
    sha = os.environ.get("GITHUB_SHA") or ""
    ref = os.environ.get("GITHUB_REF_NAME") or ""
    if not sha:
        try:
            sha = (
                subprocess.check_output(
                    ["git", "rev-parse", "--short=12", "HEAD"],
                    stderr=subprocess.DEVNULL,
                    timeout=1.0,
                )
                .decode("utf-8")
                .strip()
            )
        except Exception:
            sha = "local-dev"
    else:
        sha = sha[:12]
    if not ref:
        try:
            ref = (
                subprocess.check_output(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    stderr=subprocess.DEVNULL,
                    timeout=1.0,
                )
                .decode("utf-8")
                .strip()
            )
        except Exception:
            ref = "local"
    return sha, ref


def _probe_http_endpoint(url: str | None) -> bool:
    if not url:
        return False
    try:
        resp = httpx.get(url, timeout=0.5)
        return resp.status_code < 500
    except Exception:
        return False


def _adapter_catalog(probe_remote: bool) -> list[AdapterCatalogEntry]:
    specs: list[tuple[BackendType, type[GraphStore], AdapterCostProfile, str | None]] = [
        (
            "in_memory",
            InMemoryGraphStore,
            InMemoryGraphStore().cost_profile(),
            None,
        ),
        (
            "iceberg_puppygraph",
            IcebergPuppyGraphStore,
            IcebergPuppyGraphStore().cost_profile(),
            None,
        ),
        (
            "hugegraph",
            HugeGraphStore,
            HugeGraphStore(base_url="http://localhost:8080").cost_profile(),
            os.environ.get("HUGEGRAPH_URL"),
        ),
        (
            "neo4j",
            Neo4jGraphStore,
            Neo4jGraphStore(uri="bolt://localhost:7687").cost_profile(),
            os.environ.get("NEO4J_HTTP_URL"),
        ),
        (
            "janusgraph",
            JanusGraphStore,
            JanusGraphStore().cost_profile(),
            None,
        ),
        (
            "nebula",
            NebulaGraphStore,
            NebulaGraphStore().cost_profile(),
            None,
        ),
    ]

    entries: list[AdapterCatalogEntry] = []
    for key, cls, profile, remote_url in specs:
        availability: AdapterAvailability
        if profile.is_stub:
            availability = "contract_stub"
        elif key in ("in_memory", "iceberg_puppygraph"):
            availability = "verified_local"
        elif probe_remote and _probe_http_endpoint(remote_url):
            availability = "live_remote"
        else:
            availability = "offline_remote"
        entries.append(
            AdapterCatalogEntry(
                key=key,
                class_name=cls.__name__,
                availability=availability,
                cost_profile=profile,
            )
        )
    return entries


def _build_switch_rules(cfg: ScaleKGConfig, hot_path_threshold: int) -> list[SwitchRuleCard]:
    return [
        SwitchRuleCard(
            rule_id="supernode-bucketing",
            title="16-Way Supernode Bucketing",
            layer="Layer 2: Routing & Guardrails",
            module="scale_kg.core.supernode.SupernodeBucketer",
            threshold=f"degree >= {cfg.supernode_degree_threshold} (or seeded hub)",
            from_behavior="Direct hub-to-neighbor edges",
            to_behavior=(
                f"CRC32 split into {cfg.num_buckets} ::bucket::0..{cfg.num_buckets - 1} "
                "vertices + 0-1 BFS transparent unwrapping"
            ),
        ),
        SwitchRuleCard(
            rule_id="hot-path-promotion",
            title="Derived-Edge Hot-Path Promotion",
            layer="Layer 2: Routing & Guardrails",
            module="scale_kg.routing.coordinator.QueryCoordinator",
            threshold=f"derived hit_count >= {hot_path_threshold}",
            from_behavior="On-the-fly DerivedEdgeRule inference (SIMILAR_TO / INDIRECTLY_USES)",
            to_behavior="Auto-promoted materialized_hot_path=True fast-path edge",
        ),
        SwitchRuleCard(
            rule_id="shard-federation",
            title="Hop-0 Prefix & Ghost/Proxy Federation",
            layer="Layer 2: Routing & Guardrails",
            module="scale_kg.routing.directory.EntityDirectory",
            threshold="source.domain != target.domain",
            from_behavior="Single-shard local adjacency scan",
            to_behavior="In-shard Ghost/Proxy node + targeted remote shard hop",
        ),
        SwitchRuleCard(
            rule_id="guardrail-enforcement",
            title="Pre-Flight & Runtime Query Guardrails",
            layer="Layer 2: Routing & Guardrails",
            module="scale_kg.core.guardrails.GuardrailEnforcer",
            threshold=(
                f"depth>{cfg.guardrails.max_depth} | results>{cfg.guardrails.max_results} | "
                f"expanded>{cfg.guardrails.max_expanded_nodes} | time>{cfg.guardrails.max_execution_seconds}s"
            ),
            from_behavior="Bounded full-path traversal",
            to_behavior="HTTP 422 pre-flight block or partial result with aborted=True",
        ),
        SwitchRuleCard(
            rule_id="selective-hydration",
            title="Selective Top-N Lakehouse Hydration",
            layer="Layer 1 & 4: Hybrid RAG + Lakehouse",
            module="scale_kg.routing.hybrid_rag.HybridGraphRAGRouter",
            threshold="Post-RRF top_n candidates only",
            from_behavior="Heavy document blobs rejected in graph tier (Lean Graph Rule)",
            to_behavior="Partition-pruned Parquet hydrate_batch() for final top-N refs",
        ),
        SwitchRuleCard(
            rule_id="engine-switchboard",
            title="Pluggable GraphStore Adapter Switch",
            layer="Layer 3: Storage Adapters",
            module="scale_kg.adapters.base.GraphStore",
            threshold="Config backend / Gate 2 ADR scale tier (100M -> 1B -> 10B)",
            from_behavior="In-memory / local Parquet Zero-ETL evaluation",
            to_behavior="Zero-code-change switch to PuppyGraph / HugeGraph / Neo4j with GQ parity",
        ),
    ]


def introspect_system(
    *,
    config: ScaleKGConfig | None = None,
    routes: Sequence[str] | None = None,
    probe_remote: bool = False,
    git_sha: str | None = None,
    git_ref: str | None = None,
    hot_path_threshold: int = 3,
) -> SystemIntrospection:
    """Inspect live scale_kg symbols and produce zero-drift UI metadata."""
    cfg = config or ScaleKGConfig()
    detected_sha, detected_ref = _detect_git_metadata()
    resolved_sha = (git_sha or detected_sha)[:12]
    resolved_ref = git_ref or detected_ref

    try:
        version_str = pkg_version("scale-kg")
    except PackageNotFoundError:
        version_str = "0.1.0"

    guardrails: list[GuardrailLimitInfo] = []
    for name, value in cfg.guardrails.model_dump().items():
        unit, desc = _GUARDRAIL_META.get(name, ("", name))
        guardrails.append(
            GuardrailLimitInfo(name=name, value=value, unit=unit, description=desc)
        )

    budgets: list[LatencyBudgetInfo] = []
    for name, value in cfg.latency_budgets.model_dump().items():
        budgets.append(
            LatencyBudgetInfo(
                name=name,
                value_ms=float(value),
                description=_BUDGET_META.get(name, name),
            )
        )

    presets = [
        ScalePresetSummary(
            preset=spec.preset.value,
            node_count=spec.node_count,
            avg_degree=spec.avg_degree,
            supernode_fanout=spec.supernode_fanout,
            reference_scale_edges=spec.reference_scale_edges,
            scale_label=spec.scale_label,
            concurrency_levels=list(spec.concurrency_levels),
        )
        for spec in SCALE_PRESETS.values()
    ]

    golden_queries = [
        GoldenQuerySummary(
            query_id=q.query_id,
            name=q.name,
            category=q.category,
            max_depth=q.request.max_depth,
            include_derived=q.request.include_derived,
        )
        for q in FROZEN_GOLDEN_QUERIES
    ]

    adapters = _adapter_catalog(probe_remote=probe_remote)
    fingerprint_payload = json.dumps(
        {
            "config": cfg.model_dump(mode="json"),
            "hot_path_threshold": hot_path_threshold,
            "domains": [d.value for d in Domain],
            "adapters": [a.class_name for a in adapters],
            "presets": [p.model_dump(mode="json") for p in presets],
            "golden_queries": [g.query_id for g in golden_queries],
        },
        sort_keys=True,
    )
    fingerprint = hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()[:16]

    return SystemIntrospection(
        package_version=version_str,
        python_version=platform.python_version(),
        git_sha=resolved_sha,
        git_ref=resolved_ref,
        built_at=datetime.now(UTC).isoformat(),
        config_fingerprint=fingerprint,
        supernode_degree_threshold=cfg.supernode_degree_threshold,
        num_buckets=cfg.num_buckets,
        hot_path_threshold=hot_path_threshold,
        domains=[d.value for d in Domain],
        guardrails=guardrails,
        latency_budgets=budgets,
        switch_rules=_build_switch_rules(cfg, hot_path_threshold),
        adapters=adapters,
        presets=presets,
        golden_queries=golden_queries,
        api_routes=list(routes) if routes is not None else list(_DEFAULT_PUBLIC_ROUTES),
    )
```

Create `src/scale_kg/showcase/__init__.py`:

```python
"""Interactive showcase engine, 50-user concurrency layer, and static exporter."""

from scale_kg.showcase.introspect import introspect_system
from scale_kg.showcase.snapshot import (
    CommitTrendEntry,
    ShowcaseSnapshot,
    SystemIntrospection,
)

__all__ = [
    "CommitTrendEntry",
    "ShowcaseSnapshot",
    "SystemIntrospection",
    "introspect_system",
]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_showcase_introspect.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/scale_kg/showcase/__init__.py src/scale_kg/showcase/snapshot.py src/scale_kg/showcase/introspect.py tests/test_showcase_introspect.py
git commit -m "feat(showcase): add snapshot schemas and zero-drift introspection"
```

---

### Task 2: Deterministic Switch & Load Scenarios + Snapshot Builder

**Files:**
- Create: `src/scale_kg/showcase/scenarios.py`
- Test: `tests/test_showcase_scenarios.py`

**Interfaces:**
- Consumes: `scale_kg.showcase.snapshot.*`, `scale_kg.showcase.introspect.introspect_system`, `scale_kg.core.supernode.SupernodeBucketer`, `scale_kg.core.guardrails.GuardrailEnforcer`, `scale_kg.routing.coordinator.QueryCoordinator`, `scale_kg.routing.hybrid_rag.HybridGraphRAGRouter`, `scale_kg.lakehouse.store.LakehouseContentStore`, `scale_kg.benchmark.workloads.BenchmarkSuite`, `scale_kg.benchmark.report.rank_engines`, `generate_gate2_adr`.
- Produces:
  - `run_supernode_ramp(*, degrees: tuple[int, ...] = (8, 16, 32, 64, 128, 256), threshold: int = 32, num_buckets: int = 16) -> SupernodeRampResult`
  - `run_hot_path_demo(*, hits: int = 4, threshold: int = 3) -> HotPathDemoResult`
  - `run_federation_demo(*, shares: tuple[float, ...] = (0.10, 0.25, 0.40)) -> FederationDemoResult`
  - `run_guardrail_probe(probe_id: GuardrailProbeId) -> GuardrailProbeResult`
  - `run_hybrid_rag_demo(*, warehouse_dir: Path | None = None) -> HybridRagDemoResult`
  - `run_preset_benchmark(preset: ScalePreset, *, adapters: Sequence[BackendType] = ("in_memory", "iceberg_puppygraph"), seed: int = 42, temp_root: Path | None = None) -> PresetBenchmarkBundle`
  - `build_showcase_snapshot(*, presets: Sequence[ScalePreset] = (ScalePreset.MICRO,), adapters: Sequence[BackendType] = ("in_memory", "iceberg_puppygraph"), seed: int = 42, probe_remote: bool = False, git_sha: str | None = None, git_ref: str | None = None) -> ShowcaseSnapshot`

- [ ] **Step 1: Write the failing scenario tests**

Create `tests/test_showcase_scenarios.py`:

```python
"""Tests for executable showcase switch, load-ramp, guardrail, and RAG scenarios."""

from __future__ import annotations

import pytest

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.core.guardrails import GuardrailViolationCode
from scale_kg.showcase.scenarios import (
    build_showcase_snapshot,
    run_federation_demo,
    run_guardrail_probe,
    run_hot_path_demo,
    run_hybrid_rag_demo,
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
        "forbidden_payload": (422, GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD, False),
        "missing_lakehouse_ref": (
            422,
            GuardrailViolationCode.LEAN_GRAPH_MISSING_LAKEHOUSE_REF,
            False,
        ),
        "preflight_depth_cap": (422, GuardrailViolationCode.MAX_DEPTH_EXCEEDED, False),
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_showcase_scenarios.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scale_kg.showcase.scenarios'`

- [ ] **Step 3: Implement `src/scale_kg/showcase/scenarios.py`**

Create `src/scale_kg/showcase/scenarios.py`:

```python
"""Executable mechanism demos and snapshot assembly backed by live scale_kg code."""

from __future__ import annotations

import tempfile
import time
from collections.abc import Callable, Sequence
from pathlib import Path

from scale_kg.adapters import IcebergPuppyGraphStore, InMemoryGraphStore
from scale_kg.adapters.base import GraphStore
from scale_kg.api.models import BackendType
from scale_kg.benchmark.generator import PowerLawGraphGenerator, ScalePreset
from scale_kg.benchmark.report import generate_gate2_adr, rank_engines
from scale_kg.benchmark.workloads import BenchmarkSuite
from scale_kg.config import GuardrailConfig, ScaleKGConfig
from scale_kg.core.guardrails import (
    GuardrailEnforcer,
    GuardrailViolationCode,
    GuardrailViolationError,
)
from scale_kg.core.models import (
    Domain,
    EntityNode,
    GuardrailViolation,
    RelationshipEdge,
    TraversalRequest,
)
from scale_kg.core.supernode import SupernodeBucketer, is_bucket_node
from scale_kg.lakehouse.store import LakehouseContentStore, LakehouseDocument
from scale_kg.routing.coordinator import QueryCoordinator
from scale_kg.routing.directory import parse_canonical_id
from scale_kg.routing.hybrid_rag import (
    HybridGraphRAGRouter,
    HybridRAGRequest,
    InMemoryVectorIndex,
)
from scale_kg.showcase.introspect import introspect_system
from scale_kg.showcase.snapshot import (
    FederationDemoResult,
    FederationSharePoint,
    GuardrailProbeId,
    GuardrailProbeResult,
    HotPathDemoResult,
    HotPathStep,
    HybridRagDemoResult,
    PresetBenchmarkBundle,
    ScenarioBundle,
    ShowcaseSnapshot,
    SupernodeRampPoint,
    SupernodeRampResult,
)


async def run_supernode_ramp(
    *,
    degrees: tuple[int, ...] = (8, 16, 32, 64, 128, 256),
    threshold: int = 32,
    num_buckets: int = 16,
) -> SupernodeRampResult:
    """Measure traversal & bucketing switch behavior as hub degree crosses threshold."""
    hub_id = "technology:hub:demo-core"
    points: list[SupernodeRampPoint] = []
    switch_degree: int | None = None

    for degree in degrees:
        nodes = [
            EntityNode(
                id=hub_id,
                domain=Domain.TECHNOLOGY,
                entity_type="Hub",
                routing_props={"active": True},
            )
        ]
        edges: list[RelationshipEdge] = []
        for idx in range(degree):
            target_id = f"product:svc:leaf-{idx}"
            nodes.append(
                EntityNode(
                    id=target_id,
                    domain=Domain.PRODUCT,
                    entity_type="Service",
                    routing_props={"idx": idx},
                )
            )
            edges.append(
                RelationshipEdge(
                    source_id=hub_id,
                    target_id=target_id,
                    edge_type="POWERS",
                    domain=Domain.TECHNOLOGY,
                    target_domain=Domain.PRODUCT,
                )
            )

        bucketer = SupernodeBucketer(
            num_buckets=num_buckets,
            degree_threshold=threshold,
            seeded_hub_ids=frozenset(),
            seeded_hub_types=frozenset(),
        )
        t_nodes, t_edges, report = bucketer.transform_dataset(nodes, edges)
        bucketed = hub_id in report.bucketed_hubs
        if bucketed and switch_degree is None:
            switch_degree = degree

        store = InMemoryGraphStore()
        await store.upsert_nodes(t_nodes)
        await store.upsert_edges(t_edges)

        started = time.perf_counter()
        result = await store.traverse_k_hop(
            TraversalRequest(seed_ids=[hub_id], max_depth=1, max_results=5000)
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)

        leaks = sum(1 for n in result.nodes if is_bucket_node(n.id))
        logical_neighbors = sum(1 for n in result.nodes if n.id != hub_id)
        points.append(
            SupernodeRampPoint(
                degree=degree,
                bucketed=bucketed,
                bucket_count=report.bucket_nodes_created,
                traversal_ms=elapsed_ms,
                expanded_nodes=result.metrics.expanded_nodes,
                bucket_hops_unwrapped=result.metrics.bucket_hops_unwrapped,
                raw_bucket_leaks=leaks,
                logical_neighbors_returned=logical_neighbors,
            )
        )

    return SupernodeRampResult(
        hub_id=hub_id,
        threshold=threshold,
        num_buckets=num_buckets,
        switch_triggered_at_degree=switch_degree,
        points=points,
    )


async def run_hot_path_demo(
    *,
    hits: int = 4,
    threshold: int = 3,
) -> HotPathDemoResult:
    """Run repeated derived traversals showing automatic hot-path materialization."""
    store = InMemoryGraphStore()
    coordinator = QueryCoordinator(store, hot_path_threshold=threshold)
    await coordinator.upsert_nodes(
        [
            EntityNode(
                id="customer:org:acme",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="customer:org:beta",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="technology:lang:python",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
        ]
    )
    await coordinator.upsert_edges(
        [
            RelationshipEdge(
                source_id="customer:org:acme",
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="customer:org:beta",
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
        ]
    )

    steps: list[HotPathStep] = []
    promoted_at: int | None = None
    for hit in range(1, hits + 1):
        started = time.perf_counter()
        result = await coordinator.traverse(
            TraversalRequest(
                seed_ids=["customer:org:acme"],
                max_depth=1,
                include_derived=True,
            )
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
        derived_count = sum(1 for e in result.edges if e.is_derived)
        promoted_count = len(coordinator.promoted_hot_paths)
        is_mat = promoted_count > 0
        if is_mat and promoted_at is None:
            promoted_at = hit
        steps.append(
            HotPathStep(
                hit_number=hit,
                derived_edges_returned=derived_count,
                promoted_hot_paths=promoted_count,
                materialized=is_mat,
                execution_ms=elapsed_ms,
            )
        )

    return HotPathDemoResult(
        rule_name="similar_to_shared_neighbor",
        derived_edge_type="SIMILAR_TO",
        hot_path_threshold=threshold,
        promoted_at_hit=promoted_at,
        steps=steps,
    )


async def run_federation_demo(
    *,
    shares: tuple[float, ...] = (0.10, 0.25, 0.40),
) -> FederationDemoResult:
    """Demonstrate Hop-0 O(1) prefix parsing and Ghost/Proxy cross-shard federation."""
    sample_id = "customer:org:acme"
    parsed = parse_canonical_id(sample_id)
    assert parsed is not None

    points: list[FederationSharePoint] = []
    for share in shares:
        shards: dict[Domain, GraphStore] = {d: InMemoryGraphStore() for d in Domain}
        coordinator = QueryCoordinator(shards)
        dataset = PowerLawGraphGenerator(
            preset=ScalePreset.MICRO,
            cross_domain_edge_share=share,
            seed=42,
        ).generate(bucket_supernodes=False)
        await coordinator.upsert_nodes(dataset.nodes)
        await coordinator.upsert_edges(dataset.edges)

        proxy_count = 0
        for shard in shards.values():
            st = await shard.stats()
            proxy_count += st.proxy_count

        started = time.perf_counter()
        res = await coordinator.traverse(
            TraversalRequest(seed_ids=[dataset.seed_ids[0]], max_depth=2)
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)

        points.append(
            FederationSharePoint(
                cross_domain_share=share,
                ghost_proxies_created=proxy_count,
                cross_domain_hops=res.metrics.cross_domain_hops,
                single_domain_locality_ratio=round(
                    res.metrics.single_domain_locality_ratio, 3
                ),
                traversal_ms=elapsed_ms,
            )
        )

    return FederationDemoResult(
        sample_canonical_id=sample_id,
        parsed_domain=parsed.domain.value,
        parsed_entity_type=parsed.entity_type,
        parsed_slug=parsed.slug,
        points=points,
    )


async def run_guardrail_probe(probe_id: GuardrailProbeId) -> GuardrailProbeResult:
    """Execute one allowlisted real guardrail check on an isolated coordinator."""
    if probe_id == "forbidden_payload":
        enforcer = GuardrailEnforcer()
        bad_node = EntityNode(
            id="knowledge:doc:bloated",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            lakehouse_ref="s3://scale-kg/doc.parquet",
            routing_props={"content": "forbidden raw body blob"},
        )
        try:
            enforcer.validate_node_write(bad_node)
        except GuardrailViolationError as exc:
            return GuardrailProbeResult(
                probe_id=probe_id,
                title="Lean Graph Forbidden Payload Rejection",
                http_status=422,
                blocked=True,
                aborted=False,
                violation=exc.violation,
                summary="Rejected heavy 'content' key in routing_props before storage write.",
            )

    if probe_id == "missing_lakehouse_ref":
        enforcer = GuardrailEnforcer()
        bad_doc = EntityNode(
            id="knowledge:doc:orphan",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            routing_props={"version": 1},
        )
        try:
            enforcer.validate_node_write(bad_doc)
        except GuardrailViolationError as exc:
            return GuardrailProbeResult(
                probe_id=probe_id,
                title="Missing Lakehouse Pointer Rejection",
                http_status=422,
                blocked=True,
                aborted=False,
                violation=exc.violation,
                summary="Content-bearing Document rejected without lakehouse_ref URI.",
            )

    if probe_id == "preflight_depth_cap":
        enforcer = GuardrailEnforcer()
        req = TraversalRequest.model_construct(
            seed_ids=["customer:org:acme"],
            max_depth=4,
            max_results=500,
            timeout_seconds=2.0,
        )
        try:
            enforcer.validate_traversal_request(req)
        except GuardrailViolationError as exc:
            return GuardrailProbeResult(
                probe_id=probe_id,
                title="Pre-Flight Depth > 3 Guardrail Block",
                http_status=422,
                blocked=True,
                aborted=False,
                violation=exc.violation,
                summary="Blocked depth=4 traversal pre-flight (hard cap max_depth <= 3).",
            )

    # runtime_expansion_abort
    tight_enforcer = GuardrailEnforcer(GuardrailConfig(max_expanded_nodes=5))
    store = InMemoryGraphStore(enforcer=tight_enforcer)
    nodes = [
        EntityNode(id="customer:org:root", domain=Domain.CUSTOMER, entity_type="Organization")
    ]
    edges: list[RelationshipEdge] = []
    for i in range(12):
        leaf = f"technology:svc:n-{i}"
        nodes.append(
            EntityNode(id=leaf, domain=Domain.TECHNOLOGY, entity_type="Service")
        )
        edges.append(
            RelationshipEdge(
                source_id="customer:org:root",
                target_id=leaf,
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            )
        )
    await store.upsert_nodes(nodes)
    await store.upsert_edges(edges)
    res = await store.traverse_k_hop(
        TraversalRequest(seed_ids=["customer:org:root"], max_depth=2)
    )
    violation = res.metrics.violation or GuardrailViolation(
        code=GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED,
        message="Expanded node cap reached.",
        limit=5,
        observed=res.metrics.expanded_nodes,
    )
    return GuardrailProbeResult(
        probe_id="runtime_expansion_abort",
        title="Runtime Expansion Cap Partial Abort",
        http_status=200,
        blocked=False,
        aborted=res.metrics.aborted,
        violation=violation,
        nodes_returned=len(res.nodes),
        expanded_nodes=res.metrics.expanded_nodes,
        summary="Returned bounded partial result with aborted=True instead of OOM.",
    )


async def run_hybrid_rag_demo(
    *,
    warehouse_dir: Path | None = None,
) -> HybridRagDemoResult:
    """Run the 5-stage Seed-Then-Expand Hybrid GraphRAG pipeline with Parquet Lakehouse."""
    with tempfile.TemporaryDirectory(prefix="scale_kg_showcase_lh_") as tmp:
        lh_dir = warehouse_dir or Path(tmp)
        store = InMemoryGraphStore()
        coordinator = QueryCoordinator(store)
        lakehouse = LakehouseContentStore(lh_dir)
        vector_index = InMemoryVectorIndex()

        nodes = [
            EntityNode(
                id="customer:org:acme",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="technology:lang:python",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
        ]
        docs: list[LakehouseDocument] = []
        edges: list[RelationshipEdge] = [
            RelationshipEdge(
                source_id="customer:org:acme",
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            )
        ]

        total_docs = 12
        for idx in range(total_docs):
            doc_id = f"knowledge:doc:guide-{idx}"
            ref = f"lakehouse://knowledge/guide-{idx}.parquet"
            nodes.append(
                EntityNode(
                    id=doc_id,
                    domain=Domain.KNOWLEDGE,
                    entity_type="Document",
                    lakehouse_ref=ref,
                )
            )
            edges.append(
                RelationshipEdge(
                    source_id="technology:lang:python",
                    target_id=doc_id,
                    edge_type="DOCUMENTED_BY",
                    domain=Domain.TECHNOLOGY,
                    target_domain=Domain.KNOWLEDGE,
                )
            )
            body = (
                f"Kinetic-V GraphRAG architecture guide #{idx}: 16-way supernode bucketing "
                "and Parquet lakehouse selective hydration. " * 18
            )
            docs.append(
                LakehouseDocument(
                    lakehouse_ref=ref,
                    entity_id=doc_id,
                    domain=Domain.KNOWLEDGE,
                    title=f"Architecture Guide #{idx}",
                    content=body,
                )
            )
            vector_index.upsert(
                doc_id,
                [max(0.05, 1.0 - idx * 0.08), 0.25 + idx * 0.03, 0.1],
                domain=Domain.KNOWLEDGE,
            )

        vector_index.upsert("customer:org:acme", [0.98, 0.20, 0.10], domain=Domain.CUSTOMER)
        await coordinator.upsert_nodes(nodes)
        await coordinator.upsert_edges(edges)
        lakehouse.put_documents(docs)

        router = HybridGraphRAGRouter(
            coordinator=coordinator,
            vector_index=vector_index,
            lakehouse=lakehouse,
        )
        result = await router.retrieve(
            HybridRAGRequest(
                query_text="How does Kinetic-V scale GraphRAG across supernodes?",
                query_embedding=[0.99, 0.22, 0.10],
                seed_k=3,
                max_depth=2,
                top_n=3,
            )
        )

        naive_bytes = sum(len(d.content.encode("utf-8")) for d in docs)
        hydrated_bytes = sum(
            len(h.document.content.encode("utf-8"))
            for h in result.hits
            if h.document is not None
        )
        hydrated_docs = sum(1 for h in result.hits if h.document is not None)
        savings = (
            round((1.0 - (hydrated_bytes / naive_bytes)) * 100.0, 1)
            if naive_bytes > 0
            else 0.0
        )

        return HybridRagDemoResult(
            query_text="How does Kinetic-V scale GraphRAG across supernodes?",
            seed_count=len(result.seed_hits),
            graph_candidates=len(result.traversal.nodes),
            hydrated_documents=hydrated_docs,
            total_lakehouse_documents=total_docs,
            hydrated_bytes=hydrated_bytes,
            naive_full_graph_bytes=naive_bytes,
            payload_savings_pct=savings,
            stage_timings_ms=dict(result.stage_timings_ms),
            top_hit_ids=[h.entity_id for h in result.hits],
            citation_count=len(result.citations),
        )


def _make_adapter_factories(
    adapters: Sequence[BackendType],
    temp_root: Path,
) -> dict[str, Callable[[], GraphStore]]:
    factories: dict[str, Callable[[], GraphStore]] = {}
    counter = 0

    for name in adapters:
        if name == "in_memory":
            factories[name] = lambda: InMemoryGraphStore()
        elif name == "iceberg_puppygraph":

            def _iceberg_factory() -> GraphStore:
                nonlocal counter
                counter += 1
                wh = temp_root / f"iceberg_{counter}"
                wh.mkdir(parents=True, exist_ok=True)
                return IcebergPuppyGraphStore(warehouse_dir=wh)

            factories[name] = _iceberg_factory
    return factories


async def run_preset_benchmark(
    preset: ScalePreset,
    *,
    adapters: Sequence[BackendType] = ("in_memory", "iceberg_puppygraph"),
    seed: int = 42,
    temp_root: Path | None = None,
) -> PresetBenchmarkBundle:
    """Run the 7-workload BenchmarkSuite and Gate 2 ADR scorer for one preset."""
    with tempfile.TemporaryDirectory(prefix=f"scale_kg_bench_{preset.value}_") as tmp:
        root = temp_root or Path(tmp)
        factories = _make_adapter_factories(adapters, root)
        suite = BenchmarkSuite(preset=preset, seed=seed)
        suite_result = await suite.run(factories, run_golden_parity=len(factories) >= 2)
        scorecards = rank_engines(suite_result)
        eligible = [c for c in scorecards if c.eligible_for_production]
        winner = eligible[0].adapter_name if eligible else scorecards[0].adapter_name
        adr_md = generate_gate2_adr(suite_result)
        return PresetBenchmarkBundle(
            preset=preset,
            suite_result=suite_result,
            scorecards=scorecards,
            winner_adapter=winner,
            adr_markdown=adr_md,
        )


async def build_showcase_snapshot(
    *,
    presets: Sequence[ScalePreset] = (ScalePreset.MICRO,),
    adapters: Sequence[BackendType] = ("in_memory", "iceberg_puppygraph"),
    seed: int = 42,
    probe_remote: bool = False,
    git_sha: str | None = None,
    git_ref: str | None = None,
    config: ScaleKGConfig | None = None,
    routes: Sequence[str] | None = None,
) -> ShowcaseSnapshot:
    """Assemble a complete canonical ShowcaseSnapshot from live code and benchmarks."""
    introspection = introspect_system(
        config=config,
        routes=routes,
        probe_remote=probe_remote,
        git_sha=git_sha,
        git_ref=git_ref,
    )

    probes: dict[GuardrailProbeId, GuardrailProbeResult] = {}
    for pid in (
        "forbidden_payload",
        "missing_lakehouse_ref",
        "preflight_depth_cap",
        "runtime_expansion_abort",
    ):
        probes[pid] = await run_guardrail_probe(pid)

    scenarios = ScenarioBundle(
        supernode_ramp=await run_supernode_ramp(),
        hot_path=await run_hot_path_demo(),
        federation=await run_federation_demo(),
        guardrail_probes=probes,
        hybrid_rag=await run_hybrid_rag_demo(),
    )

    benchmarks: dict[str, PresetBenchmarkBundle] = {}
    for preset in presets:
        benchmarks[preset.value] = await run_preset_benchmark(
            preset,
            adapters=adapters,
            seed=seed,
        )

    default_preset = presets[0].value if presets else "micro"
    return ShowcaseSnapshot(
        introspection=introspection,
        scenarios=scenarios,
        benchmarks=benchmarks,
        default_preset=default_preset,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_showcase_scenarios.py -v`
Expected: PASS (6 passed)

- [ ] **Step 5: Commit**

```bash
git add src/scale_kg/showcase/scenarios.py tests/test_showcase_scenarios.py
git commit -m "feat(showcase): add executable switch, load-ramp, guardrail, and RAG scenarios"
```

---

### Task 3: 50-Concurrent-User Backend Service & Single-Flight Job Runner

**Files:**
- Create: `src/scale_kg/showcase/concurrency.py`
- Test: `tests/test_showcase_concurrency.py`

**Interfaces:**
- Consumes: `scale_kg.showcase.snapshot.*`, `scale_kg.showcase.scenarios.*`, `scale_kg.benchmark.golden_queries.GoldenQuerySuite`, `FROZEN_GOLDEN_QUERIES`.
- Produces:
  - `BackpressureError(Exception)` with `.retry_after_seconds: int`
  - `SingleFlightJobRunner(max_concurrent: int = 2, max_pending: int = 8, cache_ttl_seconds: float = 300.0, max_cache_entries: int = 32)` with `async def run(key: str, fn: Callable[[], PresetBenchmarkBundle]) -> tuple[PresetBenchmarkBundle, bool, bool]` (returns `(bundle, cache_hit, coalesced)`)
  - `ShowcaseService(snapshot_path: Path | None = None, history_path: Path | None = None, config: ScaleKGConfig | None = None, light_concurrency: int = 50, light_timeout_seconds: float = 2.0, job_runner: SingleFlightJobRunner | None = None)` with:
    - `async def get_snapshot_bytes() -> tuple[bytes, str]` (pre-serialized UTF-8 JSON + sha256 ETag)
    - `async def get_history() -> list[CommitTrendEntry]`
    - `async def simulate_adapter_switch(req: AdapterSwitchRequest) -> AdapterSwitchResponse`
    - `async def simulate_guardrail(req: GuardrailProbeRequest) -> GuardrailProbeResult`
    - `async def simulate_hot_path(req: HotPathRequest) -> HotPathDemoResult`
    - `async def simulate_hybrid() -> HybridRagDemoResult`
    - `async def run_ramp(req: RampRunRequest) -> RampRunResponse`

- [ ] **Step 1: Write the failing 50-concurrent-user service tests**

Create `tests/test_showcase_concurrency.py`:

```python
"""Tests for 50-concurrent-user scalability, single-flight coalescing, and backpressure."""

from __future__ import annotations

import asyncio
import time

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
        *[runner.run("micro:in_memory:42", slow_job) for _ in range(50)]  # type: ignore[arg-type]
    )

    assert calls == 1
    assert len(results) == 50
    coalesced_count = sum(1 for _, _, coalesced in results if coalesced)
    assert coalesced_count == 49


@pytest.mark.asyncio
async def test_single_flight_raises_backpressure_when_queue_saturated() -> None:
    bundle = await run_preset_benchmark(ScalePreset.MICRO, adapters=("in_memory",))

    def slow_job() -> object:
        time.sleep(0.15)
        return bundle

    runner = SingleFlightJobRunner(max_concurrent=1, max_pending=1)
    tasks = [
        asyncio.create_task(runner.run(f"key-{idx}", slow_job))  # type: ignore[arg-type]
        for idx in range(5)
    ]
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    backpressure = [o for o in outcomes if isinstance(o, BackpressureError)]
    assert len(backpressure) >= 1
    assert backpressure[0].retry_after_seconds >= 1


@pytest.mark.asyncio
async def test_showcase_service_serves_50_concurrent_users_under_sla() -> None:
    service = ShowcaseService()
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
            hp = await service.simulate_hot_path(HotPathRequest(hits=3, threshold=3))
            assert hp.promoted_at_hit == 3
        return (time.perf_counter() - started) * 1000.0

    latencies = await asyncio.gather(*[user_session(i) for i in range(50)])
    assert len(latencies) == 50
    ordered = sorted(latencies)
    p95_ms = ordered[int(0.95 * (len(ordered) - 1))]
    assert p95_ms < 250.0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_showcase_concurrency.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scale_kg.showcase.concurrency'`

- [ ] **Step 3: Implement `src/scale_kg/showcase/concurrency.py`**

Create `src/scale_kg/showcase/concurrency.py`:

```python
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
from typing import TypeVar

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
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._max_pending = max_pending
        self._cache_ttl = cache_ttl_seconds
        self._max_cache_entries = max_cache_entries
        self._retry_after = retry_after_seconds
        self._inflight: dict[str, asyncio.Future[object]] = {}
        self._cache: OrderedDict[str, tuple[float, object]] = OrderedDict()
        self._pending_count = 0

    def _get_cached(self, key: str) -> object | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            self._cache.pop(key, None)
            return None
        self._cache.move_to_end(key)
        return value

    def _set_cached(self, key: str, value: object) -> None:
        self._cache[key] = (time.monotonic() + self._cache_ttl, value)
        self._cache.move_to_end(key)
        while len(self._cache) > self._max_cache_entries:
            self._cache.popitem(last=False)

    async def run(self, key: str, fn: Callable[[], T]) -> tuple[T, bool, bool]:
        """Run or coalesce ``fn`` keyed by ``key``. Returns ``(value, cache_hit, coalesced)``."""
        cached = self._get_cached(key)
        if cached is not None:
            return cached, True, False  # type: ignore[return-value]

        existing = self._inflight.get(key)
        if existing is not None:
            result = await asyncio.shield(existing)
            return result, False, True  # type: ignore[return-value]

        if self._pending_count >= self._max_pending:
            raise BackpressureError(
                "Showcase benchmark worker queue is at capacity; retry shortly.",
                retry_after_seconds=self._retry_after,
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[object] = loop.create_future()
        self._inflight[key] = future
        self._pending_count += 1

        try:
            async with self._semaphore:
                value = await asyncio.to_thread(fn)
            self._set_cached(key, value)
            if not future.done():
                future.set_result(value)
            return value, False, False
        except BaseException as exc:
            if not future.done():
                future.set_exception(exc)
            raise
        finally:
            self._inflight.pop(key, None)
            self._pending_count = max(0, self._pending_count - 1)


def _run_benchmark_sync(req: RampRunRequest) -> PresetBenchmarkBundle:
    """Bridge async PresetBenchmarkBundle execution inside a worker thread."""
    return asyncio.run(
        run_preset_benchmark(
            ScalePreset(req.preset),
            adapters=tuple(req.adapters),
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
        self._golden_coordinators: dict[str, QueryCoordinator] = {}
        self._history_cache: list[CommitTrendEntry] = []
        self._history_mtime: float = -1.0

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
                self._playground_dir = tempfile.TemporaryDirectory(
                    prefix="scale_kg_showcase_pg_"
                )
                iceberg_dir = Path(self._playground_dir.name) / "iceberg"
                iceberg_dir.mkdir(parents=True, exist_ok=True)
                stores: dict[str, GraphStore] = {
                    "in_memory": InMemoryGraphStore(),
                    "iceberg_puppygraph": IcebergPuppyGraphStore(warehouse_dir=iceberg_dir),
                }
                await GoldenQuerySuite().load_dataset(stores)
                self._golden_stores = stores
                self._golden_coordinators = {
                    name: QueryCoordinator(store) for name, store in stores.items()
                }

    async def get_snapshot_bytes(self) -> tuple[bytes, str]:
        """Return immutable pre-serialized snapshot bytes and strong ETag."""
        await self._ensure_initialized()
        assert self._snapshot_bytes is not None and self._snapshot_etag is not None
        return self._snapshot_bytes, self._snapshot_etag

    async def get_history(self) -> list[CommitTrendEntry]:
        """Return rolling commit trend entries from history.json if present."""
        if self._history_path is None or not self._history_path.is_file():
            return []
        mtime = self._history_path.stat().st_mtime
        if mtime != self._history_mtime:
            data = json.loads(self._history_path.read_text(encoding="utf-8"))
            self._history_cache = [CommitTrendEntry.model_validate(item) for item in data]
            self._history_mtime = mtime
        return list(self._history_cache)

    async def simulate_adapter_switch(
        self, req: AdapterSwitchRequest
    ) -> AdapterSwitchResponse:
        """Compare 1-2 pre-seeded read-only adapters on one frozen Golden Query."""
        await self._ensure_initialized()
        spec = next(q for q in FROZEN_GOLDEN_QUERIES if q.query_id == req.query_id)

        async def _exec() -> AdapterSwitchResponse:
            items: list[AdapterSwitchItem] = []
            for name in req.adapters:
                started = time.perf_counter()
                if spec.requires_coordinator:
                    res = await self._golden_coordinators[name].traverse(spec.request)
                else:
                    res = await self._golden_stores[name].traverse_k_hop(spec.request)
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
                node_ids = sorted(n.id for n in res.nodes)
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_showcase_concurrency.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: Commit**

```bash
git add src/scale_kg/showcase/concurrency.py tests/test_showcase_concurrency.py
git commit -m "feat(showcase): add 50-user ShowcaseService and SingleFlightJobRunner"
```

---

### Task 4: FastAPI Showcase Router & `create_app()` Integration

**Files:**
- Create: `src/scale_kg/showcase/router.py`
- Modify: `src/scale_kg/api/app.py:68-100,280-290`
- Modify: `src/scale_kg/showcase/__init__.py`
- Test: `tests/test_showcase_concurrency.py` (append ASGI 50-client HTTP tests)

**Interfaces:**
- Consumes: `scale_kg.showcase.concurrency.ShowcaseService`, `BackpressureError`; `scale_kg.api.state.verify_api_key_token`.
- Produces:
  - `create_showcase_router(service: ShowcaseService, *, api_key: str | None = None, require_auth: bool = False) -> APIRouter`
  - `scale_kg.api.app.create_app(..., enable_showcase: bool = True, showcase_snapshot_path: str | Path | None = None, showcase_require_auth: bool = False)`

- [ ] **Step 1: Add failing ASGI HTTP tests to `tests/test_showcase_concurrency.py`**

Append to `tests/test_showcase_concurrency.py`:

```python
@pytest.mark.asyncio
async def test_fastapi_showcase_handles_50_concurrent_http_clients_and_etag() -> None:
    import httpx
    from scale_kg.api.app import create_app

    app = create_app(enable_showcase=True)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        first = await client.get("/showcase/api/snapshot")
        assert first.status_code == 200
        etag = first.headers["etag"]

        not_mod = await client.get(
            "/showcase/api/snapshot", headers={"If-None-Match": etag}
        )
        assert not_mod.status_code == 304

        traversal = await client.get("/showcase/assets/../../pyproject.toml")
        assert traversal.status_code == 404

        async def _hit(idx: int) -> int:
            if idx % 2 == 0:
                resp = await client.get("/showcase/api/snapshot")
                return resp.status_code
            resp = await client.post(
                "/showcase/api/simulate/guardrail",
                json={"probe_id": "preflight_depth_cap"},
            )
            return resp.status_code

        statuses = await asyncio.gather(*[_hit(i) for i in range(50)])
        assert statuses == [200] * 50

    # Verify optional API-key enforcement when showcase_require_auth=True
    auth_app = create_app(
        enable_showcase=True,
        api_key="test-secret-key",
        showcase_require_auth=True,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=auth_app), base_url="http://test"
    ) as auth_client:
        unauth = await auth_client.get("/showcase/api/snapshot")
        assert unauth.status_code == 401
        ok = await auth_client.get(
            "/showcase/api/snapshot", headers={"X-API-Key": "test-secret-key"}
        )
        assert ok.status_code == 200
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_showcase_concurrency.py::test_fastapi_showcase_handles_50_concurrent_http_clients_and_etag -v`
Expected: FAIL with `TypeError: create_app() got an unexpected keyword argument 'enable_showcase'`

- [ ] **Step 3: Implement `src/scale_kg/showcase/router.py` and mount in `src/scale_kg/api/app.py`**

Create `src/scale_kg/showcase/router.py`:

```python
"""FastAPI router serving the zero-build showcase dashboard and live simulation API."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from fastapi.responses import FileResponse, JSONResponse

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

STATIC_DIR = Path(__file__).resolve().parent / "static"

_ALLOWED_ASSETS: dict[str, str] = {
    "styles.css": "text/css; charset=utf-8",
    "app.js": "application/javascript; charset=utf-8",
    "charts.js": "application/javascript; charset=utf-8",
    "views.js": "application/javascript; charset=utf-8",
}


def create_showcase_router(
    service: ShowcaseService,
    *,
    api_key: str | None = None,
    require_auth: bool = False,
) -> APIRouter:
    """Create the FastAPI router for `/showcase` UI and `/showcase/api/*` endpoints."""

    async def _check_auth(
        x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
        authorization: Annotated[str | None, Header(alias="Authorization")] = None,
    ) -> None:
        if not require_auth:
            return
        if not verify_api_key_token(
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
    @router.get("/", include_in_schema=False)
    async def showcase_index() -> FileResponse:
        index_path = STATIC_DIR / "index.html"
        if not index_path.is_file():
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
        target = STATIC_DIR / filename
        if not target.is_file():
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
        if if_none_match and if_none_match.strip() == etag:
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
    ) -> AdapterSwitchResponse:
        return await service.simulate_adapter_switch(request)

    @router.post("/api/simulate/guardrail", response_model=GuardrailProbeResult)
    async def simulate_guardrail(
        request: GuardrailProbeRequest,
    ) -> GuardrailProbeResult:
        return await service.simulate_guardrail(request)

    @router.post("/api/simulate/hot-path", response_model=HotPathDemoResult)
    async def simulate_hot_path(request: HotPathRequest) -> HotPathDemoResult:
        return await service.simulate_hot_path(request)

    @router.post("/api/simulate/hybrid", response_model=HybridRagDemoResult)
    async def simulate_hybrid() -> HybridRagDemoResult:
        return await service.simulate_hybrid()

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
```

Modify `src/scale_kg/api/app.py`:
- Add keyword arguments to `create_app()`:
  ```python
    enable_showcase: bool = True,
    showcase_snapshot_path: str | Path | None = None,
    showcase_require_auth: bool = False,
  ```
- Before `return app`, mount the showcase router when `enable_showcase=True`:
  ```python
    if enable_showcase:
        from scale_kg.showcase.concurrency import ShowcaseService
        from scale_kg.showcase.router import create_showcase_router

        resolved_snapshot = (
            Path(showcase_snapshot_path)
            if showcase_snapshot_path is not None
            else (
                Path(os.environ["SCALE_KG_SHOWCASE_SNAPSHOT"])
                if os.environ.get("SCALE_KG_SHOWCASE_SNAPSHOT")
                else None
            )
        )
        history_path = (
            resolved_snapshot.parent / "history.json"
            if resolved_snapshot is not None
            else None
        )
        showcase_service = ShowcaseService(
            snapshot_path=resolved_snapshot,
            history_path=history_path,
            config=config,
        )
        app.state.showcase = showcase_service
        app.include_router(
            create_showcase_router(
                showcase_service,
                api_key=state.api_key,
                require_auth=showcase_require_auth,
            )
        )
  ```
- Update `src/scale_kg/showcase/__init__.py` to also export `ShowcaseService` and `create_showcase_router`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_showcase_concurrency.py tests/test_api_service.py -v`
Expected: PASS (all showcase concurrency + existing API tests pass)

- [ ] **Step 5: Commit**

```bash
git add src/scale_kg/showcase/router.py src/scale_kg/showcase/__init__.py src/scale_kg/api/app.py tests/test_showcase_concurrency.py
git commit -m "feat(showcase): mount showcase FastAPI router with ETag and backpressure"
```

---

### Task 5: Zero-Build 5-Tab Interactive Dashboard UI

**Files:**
- Create: `src/scale_kg/showcase/static/index.html`
- Create: `src/scale_kg/showcase/static/styles.css`
- Create: `src/scale_kg/showcase/static/charts.js`
- Create: `src/scale_kg/showcase/static/views.js`
- Create: `src/scale_kg/showcase/static/app.js`

**Interfaces:**
- Consumes: `ShowcaseSnapshot` JSON (from `/showcase/api/snapshot` in live mode or `./snapshot.json` in static mode), `CommitTrendEntry[]` (from `/showcase/api/history` or `./history.json`), and live `POST /showcase/api/simulate/*` + `/showcase/api/ramp/run` endpoints.
- Produces: Accessible, zero-dependency 5-tab SPA served identically by FastAPI and Netlify.

- [ ] **Step 1: Create `index.html` and `styles.css`**

Create `src/scale_kg/showcase/static/index.html`:
- Sets `<body data-mode="live">` (overwritten to `data-mode="static"` by `build.py` during static export).
- Uses relative asset paths `./assets/styles.css` / `./assets/app.js` in live mode (and `./styles.css` / `./app.js` in static export rewritten cleanly by `build.py` — OR even cleaner: keep static files at `./styles.css` and mount `/showcase/{filename}` or let `build.py` place assets in `dist/showcase/assets/` so the EXACT same `index.html` works untouched in both live `/showcase/` and static `dist/showcase/`! **Decision:** place assets under `dist/showcase/assets/` in `build.py` so `index.html` uses `./assets/styles.css` and `./assets/app.js` identically in BOTH modes!).
- Header displays product title (`Agent Nexus KG — Scale Showcase`), live/static mode badge, git SHA, build timestamp, config fingerprint, and 5 ARIA tab buttons (`System Map`, `Load Escalator`, `Adapter Switchboard`, `Hybrid GraphRAG`, `Commit Trends`).
- Main region `#app-content` with `role="tabpanel"` and `aria-live="polite"` status banner.

- Create `src/scale_kg/showcase/static/styles.css`:
- Clean high-contrast engineering dashboard theme (system monospace + sans-serif stack, zero external fonts/CDNs), CSS grid responsive layout, accessible focus rings (`:focus-visible`), semantic status pills (`ok`, `warn`, `danger`, `stub`, `live`), interactive card states, and mobile breakpoints.
- Strict CSP compliance: zero inline `<script>`, zero inline `onclick=` handlers, and zero inline `style="..."` attributes in HTML/JS (use CSS classes and SVG presentation attributes `fill`/`stroke`/`width`/`height` only so `netlify.toml` strict `style-src 'self'; script-src 'self'` passes with zero console violations).

- [ ] **Step 2: Create `charts.js` (Zero-Dependency Inline SVG Charts)**

Create `src/scale_kg/showcase/static/charts.js` exporting pure functions returning SVG markup strings (all numeric values sanitized, ARIA `role="img"` + `<title>`):
- `renderLatencyBarChart(byDepth, budgets)` — grouped 1/2/3-hop P50/P95/P99 bars with dashed Gate 2 SLA budget markers (25ms / 100ms / 300ms).
- `renderLineChart(points, { xKey, yKey, thresholdX, title, yUnit })` — supernode degree ramp & QPS ramp charts with vertical switch-threshold annotation line (`16-way bucketing activates`).
- `renderWaterfallChart(stageTimingsMs)` — 5-stage Hybrid GraphRAG latency breakdown bars.
- `renderSparkline(values, { label, unit, color })` — compact multi-commit trend SVG sparklines for `history.json`.

- [ ] **Step 3: Create `views.js` (5 Self-Explanatory Tab Renderers)**

Create `src/scale_kg/showcase/static/views.js` exporting:
- `renderSystemMap(snapshot, state)` — interactive 4-layer architecture map + live introspected threshold cards (`switch_rules`) + clickable layer detail drawer showing exact Python module & active config limits.
- `renderLoadEscalator(snapshot, state)` — preset scale ladder selector (`micro` / `local` + live `Run Coalesced Ramp` button), supernode degree ramp SVG + table showing exact threshold flip & `0` leaks, interactive derived-edge hot-path 3-hit stepper, cross-domain federation locality chart, and 4 clickable Guardrail Stress Trigger buttons showing live `422` / `aborted=True` payloads.
- `renderAdapterSwitchboard(snapshot, state)` — engine catalog table (with clear `Verified Local` / `Live Remote` / `Offline` / `Contract Stub` badges), interactive Golden Query selector (`GQ-01`..`GQ-07`) + `Compare Adapters` button, parity matrix, 7-workload benchmark charts, interactive `100M / 1B / 10B` scale-target selector dynamically updating projected cost/latency and Gate 2 weighted scorecard + winner ADR rationale.
- `renderHybridRag(snapshot, state)` — 5-stage Seed-Then-Expand trace waterfall + selective top-N Parquet hydration vs naive full-graph byte savings bar + grounded citation list + `Re-run Live Trace` button.
- `renderCommitTrends(snapshot, history)` — commit trend sparklines (1/2/3-hop P95, Peak QPS, Gate 2 Winner Score, parity streak) + build provenance table (git SHA, branch, timestamp, config fingerprint).

- [ ] **Step 4: Create `app.js` (Live/Static Controller)**

Create `src/scale_kg/showcase/static/app.js`:
- Detects mode via `document.body.dataset.mode` (`live` vs `static`).
- Fetches snapshot & history from `/showcase/api/snapshot` + `/showcase/api/history` in live mode (with graceful fallback to `./snapshot.json` + `./history.json` if static).
- Wires all interactive controls:
  - In `live` mode: calls `POST /showcase/api/simulate/*` and `POST /showcase/api/ramp/run` (handling `429 Retry-After` with clean user notice).
  - In `static` mode: replays deterministic pre-computed scenario results and runs client-side `100M / 1B / 10B` scale/cost/scorecard projections seamlessly without throwing network errors.
- Escapes all dynamic text via an `escapeHtml()` helper (preventing XSS).

- [ ] **Step 5: Smoke-test UI serving via FastAPI**

Run: `uv run python -c "from fastapi.testclient import TestClient; from scale_kg.api.app import create_app; c = TestClient(create_app()); r1 = c.get('/showcase'); r2 = c.get('/showcase/assets/app.js'); assert r1.status_code == 200 and r2.status_code == 200; print('UI OK')"`
Expected: `UI OK`

- [ ] **Step 6: Commit**

```bash
git add src/scale_kg/showcase/static/
git commit -m "feat(showcase): add zero-build 5-tab interactive dashboard UI"
```

---

### Task 6: Static Exporter CLI, Commit History Merger & PR Delta Generator

**Files:**
- Create: `src/scale_kg/showcase/build.py`
- Modify: `src/scale_kg/showcase/__init__.py`
- Test: `tests/test_showcase_build.py`

**Interfaces:**
- Consumes: `scale_kg.showcase.scenarios.build_showcase_snapshot`, `scale_kg.showcase.snapshot.*`, `scale_kg.showcase.router.STATIC_DIR`.
- Produces:
  - `extract_trend_entry(snapshot: ShowcaseSnapshot, *, preset: str | None = None) -> CommitTrendEntry`
  - `merge_history(existing: Sequence[CommitTrendEntry], new_entry: CommitTrendEntry, *, max_entries: int = 100) -> list[CommitTrendEntry]`
  - `render_pr_delta_markdown(current: CommitTrendEntry, previous: CommitTrendEntry | None, *, preview_url: str | None = None) -> str`
  - `async def build_showcase_site(*, out_dir: Path, presets: Sequence[ScalePreset] = (ScalePreset.MICRO, ScalePreset.LOCAL), history_in: Path | None = None, git_sha: str | None = None, git_ref: str | None = None, summary_out: Path | None = None, preview_url: str | None = None) -> ShowcaseSnapshot`
  - CLI: `python -m scale_kg.showcase.build --presets micro,local --out dist/showcase [--history-in ...] [--git-sha ...] [--git-ref ...] [--summary-out ...] [--preview-url ...]`

- [ ] **Step 1: Write the failing static build tests**

Create `tests/test_showcase_build.py`:

```python
"""Tests for static Netlify bundle export, history merging, and PR delta summaries."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.showcase.build import (
    build_showcase_site,
    extract_trend_entry,
    merge_history,
    render_pr_delta_markdown,
)
from scale_kg.showcase.snapshot import CommitTrendEntry, ShowcaseSnapshot


@pytest.mark.asyncio
async def test_build_showcase_site_produces_self_contained_static_bundle(
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "showcase"
    summary_path = tmp_path / "summary.md"

    snap = await build_showcase_site(
        out_dir=out_dir,
        presets=(ScalePreset.MICRO,),
        git_sha="abc123456789",
        git_ref="main",
        summary_out=summary_path,
        preview_url="https://pr-12--scale-kg.netlify.app",
    )

    assert (out_dir / "index.html").is_file()
    assert (out_dir / "snapshot.json").is_file()
    assert (out_dir / "history.json").is_file()
    for asset in ("styles.css", "app.js", "charts.js", "views.js"):
        assert (out_dir / "assets" / asset).is_file()

    html = (out_dir / "index.html").read_text(encoding="utf-8")
    assert 'data-mode="static"' in html
    assert "http://" not in html and "https://" not in html

    raw_snap = (out_dir / "snapshot.json").read_text(encoding="utf-8")
    parsed = ShowcaseSnapshot.model_validate_json(raw_snap)
    assert parsed.introspection.git_sha == "abc123456789"
    assert str(tmp_path) not in raw_snap

    history = json.loads((out_dir / "history.json").read_text(encoding="utf-8"))
    assert len(history) == 1
    assert history[0]["git_sha"] == "abc123456789"

    summary_md = summary_path.read_text(encoding="utf-8")
    assert "https://pr-12--scale-kg.netlify.app" in summary_md
    assert snap.benchmarks["micro"].winner_adapter in summary_md


def test_merge_history_deduplicates_sha_and_caps_at_100() -> None:
    entries = [
        CommitTrendEntry(
            git_sha=f"sha{i:04d}",
            git_ref="main",
            built_at="2026-11-24T00:00:00+00:00",
            config_fingerprint="abcd1234abcd1234",
            preset="micro",
            hop_1_p95_ms=1.0,
            hop_2_p95_ms=2.0,
            hop_3_p95_ms=3.0,
            peak_qps=100.0,
            winner_adapter="iceberg_puppygraph",
            winner_score=91.0,
            golden_parity_passed=True,
        )
        for i in range(105)
    ]
    merged = merge_history(entries[:-1], entries[-1], max_entries=100)
    assert len(merged) == 100
    assert merged[-1].git_sha == "sha0104"

    # Dedup same git_sha replaces previous record instead of duplicating
    updated = entries[-1].model_copy(update={"peak_qps": 222.0})
    deduped = merge_history(merged, updated, max_entries=100)
    assert len(deduped) == 100
    assert deduped[-1].peak_qps == 222.0

    delta_md = render_pr_delta_markdown(updated, merged[-2])
    assert "Peak QPS" in delta_md
    assert "222.0" in delta_md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_showcase_build.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scale_kg.showcase.build'`

- [ ] **Step 3: Implement `src/scale_kg/showcase/build.py`**

Create `src/scale_kg/showcase/build.py`:

```python
"""Static showcase exporter CLI, rolling history merger, and PR delta renderer."""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
from collections.abc import Sequence
from pathlib import Path

from scale_kg.benchmark.generator import ScalePreset
from scale_kg.showcase.router import STATIC_DIR
from scale_kg.showcase.scenarios import build_showcase_snapshot
from scale_kg.showcase.snapshot import CommitTrendEntry, ShowcaseSnapshot


def extract_trend_entry(
    snapshot: ShowcaseSnapshot,
    *,
    preset: str | None = None,
) -> CommitTrendEntry:
    """Extract a compact historical metric entry from a ShowcaseSnapshot."""
    chosen_key = preset or ("local" if "local" in snapshot.benchmarks else snapshot.default_preset)
    bundle = snapshot.benchmarks[chosen_key]
    winner_name = bundle.winner_adapter
    adapter_res = bundle.suite_result.adapters[winner_name]
    winner_card = next(
        c for c in bundle.scorecards if c.adapter_name == winner_name
    )
    by_depth = adapter_res.traversal_latency.by_depth
    parity_ok = (
        bundle.suite_result.golden_parity.passed
        if bundle.suite_result.golden_parity is not None
        else adapter_res.golden_parity_passed
    )
    return CommitTrendEntry(
        git_sha=snapshot.introspection.git_sha,
        git_ref=snapshot.introspection.git_ref,
        built_at=snapshot.introspection.built_at,
        config_fingerprint=snapshot.introspection.config_fingerprint,
        preset=chosen_key,
        hop_1_p95_ms=by_depth[1].p95_ms if 1 in by_depth else 0.0,
        hop_2_p95_ms=by_depth[2].p95_ms if 2 in by_depth else 0.0,
        hop_3_p95_ms=by_depth[3].p95_ms if 3 in by_depth else 0.0,
        peak_qps=adapter_res.concurrent_ramp.peak_qps,
        winner_adapter=winner_name,
        winner_score=winner_card.weighted_total,
        golden_parity_passed=parity_ok,
    )


def merge_history(
    existing: Sequence[CommitTrendEntry],
    new_entry: CommitTrendEntry,
    *,
    max_entries: int = 100,
) -> list[CommitTrendEntry]:
    """Append ``new_entry``, deduplicating by ``git_sha`` and capping length."""
    filtered = [e for e in existing if e.git_sha != new_entry.git_sha]
    filtered.append(new_entry)
    return filtered[-max_entries:]


def render_pr_delta_markdown(
    current: CommitTrendEntry,
    previous: CommitTrendEntry | None,
    *,
    preview_url: str | None = None,
) -> str:
    """Render a Markdown benchmark delta table for GitHub Actions job summary."""

    def _fmt_delta(curr: float, prev: float | None, unit: str = "") -> str:
        if prev is None:
            return f"{curr:.2f}{unit} (baseline)"
        diff = curr - prev
        sign = "+" if diff >= 0 else ""
        return f"{curr:.2f}{unit} ({sign}{diff:.2f}{unit})"

    lines = [
        "## Scale-KG Showcase Benchmark Delta",
        "",
        f"- **Commit:** `{current.git_sha}` (`{current.git_ref}`)",
        f"- **Preset:** `{current.preset}` | **Gate 2 Winner:** `{current.winner_adapter}` ({current.winner_score:.2f})",
        f"- **Config Fingerprint:** `{current.config_fingerprint}`"
        + (
            " (changed)"
            if previous is not None
            and previous.config_fingerprint != current.config_fingerprint
            else ""
        ),
    ]
    if preview_url:
        lines.append(f"- **Live Netlify Preview:** {preview_url}")

    lines.extend(
        [
            "",
            "| Metric | Value (Delta vs Previous) |",
            "| :--- | :--- |",
            f"| 1-Hop P95 | {_fmt_delta(current.hop_1_p95_ms, previous.hop_1_p95_ms if previous else None, ' ms')} |",
            f"| 2-Hop P95 | {_fmt_delta(current.hop_2_p95_ms, previous.hop_2_p95_ms if previous else None, ' ms')} |",
            f"| 3-Hop P95 | {_fmt_delta(current.hop_3_p95_ms, previous.hop_3_p95_ms if previous else None, ' ms')} |",
            f"| Peak QPS | {_fmt_delta(current.peak_qps, previous.peak_qps if previous else None)} |",
            f"| Gate 2 Winner Score | {_fmt_delta(current.winner_score, previous.winner_score if previous else None)} |",
            f"| Golden Query Parity | {'PASS' if current.golden_parity_passed else 'FAIL'} |",
            "",
        ]
    )
    return "\n".join(lines)


async def build_showcase_site(
    *,
    out_dir: Path,
    presets: Sequence[ScalePreset] = (ScalePreset.MICRO, ScalePreset.LOCAL),
    history_in: Path | None = None,
    git_sha: str | None = None,
    git_ref: str | None = None,
    summary_out: Path | None = None,
    preview_url: str | None = None,
) -> ShowcaseSnapshot:
    """Build a self-contained static showcase bundle suitable for Netlify deploy."""
    snapshot = await build_showcase_snapshot(
        presets=presets,
        git_sha=git_sha,
        git_ref=git_ref,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = out_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    index_html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    static_html = index_html.replace('data-mode="live"', 'data-mode="static"')
    (out_dir / "index.html").write_text(static_html, encoding="utf-8")

    for filename in ("styles.css", "app.js", "charts.js", "views.js"):
        shutil.copy2(STATIC_DIR / filename, assets_dir / filename)

    (out_dir / "snapshot.json").write_text(
        snapshot.model_dump_json(indent=2),
        encoding="utf-8",
    )

    prior_entries: list[CommitTrendEntry] = []
    if history_in is not None and history_in.is_file():
        try:
            raw_hist = json.loads(history_in.read_text(encoding="utf-8"))
            if isinstance(raw_hist, list):
                prior_entries = [CommitTrendEntry.model_validate(i) for i in raw_hist]
        except Exception:
            prior_entries = []

    current_entry = extract_trend_entry(snapshot)
    previous_entry = (
        next((e for e in reversed(prior_entries) if e.git_sha != current_entry.git_sha), None)
        if prior_entries
        else None
    )
    merged = merge_history(prior_entries, current_entry, max_entries=100)
    (out_dir / "history.json").write_text(
        json.dumps([e.model_dump(mode="json") for e in merged], indent=2),
        encoding="utf-8",
    )

    if summary_out is not None:
        summary_out.parent.mkdir(parents=True, exist_ok=True)
        summary_out.write_text(
            render_pr_delta_markdown(
                current_entry, previous_entry, preview_url=preview_url
            ),
            encoding="utf-8",
        )

    return snapshot


def main() -> None:
    """CLI entrypoint for building the static showcase site."""
    parser = argparse.ArgumentParser(
        description="Build static Scale-KG showcase dashboard & benchmark snapshot."
    )
    parser.add_argument("--presets", default="micro,local")
    parser.add_argument("--out", default="dist/showcase")
    parser.add_argument("--history-in", default=None)
    parser.add_argument("--git-sha", default=None)
    parser.add_argument("--git-ref", default=None)
    parser.add_argument("--summary-out", default=None)
    parser.add_argument("--preview-url", default=None)
    args = parser.parse_args()

    preset_enums = tuple(
        ScalePreset(p.strip()) for p in args.presets.split(",") if p.strip()
    )
    asyncio.run(
        build_showcase_site(
            out_dir=Path(args.out),
            presets=preset_enums,
            history_in=Path(args.history_in) if args.history_in else None,
            git_sha=args.git_sha,
            git_ref=args.git_ref,
            summary_out=Path(args.summary_out) if args.summary_out else None,
            preview_url=args.preview_url,
        )
    )


if __name__ == "__main__":
    main()
```

Update `src/scale_kg/showcase/__init__.py` to export `build_showcase_site`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_showcase_build.py -v`
Expected: PASS (2 passed)

- [ ] **Step 5: Commit**

```bash
git add src/scale_kg/showcase/build.py src/scale_kg/showcase/__init__.py tests/test_showcase_build.py
git commit -m "feat(showcase): add static bundle exporter, history merger, and PR delta CLI"
```

---

### Task 7: Netlify Config, CI Auto-Update Workflow, Makefile & Documentation

**Files:**
- Create: `netlify.toml`
- Create: `.github/workflows/showcase.yml`
- Modify: `.gitignore`
- Modify: `Makefile`
- Modify: `pyproject.toml`
- Modify: `portfolio.yaml`
- Modify: `README.md`
- Create/Modify: `CHANGELOG.md` (if present; check first, otherwise add concise release note section)

**Interfaces:**
- Consumes: `python -m scale_kg.showcase.build`, `dist/showcase/` output structure, Netlify secrets (`NETLIFY_AUTH_TOKEN`, `NETLIFY_SITE_ID`), custom domain `https://scale-kg.labbys-aoai.in`.
- Produces: Automated Netlify CI/CD workflow, Netlify security/cache configuration, local `make showcase-build` & `make showcase` commands, and updated docs.

- [ ] **Step 1: Create `netlify.toml` and update `.gitignore`**

Create `netlify.toml`:

```toml
"$schema" = "https://open-api.netlify.com/netlify.schema.json"

[build]
  publish = "dist/showcase"

[[headers]]
  for = "/*"
  [headers.values]
    X-Content-Type-Options = "nosniff"
    X-Frame-Options = "DENY"
    Referrer-Policy = "strict-origin-when-cross-origin"
    Content-Security-Policy = "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'self'; frame-ancestors 'none'"

[[headers]]
  for = "/snapshot.json"
  [headers.values]
    Cache-Control = "public, max-age=60, must-revalidate"

[[headers]]
  for = "/history.json"
  [headers.values]
    Cache-Control = "public, max-age=60, must-revalidate"

[[headers]]
  for = "/assets/*"
  [headers.values]
    Cache-Control = "public, max-age=3600"
```

Append `.netlify/` to `.gitignore`.

- [ ] **Step 2: Create `.github/workflows/showcase.yml`**

Create `.github/workflows/showcase.yml`:

```yaml
name: Showcase Dashboard

on:
  push:
    branches: [main]
    paths:
      - "src/scale_kg/**"
      - "pyproject.toml"
      - "Makefile"
      - "netlify.toml"
      - ".github/workflows/showcase.yml"
  pull_request:
    paths:
      - "src/scale_kg/**"
      - "pyproject.toml"
      - "Makefile"
      - "netlify.toml"
      - ".github/workflows/showcase.yml"
  workflow_dispatch:
    inputs:
      include_cluster:
        description: "Include cluster preset (5k nodes / 10B ref)"
        required: false
        default: "false"

permissions:
  contents: read

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    timeout-minutes: 15
    env:
      CI: "true"
      SHOWCASE_PUBLIC_URL: "https://scale-kg.labbys-aoai.in"
    steps:
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
      - uses: astral-sh/setup-uv@68559e99cc807c1eb7e9238fd43a15bba460eec2 # v10.3.3
        with:
          python-version: "3.11"
      - run: make setup
      - name: Fetch previous live history.json (best-effort)
        run: |
          curl -fsSL --max-time 10 "${SHOWCASE_PUBLIC_URL}/history.json" -o /tmp/prev-history.json || echo "[]" > /tmp/prev-history.json
      - name: Build static showcase bundle
        run: |
          PRESETS="micro,local"
          if [ "${{ github.event.inputs.include_cluster }}" = "true" ]; then
            PRESETS="micro,local,cluster"
          fi
          uv run python -m scale_kg.showcase.build \
            --presets "${PRESETS}" \
            --out dist/showcase \
            --history-in /tmp/prev-history.json \
            --git-sha "${GITHUB_SHA}" \
            --git-ref "${GITHUB_REF_NAME}" \
            --summary-out /tmp/showcase-summary.md
      - name: Upload showcase artifact
        uses: actions/upload-artifact@v4
        with:
          name: scale-kg-showcase
          path: dist/showcase
          retention-days: 14
      - name: Deploy Netlify PR preview
        if: github.event_name == 'pull_request'
        env:
          NETLIFY_AUTH_TOKEN: ${{ secrets.NETLIFY_AUTH_TOKEN }}
          NETLIFY_SITE_ID: ${{ secrets.NETLIFY_SITE_ID }}
        run: |
          if [ -z "${NETLIFY_AUTH_TOKEN}" ] || [ -z "${NETLIFY_SITE_ID}" ]; then
            echo "NETLIFY_AUTH_TOKEN / NETLIFY_SITE_ID secrets not set; skipping PR preview deploy."
            exit 0
          fi
          npx --yes netlify-cli deploy \
            --dir=dist/showcase \
            --alias="pr-${{ github.event.pull_request.number }}" \
            --message="PR #${{ github.event.pull_request.number }} (${GITHUB_SHA::12})"
      - name: Deploy Netlify production
        if: (github.event_name == 'push' && github.ref == 'refs/heads/main') || github.event_name == 'workflow_dispatch'
        env:
          NETLIFY_AUTH_TOKEN: ${{ secrets.NETLIFY_AUTH_TOKEN }}
          NETLIFY_SITE_ID: ${{ secrets.NETLIFY_SITE_ID }}
        run: |
          if [ -z "${NETLIFY_AUTH_TOKEN}" ] || [ -z "${NETLIFY_SITE_ID}" ]; then
            echo "NETLIFY_AUTH_TOKEN / NETLIFY_SITE_ID secrets not set; skipping deploy."
            exit 0
          fi
          npx --yes netlify-cli deploy \
            --dir=dist/showcase \
            --prod \
            --message="Commit ${GITHUB_SHA::12}"
      - name: Publish benchmark delta summary
        run: cat /tmp/showcase-summary.md >> "$GITHUB_STEP_SUMMARY"
```

- [ ] **Step 3: Update `Makefile`, `portfolio.yaml`, `README.md`, and `CHANGELOG.md`**

- `Makefile`: add `showcase-build` and `showcase` to `.PHONY` and targets:
  ```makefile
  showcase-build:
  	uv run python -m scale_kg.showcase.build --presets micro,local --out dist/showcase

  showcase: showcase-build
  	SCALE_KG_SHOWCASE_SNAPSHOT=dist/showcase/snapshot.json uv run python -m scale_kg.api.app --host 127.0.0.1 --port 8000
  ```
- `portfolio.yaml`: add `/showcase` and `/showcase/api/snapshot` to `spec.interfaces.rest.endpoints`.
- `README.md`: document `make showcase`, `/showcase` interactive dashboard, and the Netlify deployment (`https://scale-kg.labbys-aoai.in`, Hostinger CNAME + GitHub Actions secrets `NETLIFY_AUTH_TOKEN` / `NETLIFY_SITE_ID`).

- [ ] **Step 4: Run full verification suite (`make check`) and `make showcase-build`**

Run:
```bash
make showcase-build
make check
```
Expected:
- `dist/showcase/` created with `index.html`, `snapshot.json`, `history.json`, and `assets/*`
- `ruff format --check .`: PASS
- `ruff check .`: PASS
- `mypy src`: PASS (0 errors)
- `pytest`: PASS (all existing + new showcase tests pass)
- `uv build`: PASS (wheel/sdist include `scale_kg/showcase/static/*` — verify `pyproject.toml` package-data includes `static/*`!)

*(Important packaging check: in `pyproject.toml`, add `[tool.setuptools.package-data]` `"scale_kg.showcase" = ["static/*"]` so wheels built by `uv build` bundle the dashboard UI files!)*

- [ ] **Step 5: Commit**

```bash
git add netlify.toml .github/workflows/showcase.yml .gitignore Makefile pyproject.toml portfolio.yaml README.md
git commit -m "ci(showcase): add Netlify config, auto-update workflow, and Makefile targets"
```
