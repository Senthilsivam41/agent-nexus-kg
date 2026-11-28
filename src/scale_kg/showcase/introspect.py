"""Zero-drift runtime introspection of scale_kg configs, adapters, presets, and routes."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import tempfile
from collections.abc import Sequence
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as pkg_version

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
        except (subprocess.SubprocessError, OSError):
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
        except (subprocess.SubprocessError, OSError):
            ref = "local"
    return sha, ref


def _probe_http_endpoint(url: str | None) -> bool:
    if not url or not url.startswith(("http://", "https://")):
        return False
    try:
        resp = httpx.get(url, timeout=0.5)
        return resp.status_code < 500
    except (httpx.HTTPError, httpx.InvalidURL, ValueError):
        return False


def _adapter_catalog(probe_remote: bool) -> list[AdapterCatalogEntry]:
    with tempfile.TemporaryDirectory() as tmpdir:
        iceberg_profile = IcebergPuppyGraphStore(warehouse_path=tmpdir).cost_profile()

    specs: list[
        tuple[BackendType, type[GraphStore], AdapterCostProfile, str | None]
    ] = [
        (
            "in_memory",
            InMemoryGraphStore,
            InMemoryGraphStore().cost_profile(),
            None,
        ),
        (
            "iceberg_puppygraph",
            IcebergPuppyGraphStore,
            iceberg_profile,
            os.environ.get("SCALE_KG_PUPPYGRAPH_URL")
            or os.environ.get("PUPPYGRAPH_URL"),
        ),
        (
            "hugegraph",
            HugeGraphStore,
            HugeGraphStore(base_url="http://localhost:8080").cost_profile(),
            os.environ.get("SCALE_KG_HUGEGRAPH_URL") or os.environ.get("HUGEGRAPH_URL"),
        ),
        (
            "neo4j",
            Neo4jGraphStore,
            Neo4jGraphStore(uri="bolt://localhost:7687").cost_profile(),
            os.environ.get("SCALE_KG_NEO4J_URI") or os.environ.get("NEO4J_HTTP_URL"),
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
        elif key == "in_memory":
            availability = "verified_local"
        elif key == "iceberg_puppygraph":
            if probe_remote and _probe_http_endpoint(remote_url):
                availability = "live_remote"
            else:
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


def _build_switch_rules(
    cfg: ScaleKGConfig, hot_path_threshold: int
) -> list[SwitchRuleCard]:
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
                f"expanded>{cfg.guardrails.max_expanded_nodes} | "
                f"time>{cfg.guardrails.max_execution_seconds}s"
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
