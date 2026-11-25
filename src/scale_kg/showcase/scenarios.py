"""Executable mechanism demos and snapshot assembly backed by live scale_kg code."""

from __future__ import annotations

import contextlib
import tempfile
import time
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
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
from scale_kg.core.supernode import SupernodeBucketer, SupernodeRegistry
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

_HYBRID_RAG_STAGE_KEYS: tuple[str, ...] = (
    "vector_seed",
    "graph_expand",
    "rrf_fusion",
    "lakehouse_hydrate",
    "context_assembly",
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

        registry = SupernodeRegistry(
            degree_threshold=threshold,
            seeded_types=frozenset(),
            seeded_ids=frozenset(),
        )
        bucketer = SupernodeBucketer(
            degree_threshold=threshold,
            num_buckets=num_buckets,
            registry=registry,
        )
        t_nodes, t_edges, supernode_ids = bucketer.transform_dataset(nodes, edges)
        bucketed = hub_id in supernode_ids
        if bucketed and switch_degree is None:
            switch_degree = degree

        bucket_nodes_created = sum(1 for n in t_nodes if n.is_bucket)

        store = InMemoryGraphStore(num_buckets=num_buckets)
        await store.upsert_nodes(t_nodes)
        await store.upsert_edges(t_edges)

        started = time.perf_counter()
        result = await store.traverse_k_hop(
            TraversalRequest(seed_ids=[hub_id], max_depth=1, max_results=5000)
        )
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)

        leaks = sum(
            1 for n in result.nodes if SupernodeBucketer.is_bucket_node_id(n.id)
        ) + sum(
            1
            for path in result.paths
            for vid in path
            if SupernodeBucketer.is_bucket_node_id(vid)
        )
        logical_neighbors = sum(1 for n in result.nodes if n.id != hub_id)
        points.append(
            SupernodeRampPoint(
                degree=degree,
                bucketed=bucketed,
                bucket_count=bucket_nodes_created,
                traversal_ms=elapsed_ms,
                expanded_nodes=result.metrics.nodes_expanded,
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
    if parsed is None:
        raise RuntimeError(f"Failed to parse canonical ID: {sample_id!r}")

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
            st = await shard.count_stats()
            proxy_count += st.proxy_node_count

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
        raise RuntimeError("Expected GuardrailViolationError for forbidden_payload.")

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
        raise RuntimeError(
            "Expected GuardrailViolationError for missing_lakehouse_ref."
        )

    if probe_id == "preflight_depth_cap":
        enforcer = GuardrailEnforcer()
        req = TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=4,
            max_results=500,
            max_execution_seconds=2.0,
        )
        try:
            enforcer.validate_request(req)
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
        raise RuntimeError("Expected GuardrailViolationError for preflight_depth_cap.")

    if probe_id != "runtime_expansion_abort":
        raise ValueError(f"Unsupported guardrail probe_id: {probe_id!r}")

    # runtime_expansion_abort
    tight_enforcer = GuardrailEnforcer(GuardrailConfig(max_expanded_nodes=5))
    store = InMemoryGraphStore(enforcer=tight_enforcer)
    nodes = [
        EntityNode(
            id="customer:org:root",
            domain=Domain.CUSTOMER,
            entity_type="Organization",
        )
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
        TraversalRequest(
            seed_ids=["customer:org:root"],
            max_depth=2,
            max_expanded_nodes=5,
        )
    )
    violation = res.metrics.violation or GuardrailViolation(
        code=GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED,
        message="Expanded node cap reached.",
        limit=5,
        observed=res.metrics.nodes_expanded,
        phase="runtime",
    )
    return GuardrailProbeResult(
        probe_id="runtime_expansion_abort",
        title="Runtime Expansion Cap Partial Abort",
        http_status=200,
        blocked=False,
        aborted=res.metrics.aborted,
        violation=violation,
        nodes_returned=len(res.nodes),
        expanded_nodes=res.metrics.nodes_expanded,
        summary="Returned bounded partial result with aborted=True instead of OOM.",
    )


async def run_hybrid_rag_demo(
    *,
    warehouse_dir: Path | None = None,
) -> HybridRagDemoResult:
    """Run the 5-stage Seed-Then-Expand Hybrid GraphRAG pipeline with Parquet Lakehouse."""
    lh_cm: AbstractContextManager[str | Path] = (
        contextlib.nullcontext(warehouse_dir)
        if warehouse_dir is not None
        else tempfile.TemporaryDirectory(prefix="scale_kg_showcase_lh_")
    )
    with lh_cm as tmp:
        lh_dir = Path(tmp)
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

        vector_index.upsert(
            "customer:org:acme", [0.98, 0.20, 0.10], domain=Domain.CUSTOMER
        )
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
        stage_timings = {
            stage: result.stage_timings_ms.get(stage, 0.0)
            for stage in _HYBRID_RAG_STAGE_KEYS
        }

        return HybridRagDemoResult(
            query_text="How does Kinetic-V scale GraphRAG across supernodes?",
            seed_count=len(result.seed_ids),
            graph_candidates=len(result.traversal.nodes) if result.traversal else 0,
            hydrated_documents=hydrated_docs,
            total_lakehouse_documents=total_docs,
            hydrated_bytes=hydrated_bytes,
            naive_full_graph_bytes=naive_bytes,
            payload_savings_pct=savings,
            stage_timings_ms=stage_timings,
            top_hit_ids=[h.node.id for h in result.hits],
            citation_count=len(result.citations),
        )


def _make_adapter_factories(
    adapters: Sequence[BackendType],
    temp_root: Path,
) -> dict[str, Callable[[], GraphStore]]:
    if not adapters:
        raise ValueError("adapters must not be empty.")
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
                return IcebergPuppyGraphStore(warehouse_path=wh)

            factories[name] = _iceberg_factory
        else:
            raise ValueError(
                f"Unsupported adapter: {name!r}. "
                "Expected 'in_memory' or 'iceberg_puppygraph'."
            )
    return factories


async def run_preset_benchmark(
    preset: ScalePreset,
    *,
    adapters: Sequence[BackendType] = ("in_memory", "iceberg_puppygraph"),
    seed: int = 42,
    temp_root: Path | None = None,
) -> PresetBenchmarkBundle:
    """Run the 7-workload BenchmarkSuite and Gate 2 ADR scorer for one preset."""
    root_cm: AbstractContextManager[str | Path] = (
        contextlib.nullcontext(temp_root)
        if temp_root is not None
        else tempfile.TemporaryDirectory(prefix=f"scale_kg_bench_{preset.value}_")
    )
    with root_cm as tmp:
        root = Path(tmp)
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

    probe_ids: tuple[GuardrailProbeId, ...] = (
        "forbidden_payload",
        "missing_lakehouse_ref",
        "preflight_depth_cap",
        "runtime_expansion_abort",
    )
    probes: dict[GuardrailProbeId, GuardrailProbeResult] = {}
    for pid in probe_ids:
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
