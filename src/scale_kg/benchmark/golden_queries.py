"""Frozen Golden Query regression suite proving engine-independent GraphRAG parity."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Literal

from pydantic import BaseModel, Field

from scale_kg.adapters.base import GraphStore
from scale_kg.core.models import Domain, EntityNode, RelationshipEdge, TraversalRequest
from scale_kg.core.supernode import SupernodeBucketer
from scale_kg.routing.coordinator import QueryCoordinator


class GoldenQuerySpec(BaseModel):
    """Definition of a frozen regression query and its invariant expectations."""

    query_id: str
    name: str
    category: Literal[
        "single_domain_1hop",
        "multi_hop_2hop",
        "cross_domain_3hop",
        "supernode",
        "filtered_edge",
        "derived_edge",
        "guardrail_bounds",
    ]
    request: TraversalRequest
    expected_min_nodes: int = 1
    expected_required_ids: list[str] = Field(default_factory=list)
    expected_forbidden_ids: list[str] = Field(default_factory=list)
    requires_coordinator: bool = False


def build_golden_dataset() -> tuple[list[EntityNode], list[RelationshipEdge]]:
    """Return the canonical deterministic golden graph spanning all 4 semantic domains."""
    nodes = [
        EntityNode(
            id="customer:org:acme",
            domain=Domain.CUSTOMER,
            entity_type="Organization",
            routing_props={"tier": "enterprise", "active": True},
        ),
        EntityNode(
            id="customer:org:beta",
            domain=Domain.CUSTOMER,
            entity_type="Organization",
            routing_props={"tier": "growth", "active": True},
        ),
        EntityNode(
            id="customer:team:platform",
            domain=Domain.CUSTOMER,
            entity_type="Team",
            routing_props={"active": True},
        ),
        EntityNode(
            id="customer:region:india",
            domain=Domain.CUSTOMER,
            entity_type="Country",
            routing_props={"region_code": "IN", "active": True},
        ),
        EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
            routing_props={"ecosystem": "ai", "active": True},
        ),
        EntityNode(
            id="technology:lib:pydantic",
            domain=Domain.TECHNOLOGY,
            entity_type="Library",
            routing_props={"active": True},
        ),
        EntityNode(
            id="product:sdk:kinetic",
            domain=Domain.PRODUCT,
            entity_type="Product",
            routing_props={"ga": True},
        ),
        EntityNode(
            id="knowledge:doc:arch",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            lakehouse_ref="s3://scale-kg-lakehouse/knowledge/arch.parquet",
            routing_props={"version": 1},
        ),
        EntityNode(
            id="knowledge:doc:runbook",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            lakehouse_ref="s3://scale-kg-lakehouse/knowledge/runbook.parquet",
            routing_props={"version": 2},
        ),
    ]
    edges = [
        RelationshipEdge(
            source_id="customer:org:acme",
            target_id="customer:team:platform",
            edge_type="OWNS",
            domain=Domain.CUSTOMER,
            target_domain=Domain.CUSTOMER,
        ),
        RelationshipEdge(
            source_id="customer:team:platform",
            target_id="customer:region:india",
            edge_type="OPERATES_IN",
            domain=Domain.CUSTOMER,
            target_domain=Domain.CUSTOMER,
        ),
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
        RelationshipEdge(
            source_id="technology:lang:python",
            target_id="technology:lib:pydantic",
            edge_type="DEPENDS_ON",
            domain=Domain.TECHNOLOGY,
            target_domain=Domain.TECHNOLOGY,
        ),
        RelationshipEdge(
            source_id="technology:lang:python",
            target_id="product:sdk:kinetic",
            edge_type="POWERS",
            domain=Domain.TECHNOLOGY,
            target_domain=Domain.PRODUCT,
        ),
        RelationshipEdge(
            source_id="product:sdk:kinetic",
            target_id="knowledge:doc:arch",
            edge_type="DESCRIBED_BY",
            domain=Domain.PRODUCT,
            target_domain=Domain.KNOWLEDGE,
        ),
        RelationshipEdge(
            source_id="product:sdk:kinetic",
            target_id="knowledge:doc:runbook",
            edge_type="DESCRIBED_BY",
            domain=Domain.PRODUCT,
            target_domain=Domain.KNOWLEDGE,
        ),
    ]
    return nodes, edges


FROZEN_GOLDEN_QUERIES: tuple[GoldenQuerySpec, ...] = (
    GoldenQuerySpec(
        query_id="GQ-01",
        name="single_domain_1hop",
        category="single_domain_1hop",
        request=TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=1,
            domains=[Domain.CUSTOMER],
        ),
        expected_min_nodes=2,
        expected_required_ids=["customer:org:acme", "customer:team:platform"],
        expected_forbidden_ids=["technology:lang:python"],
    ),
    GoldenQuerySpec(
        query_id="GQ-02",
        name="multi_hop_2hop_with_supernode_hub",
        category="multi_hop_2hop",
        request=TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=2,
        ),
        expected_min_nodes=5,
        expected_required_ids=[
            "customer:org:acme",
            "customer:region:india",
            "technology:lang:python",
            "technology:lib:pydantic",
            "product:sdk:kinetic",
        ],
    ),
    GoldenQuerySpec(
        query_id="GQ-03",
        name="cross_domain_3hop_full_lineage",
        category="cross_domain_3hop",
        request=TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=3,
        ),
        expected_min_nodes=8,
        expected_required_ids=[
            "customer:org:acme",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
            "knowledge:doc:runbook",
        ],
    ),
    GoldenQuerySpec(
        query_id="GQ-04",
        name="supernode_seed_bucket_unwrapping",
        category="supernode",
        request=TraversalRequest(
            seed_ids=["technology:lang:python"],
            max_depth=2,
        ),
        expected_min_nodes=4,
        expected_required_ids=[
            "technology:lang:python",
            "technology:lib:pydantic",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        ],
    ),
    GoldenQuerySpec(
        query_id="GQ-05",
        name="filtered_edge_type_traversal",
        category="filtered_edge",
        request=TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=2,
            edge_types=["OWNS", "OPERATES_IN"],
        ),
        expected_min_nodes=3,
        expected_required_ids=[
            "customer:org:acme",
            "customer:team:platform",
            "customer:region:india",
        ],
        expected_forbidden_ids=["technology:lang:python", "product:sdk:kinetic"],
    ),
    GoldenQuerySpec(
        query_id="GQ-06",
        name="derived_edge_on_the_fly_inference",
        category="derived_edge",
        request=TraversalRequest(
            seed_ids=["customer:org:acme"],
            max_depth=2,
            include_derived=True,
        ),
        expected_min_nodes=6,
        expected_required_ids=[
            "customer:org:acme",
            "customer:org:beta",
            "technology:lang:python",
            "technology:lib:pydantic",
            "product:sdk:kinetic",
        ],
        requires_coordinator=True,
    ),
    GoldenQuerySpec(
        query_id="GQ-07",
        name="guardrail_bounded_max_depth_3",
        category="guardrail_bounds",
        request=TraversalRequest(
            seed_ids=["customer:org:beta"],
            max_depth=3,
            max_results=5000,
            max_execution_seconds=2.0,
            max_expanded_nodes=50000,
        ),
        expected_min_nodes=4,
        expected_required_ids=[
            "customer:org:beta",
            "technology:lang:python",
            "product:sdk:kinetic",
            "knowledge:doc:arch",
        ],
    ),
)


class GoldenQueryAdapterResult(BaseModel):
    """Normalized execution output from a single GraphStore adapter."""

    adapter_name: str
    node_ids: list[str]
    edge_keys: list[tuple[str, str, str]]
    paths: list[list[str]]
    depth_reached: int
    aborted: bool
    bucket_hops_unwrapped: int
    guardrail_compliant: bool
    execution_time_ms: float


class GoldenQueryComparison(BaseModel):
    """Cross-engine comparison result for one frozen Golden Query."""

    query_id: str
    query_name: str
    category: str
    passed: bool
    adapter_results: dict[str, GoldenQueryAdapterResult]
    mismatches: list[str] = Field(default_factory=list)


class GoldenParityReport(BaseModel):
    """Aggregate regression parity report across multiple GraphStore adapters."""

    passed: bool
    adapters_tested: list[str]
    queries_run: int
    passed_queries: int
    failed_queries: int
    comparisons: list[GoldenQueryComparison]
    mismatches: list[str] = Field(default_factory=list)

    def assert_parity(self) -> None:
        """Raise AssertionError with mismatch details if any parity check failed."""
        if not self.passed:
            raise AssertionError(
                "Golden query parity failed:\n- " + "\n- ".join(self.mismatches)
            )


class GoldenQuerySuite:
    """Executes the frozen Golden Query set across adapters and asserts deterministic parity."""

    def __init__(
        self,
        queries: Sequence[GoldenQuerySpec] | None = None,
        *,
        bucket_supernodes: bool = True,
        supernode_threshold: int = 100,
    ) -> None:
        self.queries: tuple[GoldenQuerySpec, ...] = (
            tuple(queries) if queries is not None else FROZEN_GOLDEN_QUERIES
        )
        self.bucket_supernodes = bucket_supernodes
        self.supernode_threshold = supernode_threshold

    def build_dataset(self) -> tuple[list[EntityNode], list[RelationshipEdge]]:
        """Build the golden dataset, optionally applying 16-way supernode bucketing."""
        nodes, edges = build_golden_dataset()
        if self.bucket_supernodes:
            bucketer = SupernodeBucketer(degree_threshold=self.supernode_threshold)
            nodes, edges, _ = bucketer.transform_dataset(nodes, edges)
        return nodes, edges

    async def load_dataset(
        self,
        stores: Mapping[str, GraphStore],
        *,
        clear_first: bool = True,
    ) -> None:
        """Load identical golden dataset records into every target GraphStore adapter."""
        nodes, edges = self.build_dataset()
        for store in stores.values():
            if clear_first:
                await store.clear()
            await store.upsert_nodes(nodes)
            await store.upsert_edges(edges)

    async def run_parity(
        self,
        stores: Mapping[str, GraphStore],
        *,
        load_data: bool = True,
        min_adapters: int = 2,
    ) -> GoldenParityReport:
        """Execute all golden queries across adapters and verify 100% deterministic parity."""
        if len(stores) < min_adapters:
            raise ValueError(
                f"GoldenQuerySuite requires at least {min_adapters} adapters, got {len(stores)}."
            )

        if load_data:
            await self.load_dataset(stores)

        adapter_names = list(stores.keys())
        comparisons: list[GoldenQueryComparison] = []
        all_mismatches: list[str] = []

        for spec in self.queries:
            per_adapter: dict[str, GoldenQueryAdapterResult] = {}
            query_mismatches: list[str] = []

            for adapter_name, store in stores.items():
                if spec.requires_coordinator:
                    coord = QueryCoordinator(store)
                    res = await coord.traverse(spec.request)
                else:
                    res = await store.traverse_k_hop(spec.request)

                node_ids = sorted({n.id for n in res.nodes})
                edge_keys = sorted(
                    {(e.source_id, e.target_id, e.edge_type) for e in res.edges}
                )
                paths = sorted(res.paths)

                leaked_buckets = [
                    nid for nid in node_ids if SupernodeBucketer.is_bucket_node_id(nid)
                ] + [
                    vid
                    for path in paths
                    for vid in path
                    if SupernodeBucketer.is_bucket_node_id(vid)
                ]
                guardrail_ok = (
                    not res.metrics.aborted
                    and res.metrics.depth_reached <= spec.request.max_depth
                    and len(res.nodes) <= spec.request.max_results
                    and not leaked_buckets
                )

                if not guardrail_ok:
                    query_mismatches.append(
                        f"{spec.query_id} [{adapter_name}] failed guardrail/unwrapping check "
                        f"(aborted={res.metrics.aborted}, leaked_buckets={leaked_buckets})"
                    )
                if len(node_ids) < spec.expected_min_nodes:
                    query_mismatches.append(
                        f"{spec.query_id} [{adapter_name}] returned {len(node_ids)} nodes, "
                        f"expected >= {spec.expected_min_nodes}"
                    )
                missing_req = [
                    rid for rid in spec.expected_required_ids if rid not in node_ids
                ]
                if missing_req:
                    query_mismatches.append(
                        f"{spec.query_id} [{adapter_name}] missing required IDs: {missing_req}"
                    )
                present_forbidden = [
                    fid for fid in spec.expected_forbidden_ids if fid in node_ids
                ]
                if present_forbidden:
                    query_mismatches.append(
                        f"{spec.query_id} [{adapter_name}] returned forbidden IDs: {present_forbidden}"
                    )

                per_adapter[adapter_name] = GoldenQueryAdapterResult(
                    adapter_name=adapter_name,
                    node_ids=node_ids,
                    edge_keys=edge_keys,
                    paths=paths,
                    depth_reached=res.metrics.depth_reached,
                    aborted=res.metrics.aborted,
                    bucket_hops_unwrapped=res.metrics.bucket_hops_unwrapped,
                    guardrail_compliant=guardrail_ok,
                    execution_time_ms=res.metrics.execution_time_ms,
                )

            baseline_name = adapter_names[0]
            baseline = per_adapter[baseline_name]
            for other_name in adapter_names[1:]:
                other = per_adapter[other_name]
                if other.node_ids != baseline.node_ids:
                    query_mismatches.append(
                        f"{spec.query_id} node_ids mismatch ({baseline_name} vs {other_name}): "
                        f"{baseline.node_ids} != {other.node_ids}"
                    )
                if other.edge_keys != baseline.edge_keys:
                    query_mismatches.append(
                        f"{spec.query_id} edge_keys mismatch ({baseline_name} vs {other_name})"
                    )
                if other.paths != baseline.paths:
                    query_mismatches.append(
                        f"{spec.query_id} paths mismatch ({baseline_name} vs {other_name}): "
                        f"{baseline.paths} != {other.paths}"
                    )

            passed = len(query_mismatches) == 0
            all_mismatches.extend(query_mismatches)
            comparisons.append(
                GoldenQueryComparison(
                    query_id=spec.query_id,
                    query_name=spec.name,
                    category=spec.category,
                    passed=passed,
                    adapter_results=per_adapter,
                    mismatches=query_mismatches,
                )
            )

        passed_count = sum(1 for c in comparisons if c.passed)
        failed_count = len(comparisons) - passed_count
        return GoldenParityReport(
            passed=failed_count == 0,
            adapters_tested=adapter_names,
            queries_run=len(comparisons),
            passed_queries=passed_count,
            failed_queries=failed_count,
            comparisons=comparisons,
            mismatches=all_mismatches,
        )
