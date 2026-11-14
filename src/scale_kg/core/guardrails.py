"""Lean graph write discipline and pre-flight/runtime query guardrail enforcement."""

from __future__ import annotations

import time
from collections.abc import Callable

from scale_kg.config import GuardrailConfig
from scale_kg.core.models import (
    EntityNode,
    GuardrailViolation,
    GuardrailViolationCode,
    RelationshipEdge,
    RoutingPropValue,
    TraversalMetrics,
    TraversalRequest,
    TraversalResult,
)

FORBIDDEN_PAYLOAD_KEYS: frozenset[str] = frozenset(
    {"content", "raw_text", "embedding", "history_blob"}
)

CONTENT_BEARING_ENTITY_TYPES: frozenset[str] = frozenset(
    {
        "document",
        "chunk",
        "passage",
        "article",
        "transcript",
        "file",
        "blob",
        "artifact",
    }
)

DERIVED_EDGE_TYPES: frozenset[str] = frozenset({"SIMILAR_TO", "INDIRECTLY_USES"})


class GuardrailViolationError(ValueError):
    """Raised when a write or query violates lean-graph or traversal guardrails."""

    def __init__(self, violation: GuardrailViolation) -> None:
        self.violation = violation
        super().__init__(f"[{violation.code.value}] {violation.message}")


class RuntimeGuardrailTracker:
    """Tracks live traversal budgets and constructs clean abort responses on breach."""

    def __init__(
        self,
        request: TraversalRequest,
        limits: GuardrailConfig,
        clock: Callable[[], float],
    ) -> None:
        self._request = request
        self._max_depth = min(request.max_depth, limits.max_depth)
        self._max_results = min(request.max_results, limits.max_results)
        self._max_execution_seconds = min(
            request.max_execution_seconds, limits.max_execution_seconds
        )
        self._max_expanded_nodes = min(
            request.max_expanded_nodes, limits.max_expanded_nodes
        )
        self._clock = clock
        self._started_at = clock()

    @property
    def elapsed_seconds(self) -> float:
        """Elapsed wall/monotonic time in seconds since tracker creation."""
        return max(0.0, self._clock() - self._started_at)

    def check_step(
        self,
        *,
        depth: int,
        expanded_nodes: int,
        results_count: int,
    ) -> GuardrailViolation | None:
        """Evaluate runtime budgets and return structured violation metadata if breached."""
        if depth > self._max_depth:
            return GuardrailViolation(
                code=GuardrailViolationCode.MAX_DEPTH_EXCEEDED,
                message=f"Traversal depth {depth} exceeded limit {self._max_depth}.",
                limit=self._max_depth,
                observed=depth,
                phase="runtime",
            )
        if expanded_nodes > self._max_expanded_nodes:
            return GuardrailViolation(
                code=GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED,
                message=(
                    f"Expanded nodes {expanded_nodes} exceeded limit "
                    f"{self._max_expanded_nodes}."
                ),
                limit=self._max_expanded_nodes,
                observed=expanded_nodes,
                phase="runtime",
            )
        if results_count > self._max_results:
            return GuardrailViolation(
                code=GuardrailViolationCode.MAX_RESULTS_EXCEEDED,
                message=(
                    f"Result count {results_count} exceeded limit {self._max_results}."
                ),
                limit=self._max_results,
                observed=results_count,
                phase="runtime",
            )
        elapsed = self.elapsed_seconds
        if elapsed > self._max_execution_seconds:
            return GuardrailViolation(
                code=GuardrailViolationCode.MAX_EXECUTION_TIME_EXCEEDED,
                message=(
                    f"Execution time {elapsed:.3f}s exceeded limit "
                    f"{self._max_execution_seconds:.3f}s."
                ),
                limit=self._max_execution_seconds,
                observed=round(elapsed, 6),
                phase="runtime",
            )
        return None

    def enforce_step(
        self,
        *,
        depth: int,
        expanded_nodes: int,
        results_count: int,
    ) -> None:
        """Raise GuardrailViolationError if any runtime budget is breached."""
        violation = self.check_step(
            depth=depth,
            expanded_nodes=expanded_nodes,
            results_count=results_count,
        )
        if violation is not None:
            raise GuardrailViolationError(violation)

    def build_aborted_result(
        self,
        *,
        violation: GuardrailViolation,
        nodes: list[EntityNode],
        edges: list[RelationshipEdge],
        paths: list[list[str]],
        depth_reached: int,
        expanded_nodes: int,
        cross_domain_hops: int = 0,
        single_domain_locality_ratio: float = 1.0,
        bucket_hops_unwrapped: int = 0,
    ) -> TraversalResult:
        """Build a partial TraversalResult marked aborted with structured violation info."""
        return TraversalResult(
            seed_ids=list(self._request.seed_ids),
            nodes=nodes,
            edges=edges,
            paths=paths,
            metrics=TraversalMetrics(
                depth_reached=depth_reached,
                nodes_expanded=expanded_nodes,
                results_returned=len(nodes),
                execution_time_ms=round(self.elapsed_seconds * 1000.0, 3),
                cross_domain_hops=cross_domain_hops,
                single_domain_locality_ratio=single_domain_locality_ratio,
                bucket_hops_unwrapped=bucket_hops_unwrapped,
                aborted=True,
                violation=violation,
            ),
        )


class GuardrailEnforcer:
    """Validates writes against the Lean Graph Rule and queries against safety budgets."""

    def __init__(
        self,
        config: GuardrailConfig | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.config = config or GuardrailConfig()
        self._clock = clock or time.monotonic

    @staticmethod
    def _reject_forbidden_props(
        routing_props: dict[str, RoutingPropValue],
        *,
        entity_id: str,
    ) -> None:
        for key in routing_props:
            if key.lower() in FORBIDDEN_PAYLOAD_KEYS:
                raise GuardrailViolationError(
                    GuardrailViolation(
                        code=GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD,
                        message=(
                            f"Forbidden heavy payload key '{key}' on '{entity_id}'; "
                            "store full payloads in Lakehouse and reference via lakehouse_ref."
                        ),
                        observed=key,
                        phase="write",
                        entity_id=entity_id,
                    )
                )

    def validate_node_write(self, node: EntityNode) -> None:
        """Enforce Lean Graph Rule on EntityNode writes."""
        self._reject_forbidden_props(node.routing_props, entity_id=node.id)

        requires_ref = node.entity_type.lower() in CONTENT_BEARING_ENTITY_TYPES or bool(
            node.routing_props.get("has_content")
        )
        if requires_ref and (not node.lakehouse_ref or not node.lakehouse_ref.strip()):
            raise GuardrailViolationError(
                GuardrailViolation(
                    code=GuardrailViolationCode.LEAN_GRAPH_MISSING_LAKEHOUSE_REF,
                    message=(
                        f"Out-of-graph content node '{node.id}' ({node.entity_type}) "
                        "must define a non-empty lakehouse_ref."
                    ),
                    phase="write",
                    entity_id=node.id,
                )
            )

    def validate_edge_write(self, edge: RelationshipEdge) -> None:
        """Reject heavy payloads and unmaterialized derived edges at write time."""
        edge_ref = f"{edge.source_id}-[{edge.edge_type}]->{edge.target_id}"
        self._reject_forbidden_props(edge.routing_props, entity_id=edge_ref)

        is_derived = edge.is_derived or edge.edge_type.upper() in DERIVED_EDGE_TYPES
        if is_derived and not edge.materialized_hot_path:
            raise GuardrailViolationError(
                GuardrailViolation(
                    code=GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE,
                    message=(
                        f"Derived edge '{edge_ref}' rejected at write time; "
                        "compute on-the-fly or promote with materialized_hot_path=True."
                    ),
                    observed=edge.edge_type,
                    phase="write",
                    entity_id=edge_ref,
                )
            )

    def validate_request(self, req: TraversalRequest) -> None:
        """Pre-flight validation against global query safety limits."""
        if not req.seed_ids:
            raise GuardrailViolationError(
                GuardrailViolation(
                    code=GuardrailViolationCode.EMPTY_SEED_IDS,
                    message="TraversalRequest must include at least one seed_id.",
                    limit=1,
                    observed=0,
                    phase="preflight",
                )
            )
        if req.max_depth < 1:
            raise GuardrailViolationError(
                GuardrailViolation(
                    code=GuardrailViolationCode.INVALID_DEPTH,
                    message=f"max_depth must be >= 1, got {req.max_depth}.",
                    limit=1,
                    observed=req.max_depth,
                    phase="preflight",
                )
            )
        checks = (
            (
                req.max_depth > self.config.max_depth,
                GuardrailViolationCode.MAX_DEPTH_EXCEEDED,
                "max_depth",
                self.config.max_depth,
                req.max_depth,
            ),
            (
                req.max_results < 1 or req.max_results > self.config.max_results,
                GuardrailViolationCode.MAX_RESULTS_EXCEEDED,
                "max_results",
                self.config.max_results,
                req.max_results,
            ),
            (
                req.max_execution_seconds <= 0
                or req.max_execution_seconds > self.config.max_execution_seconds,
                GuardrailViolationCode.MAX_EXECUTION_TIME_EXCEEDED,
                "max_execution_seconds",
                self.config.max_execution_seconds,
                req.max_execution_seconds,
            ),
            (
                req.max_expanded_nodes < 1
                or req.max_expanded_nodes > self.config.max_expanded_nodes,
                GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED,
                "max_expanded_nodes",
                self.config.max_expanded_nodes,
                req.max_expanded_nodes,
            ),
        )
        for violated, code, name, limit, observed in checks:
            if violated:
                raise GuardrailViolationError(
                    GuardrailViolation(
                        code=code,
                        message=f"Requested {name}={observed} violates guardrail limit {limit}.",
                        limit=limit,
                        observed=observed,
                        phase="preflight",
                    )
                )

    def create_runtime_tracker(self, req: TraversalRequest) -> RuntimeGuardrailTracker:
        """Validate pre-flight request and return a runtime budget tracker."""
        self.validate_request(req)
        return RuntimeGuardrailTracker(req, self.config, self._clock)
