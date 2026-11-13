"""Core ontology and traversal models for Scale-KG."""

from __future__ import annotations

from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

RoutingPropValue = str | int | float | bool


class Domain(str, Enum):
    """Canonical semantic domains used for shard placement and routing."""

    CUSTOMER = "customer"
    KNOWLEDGE = "knowledge"
    TECHNOLOGY = "technology"
    PRODUCT = "product"


class EntityNode(BaseModel):
    """Lean graph entity storing only routing metadata and optional Lakehouse reference."""

    id: str = Field(min_length=1)
    domain: Domain
    entity_type: str = Field(min_length=1)
    routing_props: dict[str, RoutingPropValue] = Field(default_factory=dict)
    lakehouse_ref: str | None = None
    is_proxy: bool = False
    target_domain: Domain | None = None
    is_bucket: bool = False


class RelationshipEdge(BaseModel):
    """Directed typed relationship between entities with hot-path materialization flags."""

    source_id: str = Field(min_length=1)
    target_id: str = Field(min_length=1)
    edge_type: str = Field(min_length=1)
    domain: Domain
    target_domain: Domain | None = None
    is_derived: bool = False
    materialized_hot_path: bool = False
    routing_props: dict[str, RoutingPropValue] = Field(default_factory=dict)


class GuardrailViolationCode(str, Enum):
    """Machine-readable guardrail violation identifiers."""

    EMPTY_SEED_IDS = "empty_seed_ids"
    INVALID_DEPTH = "invalid_depth"
    MAX_DEPTH_EXCEEDED = "max_depth_exceeded"
    MAX_RESULTS_EXCEEDED = "max_results_exceeded"
    MAX_EXECUTION_TIME_EXCEEDED = "max_execution_time_exceeded"
    MAX_EXPANDED_NODES_EXCEEDED = "max_expanded_nodes_exceeded"
    LEAN_GRAPH_FORBIDDEN_PAYLOAD = "lean_graph_forbidden_payload"
    LEAN_GRAPH_MISSING_LAKEHOUSE_REF = "lean_graph_missing_lakehouse_ref"
    UNMATERIALIZED_DERIVED_EDGE = "unmaterialized_derived_edge"


class GuardrailViolation(BaseModel):
    """Structured metadata emitted when a write or query violates a guardrail."""

    code: GuardrailViolationCode
    message: str
    limit: int | float | str | None = None
    observed: int | float | str | None = None
    phase: Literal["preflight", "runtime", "write"]
    entity_id: str | None = None


class TraversalRequest(BaseModel):
    """Parameters governing a bounded 1-to-3 hop graph traversal."""

    seed_ids: list[str]
    max_depth: int = 2
    edge_types: list[str] | None = None
    domains: list[Domain] | None = None
    max_results: int = 5000
    max_execution_seconds: float = 2.0
    max_expanded_nodes: int = 50000
    include_derived: bool = False


class TraversalMetrics(BaseModel):
    """Execution telemetry and guardrail status returned with a traversal."""

    depth_reached: int = 0
    nodes_expanded: int = 0
    results_returned: int = 0
    execution_time_ms: float = 0.0
    cross_domain_hops: int = 0
    single_domain_locality_ratio: float = 1.0
    bucket_hops_unwrapped: int = 0
    aborted: bool = False
    violation: GuardrailViolation | None = None


class TraversalResult(BaseModel):
    """Result subgraph, logical paths, and metrics for a traversal request."""

    seed_ids: list[str]
    nodes: list[EntityNode] = Field(default_factory=list)
    edges: list[RelationshipEdge] = Field(default_factory=list)
    paths: list[list[str]] = Field(default_factory=list)
    metrics: TraversalMetrics = Field(default_factory=TraversalMetrics)
