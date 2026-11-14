"""Abstract async GraphStore interface, health/stats models, and fault injection."""

from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field

from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalRequest,
    TraversalResult,
)


class FaultMode(str, Enum):
    """Supported fault-injection modes for HA and resilience testing."""

    NONE = "none"
    NODE_DOWN = "node_down"
    REPLICA_DEGRADED = "replica_degraded"
    TIMEOUT = "timeout"
    TRANSIENT_ERROR = "transient_error"


class GraphStoreError(RuntimeError):
    """Base error raised by GraphStore adapters on transport or storage failures."""


class GraphStoreUnavailableError(GraphStoreError):
    """Raised when an adapter or targeted domain shard is unavailable."""


class HealthStatus(BaseModel):
    """Health report returned by GraphStore.health_check()."""

    adapter_name: str
    healthy: bool
    status: Literal["healthy", "degraded", "unavailable"]
    fault_mode: str = FaultMode.NONE.value
    details: dict[str, str | int | float | bool] = Field(default_factory=dict)


class GraphStats(BaseModel):
    """Vertex, edge, and domain distribution counts for a GraphStore."""

    node_count: int = 0
    edge_count: int = 0
    bucket_node_count: int = 0
    proxy_node_count: int = 0
    nodes_by_domain: dict[str, int] = Field(default_factory=dict)
    edges_by_domain: dict[str, int] = Field(default_factory=dict)
    extra: dict[str, str | int | float | bool] = Field(default_factory=dict)


class AdapterCostProfile(BaseModel):
    """Engine characteristics and cost metadata for Gate 2 ADR comparison."""

    engine: str
    tier: Literal["reference", "tier1", "tier2"]
    storage_backend: str
    query_languages: list[str]
    supports_zero_etl: bool = False
    supports_cross_shard_edges: bool = True
    estimated_cost_per_billion_edges_usd_month: float
    operational_complexity: Literal["low", "medium", "high"]
    is_stub: bool = False
    notes: str = ""


class FaultState:
    """Shared fault-injection state machine for GraphStore adapters."""

    def __init__(self) -> None:
        self.mode: FaultMode = FaultMode.NONE
        self.target_domain: Domain | None = None
        self.latency_ms: float = 0.0
        self.remaining_failures: int | None = None

    def configure(
        self,
        mode: str | FaultMode,
        *,
        target_domain: Domain | None = None,
        latency_ms: float = 0.0,
        recover_after_calls: int | None = None,
    ) -> None:
        """Update active fault mode and optional auto-recovery countdown."""
        self.mode = FaultMode(mode)
        self.target_domain = target_domain
        self.latency_ms = max(0.0, latency_ms)
        self.remaining_failures = (
            None
            if self.mode == FaultMode.NONE or recover_after_calls is None
            else max(0, recover_after_calls)
        )

    def _consume_failure_budget(self) -> bool:
        """Return True if fault should fire on this call, auto-recovering at zero."""
        if self.remaining_failures is None:
            return True
        if self.remaining_failures <= 0:
            self.mode = FaultMode.NONE
            self.target_domain = None
            self.latency_ms = 0.0
            self.remaining_failures = None
            return False
        self.remaining_failures -= 1
        return True

    def applies_to(self, domain: Domain | None) -> bool:
        """Return True if fault targets all domains or the specified domain."""
        return self.target_domain is None or domain is None or self.target_domain == domain

    def check(self, domain: Domain | None = None) -> None:
        """Raise an appropriate error if an active fault blocks the operation."""
        if self.mode == FaultMode.NONE or not self.applies_to(domain):
            return
        if self.mode == FaultMode.REPLICA_DEGRADED:
            return
        if not self._consume_failure_budget():
            return
        scope = f" for domain '{self.target_domain.value}'" if self.target_domain else ""
        if self.mode == FaultMode.NODE_DOWN:
            raise GraphStoreUnavailableError(f"Simulated node outage{scope}.")
        if self.mode == FaultMode.TIMEOUT:
            raise TimeoutError(f"Simulated query timeout{scope}.")
        if self.mode == FaultMode.TRANSIENT_ERROR:
            raise GraphStoreUnavailableError(f"Simulated transient failure{scope}.")

    def health_state(self) -> tuple[bool, Literal["healthy", "degraded", "unavailable"]]:
        """Return (healthy, status) tuple reflecting current fault state."""
        if self.mode == FaultMode.NONE:
            return True, "healthy"
        if self.mode == FaultMode.REPLICA_DEGRADED:
            return True, "degraded"
        return False, "unavailable"


def canonical_domain_from_id(node_id: str) -> Domain | None:
    """Extract Domain from a canonical ``<domain>:<type>:<slug>`` entity ID."""
    prefix, sep, _ = node_id.partition(":")
    if not sep:
        return None
    try:
        return Domain(prefix)
    except ValueError:
        return None


class GraphStore(ABC):
    """Engine-agnostic async GraphStore protocol implemented by all storage adapters."""

    name: str = "graph_store"

    @abstractmethod
    def cost_profile(self) -> AdapterCostProfile:
        """Return engine capabilities and cost metadata for Gate 2 evaluation."""

    @abstractmethod
    async def upsert_nodes(self, nodes: list[EntityNode]) -> int:
        """Validate and upsert EntityNode records, returning count persisted."""

    @abstractmethod
    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        """Validate and upsert RelationshipEdge records, returning count persisted."""

    @abstractmethod
    async def get_node(self, node_id: str) -> EntityNode | None:
        """Fetch a single EntityNode by global ID, or None if absent."""

    @abstractmethod
    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> list[tuple[RelationshipEdge, EntityNode]]:
        """Return immediate logical neighbors with transparent supernode bucket collapse."""

    @abstractmethod
    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        """Execute a guardrail-enforced 1-to-3 hop traversal."""

    @abstractmethod
    async def set_fault_mode(
        self,
        mode: str | FaultMode,
        *,
        target_domain: Domain | None = None,
        latency_ms: float = 0.0,
        recover_after_calls: int | None = None,
    ) -> None:
        """Configure simulated faults for HA and failover testing."""

    @abstractmethod
    async def health_check(self) -> HealthStatus:
        """Return current adapter health status."""

    @abstractmethod
    async def count_stats(self) -> GraphStats:
        """Return current node, edge, and domain-level statistics."""

    @abstractmethod
    async def clear(self) -> None:
        """Remove all stored vertices and edges and reset local state."""
