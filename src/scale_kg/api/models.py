"""Pydantic request and response schemas for the Scale-KG REST and gRPC APIs."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from scale_kg.adapters.base import GraphStats, HealthStatus
from scale_kg.benchmark.generator import ScalePreset
from scale_kg.benchmark.report import EngineScorecard
from scale_kg.benchmark.workloads import BenchmarkSuiteResult
from scale_kg.core.models import (
    Domain,
    EntityNode,
    GuardrailViolation,
    RelationshipEdge,
)
from scale_kg.lakehouse.store import LakehouseDocument, LakehouseStats
from scale_kg.routing.coordinator import CoordinatorStats
from scale_kg.routing.directory import CanonicalEntityId, DirectoryStats

BackendType = Literal[
    "in_memory",
    "iceberg_puppygraph",
    "hugegraph",
    "neo4j",
    "janusgraph",
    "nebula",
]


def _default_benchmark_adapters() -> list[BackendType]:
    return ["in_memory", "iceberg_puppygraph"]


class GuardrailErrorResponse(BaseModel):
    """Structured error payload emitted when a write or query violates a guardrail."""

    error: Literal["guardrail_violation"] = "guardrail_violation"
    violation: GuardrailViolation


class ApiErrorResponse(BaseModel):
    """Structured non-guardrail error payload."""

    error: str
    detail: str


class HealthResponse(BaseModel):
    """Aggregated health and subsystem telemetry for the Scale-KG API service."""

    status: Literal["healthy", "degraded", "unavailable"]
    healthy: bool
    backend: str
    shards: dict[str, HealthStatus]
    coordinator_stats: CoordinatorStats
    directory_stats: DirectoryStats
    lakehouse_stats: LakehouseStats


class IngestRequest(BaseModel):
    """Batch ingestion payload across graph, directory aliases, lakehouse, and vectors."""

    nodes: list[EntityNode] = Field(default_factory=list)
    edges: list[RelationshipEdge] = Field(default_factory=list)
    aliases: dict[str, list[str]] | None = None
    documents: list[LakehouseDocument] = Field(default_factory=list)
    embeddings: dict[str, list[float]] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    """Counts persisted across graph shards, lakehouse, and vector index."""

    nodes_upserted: int
    edges_upserted: int
    documents_stored: int
    embeddings_indexed: int


class NeighborItem(BaseModel):
    """Single edge-target pair returned by neighbor inspection."""

    edge: RelationshipEdge
    node: EntityNode


class NeighborsResponse(BaseModel):
    """Logical neighbors around an entity after bucket unwrapping."""

    node_id: str
    neighbors: list[NeighborItem]


class StatsResponse(BaseModel):
    """Combined graph, coordinator, directory, and lakehouse counters."""

    graph: GraphStats
    coordinator: CoordinatorStats
    directory: DirectoryStats
    lakehouse: LakehouseStats


class PutDocumentsRequest(BaseModel):
    """Request body for storing authoritative Lakehouse documents."""

    documents: list[LakehouseDocument] = Field(default_factory=list)


class PutDocumentsResponse(BaseModel):
    """Response body confirming stored Lakehouse document count."""

    documents_stored: int


class WriteSliceResponse(BaseModel):
    """Response returned after persisting a historical graph slice."""

    slice_id: str
    domain: Domain
    path: str


class SliceListResponse(BaseModel):
    """List of available historical graph slice IDs."""

    slices: list[str]


class RegisterAliasesRequest(BaseModel):
    """Request to register entity aliases and explicit opaque-ID domain mappings."""

    aliases: dict[str, str] = Field(default_factory=dict)
    entities: dict[str, Domain] = Field(default_factory=dict)


class RegisterAliasesResponse(BaseModel):
    """Summary of newly registered aliases and entities in EntityDirectory."""

    aliases_registered: int
    entities_registered: int
    stats: DirectoryStats


class DirectoryResolveResponse(BaseModel):
    """Resolved canonical ID, owning domain shard, and parsed components."""

    raw_id: str
    canonical_id: str
    domain: Domain
    parsed: CanonicalEntityId | None = None
    stats: DirectoryStats


class BenchmarkRunRequest(BaseModel):
    """Request parameters to execute the 7-workload Gate 2 benchmark suite."""

    preset: ScalePreset = ScalePreset.MICRO
    adapters: list[BackendType] = Field(default_factory=_default_benchmark_adapters)
    cross_domain_edge_share: float = Field(default=0.15, ge=0.0, le=1.0)
    seed: int = 42
    run_golden_parity: bool = True
    generate_adr: bool = True


class BenchmarkRunResponse(BaseModel):
    """Empirical benchmark suite output, Gate 2 scorecards, and optional ADR report."""

    suite_result: BenchmarkSuiteResult
    scorecards: list[EngineScorecard]
    adr_markdown: str | None = None
