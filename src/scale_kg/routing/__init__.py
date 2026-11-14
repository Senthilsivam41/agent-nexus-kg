"""Semantic domain routing, federated QueryCoordinator, and Hybrid GraphRAG."""

from scale_kg.routing.coordinator import (
    DEFAULT_DERIVED_EDGE_RULES,
    CoordinatorStats,
    DerivedEdgeRule,
    QueryCoordinator,
)
from scale_kg.routing.directory import (
    CanonicalEntityId,
    DirectoryStats,
    EntityDirectory,
    EntityResolutionError,
    build_canonical_id,
    parse_canonical_id,
)
from scale_kg.routing.hybrid_rag import (
    HybridGraphRAGRouter,
    HybridRAGHit,
    HybridRAGRequest,
    HybridRAGResult,
    InMemoryVectorIndex,
    SourceCitation,
    VectorIndex,
    VectorSearchHit,
    cosine_similarity,
)

__all__ = [
    "DEFAULT_DERIVED_EDGE_RULES",
    "CanonicalEntityId",
    "CoordinatorStats",
    "DerivedEdgeRule",
    "DirectoryStats",
    "EntityDirectory",
    "EntityResolutionError",
    "HybridGraphRAGRouter",
    "HybridRAGHit",
    "HybridRAGRequest",
    "HybridRAGResult",
    "InMemoryVectorIndex",
    "QueryCoordinator",
    "SourceCitation",
    "VectorIndex",
    "VectorSearchHit",
    "build_canonical_id",
    "cosine_similarity",
    "parse_canonical_id",
]
