"""Pluggable GraphStore interface and Tier-1 / Tier-2 engine adapters."""

from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    FaultState,
    GraphStats,
    GraphStore,
    GraphStoreError,
    GraphStoreUnavailableError,
    HealthStatus,
)
from scale_kg.adapters.hugegraph_adapter import HugeGraphStore
from scale_kg.adapters.iceberg_puppygraph import (
    IcebergPuppyGraphStore,
    PartitionScanStats,
)
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.adapters.neo4j_adapter import Neo4jGraphStore
from scale_kg.adapters.stubs import JanusGraphStore, NebulaGraphStore

__all__ = [
    "AdapterCostProfile",
    "FaultMode",
    "FaultState",
    "GraphStats",
    "GraphStore",
    "GraphStoreError",
    "GraphStoreUnavailableError",
    "HealthStatus",
    "HugeGraphStore",
    "IcebergPuppyGraphStore",
    "InMemoryGraphStore",
    "JanusGraphStore",
    "NebulaGraphStore",
    "Neo4jGraphStore",
    "PartitionScanStats",
]
