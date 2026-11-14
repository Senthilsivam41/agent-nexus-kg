"""Tier-2 contract stubs for JanusGraph (ScyllaDB + OpenSearch) and NebulaGraph."""

from __future__ import annotations

from typing import Any

from scale_kg.adapters.base import AdapterCostProfile, HealthStatus
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import TraversalRequest


class _ContractStubBase(InMemoryGraphStore):
    """Shared GraphStore contract base for Tier-2 engine evaluation stubs."""

    is_stub: bool = True

    def __init__(
        self,
        *,
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
    ) -> None:
        super().__init__(enforcer=enforcer, num_buckets=num_buckets)


class JanusGraphStore(_ContractStubBase):
    """Tier-2 contract stub for JanusGraph backed by ScyllaDB + OpenSearch."""

    name = "janusgraph"

    def __init__(
        self,
        *,
        gremlin_url: str = "ws://localhost:8182/gremlin",
        scylla_keyspace: str = "janusgraph_kg",
        opensearch_index: str = "janusgraph_entities",
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
    ) -> None:
        super().__init__(enforcer=enforcer, num_buckets=num_buckets)
        self.gremlin_url = gremlin_url
        self.scylla_keyspace = scylla_keyspace
        self.opensearch_index = opensearch_index

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="janusgraph",
            tier="tier2",
            storage_backend="ScyllaDB (storage) + OpenSearch (mixed index)",
            query_languages=["gremlin"],
            supports_zero_etl=False,
            supports_cross_shard_edges=True,
            estimated_cost_per_billion_edges_usd_month=1350.0,
            operational_complexity="high",
            is_stub=True,
            notes="Secondary 1B-10B+ option requiring 3 distributed clusters; contract stub.",
        )

    def build_gremlin_query(
        self, request: TraversalRequest
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded Gremlin traversal string for JanusGraph."""
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        limit = min(request.max_results, self._enforcer.config.max_results)
        edge_arg = (
            ", ".join(f"'{et}'" for et in request.edge_types)
            if request.edge_types
            else ""
        )
        script = (
            "g.V().has('global_id', within(seed_ids))"
            f".repeat(outE({edge_arg}).inV()).times({depth}).emit()"
            f".path().limit({limit})"
        )
        return script, {"seed_ids": list(request.seed_ids), "limit": limit}

    async def health_check(self) -> HealthStatus:
        status = await super().health_check()
        status.details.update(
            {
                "is_stub": True,
                "gremlin_url": self.gremlin_url,
                "scylla_keyspace": self.scylla_keyspace,
                "opensearch_index": self.opensearch_index,
            }
        )
        return status


class NebulaGraphStore(_ContractStubBase):
    """Tier-2 contract stub for NebulaGraph (Storage + Meta + Graphd)."""

    name = "nebulagraph"

    def __init__(
        self,
        *,
        graphd_hosts: list[str] | None = None,
        meta_hosts: list[str] | None = None,
        space_name: str = "scale_kg",
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
    ) -> None:
        super().__init__(enforcer=enforcer, num_buckets=num_buckets)
        self.graphd_hosts = graphd_hosts or ["localhost:9669"]
        self.meta_hosts = meta_hosts or ["localhost:9559"]
        self.space_name = space_name

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="nebulagraph",
            tier="tier2",
            storage_backend="NebulaGraph Distributed Storage + Meta Service",
            query_languages=["ngql", "opencypher"],
            supports_zero_etl=False,
            supports_cross_shard_edges=True,
            estimated_cost_per_billion_edges_usd_month=980.0,
            operational_complexity="medium",
            is_stub=True,
            notes="Optional distributed nGQL benchmark candidate; contract stub.",
        )

    def build_ngql_query(self, request: TraversalRequest) -> tuple[str, dict[str, Any]]:
        """Build a bounded nGQL GO/MATCH traversal statement for NebulaGraph."""
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        limit = min(request.max_results, self._enforcer.config.max_results)
        edge_clause = (
            " OVER " + ", ".join(request.edge_types)
            if request.edge_types
            else " OVER *"
        )
        seeds = ", ".join(f"'{sid}'" for sid in request.seed_ids)
        ngql = (
            f"USE {self.space_name}; "
            f"GO 1 TO {depth} STEPS FROM {seeds}{edge_clause} "
            f"YIELD edge AS e, dst(edge) AS dst | LIMIT {limit};"
        )
        return ngql, {"space": self.space_name, "limit": limit}

    async def health_check(self) -> HealthStatus:
        status = await super().health_check()
        status.details.update(
            {
                "is_stub": True,
                "space_name": self.space_name,
                "graphd_count": len(self.graphd_hosts),
                "meta_count": len(self.meta_hosts),
            }
        )
        return status
