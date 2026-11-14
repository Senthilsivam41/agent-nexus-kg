"""Partitioned in-memory reference GraphStore with supernode collapse and fault injection."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Literal

from scale_kg.adapters._traversal import resolve_logical_neighbors, run_guarded_bfs
from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    FaultState,
    GraphStats,
    GraphStore,
    HealthStatus,
)
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalRequest,
    TraversalResult,
)


class InMemoryGraphStore(GraphStore):
    """Domain-partitioned reference GraphStore for deterministic tests and parity checks."""

    name = "in_memory"

    def __init__(
        self,
        *,
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
    ) -> None:
        self._enforcer = enforcer or GuardrailEnforcer()
        self._num_buckets = num_buckets
        self._fault = FaultState()
        self._nodes: dict[Domain, dict[str, EntityNode]] = {d: {} for d in Domain}
        self._node_domain: dict[str, Domain] = {}
        self._out_edges: dict[Domain, dict[str, list[RelationshipEdge]]] = {
            d: defaultdict(list) for d in Domain
        }
        self._in_edges: dict[Domain, dict[str, list[RelationshipEdge]]] = {
            d: defaultdict(list) for d in Domain
        }

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="in_memory",
            tier="reference",
            storage_backend="In-process domain-partitioned adjacency dictionaries",
            query_languages=["python-bfs"],
            supports_zero_etl=False,
            supports_cross_shard_edges=True,
            estimated_cost_per_billion_edges_usd_month=0.0,
            operational_complexity="low",
            notes="Reference baseline for contract parity and unit testing; not persistent.",
        )

    async def _maybe_delay(self) -> None:
        if self._fault.latency_ms > 0:
            await asyncio.sleep(self._fault.latency_ms / 1000.0)

    async def upsert_nodes(self, nodes: list[EntityNode]) -> int:
        for node in nodes:
            self._enforcer.validate_node_write(node)
        for node in nodes:
            self._fault.check(node.domain)
            await self._maybe_delay()
            prev_domain = self._node_domain.get(node.id)
            if prev_domain is not None and prev_domain != node.domain:
                self._nodes[prev_domain].pop(node.id, None)
            self._nodes[node.domain][node.id] = node
            self._node_domain[node.id] = node.domain
        return len(nodes)

    @staticmethod
    def _upsert_edge_into_bucket(
        bucket: list[RelationshipEdge], edge: RelationshipEdge
    ) -> None:
        key = (edge.source_id, edge.target_id, edge.edge_type)
        for idx, existing in enumerate(bucket):
            if (existing.source_id, existing.target_id, existing.edge_type) == key:
                bucket[idx] = edge
                return
        bucket.append(edge)

    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        for edge in edges:
            self._enforcer.validate_edge_write(edge)
        for edge in edges:
            self._fault.check(edge.domain)
            await self._maybe_delay()
            self._upsert_edge_into_bucket(
                self._out_edges[edge.domain][edge.source_id], edge
            )
            target_domain = (
                edge.target_domain
                or self._node_domain.get(edge.target_id)
                or edge.domain
            )
            self._upsert_edge_into_bucket(
                self._in_edges[target_domain][edge.target_id], edge
            )
        return len(edges)

    async def get_node(self, node_id: str) -> EntityNode | None:
        domain = self._node_domain.get(node_id)
        self._fault.check(domain)
        await self._maybe_delay()
        if domain is None:
            return None
        return self._nodes[domain].get(node_id)

    async def _fetch_out_edges(self, node_id: str) -> list[RelationshipEdge]:
        domain = self._node_domain.get(node_id)
        self._fault.check(domain)
        out: list[RelationshipEdge] = []
        for d in Domain:
            edges = self._out_edges[d].get(node_id)
            if edges:
                self._fault.check(d)
                out.extend(edges)
        return out

    async def _fetch_in_edges(self, node_id: str) -> list[RelationshipEdge]:
        domain = self._node_domain.get(node_id)
        self._fault.check(domain)
        incoming: list[RelationshipEdge] = []
        for d in Domain:
            edges = self._in_edges[d].get(node_id)
            if edges:
                self._fault.check(d)
                incoming.extend(edges)
        return incoming

    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> list[tuple[RelationshipEdge, EntityNode]]:
        self._fault.check(self._node_domain.get(node_id))
        await self._maybe_delay()
        return await resolve_logical_neighbors(
            node_id,
            fetch_node=self.get_node,
            fetch_out_edges=self._fetch_out_edges,
            fetch_in_edges=self._fetch_in_edges,
            edge_types=edge_types,
            domains=domains,
            direction=direction,
            limit=limit,
            num_buckets=self._num_buckets,
        )

    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        for d in request.domains or []:
            self._fault.check(d)
        if not request.domains:
            self._fault.check(None)
        await self._maybe_delay()
        return await run_guarded_bfs(
            request,
            enforcer=self._enforcer,
            fetch_node=self.get_node,
            fetch_out_edges=self._fetch_out_edges,
        )

    async def set_fault_mode(
        self,
        mode: str | FaultMode,
        *,
        target_domain: Domain | None = None,
        latency_ms: float = 0.0,
        recover_after_calls: int | None = None,
    ) -> None:
        self._fault.configure(
            mode,
            target_domain=target_domain,
            latency_ms=latency_ms,
            recover_after_calls=recover_after_calls,
        )

    async def health_check(self) -> HealthStatus:
        healthy, status = self._fault.health_state()
        details: dict[str, str | int | float | bool] = {
            "partitions": len(Domain),
            "latency_ms": self._fault.latency_ms,
        }
        if self._fault.target_domain is not None:
            details["target_domain"] = self._fault.target_domain.value
        return HealthStatus(
            adapter_name=self.name,
            healthy=healthy,
            status=status,
            fault_mode=self._fault.mode.value,
            details=details,
        )

    async def count_stats(self) -> GraphStats:
        nodes_by_domain = {d.value: len(self._nodes[d]) for d in Domain}
        edges_by_domain = {
            d.value: sum(len(el) for el in self._out_edges[d].values()) for d in Domain
        }
        all_nodes = [n for d in Domain for n in self._nodes[d].values()]
        return GraphStats(
            node_count=sum(nodes_by_domain.values()),
            edge_count=sum(edges_by_domain.values()),
            bucket_node_count=sum(1 for n in all_nodes if n.is_bucket),
            proxy_node_count=sum(1 for n in all_nodes if n.is_proxy),
            nodes_by_domain=nodes_by_domain,
            edges_by_domain=edges_by_domain,
        )

    async def clear(self) -> None:
        self._nodes = {d: {} for d in Domain}
        self._node_domain.clear()
        self._out_edges = {d: defaultdict(list) for d in Domain}
        self._in_edges = {d: defaultdict(list) for d in Domain}
        self._fault.configure(FaultMode.NONE)
