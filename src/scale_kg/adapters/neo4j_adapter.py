"""Async Neo4j Composite/shard adapter with bounded Cypher and cross-shard proxy linking."""

from __future__ import annotations

import json
import time
from types import TracebackType
from typing import Any, Literal, Protocol, Self

from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    FaultState,
    GraphStats,
    GraphStore,
    GraphStoreError,
    GraphStoreUnavailableError,
    HealthStatus,
    canonical_domain_from_id,
)
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalMetrics,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.core.supernode import BUCKET_EDGE_TYPE, SupernodeBucketer


class AsyncCypherResult(Protocol):
    """Minimal async Neo4j result protocol for driver mocking."""

    async def data(self) -> list[dict[str, Any]]: ...


class AsyncCypherSession(Protocol):
    """Minimal async Neo4j session protocol."""

    async def run(
        self, query: str, parameters: dict[str, Any] | None = None
    ) -> AsyncCypherResult: ...

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...


class AsyncCypherDriver(Protocol):
    """Minimal async Neo4j driver protocol."""

    def session(self, *, database: str | None = None) -> AsyncCypherSession: ...

    async def verify_connectivity(self) -> None: ...

    async def close(self) -> None: ...


class Neo4jGraphStore(GraphStore):
    """Baseline async Neo4j adapter with Composite shard routing and proxy nodes."""

    name = "neo4j"

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        *,
        auth: tuple[str, str] = ("neo4j", "password"),
        database: str = "composite",
        shard_databases: dict[Domain, str] | None = None,
        driver: AsyncCypherDriver | None = None,
        enforcer: GuardrailEnforcer | None = None,
    ) -> None:
        self.uri = uri
        self.auth = auth
        self.database = database
        self.shard_databases: dict[Domain, str] = shard_databases or {
            d: f"kg_{d.value}" for d in Domain
        }
        self._driver: AsyncCypherDriver | None = driver
        self._enforcer = enforcer or GuardrailEnforcer()
        self._fault = FaultState()

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="neo4j",
            tier="tier1",
            storage_backend="Neo4j Native Property Graph (Composite Shards + Proxy Nodes)",
            query_languages=["cypher"],
            supports_zero_etl=False,
            supports_cross_shard_edges=False,
            estimated_cost_per_billion_edges_usd_month=2450.0,
            operational_complexity="low",
            notes="Baseline small/medium engine; cross-shard hops use ghost/proxy vertices.",
        )

    def _get_driver(self) -> AsyncCypherDriver:
        if self._driver is not None:
            return self._driver
        try:
            from neo4j import AsyncGraphDatabase  # type: ignore[import-not-found]
        except ImportError as exc:
            raise GraphStoreError(
                "neo4j driver is not installed; install scale-kg[neo4j] or pass a mock driver."
            ) from exc
        self._driver = AsyncGraphDatabase.driver(self.uri, auth=self.auth)
        return self._driver

    def shard_for_domain(self, domain: Domain | None) -> str:
        """Resolve Neo4j shard database name for a semantic domain."""
        if domain is None:
            return self.database
        return self.shard_databases.get(domain, self.database)

    async def _run_cypher(
        self,
        query: str,
        parameters: dict[str, Any] | None = None,
        *,
        domain: Domain | None = None,
    ) -> list[dict[str, Any]]:
        self._fault.check(domain)
        driver = self._get_driver()
        db_name = self.shard_for_domain(domain)
        try:
            async with driver.session(database=db_name) as session:
                result = await session.run(query, parameters or {})
                return await result.data()
        except TimeoutError:
            raise
        except GraphStoreError:
            raise
        except Exception as exc:
            raise GraphStoreUnavailableError(
                f"Neo4j Cypher execution failed on shard '{db_name}': {exc}"
            ) from exc

    @staticmethod
    def build_upsert_nodes_cypher() -> str:
        return (
            "UNWIND $nodes AS n "
            "MERGE (v:Entity {id: n.id}) "
            "SET v.domain = n.domain, "
            "v.entity_type = n.entity_type, "
            "v.lakehouse_ref = n.lakehouse_ref, "
            "v.is_proxy = n.is_proxy, "
            "v.target_domain = n.target_domain, "
            "v.is_bucket = n.is_bucket, "
            "v.routing_props_json = n.routing_props_json "
            "RETURN count(v) AS upserted"
        )

    @staticmethod
    def build_upsert_edges_cypher() -> str:
        return (
            "UNWIND $edges AS e "
            "MERGE (s:Entity {id: e.source_id}) "
            "MERGE (t:Entity {id: e.target_id}) "
            "MERGE (s)-[r:REL {edge_type: e.edge_type}]->(t) "
            "SET r.domain = e.domain, "
            "r.target_domain = e.target_domain, "
            "r.is_derived = e.is_derived, "
            "r.materialized_hot_path = e.materialized_hot_path, "
            "r.routing_props_json = e.routing_props_json "
            "RETURN count(r) AS upserted"
        )

    @staticmethod
    def build_neighbors_cypher(
        *,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> str:
        if direction == "out":
            pattern = "(s:Entity {id: $node_id})-[r:REL]->(t:Entity)"
        elif direction == "in":
            pattern = "(s:Entity {id: $node_id})<-[r:REL]-(t:Entity)"
        else:
            pattern = "(s:Entity {id: $node_id})-[r:REL]-(t:Entity)"
        return (
            f"MATCH {pattern} "
            "WHERE ($edge_types_empty OR r.edge_type IN $edge_types) "
            "AND ($domains_empty OR t.domain IN $domains) "
            f"RETURN r, t LIMIT {limit}"
        )

    def build_k_hop_cypher(
        self, request: TraversalRequest, *, max_depth_override: int | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded parameterized Cypher k-hop traversal query."""
        depth = max_depth_override or min(
            request.max_depth, self._enforcer.config.max_depth
        )
        limit = min(request.max_results, self._enforcer.config.max_results)
        cypher = (
            f"MATCH p = (s:Entity)-[r:REL*1..{depth}]->(t:Entity) "
            "WHERE s.id IN $seed_ids "
            "AND ($edge_types_empty OR ALL(rel IN relationships(p) WHERE rel.edge_type IN $edge_types)) "
            "AND ($domains_empty OR ALL(n IN nodes(p) WHERE n.domain IN $domains OR n.is_proxy = true)) "
            "AND ($include_derived OR ALL(rel IN relationships(p) WHERE coalesce(rel.is_derived, false) = false)) "
            f"RETURN nodes(p) AS nodes, relationships(p) AS edges, [n IN nodes(p) | n.id] AS path LIMIT {limit}"
        )
        params: dict[str, Any] = {
            "seed_ids": list(request.seed_ids),
            "edge_types": list(request.edge_types) if request.edge_types else [],
            "edge_types_empty": not bool(request.edge_types),
            "domains": [d.value for d in request.domains] if request.domains else [],
            "domains_empty": not bool(request.domains),
            "include_derived": request.include_derived,
            "limit": limit,
        }
        return cypher, params

    @staticmethod
    def create_proxy_node(
        target_id: str,
        *,
        owning_domain: Domain,
        target_domain: Domain,
    ) -> EntityNode:
        """Build a cross-shard ghost/proxy node pointing to the owning remote shard."""
        return EntityNode(
            id=target_id,
            domain=owning_domain,
            entity_type="Proxy",
            routing_props={"proxy_target_domain": target_domain.value},
            is_proxy=True,
            target_domain=target_domain,
        )

    @staticmethod
    def _serialize_node(node: EntityNode) -> dict[str, Any]:
        return {
            "id": node.id,
            "domain": node.domain.value,
            "entity_type": node.entity_type,
            "lakehouse_ref": node.lakehouse_ref,
            "is_proxy": node.is_proxy,
            "target_domain": node.target_domain.value if node.target_domain else None,
            "is_bucket": node.is_bucket,
            "routing_props_json": json.dumps(node.routing_props, sort_keys=True),
        }

    @staticmethod
    def _deserialize_node(record: dict[str, Any]) -> EntityNode:
        routing_raw = record.get("routing_props_json")
        routing_props = (
            json.loads(routing_raw)
            if isinstance(routing_raw, str)
            else record.get("routing_props", {})
        )
        return EntityNode(
            id=str(record["id"]),
            domain=Domain(record["domain"]),
            entity_type=str(record.get("entity_type") or "Entity"),
            routing_props=routing_props,
            lakehouse_ref=record.get("lakehouse_ref"),
            is_proxy=bool(record.get("is_proxy", False)),
            target_domain=Domain(record["target_domain"])
            if record.get("target_domain")
            else None,
            is_bucket=bool(record.get("is_bucket", False)),
        )

    @staticmethod
    def _serialize_edge(edge: RelationshipEdge) -> dict[str, Any]:
        return {
            "source_id": edge.source_id,
            "target_id": edge.target_id,
            "edge_type": edge.edge_type,
            "domain": edge.domain.value,
            "target_domain": edge.target_domain.value if edge.target_domain else None,
            "is_derived": edge.is_derived,
            "materialized_hot_path": edge.materialized_hot_path,
            "routing_props_json": json.dumps(edge.routing_props, sort_keys=True),
        }

    @staticmethod
    def _deserialize_edge(record: dict[str, Any]) -> RelationshipEdge:
        routing_raw = record.get("routing_props_json")
        routing_props = (
            json.loads(routing_raw)
            if isinstance(routing_raw, str)
            else record.get("routing_props", {})
        )
        return RelationshipEdge(
            source_id=str(record["source_id"]),
            target_id=str(record["target_id"]),
            edge_type=str(record["edge_type"]),
            domain=Domain(record["domain"]),
            target_domain=Domain(record["target_domain"])
            if record.get("target_domain")
            else None,
            is_derived=bool(record.get("is_derived", False)),
            materialized_hot_path=bool(record.get("materialized_hot_path", False)),
            routing_props=routing_props,
        )

    async def upsert_nodes(self, nodes: list[EntityNode]) -> int:
        for node in nodes:
            self._enforcer.validate_node_write(node)
        if not nodes:
            return 0
        by_domain: dict[Domain, list[dict[str, Any]]] = {}
        for node in nodes:
            by_domain.setdefault(node.domain, []).append(self._serialize_node(node))
        query = self.build_upsert_nodes_cypher()
        for domain, batch in by_domain.items():
            await self._run_cypher(query, {"nodes": batch}, domain=domain)
        return len(nodes)

    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        for edge in edges:
            self._enforcer.validate_edge_write(edge)
        if not edges:
            return 0

        proxies_by_domain: dict[Domain, dict[str, dict[str, Any]]] = {}
        edges_by_domain: dict[Domain, list[dict[str, Any]]] = {}

        for edge in edges:
            edges_by_domain.setdefault(edge.domain, []).append(
                self._serialize_edge(edge)
            )
            if edge.target_domain is not None and edge.target_domain != edge.domain:
                proxy = self.create_proxy_node(
                    edge.target_id,
                    owning_domain=edge.domain,
                    target_domain=edge.target_domain,
                )
                proxies_by_domain.setdefault(edge.domain, {})[proxy.id] = (
                    self._serialize_node(proxy)
                )

        proxy_query = self.build_upsert_nodes_cypher()
        for domain, proxy_map in proxies_by_domain.items():
            await self._run_cypher(
                proxy_query, {"nodes": list(proxy_map.values())}, domain=domain
            )

        edge_query = self.build_upsert_edges_cypher()
        for domain, batch in edges_by_domain.items():
            await self._run_cypher(edge_query, {"edges": batch}, domain=domain)
        return len(edges)

    async def get_node(self, node_id: str) -> EntityNode | None:
        hint = canonical_domain_from_id(node_id)
        domains_to_query = (
            [hint, *(d for d in Domain if d != hint)] if hint else list(Domain)
        )
        query = (
            "MATCH (n:Entity {id: $id}) "
            "WHERE coalesce(n.is_proxy, false) = false "
            "RETURN n.id AS id, n.domain AS domain, n.entity_type AS entity_type, "
            "n.lakehouse_ref AS lakehouse_ref, n.is_proxy AS is_proxy, "
            "n.target_domain AS target_domain, n.is_bucket AS is_bucket, "
            "n.routing_props_json AS routing_props_json LIMIT 1"
        )
        for domain in domains_to_query:
            rows = await self._run_cypher(query, {"id": node_id}, domain=domain)
            if rows:
                record = rows[0]["n"] if "n" in rows[0] else rows[0]
                return self._deserialize_node(record)
        return None

    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> list[tuple[RelationshipEdge, EntityNode]]:
        hint = canonical_domain_from_id(node_id)
        target_domains = [hint] if hint else list(Domain)
        cypher = self.build_neighbors_cypher(direction=direction, limit=limit)
        params = {
            "node_id": node_id,
            "edge_types": list(edge_types) if edge_types else [],
            "edge_types_empty": not bool(edge_types),
            "domains": [d.value for d in domains] if domains else [],
            "domains_empty": not bool(domains),
        }

        results: list[tuple[RelationshipEdge, EntityNode]] = []
        seen_keys: set[tuple[str, str, str]] = set()
        for dom in target_domains:
            rows = await self._run_cypher(cypher, params, domain=dom)
            for row in rows:
                edge = self._deserialize_edge(row["r"])
                if edge.edge_type == BUCKET_EDGE_TYPE:
                    continue
                edge = edge.model_copy(
                    update={
                        "source_id": SupernodeBucketer.unwrap_bucket_id(edge.source_id),
                        "target_id": SupernodeBucketer.unwrap_bucket_id(edge.target_id),
                    }
                )
                node = self._deserialize_node(row["t"])
                if node.is_proxy and node.target_domain is not None:
                    resolved = await self.get_node(node.id)
                    if resolved is not None:
                        node = resolved
                if domains and node.domain not in domains:
                    continue
                key = (edge.source_id, edge.target_id, edge.edge_type)
                if key not in seen_keys:
                    seen_keys.add(key)
                    results.append((edge, node))
                    if len(results) >= limit:
                        return results
        return results

    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        tracker = self._enforcer.create_runtime_tracker(request)
        effective_max_depth = min(request.max_depth, self._enforcer.config.max_depth)
        started = time.monotonic()

        collected_nodes: dict[str, EntityNode] = {}
        collected_edges: dict[tuple[str, str, str], RelationshipEdge] = {}
        logical_paths: list[list[str]] = []
        seen_paths: set[tuple[str, ...]] = set()
        unwrapped_hops_total = 0
        depth_reached = 0
        nodes_expanded = 0

        # Track (seed_id, shard_domain, depth_used, prefix_path) for cross-shard proxy jumps
        pending_shards: list[tuple[str, Domain, int, list[str]]] = []
        for seed_id in request.seed_ids:
            dom = canonical_domain_from_id(seed_id)
            if dom is None:
                node = await self.get_node(seed_id)
                dom = node.domain if node is not None else Domain.CUSTOMER
            pending_shards.append((seed_id, dom, 0, []))

        visited_shard_seeds: set[tuple[str, Domain]] = set()
        while pending_shards:
            seed_id, shard_domain, depth_used, prefix_path = pending_shards.pop(0)
            if (seed_id, shard_domain) in visited_shard_seeds:
                continue
            visited_shard_seeds.add((seed_id, shard_domain))

            remaining_depth = effective_max_depth - depth_used
            if remaining_depth < 1:
                continue

            sub_req = request.model_copy(
                update={"seed_ids": [seed_id], "max_depth": remaining_depth}
            )
            cypher, params = self.build_k_hop_cypher(
                sub_req, max_depth_override=remaining_depth
            )
            rows = await self._run_cypher(cypher, params, domain=shard_domain)

            for row in rows:
                row_nodes = [self._deserialize_node(n) for n in row.get("nodes", [])]
                row_edges = [self._deserialize_edge(e) for e in row.get("edges", [])]
                nodes_expanded += len(row_nodes)

                for node in row_nodes:
                    if node.is_bucket:
                        continue
                    if node.is_proxy and node.target_domain is not None:
                        continue
                    collected_nodes.setdefault(node.id, node)

                for edge in row_edges:
                    if edge.edge_type == BUCKET_EDGE_TYPE:
                        continue
                    if edge.is_derived and not request.include_derived:
                        continue
                    unwrapped = edge.model_copy(
                        update={
                            "source_id": SupernodeBucketer.unwrap_bucket_id(
                                edge.source_id
                            ),
                            "target_id": SupernodeBucketer.unwrap_bucket_id(
                                edge.target_id
                            ),
                        }
                    )
                    collected_edges.setdefault(
                        (unwrapped.source_id, unwrapped.target_id, unwrapped.edge_type),
                        unwrapped,
                    )

                raw_path = [str(v) for v in row.get("path", [n.id for n in row_nodes])]
                local_lp, unwrapped_hops = SupernodeBucketer.unwrap_path(raw_path)
                unwrapped_hops_total += unwrapped_hops
                full_lp = [*prefix_path[:-1], *local_lp] if prefix_path else local_lp
                if len(full_lp) > 1 and tuple(full_lp) not in seen_paths:
                    seen_paths.add(tuple(full_lp))
                    logical_paths.append(full_lp)
                    depth_reached = max(depth_reached, len(full_lp) - 1)

                # Schedule cross-shard continuation when a proxy node is encountered
                local_depth = max(0, len(local_lp) - 1)
                for node in row_nodes:
                    if node.is_proxy and node.target_domain is not None:
                        resolved = await self.get_node(node.id)
                        if resolved is not None:
                            collected_nodes.setdefault(resolved.id, resolved)
                        next_used = depth_used + local_depth
                        if next_used < effective_max_depth:
                            pending_shards.append(
                                (node.id, node.target_domain, next_used, full_lp)
                            )

                violation = tracker.check_step(
                    depth=depth_reached,
                    expanded_nodes=nodes_expanded,
                    results_count=len(collected_nodes),
                )
                if violation is not None:
                    cross_domain = sum(
                        1
                        for e in collected_edges.values()
                        if e.target_domain is not None and e.target_domain != e.domain
                    )
                    locality = (
                        round(
                            (len(collected_edges) - cross_domain)
                            / len(collected_edges),
                            4,
                        )
                        if collected_edges
                        else 1.0
                    )
                    return tracker.build_aborted_result(
                        violation=violation,
                        nodes=list(collected_nodes.values()),
                        edges=list(collected_edges.values()),
                        paths=logical_paths,
                        depth_reached=depth_reached,
                        expanded_nodes=nodes_expanded,
                        cross_domain_hops=cross_domain,
                        single_domain_locality_ratio=locality,
                        bucket_hops_unwrapped=unwrapped_hops_total,
                    )

        edges_list = list(collected_edges.values())
        cross_domain = sum(
            1
            for e in edges_list
            if e.target_domain is not None and e.target_domain != e.domain
        )
        locality = (
            round((len(edges_list) - cross_domain) / len(edges_list), 4)
            if edges_list
            else 1.0
        )
        elapsed_ms = round((time.monotonic() - started) * 1000.0, 3)

        return TraversalResult(
            seed_ids=list(request.seed_ids),
            nodes=list(collected_nodes.values()),
            edges=edges_list,
            paths=logical_paths,
            metrics=TraversalMetrics(
                depth_reached=depth_reached,
                nodes_expanded=nodes_expanded,
                results_returned=len(collected_nodes),
                execution_time_ms=elapsed_ms,
                cross_domain_hops=cross_domain,
                single_domain_locality_ratio=locality,
                bucket_hops_unwrapped=unwrapped_hops_total,
                aborted=False,
                violation=None,
            ),
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
        if healthy and self._driver is not None:
            try:
                await self._driver.verify_connectivity()
            except Exception:  # noqa: BLE001 — any driver failure marks the adapter unavailable
                healthy = False
                status = "unavailable"
        return HealthStatus(
            adapter_name=self.name,
            healthy=healthy,
            status=status,
            fault_mode=self._fault.mode.value,
            details={
                "database": self.database,
                "shard_count": len(self.shard_databases),
            },
        )

    async def count_stats(self) -> GraphStats:
        nodes_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        edges_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        bucket_nodes = 0
        proxy_nodes = 0

        node_q = (
            "MATCH (n:Entity) RETURN count(n) AS total, "
            "sum(CASE WHEN coalesce(n.is_bucket, false) THEN 1 ELSE 0 END) AS buckets, "
            "sum(CASE WHEN coalesce(n.is_proxy, false) THEN 1 ELSE 0 END) AS proxies"
        )
        edge_q = "MATCH ()-[r:REL]->() RETURN count(r) AS total"

        for domain in Domain:
            nrows = await self._run_cypher(node_q, domain=domain)
            if nrows:
                nodes_by_domain[domain.value] = int(nrows[0].get("total", 0))
                bucket_nodes += int(nrows[0].get("buckets", 0))
                proxy_nodes += int(nrows[0].get("proxies", 0))
            erows = await self._run_cypher(edge_q, domain=domain)
            if erows:
                edges_by_domain[domain.value] = int(erows[0].get("total", 0))

        return GraphStats(
            node_count=sum(nodes_by_domain.values()),
            edge_count=sum(edges_by_domain.values()),
            bucket_node_count=bucket_nodes,
            proxy_node_count=proxy_nodes,
            nodes_by_domain=nodes_by_domain,
            edges_by_domain=edges_by_domain,
        )

    async def clear(self) -> None:
        for domain in Domain:
            await self._run_cypher("MATCH (n:Entity) DETACH DELETE n", domain=domain)
        self._fault.configure(FaultMode.NONE)
