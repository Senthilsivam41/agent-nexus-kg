"""Federated multi-hop QueryCoordinator, locality telemetry, and on-the-fly derived edges."""

from __future__ import annotations

from collections import Counter, defaultdict, deque
from collections.abc import Mapping
from typing import Literal

from pydantic import BaseModel, Field

from scale_kg.adapters.base import GraphStore, GraphStoreUnavailableError
from scale_kg.core.guardrails import DERIVED_EDGE_TYPES, GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalMetrics,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.routing.directory import EntityDirectory


class DerivedEdgeRule(BaseModel):
    """Declarative rule for computing derived edges at query time."""

    name: str
    derived_edge_type: str
    pattern: Literal["shared_neighbor", "two_hop_composition"]
    first_edge_types: list[str] | None = None
    second_edge_types: list[str] | None = None
    min_shared_neighbors: int = Field(default=1, ge=1)


DEFAULT_DERIVED_EDGE_RULES: tuple[DerivedEdgeRule, ...] = (
    DerivedEdgeRule(
        name="similar_to_shared_neighbor",
        derived_edge_type="SIMILAR_TO",
        pattern="shared_neighbor",
        min_shared_neighbors=1,
    ),
    DerivedEdgeRule(
        name="indirectly_uses_two_hop",
        derived_edge_type="INDIRECTLY_USES",
        pattern="two_hop_composition",
        first_edge_types=["USES", "DEPENDS_ON"],
        second_edge_types=["DEPENDS_ON", "USES", "POWERS", "REQUIRES"],
    ),
)


class CoordinatorStats(BaseModel):
    """Telemetry for federated query execution and derived edge materialization."""

    queries_executed: int = 0
    derived_edges_computed: int = 0
    hot_paths_promoted: int = 0
    promoted_edges: list[tuple[str, str, str]] = Field(default_factory=list)


class QueryCoordinator:
    """Routes bounded 1-to-3 hop traversals across domain shards and derives hot-path edges."""

    def __init__(
        self,
        stores: GraphStore | Mapping[Domain, GraphStore],
        *,
        directory: EntityDirectory | None = None,
        enforcer: GuardrailEnforcer | None = None,
        derived_rules: list[DerivedEdgeRule] | None = None,
        hot_path_threshold: int = 3,
    ) -> None:
        if hot_path_threshold < 1:
            raise ValueError(
                f"hot_path_threshold must be >= 1, got {hot_path_threshold}."
            )
        if isinstance(stores, GraphStore):
            self._shards: dict[Domain, GraphStore] = {d: stores for d in Domain}
        else:
            self._shards = dict(stores)
        self.directory = directory or EntityDirectory()
        self._enforcer = enforcer or GuardrailEnforcer()
        self.derived_rules: list[DerivedEdgeRule] = (
            list(derived_rules)
            if derived_rules is not None
            else list(DEFAULT_DERIVED_EDGE_RULES)
        )
        self.hot_path_threshold = hot_path_threshold

        self._derived_hit_counts: Counter[tuple[str, str, str]] = Counter()
        self.promoted_hot_paths: dict[tuple[str, str, str], RelationshipEdge] = {}
        self._queries_executed = 0
        self._derived_edges_computed = 0

    def shard_for(self, domain: Domain) -> GraphStore:
        """Return the GraphStore shard owning ``domain``."""
        shard = self._shards.get(domain)
        if shard is None:
            raise GraphStoreUnavailableError(
                f"No GraphStore shard configured for domain '{domain.value}'."
            )
        return shard

    def _unique_shards(self) -> list[GraphStore]:
        seen: set[int] = set()
        unique: list[GraphStore] = []
        for shard in self._shards.values():
            if id(shard) not in seen:
                seen.add(id(shard))
                unique.append(shard)
        return unique

    def _is_split_shard_topology(self, src_domain: Domain, dst_domain: Domain) -> bool:
        src_shard = self._shards.get(src_domain)
        dst_shard = self._shards.get(dst_domain)
        return (
            src_shard is not None
            and dst_shard is not None
            and src_shard is not dst_shard
        )

    async def upsert_nodes(
        self,
        nodes: list[EntityNode],
        *,
        aliases: Mapping[str, list[str]] | None = None,
    ) -> int:
        """Validate, register in EntityDirectory, and upsert nodes into owning shards."""
        for node in nodes:
            self._enforcer.validate_node_write(node)
        if not nodes:
            return 0

        by_domain: dict[Domain, list[EntityNode]] = defaultdict(list)
        for node in nodes:
            node_aliases = aliases.get(node.id) if aliases else None
            self.directory.register_node(node, aliases=node_aliases)
            by_domain[node.domain].append(node)

        total = 0
        for domain, batch in by_domain.items():
            total += await self.shard_for(domain).upsert_nodes(batch)
        return total

    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        """Validate edges, register cross-domain proxy vertices, and write to shards."""
        for edge in edges:
            self._enforcer.validate_edge_write(edge)
        if not edges:
            return 0

        edges_by_domain: dict[Domain, list[RelationshipEdge]] = defaultdict(list)
        proxies_by_domain: dict[Domain, dict[str, EntityNode]] = defaultdict(dict)

        for edge in edges:
            edges_by_domain[edge.domain].append(edge)
            proxy = self.directory.register_cross_domain_edge(edge)
            if (
                proxy is not None
                and proxy.target_domain is not None
                and self._is_split_shard_topology(edge.domain, proxy.target_domain)
            ):
                proxies_by_domain[edge.domain][proxy.id] = proxy

        for domain, proxy_map in proxies_by_domain.items():
            if proxy_map:
                await self.shard_for(domain).upsert_nodes(list(proxy_map.values()))

        total = 0
        for domain, batch in edges_by_domain.items():
            total += await self.shard_for(domain).upsert_edges(batch)
        return total

    async def get_node(self, entity_id: str) -> EntityNode | None:
        """Fetch an EntityNode via Hop-0 canonical routing or EntityDirectory fallback."""
        canonical_id = self.directory.canonicalize(entity_id)
        domain = self.directory.try_resolve_domain(canonical_id)
        if domain is not None:
            node = await self.shard_for(domain).get_node(canonical_id)
            if node is not None and not node.is_proxy:
                return node
        for shard in self._unique_shards():
            node = await shard.get_node(canonical_id)
            if node is not None and not node.is_proxy:
                self.directory.register_node(node)
                return node
        return None

    async def _resolve_seed(
        self, raw_seed_id: str
    ) -> tuple[str, EntityNode | None, Domain | None]:
        canonical_id = self.directory.canonicalize(raw_seed_id)
        domain = self.directory.try_resolve_domain(canonical_id)
        if domain is not None:
            node = await self.shard_for(domain).get_node(canonical_id)
            if node is not None and node.is_proxy and node.target_domain is not None:
                domain = node.target_domain
                node = await self.shard_for(domain).get_node(canonical_id)
            return canonical_id, node, domain

        for shard_domain, shard in self._shards.items():
            node = await shard.get_node(canonical_id)
            if node is not None and not node.is_proxy:
                self.directory.register_node(node)
                return canonical_id, node, node.domain or shard_domain
        return canonical_id, None, None

    async def _resolve_neighbor_real_node(
        self,
        neighbor: EntityNode,
        edge: RelationshipEdge,
    ) -> tuple[EntityNode, Domain]:
        if neighbor.is_proxy and neighbor.target_domain is not None:
            target_domain = self.directory.resolve_next_hop_domain(
                neighbor=neighbor, edge=edge
            )
            assert target_domain is not None
            real_node = await self.shard_for(target_domain).get_node(neighbor.id)
            if real_node is not None and not real_node.is_proxy:
                return real_node, target_domain
            return (
                neighbor.model_copy(
                    update={"domain": target_domain, "is_proxy": False}
                ),
                target_domain,
            )
        target_domain = edge.target_domain or neighbor.domain
        return neighbor, target_domain

    async def _apply_derived_rules(
        self,
        *,
        collected_nodes: dict[str, EntityNode],
        collected_edges: dict[tuple[str, str, str], RelationshipEdge],
        logical_paths: list[list[str]],
        seen_paths: set[tuple[str, ...]],
        effective_max_depth: int,
        allowed_domains: set[Domain] | None,
        allowed_edge_types: set[str] | None,
    ) -> list[RelationshipEdge]:
        core_edges = [e for e in collected_edges.values() if not e.is_derived]
        synthesized: list[RelationshipEdge] = []
        seen_derived: set[tuple[str, str, str]] = set()

        def _maybe_add_derived(
            src_id: str,
            dst_node: EntityNode,
            edge_type: str,
            src_domain: Domain,
            routing_props: dict[str, str | int | float | bool],
        ) -> None:
            if src_id == dst_node.id:
                return
            if allowed_edge_types and edge_type not in allowed_edge_types:
                return
            if allowed_domains and dst_node.domain not in allowed_domains:
                return
            key = (src_id, dst_node.id, edge_type)
            if key in seen_derived or key in collected_edges:
                return
            seen_derived.add(key)
            is_hot = key in self.promoted_hot_paths
            derived_edge = RelationshipEdge(
                source_id=src_id,
                target_id=dst_node.id,
                edge_type=edge_type,
                domain=src_domain,
                target_domain=dst_node.domain,
                is_derived=True,
                materialized_hot_path=is_hot,
                routing_props=routing_props,
            )
            synthesized.append(derived_edge)
            collected_nodes.setdefault(dst_node.id, dst_node)
            path = [src_id, dst_node.id]
            if tuple(path) not in seen_paths:
                seen_paths.add(tuple(path))
                logical_paths.append(path)

        for rule in self.derived_rules:
            if allowed_edge_types and rule.derived_edge_type not in allowed_edge_types:
                continue

            if rule.pattern == "two_hop_composition" and effective_max_depth >= 2:
                out_by_src: dict[str, list[RelationshipEdge]] = defaultdict(list)
                for edge in core_edges:
                    out_by_src[edge.source_id].append(edge)

                for e1 in core_edges:
                    if (
                        rule.first_edge_types
                        and e1.edge_type not in rule.first_edge_types
                    ):
                        continue
                    for e2 in out_by_src.get(e1.target_id, []):
                        if (
                            rule.second_edge_types
                            and e2.edge_type not in rule.second_edge_types
                        ):
                            continue
                        dst_node = collected_nodes.get(e2.target_id)
                        if dst_node is None:
                            continue
                        _maybe_add_derived(
                            e1.source_id,
                            dst_node,
                            rule.derived_edge_type,
                            e1.domain,
                            {"derived_rule": rule.name, "via": e1.target_id},
                        )

            elif rule.pattern == "shared_neighbor":
                # For every core edge u -> w, discover peers v != u that also point to w.
                out_by_src_w: dict[str, list[RelationshipEdge]] = defaultdict(list)
                for edge in core_edges:
                    if (
                        rule.first_edge_types
                        and edge.edge_type not in rule.first_edge_types
                    ):
                        continue
                    out_by_src_w[edge.source_id].append(edge)

                unique_shards = self._unique_shards()
                for src_id, out_edges in out_by_src_w.items():
                    src_node = collected_nodes.get(src_id)
                    if src_node is None:
                        continue
                    shared_counts: Counter[str] = Counter()
                    peer_nodes: dict[str, EntityNode] = {}
                    via_examples: dict[str, str] = {}
                    for edge in out_edges:
                        for shard in unique_shards:
                            in_neighbors = await shard.get_neighbors(
                                edge.target_id,
                                edge_types=rule.second_edge_types,
                                direction="in",
                                limit=self._enforcer.config.max_results,
                            )
                            for in_edge, peer in in_neighbors:
                                if in_edge.is_derived or peer.id == src_id:
                                    continue
                                real_peer, _ = await self._resolve_neighbor_real_node(
                                    peer, in_edge
                                )
                                if real_peer.id == src_id:
                                    continue
                                shared_counts[real_peer.id] += 1
                                peer_nodes[real_peer.id] = real_peer
                                via_examples.setdefault(real_peer.id, edge.target_id)

                    for peer_id, count in sorted(shared_counts.items()):
                        if count < rule.min_shared_neighbors:
                            continue
                        _maybe_add_derived(
                            src_id,
                            peer_nodes[peer_id],
                            rule.derived_edge_type,
                            src_node.domain,
                            {
                                "derived_rule": rule.name,
                                "shared_count": count,
                                "via": via_examples[peer_id],
                            },
                        )

        return synthesized

    async def _record_and_promote_hot_paths(
        self, derived_edges: list[RelationshipEdge]
    ) -> list[RelationshipEdge]:
        updated: list[RelationshipEdge] = []
        for edge in derived_edges:
            key = (edge.source_id, edge.target_id, edge.edge_type)
            self._derived_hit_counts[key] += 1
            if (
                key not in self.promoted_hot_paths
                and self._derived_hit_counts[key] >= self.hot_path_threshold
            ):
                promoted = edge.model_copy(update={"materialized_hot_path": True})
                self.promoted_hot_paths[key] = promoted
                await self.upsert_edges([promoted])
                updated.append(promoted)
            elif key in self.promoted_hot_paths:
                updated.append(self.promoted_hot_paths[key])
            else:
                updated.append(edge)
        return updated

    async def traverse(self, request: TraversalRequest) -> TraversalResult:
        """Execute a guardrail-enforced federated 1-to-3 hop traversal across domain shards."""
        self._queries_executed += 1
        tracker = self._enforcer.create_runtime_tracker(request)
        effective_max_depth = min(request.max_depth, self._enforcer.config.max_depth)
        allowed_edge_types = set(request.edge_types) if request.edge_types else None
        allowed_domains = set(request.domains) if request.domains else None

        derived_type_names = set(DERIVED_EDGE_TYPES) | {
            r.derived_edge_type for r in self.derived_rules
        }
        core_edge_filter: list[str] | None = request.edge_types
        if (
            request.include_derived
            and allowed_edge_types is not None
            and allowed_edge_types & derived_type_names
        ):
            # When derived edge types are requested, core expansion must still follow core edges.
            core_edge_filter = None

        collected_nodes: dict[str, EntityNode] = {}
        collected_edges: dict[tuple[str, str, str], RelationshipEdge] = {}
        logical_paths: list[list[str]] = []
        seen_paths: set[tuple[str, ...]] = set()

        queue: deque[tuple[str, int, list[str], Domain | None]] = deque()
        best_depth: dict[str, int] = {}

        for raw_seed in request.seed_ids:
            cid, seed_node, seed_domain = await self._resolve_seed(raw_seed)
            if seed_node is not None:
                collected_nodes.setdefault(seed_node.id, seed_node)
            if cid not in best_depth:
                best_depth[cid] = 0
                queue.append((cid, 0, [cid], seed_domain))

        nodes_expanded = 0
        depth_reached = 0
        total_logical_edges = 0
        cross_domain_hops = 0

        def _locality_ratio() -> float:
            if total_logical_edges == 0:
                return 1.0
            return round(
                (total_logical_edges - cross_domain_hops) / total_logical_edges, 4
            )

        def _abort(violation) -> TraversalResult:
            return tracker.build_aborted_result(
                violation=violation,
                nodes=list(collected_nodes.values()),
                edges=list(collected_edges.values()),
                paths=logical_paths,
                depth_reached=depth_reached,
                expanded_nodes=nodes_expanded,
                cross_domain_hops=cross_domain_hops,
                single_domain_locality_ratio=_locality_ratio(),
            )

        while queue:
            curr_id, depth, path, curr_domain = queue.popleft()
            nodes_expanded += 1

            violation = tracker.check_step(
                depth=depth,
                expanded_nodes=nodes_expanded,
                results_count=len(collected_nodes),
            )
            if violation is not None:
                return _abort(violation)

            if depth >= effective_max_depth or curr_domain is None:
                continue

            shard = self.shard_for(curr_domain)
            neighbors = await shard.get_neighbors(
                curr_id,
                edge_types=core_edge_filter,
                direction="out",
                limit=self._enforcer.config.max_results,
            )

            for edge, neighbor in neighbors:
                if edge.is_derived and not request.include_derived:
                    continue
                if (
                    allowed_edge_types
                    and not (
                        request.include_derived and edge.edge_type in derived_type_names
                    )
                    and edge.edge_type not in allowed_edge_types
                    and core_edge_filter is not None
                ):
                    continue

                real_node, next_domain = await self._resolve_neighbor_real_node(
                    neighbor, edge
                )
                if allowed_domains and next_domain not in allowed_domains:
                    continue

                next_depth = depth + 1
                depth_reached = max(depth_reached, next_depth)
                new_path = [*path, real_node.id]

                collected_nodes.setdefault(real_node.id, real_node)
                edge_key = (edge.source_id, edge.target_id, edge.edge_type)
                if edge_key not in collected_edges:
                    collected_edges[edge_key] = edge
                    total_logical_edges += 1
                    if edge.domain != next_domain:
                        cross_domain_hops += 1

                if tuple(new_path) not in seen_paths:
                    seen_paths.add(tuple(new_path))
                    logical_paths.append(new_path)

                violation = tracker.check_step(
                    depth=next_depth,
                    expanded_nodes=nodes_expanded,
                    results_count=len(collected_nodes),
                )
                if violation is not None:
                    return _abort(violation)

                if best_depth.get(real_node.id, effective_max_depth + 1) > next_depth:
                    best_depth[real_node.id] = next_depth
                    queue.append((real_node.id, next_depth, new_path, next_domain))

        if request.include_derived:
            derived = await self._apply_derived_rules(
                collected_nodes=collected_nodes,
                collected_edges=collected_edges,
                logical_paths=logical_paths,
                seen_paths=seen_paths,
                effective_max_depth=effective_max_depth,
                allowed_domains=allowed_domains,
                allowed_edge_types=allowed_edge_types,
            )
            self._derived_edges_computed += len(derived)
            promoted_or_derived = await self._record_and_promote_hot_paths(derived)
            for d_edge in promoted_or_derived:
                d_key = (d_edge.source_id, d_edge.target_id, d_edge.edge_type)
                if d_key not in collected_edges:
                    collected_edges[d_key] = d_edge
                    total_logical_edges += 1
                    d_target = d_edge.target_domain or d_edge.domain
                    if d_edge.domain != d_target:
                        cross_domain_hops += 1

        if allowed_edge_types is not None:
            filtered_edges = [
                e for e in collected_edges.values() if e.edge_type in allowed_edge_types
            ]
        else:
            filtered_edges = list(collected_edges.values())

        canonical_seeds = [self.directory.canonicalize(s) for s in request.seed_ids]
        return TraversalResult(
            seed_ids=canonical_seeds,
            nodes=list(collected_nodes.values()),
            edges=filtered_edges,
            paths=logical_paths,
            metrics=TraversalMetrics(
                depth_reached=depth_reached,
                nodes_expanded=nodes_expanded,
                results_returned=len(collected_nodes),
                execution_time_ms=round(tracker.elapsed_seconds * 1000.0, 3),
                cross_domain_hops=cross_domain_hops,
                single_domain_locality_ratio=_locality_ratio(),
                aborted=False,
                violation=None,
            ),
        )

    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        """Alias for ``traverse()`` matching the GraphStore traversal method name."""
        return await self.traverse(request)

    def stats(self) -> CoordinatorStats:
        """Return coordinator execution and hot-path promotion telemetry."""
        return CoordinatorStats(
            queries_executed=self._queries_executed,
            derived_edges_computed=self._derived_edges_computed,
            hot_paths_promoted=len(self.promoted_hot_paths),
            promoted_edges=sorted(self.promoted_hot_paths.keys()),
        )
