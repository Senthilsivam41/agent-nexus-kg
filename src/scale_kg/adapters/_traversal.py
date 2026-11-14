"""Shared bounded BFS traversal, supernode bucket hop collapse, and neighbor unwrapping."""

from __future__ import annotations

from collections import deque
from collections.abc import Awaitable, Callable
from typing import Literal

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

NodeFetcher = Callable[[str], Awaitable[EntityNode | None]]
OutEdgeFetcher = Callable[[str], Awaitable[list[RelationshipEdge]]]
InEdgeFetcher = Callable[[str], Awaitable[list[RelationshipEdge]]]


def _unwrap_edge(edge: RelationshipEdge) -> RelationshipEdge:
    """Return an edge with any ``::bucket::k`` endpoints collapsed to logical hub IDs."""
    logical_src = SupernodeBucketer.unwrap_bucket_id(edge.source_id)
    logical_dst = SupernodeBucketer.unwrap_bucket_id(edge.target_id)
    if logical_src == edge.source_id and logical_dst == edge.target_id:
        return edge
    return edge.model_copy(update={"source_id": logical_src, "target_id": logical_dst})


async def resolve_logical_neighbors(
    node_id: str,
    *,
    fetch_node: NodeFetcher,
    fetch_out_edges: OutEdgeFetcher,
    fetch_in_edges: InEdgeFetcher,
    edge_types: list[str] | None = None,
    domains: list[Domain] | None = None,
    direction: Literal["out", "in", "both"] = "out",
    limit: int = 5000,
    num_buckets: int = 16,
) -> list[tuple[RelationshipEdge, EntityNode]]:
    """Return immediate logical neighbors, collapsing intermediate supernode buckets."""
    allowed_edge_types = set(edge_types) if edge_types else None
    allowed_domains = set(domains) if domains else None

    candidate_ids = [node_id]
    if not SupernodeBucketer.is_bucket_node_id(node_id):
        candidate_ids.extend(
            SupernodeBucketer.make_bucket_node_id(node_id, b)
            for b in range(num_buckets)
        )

    results: list[tuple[RelationshipEdge, EntityNode]] = []
    seen_keys: set[tuple[str, str, str]] = set()

    async def _collect_from_edges(
        edges: list[RelationshipEdge],
        *,
        is_outbound: bool,
    ) -> None:
        for edge in sorted(
            edges, key=lambda e: (e.edge_type, e.source_id, e.target_id)
        ):
            if edge.edge_type == BUCKET_EDGE_TYPE:
                continue
            if allowed_edge_types and edge.edge_type not in allowed_edge_types:
                continue
            unwrapped = _unwrap_edge(edge)
            neighbor_id = unwrapped.target_id if is_outbound else unwrapped.source_id
            if neighbor_id == node_id:
                continue
            key = (unwrapped.source_id, unwrapped.target_id, unwrapped.edge_type)
            if key in seen_keys:
                continue
            neighbor = await fetch_node(neighbor_id)
            if neighbor is None:
                continue
            if allowed_domains and neighbor.domain not in allowed_domains:
                continue
            seen_keys.add(key)
            results.append((unwrapped, neighbor))
            if len(results) >= limit:
                return

    if direction in ("out", "both"):
        for cid in candidate_ids:
            if len(results) >= limit:
                break
            await _collect_from_edges(await fetch_out_edges(cid), is_outbound=True)

    if direction in ("in", "both") and len(results) < limit:
        for cid in candidate_ids:
            if len(results) >= limit:
                break
            await _collect_from_edges(await fetch_in_edges(cid), is_outbound=False)

    return results[:limit]


async def run_guarded_bfs(
    request: TraversalRequest,
    *,
    enforcer: GuardrailEnforcer,
    fetch_node: NodeFetcher,
    fetch_out_edges: OutEdgeFetcher,
) -> TraversalResult:
    """Execute bounded 0-1 BFS traversal with guardrail checks and bucket hop collapse."""
    tracker = enforcer.create_runtime_tracker(request)
    effective_max_depth = min(request.max_depth, enforcer.config.max_depth)
    allowed_edge_types = set(request.edge_types) if request.edge_types else None
    allowed_domains = set(request.domains) if request.domains else None

    collected_nodes: dict[str, EntityNode] = {}
    collected_edges: dict[tuple[str, str, str], RelationshipEdge] = {}
    logical_paths: list[list[str]] = []
    seen_paths: set[tuple[str, ...]] = set()
    collapsed_bucket_nodes: set[str] = set()

    queue: deque[tuple[str, int, list[str]]] = deque()
    best_depth: dict[str, int] = {}

    for seed_id in request.seed_ids:
        seed_node = await fetch_node(seed_id)
        if seed_node is not None:
            collected_nodes.setdefault(seed_node.id, seed_node)
        if seed_id not in best_depth:
            best_depth[seed_id] = 0
            queue.append((seed_id, 0, [seed_id]))

    nodes_expanded = 0
    depth_reached = 0
    total_logical_edges = 0
    cross_domain_hops = 0

    def _locality_ratio() -> float:
        if total_logical_edges == 0:
            return 1.0
        return round((total_logical_edges - cross_domain_hops) / total_logical_edges, 4)

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
            bucket_hops_unwrapped=len(collapsed_bucket_nodes),
        )

    while queue:
        curr_id, depth, raw_path = queue.popleft()
        nodes_expanded += 1

        violation = tracker.check_step(
            depth=depth,
            expanded_nodes=nodes_expanded,
            results_count=len(collected_nodes),
        )
        if violation is not None:
            return _abort(violation)

        out_edges = await fetch_out_edges(curr_id)
        for edge in sorted(out_edges, key=lambda e: (e.edge_type, e.target_id)):
            if edge.edge_type == BUCKET_EDGE_TYPE:
                if edge.target_id in raw_path:
                    continue
                is_curr_bucket = SupernodeBucketer.is_bucket_node_id(curr_id)
                if depth >= effective_max_depth and not is_curr_bucket:
                    continue

                new_raw_path = [*raw_path, edge.target_id]
                if not SupernodeBucketer.is_bucket_node_id(edge.target_id):
                    hub_node = await fetch_node(edge.target_id)
                    if hub_node is None:
                        continue
                    if allowed_domains and hub_node.domain not in allowed_domains:
                        continue
                    collected_nodes.setdefault(hub_node.id, hub_node)
                    logical_path, _ = SupernodeBucketer.unwrap_path(new_raw_path)
                    for vid in new_raw_path:
                        if SupernodeBucketer.is_bucket_node_id(vid):
                            collapsed_bucket_nodes.add(vid)
                    if len(logical_path) > 1 and tuple(logical_path) not in seen_paths:
                        seen_paths.add(tuple(logical_path))
                        logical_paths.append(logical_path)

                    violation = tracker.check_step(
                        depth=depth,
                        expanded_nodes=nodes_expanded,
                        results_count=len(collected_nodes),
                    )
                    if violation is not None:
                        return _abort(violation)

                if best_depth.get(edge.target_id, effective_max_depth + 1) > depth:
                    best_depth[edge.target_id] = depth
                    queue.appendleft((edge.target_id, depth, new_raw_path))
                continue

            if depth >= effective_max_depth:
                continue
            if edge.is_derived and not request.include_derived:
                continue
            if allowed_edge_types and edge.edge_type not in allowed_edge_types:
                continue

            target_node = await fetch_node(edge.target_id)
            if target_node is None:
                continue

            logical_target_domain = edge.target_domain or target_node.domain
            if allowed_domains and logical_target_domain not in allowed_domains:
                continue

            next_depth = depth + 1
            depth_reached = max(depth_reached, next_depth)
            new_raw_path = [*raw_path, edge.target_id]

            unwrapped = _unwrap_edge(edge)
            edge_key = (unwrapped.source_id, unwrapped.target_id, unwrapped.edge_type)
            if edge_key not in collected_edges:
                collected_edges[edge_key] = unwrapped
                total_logical_edges += 1
                if unwrapped.domain != logical_target_domain:
                    cross_domain_hops += 1

            if not SupernodeBucketer.is_bucket_node_id(edge.target_id):
                collected_nodes.setdefault(target_node.id, target_node)
                logical_path, _ = SupernodeBucketer.unwrap_path(new_raw_path)
                for vid in new_raw_path:
                    if SupernodeBucketer.is_bucket_node_id(vid):
                        collapsed_bucket_nodes.add(vid)
                if len(logical_path) > 1 and tuple(logical_path) not in seen_paths:
                    seen_paths.add(tuple(logical_path))
                    logical_paths.append(logical_path)

            violation = tracker.check_step(
                depth=next_depth,
                expanded_nodes=nodes_expanded,
                results_count=len(collected_nodes),
            )
            if violation is not None:
                return _abort(violation)

            if best_depth.get(edge.target_id, effective_max_depth + 1) > next_depth:
                best_depth[edge.target_id] = next_depth
                queue.append((edge.target_id, next_depth, new_raw_path))

    return TraversalResult(
        seed_ids=list(request.seed_ids),
        nodes=list(collected_nodes.values()),
        edges=list(collected_edges.values()),
        paths=logical_paths,
        metrics=TraversalMetrics(
            depth_reached=depth_reached,
            nodes_expanded=nodes_expanded,
            results_returned=len(collected_nodes),
            execution_time_ms=round(tracker.elapsed_seconds * 1000.0, 3),
            cross_domain_hops=cross_domain_hops,
            single_domain_locality_ratio=_locality_ratio(),
            bucket_hops_unwrapped=len(collapsed_bucket_nodes),
            aborted=False,
            violation=None,
        ),
    )
