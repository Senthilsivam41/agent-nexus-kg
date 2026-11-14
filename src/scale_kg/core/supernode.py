"""Deterministic 16-way supernode bucketing, registry seeding, and runtime promotion."""

from __future__ import annotations

import zlib
from collections import Counter

from scale_kg.core.models import Domain, EntityNode, RelationshipEdge

BUCKET_SEPARATOR = "::bucket::"
BUCKET_EDGE_TYPE = "BUCKET_LINK"
DEFAULT_SEEDED_HUB_TYPES: frozenset[str] = frozenset({"language", "country"})
DEFAULT_SEEDED_HUB_IDS: frozenset[str] = frozenset(
    {"technology:lang:python", "customer:region:india"}
)


def compute_bucket_id(node_id: str, num_buckets: int = 16) -> int:
    """Deterministically hash a node ID into a bucket index in ``[0, num_buckets)``."""
    if num_buckets < 1:
        raise ValueError(f"num_buckets must be >= 1, got {num_buckets}.")
    return zlib.crc32(node_id.encode("utf-8")) % num_buckets


class SupernodeRegistry:
    """Tracks seeded high-cardinality hubs and runtime degree promotions."""

    def __init__(
        self,
        *,
        degree_threshold: int = 1000,
        seeded_types: frozenset[str] | set[str] | None = None,
        seeded_ids: frozenset[str] | set[str] | None = None,
    ) -> None:
        if degree_threshold < 1:
            raise ValueError(f"degree_threshold must be >= 1, got {degree_threshold}.")
        self.degree_threshold = degree_threshold
        self.seeded_types: frozenset[str] = (
            frozenset(t.lower() for t in seeded_types)
            if seeded_types is not None
            else DEFAULT_SEEDED_HUB_TYPES
        )
        self.seeded_ids: frozenset[str] = (
            frozenset(seeded_ids) if seeded_ids is not None else DEFAULT_SEEDED_HUB_IDS
        )
        self.promoted_ids: set[str] = set()

    def is_supernode(self, node: EntityNode | str, degree: int | None = None) -> bool:
        """Return True if the entity is seeded, promoted, or meets degree_threshold."""
        node_id = node.id if isinstance(node, EntityNode) else node
        if SupernodeBucketer.is_bucket_node_id(node_id):
            return False
        if node_id in self.seeded_ids or node_id in self.promoted_ids:
            return True
        if (
            isinstance(node, EntityNode)
            and node.entity_type.lower() in self.seeded_types
        ):
            return True
        return degree is not None and degree >= self.degree_threshold

    def promote_if_needed(self, node_id: str, degree: int) -> bool:
        """Promote a node at runtime if its degree crosses degree_threshold."""
        if SupernodeBucketer.is_bucket_node_id(node_id):
            return False
        if node_id in self.seeded_ids or node_id in self.promoted_ids:
            return False
        if degree >= self.degree_threshold:
            self.promoted_ids.add(node_id)
            return True
        return False


class SupernodeBucketer:
    """Splits supernodes into 16 deterministic bucket vertices and unwraps bucket hops."""

    def __init__(
        self,
        degree_threshold: int = 100,
        num_buckets: int = 16,
        registry: SupernodeRegistry | None = None,
    ) -> None:
        if num_buckets < 2:
            raise ValueError(f"num_buckets must be >= 2, got {num_buckets}.")
        self.degree_threshold = degree_threshold
        self.num_buckets = num_buckets
        self.registry = registry or SupernodeRegistry(degree_threshold=degree_threshold)

    @staticmethod
    def make_bucket_node_id(hub_id: str, bucket_idx: int) -> str:
        """Format a deterministic intermediate bucket vertex ID."""
        return f"{hub_id}{BUCKET_SEPARATOR}{bucket_idx}"

    @staticmethod
    def is_bucket_node_id(node_id: str) -> bool:
        """Return True if ``node_id`` represents a supernode bucket vertex."""
        if BUCKET_SEPARATOR not in node_id:
            return False
        _, suffix = node_id.rsplit(BUCKET_SEPARATOR, 1)
        return suffix.isdigit()

    @staticmethod
    def unwrap_bucket_id(node_id: str) -> str:
        """Map ``<hub_id>::bucket::<k>`` back to ``<hub_id>``, or return unchanged."""
        if not SupernodeBucketer.is_bucket_node_id(node_id):
            return node_id
        hub_id, _ = node_id.rsplit(BUCKET_SEPARATOR, 1)
        return hub_id

    @classmethod
    def unwrap_path(cls, raw_path: list[str]) -> tuple[list[str], int]:
        """Collapse bucket vertices into logical hub IDs and count unwrapped hops."""
        if not raw_path:
            return [], 0

        logical_path: list[str] = []
        bucket_nodes_seen = 0
        for vertex_id in raw_path:
            if cls.is_bucket_node_id(vertex_id):
                bucket_nodes_seen += 1
            unwrapped = cls.unwrap_bucket_id(vertex_id)
            if not logical_path or logical_path[-1] != unwrapped:
                logical_path.append(unwrapped)

        bucket_hops_unwrapped = max(0, len(raw_path) - len(logical_path))
        if bucket_hops_unwrapped == 0 and bucket_nodes_seen > 0:
            bucket_hops_unwrapped = bucket_nodes_seen
        return logical_path, bucket_hops_unwrapped

    @classmethod
    def logical_hop_count(cls, raw_path: list[str]) -> int:
        """Return logical hop count excluding intermediate bucket hops."""
        logical_path, _ = cls.unwrap_path(raw_path)
        return max(0, len(logical_path) - 1)

    def _build_buckets_for_hub(
        self, hub: EntityNode
    ) -> tuple[list[EntityNode], list[RelationshipEdge]]:
        bucket_nodes: list[EntityNode] = []
        structural_edges: list[RelationshipEdge] = []
        for idx in range(self.num_buckets):
            bucket_id = self.make_bucket_node_id(hub.id, idx)
            bucket_nodes.append(
                EntityNode(
                    id=bucket_id,
                    domain=hub.domain,
                    entity_type=f"{hub.entity_type}Bucket",
                    routing_props={
                        "hub_id": hub.id,
                        "bucket_id": idx,
                        "is_bucket": True,
                    },
                    is_bucket=True,
                )
            )
            structural_edges.extend(
                [
                    RelationshipEdge(
                        source_id=hub.id,
                        target_id=bucket_id,
                        edge_type=BUCKET_EDGE_TYPE,
                        domain=hub.domain,
                        target_domain=hub.domain,
                        routing_props={"bucket_id": idx},
                    ),
                    RelationshipEdge(
                        source_id=bucket_id,
                        target_id=hub.id,
                        edge_type=BUCKET_EDGE_TYPE,
                        domain=hub.domain,
                        target_domain=hub.domain,
                        routing_props={"bucket_id": idx},
                    ),
                ]
            )
        return bucket_nodes, structural_edges

    def _rewrite_edge(
        self, edge: RelationshipEdge, supernode_ids: set[str]
    ) -> RelationshipEdge:
        if edge.edge_type == BUCKET_EDGE_TYPE:
            return edge

        src_super = edge.source_id in supernode_ids and not self.is_bucket_node_id(
            edge.source_id
        )
        dst_super = edge.target_id in supernode_ids and not self.is_bucket_node_id(
            edge.target_id
        )
        if not src_super and not dst_super:
            return edge

        new_src = (
            self.make_bucket_node_id(
                edge.source_id, compute_bucket_id(edge.target_id, self.num_buckets)
            )
            if src_super
            else edge.source_id
        )
        new_dst = (
            self.make_bucket_node_id(
                edge.target_id, compute_bucket_id(edge.source_id, self.num_buckets)
            )
            if dst_super
            else edge.target_id
        )
        return edge.model_copy(update={"source_id": new_src, "target_id": new_dst})

    def transform_dataset(
        self,
        nodes: list[EntityNode],
        edges: list[RelationshipEdge],
    ) -> tuple[list[EntityNode], list[RelationshipEdge], set[str]]:
        """Identify supernodes, create 16 bucket vertices per hub, and rewrite edges."""
        degrees: Counter[str] = Counter()
        for edge in edges:
            if edge.edge_type == BUCKET_EDGE_TYPE:
                continue
            degrees[edge.source_id] += 1
            degrees[edge.target_id] += 1

        nodes_by_id: dict[str, EntityNode] = {n.id: n for n in nodes}
        supernode_ids: set[str] = set()

        for node in nodes:
            deg = degrees[node.id]
            self.registry.promote_if_needed(node.id, deg)
            if self.registry.is_supernode(node, deg):
                supernode_ids.add(node.id)

        # Also handle edge-referenced hubs that were not explicitly listed in `nodes`.
        for node_id, deg in degrees.items():
            if node_id in nodes_by_id:
                continue
            self.registry.promote_if_needed(node_id, deg)
            if self.registry.is_supernode(node_id, deg):
                supernode_ids.add(node_id)

        transformed_nodes = list(nodes)
        structural_edges: list[RelationshipEdge] = []
        existing_node_ids = {n.id for n in transformed_nodes}

        for hub_id in sorted(supernode_ids):
            hub = nodes_by_id.get(hub_id) or EntityNode(
                id=hub_id,
                domain=Domain.TECHNOLOGY,
                entity_type="Supernode",
            )
            bucket_nodes, hub_bucket_edges = self._build_buckets_for_hub(hub)
            for b_node in bucket_nodes:
                if b_node.id not in existing_node_ids:
                    transformed_nodes.append(b_node)
                    existing_node_ids.add(b_node.id)
            structural_edges.extend(hub_bucket_edges)

        rewritten_edges = [self._rewrite_edge(edge, supernode_ids) for edge in edges]
        rewritten_edges.extend(structural_edges)
        return transformed_nodes, rewritten_edges, supernode_ids

    def promote_node(
        self,
        hub: EntityNode,
        existing_edges: list[RelationshipEdge],
    ) -> tuple[list[EntityNode], list[RelationshipEdge], list[RelationshipEdge]]:
        """Promote a single vertex at runtime and return (bucket_nodes, new_edges, removed_edges)."""
        self.registry.promoted_ids.add(hub.id)
        bucket_nodes, structural_edges = self._build_buckets_for_hub(hub)

        removed_edges: list[RelationshipEdge] = []
        rewritten_edges: list[RelationshipEdge] = list(structural_edges)
        hub_set = {hub.id}

        for edge in existing_edges:
            if edge.edge_type == BUCKET_EDGE_TYPE:
                continue
            if edge.source_id == hub.id or edge.target_id == hub.id:
                removed_edges.append(edge)
                rewritten_edges.append(self._rewrite_edge(edge, hub_set))

        return bucket_nodes, rewritten_edges, removed_edges
