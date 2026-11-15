"""Synthetic power-law graph generator with scale presets and supernode injection."""

from __future__ import annotations

import math
import random
from collections import Counter
from enum import Enum

from pydantic import BaseModel, Field

from scale_kg.core.models import Domain, EntityNode, RelationshipEdge
from scale_kg.core.supernode import SupernodeBucketer


class ScalePreset(str, Enum):
    """Benchmark scale presets mapping local test sizes to 100M/1B/10B targets."""

    MICRO = "micro"
    LOCAL = "local"
    CLUSTER = "cluster"


class ScalePresetSpec(BaseModel):
    """Configuration defining graph size, supernode fan-out, and extrapolation target."""

    preset: ScalePreset
    node_count: int = Field(ge=16)
    avg_degree: float = Field(gt=0.0)
    power_law_exponent: float = Field(default=2.2, gt=1.0)
    supernode_fanout: int = Field(ge=4)
    reference_scale_edges: int = Field(ge=1)
    scale_label: str
    concurrency_levels: list[int]
    latency_samples: int = Field(ge=3)


SCALE_PRESETS: dict[ScalePreset, ScalePresetSpec] = {
    ScalePreset.MICRO: ScalePresetSpec(
        preset=ScalePreset.MICRO,
        node_count=80,
        avg_degree=3.5,
        power_law_exponent=2.2,
        supernode_fanout=24,
        reference_scale_edges=100_000_000,
        scale_label="100M",
        concurrency_levels=[2, 4, 8],
        latency_samples=6,
    ),
    ScalePreset.LOCAL: ScalePresetSpec(
        preset=ScalePreset.LOCAL,
        node_count=1_000,
        avg_degree=6.0,
        power_law_exponent=2.2,
        supernode_fanout=120,
        reference_scale_edges=1_000_000_000,
        scale_label="1B",
        concurrency_levels=[4, 16, 32],
        latency_samples=30,
    ),
    ScalePreset.CLUSTER: ScalePresetSpec(
        preset=ScalePreset.CLUSTER,
        node_count=5_000,
        avg_degree=8.0,
        power_law_exponent=2.2,
        supernode_fanout=400,
        reference_scale_edges=10_000_000_000,
        scale_label="10B",
        concurrency_levels=[8, 32, 64],
        latency_samples=60,
    ),
}

_DOMAIN_ENTITY_TYPES: dict[Domain, tuple[str, ...]] = {
    Domain.CUSTOMER: ("Organization", "Account", "Team"),
    Domain.TECHNOLOGY: ("Library", "Service", "Framework"),
    Domain.PRODUCT: ("Product", "Module", "Capability"),
    Domain.KNOWLEDGE: ("Document", "Concept", "Runbook"),
}

_INTRA_EDGE_TYPES: dict[Domain, tuple[str, ...]] = {
    Domain.CUSTOMER: ("PARTNERS_WITH", "SUBSIDIARY_OF"),
    Domain.TECHNOLOGY: ("DEPENDS_ON", "INTEGRATES_WITH"),
    Domain.PRODUCT: ("CONTAINS", "EXTENDS"),
    Domain.KNOWLEDGE: ("CITES", "REFINES"),
}

_CROSS_EDGE_TYPES: dict[tuple[Domain, Domain], str] = {
    (Domain.CUSTOMER, Domain.TECHNOLOGY): "USES",
    (Domain.CUSTOMER, Domain.PRODUCT): "ADOPTED",
    (Domain.CUSTOMER, Domain.KNOWLEDGE): "AUTHORED",
    (Domain.TECHNOLOGY, Domain.PRODUCT): "POWERS",
    (Domain.TECHNOLOGY, Domain.KNOWLEDGE): "DOCUMENTED_IN",
    (Domain.TECHNOLOGY, Domain.CUSTOMER): "SERVES",
    (Domain.PRODUCT, Domain.KNOWLEDGE): "DESCRIBED_BY",
    (Domain.PRODUCT, Domain.TECHNOLOGY): "BUILT_WITH",
    (Domain.PRODUCT, Domain.CUSTOMER): "LICENSED_TO",
    (Domain.KNOWLEDGE, Domain.PRODUCT): "COVERS",
    (Domain.KNOWLEDGE, Domain.TECHNOLOGY): "SPECIFIES",
    (Domain.KNOWLEDGE, Domain.CUSTOMER): "TARGETS",
}


class GeneratedGraphStats(BaseModel):
    """Summary metrics for a generated synthetic power-law dataset."""

    preset: ScalePreset
    node_count: int
    edge_count: int
    nodes_by_domain: dict[str, int]
    edges_by_domain: dict[str, int]
    cross_domain_edges: int
    realized_cross_domain_share: float
    max_degree: int
    p95_degree: float
    supernode_degrees: dict[str, int]
    invalid_derived_edge_count: int


class GeneratedDataset(BaseModel):
    """Synthetic power-law graph dataset plus unmaterialized derived-edge test payloads."""

    preset: ScalePreset
    seed: int
    cross_domain_edge_share: float
    nodes: list[EntityNode]
    edges: list[RelationshipEdge]
    invalid_derived_edges: list[RelationshipEdge]
    supernode_ids: list[str]
    seed_ids: list[str]
    stats: GeneratedGraphStats


class PowerLawGraphGenerator:
    """Generates reproducible power-law graphs across the 4 semantic domains."""

    def __init__(
        self,
        *,
        preset: ScalePreset | str = ScalePreset.MICRO,
        cross_domain_edge_share: float = 0.15,
        seed: int = 42,
        node_count_override: int | None = None,
        avg_degree_override: float | None = None,
        supernode_fanout_override: int | None = None,
    ) -> None:
        self.preset = ScalePreset(preset)
        if not 0.0 <= cross_domain_edge_share <= 1.0:
            raise ValueError(
                f"cross_domain_edge_share must be in [0.0, 1.0], got {cross_domain_edge_share}."
            )
        self.cross_domain_edge_share = cross_domain_edge_share
        self.seed = seed

        base_spec = SCALE_PRESETS[self.preset]
        self.spec = base_spec.model_copy(
            update={
                "node_count": (
                    node_count_override
                    if node_count_override is not None
                    else base_spec.node_count
                ),
                "avg_degree": (
                    avg_degree_override
                    if avg_degree_override is not None
                    else base_spec.avg_degree
                ),
                "supernode_fanout": (
                    supernode_fanout_override
                    if supernode_fanout_override is not None
                    else base_spec.supernode_fanout
                ),
            }
        )

    @staticmethod
    def _make_node(
        node_id: str,
        domain: Domain,
        entity_type: str,
        idx: int,
    ) -> EntityNode:
        lakehouse_ref: str | None = None
        if entity_type.lower() in {"document", "runbook"}:
            lakehouse_ref = f"s3://scale-kg-lakehouse/{domain.value}/{idx:06d}.parquet"
        return EntityNode(
            id=node_id,
            domain=domain,
            entity_type=entity_type,
            routing_props={"tier": idx % 4, "active": True},
            lakehouse_ref=lakehouse_ref,
        )

    def generate(
        self,
        *,
        bucket_supernodes: bool = False,
        degree_threshold: int = 100,
    ) -> GeneratedDataset:
        """Generate a synthetic power-law graph with deliberate supernodes and derived test edges."""
        rng = random.Random(self.seed)
        domains = list(Domain)

        # Deliberate high-cardinality supernodes matching SupernodeRegistry defaults.
        supernode_python = EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
            routing_props={"tier": 0, "active": True, "hub": True},
        )
        supernode_india = EntityNode(
            id="customer:region:india",
            domain=Domain.CUSTOMER,
            entity_type="Country",
            routing_props={"tier": 0, "active": True, "hub": True},
        )
        nodes: list[EntityNode] = [supernode_python, supernode_india]
        nodes_by_domain: dict[Domain, list[EntityNode]] = {d: [] for d in domains}
        nodes_by_domain[Domain.TECHNOLOGY].append(supernode_python)
        nodes_by_domain[Domain.CUSTOMER].append(supernode_india)

        remaining = max(0, self.spec.node_count - len(nodes))
        for idx in range(remaining):
            domain = domains[idx % len(domains)]
            type_choices = _DOMAIN_ENTITY_TYPES[domain]
            entity_type = type_choices[(idx // len(domains)) % len(type_choices)]
            slug = f"ent-{idx:05d}"
            node_id = f"{domain.value}:{entity_type.lower()}:{slug}"
            node = self._make_node(node_id, domain, entity_type, idx)
            nodes.append(node)
            nodes_by_domain[domain].append(node)

        edge_map: dict[tuple[str, str, str], RelationshipEdge] = {}

        def _add_edge(
            src: EntityNode, dst: EntityNode, edge_type: str | None = None
        ) -> bool:
            if src.id == dst.id:
                return False
            if edge_type is None:
                if src.domain == dst.domain:
                    choices = _INTRA_EDGE_TYPES[src.domain]
                    edge_type = choices[len(edge_map) % len(choices)]
                else:
                    edge_type = _CROSS_EDGE_TYPES[(src.domain, dst.domain)]
            key = (src.id, dst.id, edge_type)
            if key in edge_map:
                return False
            edge_map[key] = RelationshipEdge(
                source_id=src.id,
                target_id=dst.id,
                edge_type=edge_type,
                domain=src.domain,
                target_domain=dst.domain,
                routing_props={"weight": round(rng.uniform(0.5, 1.0), 3)},
            )
            return True

        # 1. Guarantee 3-hop reachable paths from primary seed nodes (for latency workloads).
        seed_nodes: list[EntityNode] = []
        for domain in domains:
            domain_pool = [
                n
                for n in nodes_by_domain[domain]
                if n.id not in {"technology:lang:python", "customer:region:india"}
            ]
            if len(domain_pool) >= 4:
                seed_nodes.append(domain_pool[0])
                for hop_idx in range(3):
                    _add_edge(domain_pool[hop_idx], domain_pool[hop_idx + 1])

        # Also wire at least one deterministic 3-hop cross-domain chain across all 4 domains.
        c_node = nodes_by_domain[Domain.CUSTOMER][1]
        p_node = nodes_by_domain[Domain.PRODUCT][0]
        k_node = nodes_by_domain[Domain.KNOWLEDGE][0]
        _add_edge(c_node, supernode_python, "USES")
        _add_edge(supernode_python, p_node, "POWERS")
        _add_edge(p_node, k_node, "DESCRIBED_BY")
        if c_node not in seed_nodes:
            seed_nodes.insert(0, c_node)

        # 2. Inject deliberate supernode fan-out / fan-in edges respecting cross-domain share.
        fanout = min(self.spec.supernode_fanout, max(4, len(nodes) - 2))
        for hub in (supernode_python, supernode_india):
            same_domain_peers = [
                n for n in nodes_by_domain[hub.domain] if n.id != hub.id
            ]
            cross_domain_peers = [n for n in nodes if n.domain != hub.domain]
            target_cross = round(fanout * self.cross_domain_edge_share)
            target_same = fanout - target_cross

            chosen_same = rng.sample(
                same_domain_peers, min(target_same, len(same_domain_peers))
            )
            chosen_cross = rng.sample(
                cross_domain_peers, min(target_cross, len(cross_domain_peers))
            )
            # Backfill from the other pool if one pool is smaller than requested fanout.
            deficit = fanout - (len(chosen_same) + len(chosen_cross))
            if deficit > 0:
                extras = [
                    n
                    for n in nodes
                    if n.id != hub.id and n not in chosen_same and n not in chosen_cross
                ]
                chosen_cross.extend(rng.sample(extras, min(deficit, len(extras))))

            for idx, peer in enumerate([*chosen_same, *chosen_cross]):
                if idx % 2 == 0:
                    _add_edge(peer, hub)
                else:
                    _add_edge(hub, peer)

        # 3. Fill remaining target edges using Zipf/power-law degree selection per domain.
        target_total_edges = max(len(edge_map), int(len(nodes) * self.spec.avg_degree))
        power_weights_by_domain: dict[Domain, list[float]] = {}
        exponent = 1.0 / (self.spec.power_law_exponent - 1.0)
        for domain in domains:
            pool = nodes_by_domain[domain]
            power_weights_by_domain[domain] = [
                1.0 / math.pow(rank + 1, exponent) for rank in range(len(pool))
            ]

        attempts = 0
        max_attempts = max(1000, target_total_edges * 12)
        while len(edge_map) < target_total_edges and attempts < max_attempts:
            attempts += 1
            src_domain = domains[attempts % len(domains)]
            src_pool = nodes_by_domain[src_domain]
            src_weights = power_weights_by_domain[src_domain]
            src = rng.choices(src_pool, weights=src_weights, k=1)[0]

            current_cross = sum(
                1
                for e in edge_map.values()
                if e.target_domain is not None and e.target_domain != e.domain
            )
            current_share = current_cross / max(1, len(edge_map))
            want_cross = (
                self.cross_domain_edge_share > 0.0
                and current_share < self.cross_domain_edge_share
            )

            if want_cross:
                other_domains = [d for d in domains if d != src_domain]
                dst_domain = rng.choice(other_domains)
            else:
                dst_domain = src_domain

            dst_pool = nodes_by_domain[dst_domain]
            dst_weights = power_weights_by_domain[dst_domain]
            dst = rng.choices(dst_pool, weights=dst_weights, k=1)[0]
            _add_edge(src, dst)

        edges = list(edge_map.values())
        supernode_ids = [supernode_python.id, supernode_india.id]

        # 4. Generate unmaterialized derived edges for write-discipline rejection tests.
        t_peers = nodes_by_domain[Domain.TECHNOLOGY]
        c_peers = nodes_by_domain[Domain.CUSTOMER]
        invalid_derived_edges = [
            RelationshipEdge(
                source_id=c_peers[0].id,
                target_id=c_peers[min(1, len(c_peers) - 1)].id,
                edge_type="SIMILAR_TO",
                domain=Domain.CUSTOMER,
                target_domain=Domain.CUSTOMER,
                is_derived=True,
                materialized_hot_path=False,
            ),
            RelationshipEdge(
                source_id=c_peers[0].id,
                target_id=t_peers[min(1, len(t_peers) - 1)].id,
                edge_type="INDIRECTLY_USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
                is_derived=True,
                materialized_hot_path=False,
            ),
        ]

        if bucket_supernodes:
            bucketer = SupernodeBucketer(degree_threshold=degree_threshold)
            nodes, edges, discovered = bucketer.transform_dataset(nodes, edges)
            supernode_ids = sorted(set(supernode_ids) | discovered)

        stats = self._compute_stats(
            nodes=nodes,
            edges=edges,
            supernode_ids=supernode_ids,
            invalid_derived_count=len(invalid_derived_edges),
        )
        return GeneratedDataset(
            preset=self.preset,
            seed=self.seed,
            cross_domain_edge_share=self.cross_domain_edge_share,
            nodes=nodes,
            edges=edges,
            invalid_derived_edges=invalid_derived_edges,
            supernode_ids=supernode_ids,
            seed_ids=[n.id for n in seed_nodes],
            stats=stats,
        )

    def _compute_stats(
        self,
        *,
        nodes: list[EntityNode],
        edges: list[RelationshipEdge],
        supernode_ids: list[str],
        invalid_derived_count: int,
    ) -> GeneratedGraphStats:
        nodes_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        for node in nodes:
            if not node.is_bucket:
                nodes_by_domain[node.domain.value] += 1

        edges_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        degrees: Counter[str] = Counter()
        cross_domain = 0
        for edge in edges:
            edges_by_domain[edge.domain.value] += 1
            if edge.target_domain is not None and edge.target_domain != edge.domain:
                cross_domain += 1
            src_logical = SupernodeBucketer.unwrap_bucket_id(edge.source_id)
            dst_logical = SupernodeBucketer.unwrap_bucket_id(edge.target_id)
            if src_logical != dst_logical:
                degrees[src_logical] += 1
                degrees[dst_logical] += 1

        all_degrees = sorted(degrees.get(n.id, 0) for n in nodes if not n.is_bucket)
        p95_idx = (
            min(len(all_degrees) - 1, int(0.95 * len(all_degrees)))
            if all_degrees
            else 0
        )
        p95_deg = float(all_degrees[p95_idx]) if all_degrees else 0.0
        max_deg = max(all_degrees) if all_degrees else 0

        return GeneratedGraphStats(
            preset=self.preset,
            node_count=len(nodes),
            edge_count=len(edges),
            nodes_by_domain=nodes_by_domain,
            edges_by_domain=edges_by_domain,
            cross_domain_edges=cross_domain,
            realized_cross_domain_share=(
                round(cross_domain / len(edges), 4) if edges else 0.0
            ),
            max_degree=max_deg,
            p95_degree=p95_deg,
            supernode_degrees={sid: degrees.get(sid, 0) for sid in supernode_ids},
            invalid_derived_edge_count=invalid_derived_count,
        )
