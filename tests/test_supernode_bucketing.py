"""Tests for deterministic supernode bucketing, registry seeding, runtime promotion, and path unwrapping."""

from __future__ import annotations

from scale_kg.core.models import Domain, EntityNode, RelationshipEdge
from scale_kg.core.supernode import (
    SupernodeBucketer,
    SupernodeRegistry,
    compute_bucket_id,
)


def _node(
    node_id: str, domain: Domain = Domain.CUSTOMER, entity_type: str = "Customer"
) -> EntityNode:
    return EntityNode(id=node_id, domain=domain, entity_type=entity_type)


def _edge(
    src: str,
    dst: str,
    edge_type: str = "USES",
    domain: Domain = Domain.CUSTOMER,
    target_domain: Domain | None = Domain.TECHNOLOGY,
) -> RelationshipEdge:
    return RelationshipEdge(
        source_id=src,
        target_id=dst,
        edge_type=edge_type,
        domain=domain,
        target_domain=target_domain,
    )


class TestComputeBucketId:
    def test_deterministic_across_calls_and_bounded_to_16(self) -> None:
        buckets = [compute_bucket_id(f"customer:org:{i}", 16) for i in range(200)]
        assert all(0 <= b < 16 for b in buckets)
        # Repeat calls must produce identical bucket IDs
        buckets_repeat = [
            compute_bucket_id(f"customer:org:{i}", 16) for i in range(200)
        ]
        assert buckets == buckets_repeat
        # Spread across buckets (not all collapsing to one bucket)
        assert len(set(buckets)) >= 12


class TestSupernodeRegistry:
    def test_seeded_hubs_and_types_are_recognized(self) -> None:
        registry = SupernodeRegistry(degree_threshold=50)

        # Seeded IDs
        assert registry.is_supernode("technology:lang:python") is True
        assert registry.is_supernode("customer:region:india") is True

        # Seeded entity types
        rust_node = _node("technology:lang:rust", Domain.TECHNOLOGY, "Language")
        france_node = _node("customer:country:fr", Domain.CUSTOMER, "Country")
        normal_node = _node("customer:org:acme", Domain.CUSTOMER, "Organization")

        assert registry.is_supernode(rust_node) is True
        assert registry.is_supernode(france_node) is True
        assert registry.is_supernode(normal_node) is False

    def test_runtime_promotion_when_degree_crosses_threshold(self) -> None:
        registry = SupernodeRegistry(degree_threshold=5)
        hub_id = "product:sku:popular"

        assert registry.is_supernode(hub_id, degree=4) is False
        assert registry.promote_if_needed(hub_id, degree=4) is False

        assert registry.promote_if_needed(hub_id, degree=5) is True
        assert registry.is_supernode(hub_id) is True
        # Idempotent once promoted
        assert registry.promote_if_needed(hub_id, degree=6) is False


class TestSupernodeBucketer:
    def test_splits_supernode_into_16_buckets_and_distributes_edges(self) -> None:
        hub = _node("technology:lang:python", Domain.TECHNOLOGY, "Language")
        users = [_node(f"customer:org:{i}") for i in range(64)]
        edges = [_edge(u.id, hub.id, "USES") for u in users]

        bucketer = SupernodeBucketer(degree_threshold=100, num_buckets=16)
        new_nodes, new_edges, supernode_ids = bucketer.transform_dataset(
            [hub, *users], edges
        )

        assert supernode_ids == {"technology:lang:python"}
        bucket_nodes = [n for n in new_nodes if bucketer.is_bucket_node_id(n.id)]
        assert len(bucket_nodes) == 16
        assert {n.id for n in bucket_nodes} == {
            f"technology:lang:python::bucket::{b}" for b in range(16)
        }
        assert all(n.is_bucket is True for n in bucket_nodes)

        # Original direct edges (customer -> hub) must be replaced by (customer -> bucket)
        direct_to_hub = [
            e
            for e in new_edges
            if e.target_id == hub.id and not bucketer.is_bucket_node_id(e.source_id)
        ]
        assert direct_to_hub == []

        user_to_bucket = [
            e
            for e in new_edges
            if e.source_id.startswith("customer:org:")
            and bucketer.is_bucket_node_id(e.target_id)
        ]
        assert len(user_to_bucket) == 64
        for e in user_to_bucket:
            expected_b = compute_bucket_id(e.source_id, 16)
            assert e.target_id == f"technology:lang:python::bucket::{expected_b}"

    def test_auto_promotes_unseeded_node_exceeding_threshold(self) -> None:
        hot_product = _node("product:sku:viral", Domain.PRODUCT, "Product")
        buyers = [_node(f"customer:user:{i}") for i in range(12)]
        edges = [
            _edge(b.id, hot_product.id, "PURCHASED", target_domain=Domain.PRODUCT)
            for b in buyers
        ]

        bucketer = SupernodeBucketer(degree_threshold=10, num_buckets=16)
        new_nodes, new_edges, supernode_ids = bucketer.transform_dataset(
            [hot_product, *buyers], edges
        )

        assert "product:sku:viral" in supernode_ids
        assert "product:sku:viral" in bucketer.registry.promoted_ids
        assert any(bucketer.is_bucket_node_id(n.id) for n in new_nodes)
        assert any(
            bucketer.is_bucket_node_id(e.target_id) and e.edge_type == "PURCHASED"
            for e in new_edges
        )

    def test_rewrites_hub_to_hub_edges_through_both_buckets(self) -> None:
        hub_a = _node("technology:lang:python", Domain.TECHNOLOGY, "Language")
        hub_b = _node("customer:region:india", Domain.CUSTOMER, "Country")
        edge = _edge(
            hub_a.id, hub_b.id, "POPULAR_IN", Domain.TECHNOLOGY, Domain.CUSTOMER
        )

        bucketer = SupernodeBucketer(degree_threshold=100, num_buckets=16)
        _, new_edges, supernode_ids = bucketer.transform_dataset([hub_a, hub_b], [edge])

        assert supernode_ids == {hub_a.id, hub_b.id}
        expected_a_bucket = f"{hub_a.id}::bucket::{compute_bucket_id(hub_b.id, 16)}"
        expected_b_bucket = f"{hub_b.id}::bucket::{compute_bucket_id(hub_a.id, 16)}"

        assert any(
            e.source_id == expected_a_bucket
            and e.target_id == expected_b_bucket
            and e.edge_type == "POPULAR_IN"
            for e in new_edges
        )

    def test_runtime_promote_node_rewrites_incident_edges_cleanly(self) -> None:
        hub = _node("product:sku:hot", Domain.PRODUCT, "Product")
        unrelated = _node("customer:org:other")
        buyers = [_node(f"customer:user:{i}") for i in range(6)]
        existing_edges = [
            _edge(b.id, hub.id, "PURCHASED", target_domain=Domain.PRODUCT)
            for b in buyers
        ] + [_edge(unrelated.id, "technology:db:pg", "USES")]

        bucketer = SupernodeBucketer(degree_threshold=5, num_buckets=16)
        bucket_nodes, new_edges, removed_edges = bucketer.promote_node(
            hub, existing_edges
        )

        assert len(bucket_nodes) == 16
        assert len(removed_edges) == 6
        assert hub.id in bucketer.registry.promoted_ids
        # Unrelated edge is not in removed_edges
        assert all(e.target_id == hub.id for e in removed_edges)
        # Rewritten PURCHASED edges now target bucket nodes
        rewritten_purchased = [e for e in new_edges if e.edge_type == "PURCHASED"]
        assert len(rewritten_purchased) == 6
        assert all(bucketer.is_bucket_node_id(e.target_id) for e in rewritten_purchased)

    def test_unwrap_bucket_id_and_collapse_bucket_hops_for_3_hop_cap(self) -> None:
        assert (
            SupernodeBucketer.is_bucket_node_id("technology:lang:python::bucket::12")
            is True
        )
        assert SupernodeBucketer.is_bucket_node_id("technology:lang:python") is False
        assert SupernodeBucketer.unwrap_bucket_id(
            "technology:lang:python::bucket::12"
        ) == ("technology:lang:python")
        assert SupernodeBucketer.unwrap_bucket_id("customer:org:1") == "customer:org:1"

        # 3 logical hops traversing through two supernode buckets (6 raw edges!)
        raw_path = [
            "customer:org:1",
            "technology:lang:python::bucket::3",
            "technology:lang:python",
            "technology:lang:python::bucket::9",
            "customer:region:india::bucket::4",
            "customer:region:india",
            "product:plan:enterprise",
        ]
        logical_path, unwrapped_hops = SupernodeBucketer.unwrap_path(raw_path)
        assert logical_path == [
            "customer:org:1",
            "technology:lang:python",
            "customer:region:india",
            "product:plan:enterprise",
        ]
        assert unwrapped_hops == 3
        assert SupernodeBucketer.logical_hop_count(raw_path) == 3
