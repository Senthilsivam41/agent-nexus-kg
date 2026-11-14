"""Tests for canonical ID routing, EntityDirectory, proxy nodes, and QueryCoordinator."""

from __future__ import annotations

from pathlib import Path

import pytest

from scale_kg.adapters.iceberg_puppygraph import IcebergPuppyGraphStore
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.core.guardrails import GuardrailViolationError
from scale_kg.core.models import (
    Domain,
    EntityNode,
    GuardrailViolationCode,
    RelationshipEdge,
    TraversalRequest,
)
from scale_kg.routing import (
    EntityDirectory,
    EntityResolutionError,
    QueryCoordinator,
    build_canonical_id,
    parse_canonical_id,
)


def _split_in_memory_shards() -> dict[Domain, InMemoryGraphStore]:
    return {domain: InMemoryGraphStore() for domain in Domain}


class TestCanonicalRoutingAndDirectory:
    def test_parse_and_build_canonical_id(self) -> None:
        cid = build_canonical_id(Domain.TECHNOLOGY, "lang", "python")
        assert cid == "technology:lang:python"

        parsed = parse_canonical_id(cid)
        assert parsed is not None
        assert parsed.domain == Domain.TECHNOLOGY
        assert parsed.entity_type == "lang"
        assert parsed.slug == "python"

        bucketed = parse_canonical_id("technology:lang:python::bucket::7")
        assert bucketed is not None
        assert bucketed.domain == Domain.TECHNOLOGY
        assert bucketed.slug == "python"

        assert parse_canonical_id("legacy-opaque-991") is None
        assert parse_canonical_id("unknown:lang:python") is None
        assert parse_canonical_id("customer:org:") is None

    def test_directory_zero_lookup_canonical_vs_legacy_and_aliases(self) -> None:
        directory = EntityDirectory()
        assert directory.resolve_domain("customer:org:acme") == Domain.CUSTOMER
        stats = directory.stats()
        assert stats.canonical_hits == 1
        assert stats.directory_hits == 0

        # Legacy opaque ID + alias fallback
        directory.register("legacy-cust-42", Domain.CUSTOMER, aliases=["acme_old"])
        assert directory.resolve_domain("legacy-cust-42") == Domain.CUSTOMER
        assert directory.resolve_domain("acme_old") == Domain.CUSTOMER
        stats = directory.stats()
        assert stats.directory_hits == 2

        # Alias mapping to canonical ID routes in O(1) canonical path
        directory.register_alias("py3", "technology:lang:python")
        assert directory.canonicalize("py3") == "technology:lang:python"
        assert directory.resolve_domain("py3") == Domain.TECHNOLOGY
        assert directory.stats().canonical_hits == 2

        with pytest.raises(EntityResolutionError):
            directory.resolve_domain("unregistered-opaque-id")

    def test_proxy_registration_and_next_hop_resolution(self) -> None:
        directory = EntityDirectory()
        edge = RelationshipEdge(
            source_id="customer:org:acme",
            target_id="technology:lang:python",
            edge_type="USES",
            domain=Domain.CUSTOMER,
            target_domain=Domain.TECHNOLOGY,
        )
        proxy = directory.register_cross_domain_edge(edge)
        assert proxy is not None
        assert proxy.is_proxy is True
        assert proxy.domain == Domain.CUSTOMER
        assert proxy.target_domain == Domain.TECHNOLOGY

        fetched = directory.get_proxy(Domain.CUSTOMER, "technology:lang:python")
        assert fetched == proxy
        assert len(directory.proxies_for_domain(Domain.CUSTOMER)) == 1

        next_domain = directory.resolve_next_hop_domain(neighbor=proxy, edge=edge)
        assert next_domain == Domain.TECHNOLOGY
        assert directory.stats().proxy_hints_used == 1


class TestQueryCoordinatorFederationAndDerivedEdges:
    @pytest.mark.asyncio
    async def test_cross_shard_proxy_writes_and_3hop_federated_traversal(self) -> None:
        shards = _split_in_memory_shards()
        coord = QueryCoordinator(shards)

        nodes = [
            EntityNode(
                id="customer:org:acme",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="customer:team:platform",
                domain=Domain.CUSTOMER,
                entity_type="Team",
            ),
            EntityNode(
                id="technology:lang:python",
                domain=Domain.TECHNOLOGY,
                entity_type="Language",
            ),
            EntityNode(
                id="product:sdk:scale_kg",
                domain=Domain.PRODUCT,
                entity_type="Product",
            ),
            EntityNode(
                id="knowledge:doc:arch",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lakehouse/arch.parquet",
            ),
        ]
        edges = [
            # Intra-domain edge (customer -> customer)
            RelationshipEdge(
                source_id="customer:org:acme",
                target_id="customer:team:platform",
                edge_type="OWNS",
                domain=Domain.CUSTOMER,
                target_domain=Domain.CUSTOMER,
            ),
            # Cross-domain edge (customer -> technology)
            RelationshipEdge(
                source_id="customer:team:platform",
                target_id="technology:lang:python",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            # Cross-domain edge (technology -> product)
            RelationshipEdge(
                source_id="technology:lang:python",
                target_id="product:sdk:scale_kg",
                edge_type="POWERS",
                domain=Domain.TECHNOLOGY,
                target_domain=Domain.PRODUCT,
            ),
            # Cross-domain edge (product -> knowledge)
            RelationshipEdge(
                source_id="product:sdk:scale_kg",
                target_id="knowledge:doc:arch",
                edge_type="DESCRIBED_BY",
                domain=Domain.PRODUCT,
                target_domain=Domain.KNOWLEDGE,
            ),
        ]
        await coord.upsert_nodes(nodes)
        await coord.upsert_edges(edges)

        # Verify ghost/proxy vertex persisted inside the source domain shard
        proxy_in_customer = await shards[Domain.CUSTOMER].get_node(
            "technology:lang:python"
        )
        assert proxy_in_customer is not None
        assert proxy_in_customer.is_proxy is True
        assert proxy_in_customer.target_domain == Domain.TECHNOLOGY

        # Execute 3-hop federated traversal starting from team -> python -> scale_kg -> arch
        res = await coord.traverse(
            TraversalRequest(seed_ids=["customer:team:platform"], max_depth=3)
        )
        assert res.metrics.aborted is False
        assert res.metrics.depth_reached == 3
        assert {n.id for n in res.nodes} == {
            "customer:team:platform",
            "technology:lang:python",
            "product:sdk:scale_kg",
            "knowledge:doc:arch",
        }
        assert res.metrics.cross_domain_hops == 3
        assert res.metrics.single_domain_locality_ratio == 0.0

        # Intra-domain check from org -> team only (max_depth=1)
        local_res = await coord.traverse(
            TraversalRequest(seed_ids=["customer:org:acme"], max_depth=1)
        )
        assert local_res.metrics.cross_domain_hops == 0
        assert local_res.metrics.single_domain_locality_ratio == 1.0

    @pytest.mark.asyncio
    async def test_on_the_fly_derived_edges_and_hot_path_promotion(self) -> None:
        shards = _split_in_memory_shards()
        coord = QueryCoordinator(shards, hot_path_threshold=3)

        nodes = [
            EntityNode(
                id="customer:org:alpha",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="customer:org:beta",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
            ),
            EntityNode(
                id="technology:framework:fastapi",
                domain=Domain.TECHNOLOGY,
                entity_type="Framework",
            ),
            EntityNode(
                id="technology:db:iceberg",
                domain=Domain.TECHNOLOGY,
                entity_type="Storage",
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="customer:org:alpha",
                target_id="technology:framework:fastapi",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="customer:org:beta",
                target_id="technology:framework:fastapi",
                edge_type="USES",
                domain=Domain.CUSTOMER,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="technology:framework:fastapi",
                target_id="technology:db:iceberg",
                edge_type="DEPENDS_ON",
                domain=Domain.TECHNOLOGY,
                target_domain=Domain.TECHNOLOGY,
            ),
        ]
        await coord.upsert_nodes(nodes)
        await coord.upsert_edges(edges)

        # Without include_derived, no derived edges returned
        plain = await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:alpha"], max_depth=2, include_derived=False
            )
        )
        assert all(not e.is_derived for e in plain.edges)

        # Hits 1 & 2 compute SIMILAR_TO and INDIRECTLY_USES on-the-fly (not yet hot-path)
        req = TraversalRequest(
            seed_ids=["customer:org:alpha"], max_depth=2, include_derived=True
        )
        res1 = await coord.traverse(req)
        derived_1 = {
            (e.source_id, e.target_id, e.edge_type): e
            for e in res1.edges
            if e.is_derived
        }
        assert ("customer:org:alpha", "customer:org:beta", "SIMILAR_TO") in derived_1
        assert (
            "customer:org:alpha",
            "technology:db:iceberg",
            "INDIRECTLY_USES",
        ) in derived_1
        assert all(not e.materialized_hot_path for e in derived_1.values())
        assert coord.stats().hot_paths_promoted == 0

        await coord.traverse(req)
        assert coord.stats().hot_paths_promoted == 0

        # Hit 3 crosses hot_path_threshold=3 and materializes hot-path edges into shard
        res3 = await coord.traverse(req)
        derived_3 = [e for e in res3.edges if e.is_derived]
        assert len(derived_3) >= 2
        assert all(e.materialized_hot_path is True for e in derived_3)
        assert coord.stats().hot_paths_promoted >= 2

        # Persisted into customer shard with materialized_hot_path=True
        persisted = await shards[Domain.CUSTOMER].get_neighbors(
            "customer:org:alpha", direction="out"
        )
        persisted_derived = [e for e, _ in persisted if e.is_derived]
        assert any(
            e.edge_type == "INDIRECTLY_USES" and e.materialized_hot_path
            for e in persisted_derived
        )

    @pytest.mark.asyncio
    async def test_coordinator_guardrails_and_iceberg_shards(
        self, tmp_path: Path
    ) -> None:
        shards = {d: IcebergPuppyGraphStore(tmp_path / d.value) for d in Domain}
        coord = QueryCoordinator(shards)
        await coord.upsert_nodes(
            [
                EntityNode(
                    id="customer:org:one",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                ),
                EntityNode(
                    id="product:app:kg",
                    domain=Domain.PRODUCT,
                    entity_type="Product",
                ),
            ]
        )
        await coord.upsert_edges(
            [
                RelationshipEdge(
                    source_id="customer:org:one",
                    target_id="product:app:kg",
                    edge_type="EVALUATES",
                    domain=Domain.CUSTOMER,
                    target_domain=Domain.PRODUCT,
                )
            ]
        )

        # Pre-flight depth > 3 rejected
        with pytest.raises(GuardrailViolationError) as exc:
            await coord.traverse(
                TraversalRequest(seed_ids=["customer:org:one"], max_depth=4)
            )
        assert exc.value.violation.code == GuardrailViolationCode.MAX_DEPTH_EXCEEDED

        # Runtime expanded nodes abort
        aborted = await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:one"], max_depth=2, max_expanded_nodes=1
            )
        )
        assert aborted.metrics.aborted is True
        assert (
            aborted.metrics.violation is not None
            and aborted.metrics.violation.code
            == GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED
        )

        ok = await coord.traverse(
            TraversalRequest(seed_ids=["customer:org:one"], max_depth=1)
        )
        assert ok.metrics.aborted is False
        assert {n.id for n in ok.nodes} == {"customer:org:one", "product:app:kg"}
        assert ok.metrics.cross_domain_hops == 1
