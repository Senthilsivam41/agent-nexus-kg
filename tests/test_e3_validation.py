"""Comprehensive E3 validation suite covering SEN-63, SEN-73, SEN-74, and SEN-75."""

from __future__ import annotations

from pathlib import Path

import pytest

from scale_kg.adapters.iceberg_puppygraph import IcebergPuppyGraphStore
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.core.models import Domain, EntityNode, RelationshipEdge, TraversalRequest
from scale_kg.lakehouse import LakehouseContentStore, LakehouseDocument
from scale_kg.routing import (
    EntityDirectory,
    HybridGraphRAGRouter,
    HybridRAGRequest,
    InMemoryVectorIndex,
    QueryCoordinator,
    parse_canonical_id,
)


class TestE31CanonicalRouterAndDirectory:
    """Validation for SEN-73 ([E3.1])."""

    def test_o1_hop0_routing_opaque_fallback_and_proxy_hints(self) -> None:
        directory = EntityDirectory()
        assert parse_canonical_id("product:sdk:scale-kg") is not None
        assert directory.resolve_domain("product:sdk:scale-kg") == Domain.PRODUCT
        assert directory.stats().canonical_hits == 1
        assert directory.stats().directory_hits == 0

        directory.register("legacy_9001", Domain.KNOWLEDGE, aliases=["doc-legacy"])
        assert directory.resolve_domain("doc-legacy") == Domain.KNOWLEDGE
        assert directory.stats().directory_hits == 1

        proxy = directory.register_cross_domain_edge(
            RelationshipEdge(
                source_id="knowledge:doc:1",
                target_id="product:sdk:scale-kg",
                edge_type="MENTIONS",
                domain=Domain.KNOWLEDGE,
                target_domain=Domain.PRODUCT,
            )
        )
        assert proxy is not None
        assert proxy.is_proxy is True
        assert proxy.target_domain == Domain.PRODUCT
        assert directory.resolve_next_hop_domain(neighbor=proxy) == Domain.PRODUCT


class TestE32FederatedCoordinatorAndDerivedEdges:
    """Validation for SEN-74 ([E3.2])."""

    @pytest.mark.asyncio
    async def test_multi_shard_locality_and_derived_promotion(self) -> None:
        shards = {d: InMemoryGraphStore() for d in Domain}
        coord = QueryCoordinator(shards, hot_path_threshold=2)

        await coord.upsert_nodes(
            [
                EntityNode(
                    id="customer:org:a",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                ),
                EntityNode(
                    id="customer:org:b",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                ),
                EntityNode(
                    id="technology:lang:python",
                    domain=Domain.TECHNOLOGY,
                    entity_type="Language",
                ),
                EntityNode(
                    id="technology:lib:pydantic",
                    domain=Domain.TECHNOLOGY,
                    entity_type="Library",
                ),
            ]
        )
        await coord.upsert_edges(
            [
                RelationshipEdge(
                    source_id="customer:org:a",
                    target_id="technology:lang:python",
                    edge_type="USES",
                    domain=Domain.CUSTOMER,
                    target_domain=Domain.TECHNOLOGY,
                ),
                RelationshipEdge(
                    source_id="customer:org:b",
                    target_id="technology:lang:python",
                    edge_type="USES",
                    domain=Domain.CUSTOMER,
                    target_domain=Domain.TECHNOLOGY,
                ),
                RelationshipEdge(
                    source_id="technology:lang:python",
                    target_id="technology:lib:pydantic",
                    edge_type="DEPENDS_ON",
                    domain=Domain.TECHNOLOGY,
                    target_domain=Domain.TECHNOLOGY,
                ),
            ]
        )

        # First query computes derived edges on-the-fly, emits locality metrics
        r1 = await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:a"], max_depth=2, include_derived=True
            )
        )
        assert r1.metrics.cross_domain_hops >= 1
        assert 0.0 <= r1.metrics.single_domain_locality_ratio <= 1.0
        assert any(
            e.edge_type == "SIMILAR_TO" and e.is_derived and not e.materialized_hot_path
            for e in r1.edges
        )
        assert any(
            e.edge_type == "INDIRECTLY_USES"
            and e.is_derived
            and not e.materialized_hot_path
            for e in r1.edges
        )

        # Second query hits hot_path_threshold=2 -> promotes to materialized_hot_path=True
        r2 = await coord.traverse(
            TraversalRequest(
                seed_ids=["customer:org:a"], max_depth=2, include_derived=True
            )
        )
        promoted = [e for e in r2.edges if e.is_derived and e.materialized_hot_path]
        assert len(promoted) >= 2
        assert coord.stats().hot_paths_promoted >= 2


class TestE33HybridGraphRAGWithIcebergAndLakehouse:
    """End-to-end validation for SEN-63 and SEN-75 ([E3.3])."""

    @pytest.mark.asyncio
    async def test_5_stage_pipeline_over_iceberg_shards(self, tmp_path: Path) -> None:
        shards = {
            d: IcebergPuppyGraphStore(tmp_path / "graph" / d.value) for d in Domain
        }
        coord = QueryCoordinator(shards)
        lakehouse = LakehouseContentStore(tmp_path / "lakehouse")
        vector_index = InMemoryVectorIndex()

        nodes = [
            EntityNode(
                id="customer:org:acme",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
                lakehouse_ref="s3://lh/customer/acme.parquet",
            ),
            EntityNode(
                id="product:sdk:scale-kg",
                domain=Domain.PRODUCT,
                entity_type="Product",
                lakehouse_ref="s3://lh/product/scale-kg.parquet",
            ),
            EntityNode(
                id="knowledge:doc:e3-spec",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lh/knowledge/e3-spec.parquet",
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="customer:org:acme",
                target_id="product:sdk:scale-kg",
                edge_type="ADOPTED",
                domain=Domain.CUSTOMER,
                target_domain=Domain.PRODUCT,
            ),
            RelationshipEdge(
                source_id="product:sdk:scale-kg",
                target_id="knowledge:doc:e3-spec",
                edge_type="DESCRIBED_BY",
                domain=Domain.PRODUCT,
                target_domain=Domain.KNOWLEDGE,
            ),
        ]
        await coord.upsert_nodes(nodes)
        await coord.upsert_edges(edges)

        lakehouse.put_documents(
            [
                LakehouseDocument(
                    lakehouse_ref=n.lakehouse_ref or "",
                    entity_id=n.id,
                    domain=n.domain,
                    title=n.id,
                    content=f"Payload for {n.id}",
                )
                for n in nodes
            ]
        )
        vector_index.upsert("customer:org:acme", [1.0, 0.0])
        vector_index.upsert("product:sdk:scale-kg", [0.6, 0.8])

        router = HybridGraphRAGRouter(
            vector_index=vector_index,
            coordinator=coord,
            lakehouse=lakehouse,
            embedder=lambda _q: [1.0, 0.0],
        )
        res = await router.retrieve(
            HybridRAGRequest(
                query_text="Explain Acme adoption of scale-kg",
                seed_k=1,
                top_n=2,
                max_depth=2,
            )
        )
        assert res.hydrated_count == 2
        assert lakehouse.stats().hydrated_doc_count == 2
        assert len(res.hits) == 2
        assert len(res.citations) == 2
        assert res.traversal is not None
        assert res.traversal.metrics.cross_domain_hops == 2
