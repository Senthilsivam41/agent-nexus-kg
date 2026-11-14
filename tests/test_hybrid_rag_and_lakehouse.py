"""Tests for LakehouseContentStore, InMemoryVectorIndex, and HybridGraphRAGRouter."""

from __future__ import annotations

from pathlib import Path

import pytest

from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.core.models import Domain, EntityNode, RelationshipEdge
from scale_kg.lakehouse import (
    HistoricalGraphSlice,
    LakehouseContentStore,
    LakehouseDocument,
)
from scale_kg.routing import (
    HybridGraphRAGRouter,
    HybridRAGRequest,
    InMemoryVectorIndex,
    QueryCoordinator,
    cosine_similarity,
)


class TestLakehouseContentStore:
    def test_documents_persist_across_reopen_and_selective_hydration(
        self, tmp_path: Path
    ) -> None:
        warehouse = tmp_path / "lakehouse"
        store1 = LakehouseContentStore(warehouse)
        docs = [
            LakehouseDocument(
                lakehouse_ref="s3://lakehouse/knowledge/doc1.parquet",
                entity_id="knowledge:doc:1",
                domain=Domain.KNOWLEDGE,
                title="GraphRAG Guide",
                content="Detailed guide on Seed-Then-Expand GraphRAG.",
                source_uri="https://docs.example.com/graphrag",
            ),
            LakehouseDocument(
                lakehouse_ref="s3://lakehouse/knowledge/doc2.parquet",
                entity_id="knowledge:doc:2",
                domain=Domain.KNOWLEDGE,
                title="Supernode Mitigation",
                content="Bucket high-degree hubs across 16 partitions.",
            ),
            LakehouseDocument(
                lakehouse_ref="s3://lakehouse/product/spec1.parquet",
                entity_id="product:doc:spec1",
                domain=Domain.PRODUCT,
                title="Scale-KG Product Spec",
                content="Architecture overview for 10B edges.",
            ),
        ]
        assert store1.put_documents(docs) == 3

        # Reopen from Parquet on disk
        store2 = LakehouseContentStore(warehouse)
        assert store2.stats().document_count == 3
        doc1 = store2.get_document("s3://lakehouse/knowledge/doc1.parquet")
        assert doc1 is not None
        assert doc1.title == "GraphRAG Guide"

        # Hydrate only 1 selected ref
        hydrated = store2.hydrate_batch(["s3://lakehouse/product/spec1.parquet"])
        assert list(hydrated.keys()) == ["s3://lakehouse/product/spec1.parquet"]
        assert store2.stats().hydration_calls == 1
        assert store2.stats().hydrated_doc_count == 1

    def test_historical_graph_slice_roundtrip(self, tmp_path: Path) -> None:
        store = LakehouseContentStore(tmp_path / "lakehouse")
        graph_slice = HistoricalGraphSlice(
            slice_id="2026-11-q4-customer",
            as_of="2026-11-01T00:00:00Z",
            domain=Domain.CUSTOMER,
            nodes=[
                EntityNode(
                    id="customer:org:acme",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                ),
                EntityNode(
                    id="customer:org:beta",
                    domain=Domain.CUSTOMER,
                    entity_type="Organization",
                ),
            ],
            edges=[
                RelationshipEdge(
                    source_id="customer:org:acme",
                    target_id="customer:org:beta",
                    edge_type="PARTNERED_WITH",
                    domain=Domain.CUSTOMER,
                    target_domain=Domain.CUSTOMER,
                    routing_props={"year": 2025},
                )
            ],
        )
        store.write_historical_slice(graph_slice)
        assert store.list_slices(Domain.CUSTOMER) == ["2026-11-q4-customer"]

        reloaded = store.load_historical_slice("2026-11-q4-customer")
        assert reloaded is not None
        assert len(reloaded.nodes) == 2
        assert len(reloaded.edges) == 1
        assert reloaded.edges[0].routing_props["year"] == 2025


class TestHybridGraphRAGRouter:
    def test_cosine_similarity_and_vector_index(self) -> None:
        assert cosine_similarity([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)
        assert cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0
        with pytest.raises(ValueError):
            cosine_similarity([1.0], [1.0, 2.0])

        idx = InMemoryVectorIndex()
        idx.upsert("knowledge:doc:a", [1.0, 0.0, 0.0])
        idx.upsert("technology:lib:b", [0.8, 0.6, 0.0])
        idx.upsert("product:app:c", [0.0, 1.0, 0.0])

        hits = idx.search([1.0, 0.0, 0.0], top_k=2)
        assert [h.entity_id for h in hits] == ["knowledge:doc:a", "technology:lib:b"]

        filtered = idx.search([1.0, 0.0, 0.0], top_k=5, domains=[Domain.TECHNOLOGY])
        assert [h.entity_id for h in filtered] == ["technology:lib:b"]

    @pytest.mark.asyncio
    async def test_5_stage_seed_then_expand_rrf_and_selective_hydration(
        self, tmp_path: Path
    ) -> None:
        shards = {d: InMemoryGraphStore() for d in Domain}
        coord = QueryCoordinator(shards)
        lakehouse = LakehouseContentStore(tmp_path / "lakehouse")
        vector_index = InMemoryVectorIndex()

        nodes = [
            EntityNode(
                id="knowledge:doc:seed",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lh/seed.parquet",
            ),
            EntityNode(
                id="technology:engine:hugegraph",
                domain=Domain.TECHNOLOGY,
                entity_type="Engine",
                lakehouse_ref="s3://lh/hugegraph.parquet",
            ),
            EntityNode(
                id="product:platform:kineticv",
                domain=Domain.PRODUCT,
                entity_type="Product",
                lakehouse_ref="s3://lh/kineticv.parquet",
            ),
            EntityNode(
                id="customer:org:enterprise",
                domain=Domain.CUSTOMER,
                entity_type="Organization",
                lakehouse_ref="s3://lh/enterprise.parquet",
            ),
            EntityNode(
                id="knowledge:doc:unrelated",
                domain=Domain.KNOWLEDGE,
                entity_type="Document",
                lakehouse_ref="s3://lh/unrelated.parquet",
            ),
        ]
        edges = [
            RelationshipEdge(
                source_id="knowledge:doc:seed",
                target_id="technology:engine:hugegraph",
                edge_type="DOCUMENTS",
                domain=Domain.KNOWLEDGE,
                target_domain=Domain.TECHNOLOGY,
            ),
            RelationshipEdge(
                source_id="technology:engine:hugegraph",
                target_id="product:platform:kineticv",
                edge_type="POWERS",
                domain=Domain.TECHNOLOGY,
                target_domain=Domain.PRODUCT,
            ),
            RelationshipEdge(
                source_id="product:platform:kineticv",
                target_id="customer:org:enterprise",
                edge_type="SERVES",
                domain=Domain.PRODUCT,
                target_domain=Domain.CUSTOMER,
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
                    title=f"Title for {n.id}",
                    content=f"Authoritative payload for {n.id}.",
                    source_uri=f"https://kb.internal/{n.id}",
                )
                for n in nodes
            ]
        )

        vector_index.upsert_batch(
            [
                ("knowledge:doc:seed", [0.99, 0.1, 0.0], Domain.KNOWLEDGE),
                ("technology:engine:hugegraph", [0.7, 0.7, 0.0], Domain.TECHNOLOGY),
                ("knowledge:doc:unrelated", [0.0, 0.0, 1.0], Domain.KNOWLEDGE),
            ]
        )

        router = HybridGraphRAGRouter(
            vector_index=vector_index,
            coordinator=coord,
            lakehouse=lakehouse,
        )

        result = await router.retrieve(
            HybridRAGRequest(
                query_text="How does HugeGraph power Kinetic-V?",
                query_embedding=[1.0, 0.0, 0.0],
                seed_k=1,
                top_n=2,
                max_depth=3,
            )
        )

        # Stage 1: top-1 vector seed
        assert result.seed_ids == ["knowledge:doc:seed"]
        # Stage 2: expanded 4 connected nodes across 3 hops
        assert result.candidates_considered == 4
        assert result.traversal is not None
        assert result.traversal.metrics.cross_domain_hops == 3
        # Stage 3: RRF top-2 ranked nodes
        assert len(result.hits) == 2
        assert result.hits[0].node.id == "knowledge:doc:seed"
        assert result.hits[1].node.id == "technology:engine:hugegraph"
        assert result.hits[0].rrf_score >= result.hits[1].rrf_score
        # Stage 4: ONLY top_n=2 hydrated from Lakehouse (not all 4 expanded nodes)
        assert result.hydrated_count == 2
        assert lakehouse.stats().hydrated_doc_count == 2
        # Stage 5: Citations + supporting paths present
        assert len(result.citations) == 2
        assert result.hits[1].document is not None
        assert (
            "Authoritative payload for technology:engine:hugegraph."
            in result.hits[1].document.content
        )
        assert result.hits[1].citation is not None
        assert result.hits[1].citation.source_uri == (
            "https://kb.internal/technology:engine:hugegraph"
        )
        assert [
            "knowledge:doc:seed",
            "technology:engine:hugegraph",
        ] in result.hits[1].supporting_paths
