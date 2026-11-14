"""5-stage Seed-Then-Expand Hybrid Vector + KG + Lakehouse GraphRAG retrieval router."""

from __future__ import annotations

import math
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from typing import Protocol

from pydantic import BaseModel, Field

from scale_kg.core.models import Domain, EntityNode, TraversalRequest, TraversalResult
from scale_kg.lakehouse.store import LakehouseContentStore, LakehouseDocument
from scale_kg.routing.coordinator import QueryCoordinator
from scale_kg.routing.directory import parse_canonical_id


class VectorSearchHit(BaseModel):
    """Single entity match returned by a VectorIndex search."""

    entity_id: str
    score: float
    rank: int
    domain: Domain | None = None


class VectorIndex(Protocol):
    """Protocol for pluggable vector seed indexes."""

    def search(
        self,
        query_embedding: list[float],
        *,
        top_k: int = 5,
        domains: list[Domain] | None = None,
    ) -> list[VectorSearchHit]: ...


def cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two equal-length vectors safely."""
    if not a or len(a) != len(b):
        raise ValueError(
            f"Vector dimension mismatch: {len(a)} != {len(b)} (or empty vector)."
        )
    dot = sum(x * y for x, y in zip(a, b, strict=True))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


class InMemoryVectorIndex:
    """In-memory cosine-similarity vector index for Seed-Then-Expand retrieval."""

    def __init__(self) -> None:
        self._vectors: dict[str, list[float]] = {}
        self._domains: dict[str, Domain | None] = {}

    def upsert(
        self,
        entity_id: str,
        embedding: list[float],
        *,
        domain: Domain | None = None,
    ) -> None:
        """Store or update an entity embedding vector."""
        if not embedding:
            raise ValueError("embedding must be non-empty.")
        inferred_domain = domain
        if inferred_domain is None:
            parsed = parse_canonical_id(entity_id)
            if parsed is not None:
                inferred_domain = parsed.domain
        self._vectors[entity_id] = list(embedding)
        self._domains[entity_id] = inferred_domain

    def upsert_batch(
        self,
        items: Iterable[tuple[str, list[float], Domain | None]],
    ) -> int:
        """Batch-upsert ``(entity_id, embedding, domain)`` tuples."""
        count = 0
        for entity_id, embedding, domain in items:
            self.upsert(entity_id, embedding, domain=domain)
            count += 1
        return count

    def search(
        self,
        query_embedding: list[float],
        *,
        top_k: int = 5,
        domains: list[Domain] | None = None,
    ) -> list[VectorSearchHit]:
        """Return top-K entities ranked by cosine similarity."""
        if top_k < 1 or not self._vectors:
            return []
        allowed_domains = set(domains) if domains else None
        scored: list[tuple[float, str, Domain | None]] = []
        for entity_id, vec in self._vectors.items():
            ent_domain = self._domains.get(entity_id)
            if allowed_domains is not None and ent_domain not in allowed_domains:
                continue
            sim = cosine_similarity(query_embedding, vec)
            scored.append((sim, entity_id, ent_domain))

        scored.sort(key=lambda item: (-item[0], item[1]))
        return [
            VectorSearchHit(
                entity_id=entity_id,
                score=round(score, 6),
                rank=idx + 1,
                domain=ent_domain,
            )
            for idx, (score, entity_id, ent_domain) in enumerate(scored[:top_k])
        ]

    def clear(self) -> None:
        """Remove all stored embeddings."""
        self._vectors.clear()
        self._domains.clear()


class SourceCitation(BaseModel):
    """Grounded citation linking a retrieved entity to its Lakehouse record and paths."""

    entity_id: str
    domain: Domain
    lakehouse_ref: str
    source_uri: str
    title: str = ""
    supporting_paths: list[list[str]] = Field(default_factory=list)


class HybridRAGRequest(BaseModel):
    """Input specification for the 5-stage Hybrid GraphRAG retrieval pipeline."""

    query_text: str | None = None
    query_embedding: list[float] | None = None
    seed_ids: list[str] | None = None
    seed_k: int = Field(default=5, ge=1)
    top_n: int = Field(default=5, ge=1)
    max_depth: int = Field(default=2, ge=1, le=3)
    edge_types: list[str] | None = None
    domains: list[Domain] | None = None
    include_derived: bool = False
    max_results: int = Field(default=5000, ge=1, le=5000)
    max_execution_seconds: float = Field(default=2.0, gt=0.0, le=2.0)
    max_expanded_nodes: int = Field(default=50000, ge=1, le=50000)
    require_lakehouse_ref: bool = False


class HybridRAGHit(BaseModel):
    """Single ranked node with RRF fusion breakdown, hydrated payload, and citation."""

    node: EntityNode
    rank: int
    rrf_score: float
    vector_score: float | None = None
    vector_rank: int | None = None
    graph_hop_distance: int
    graph_rank: int
    document: LakehouseDocument | None = None
    citation: SourceCitation | None = None
    supporting_paths: list[list[str]] = Field(default_factory=list)


class HybridRAGResult(BaseModel):
    """Output of the 5-stage Seed-Then-Expand RRF + Lakehouse retrieval pipeline."""

    query_text: str | None = None
    seed_ids: list[str] = Field(default_factory=list)
    hits: list[HybridRAGHit] = Field(default_factory=list)
    citations: list[SourceCitation] = Field(default_factory=list)
    traversal: TraversalResult | None = None
    candidates_considered: int = 0
    hydrated_count: int = 0
    stage_timings_ms: dict[str, float] = Field(default_factory=dict)


class HybridGraphRAGRouter:
    """Executes 5-stage Seed-Then-Expand GraphRAG with RRF and Lakehouse hydration."""

    def __init__(
        self,
        *,
        vector_index: VectorIndex,
        coordinator: QueryCoordinator,
        lakehouse: LakehouseContentStore,
        embedder: Callable[[str], list[float]] | None = None,
        rrf_k: int = 60,
        vector_weight: float = 1.0,
        graph_weight: float = 1.0,
    ) -> None:
        if rrf_k < 1:
            raise ValueError(f"rrf_k must be >= 1, got {rrf_k}.")
        self.vector_index = vector_index
        self.coordinator = coordinator
        self.lakehouse = lakehouse
        self.embedder = embedder
        self.rrf_k = rrf_k
        self.vector_weight = vector_weight
        self.graph_weight = graph_weight

    def _resolve_query_embedding(self, request: HybridRAGRequest) -> list[float] | None:
        if request.query_embedding is not None:
            return request.query_embedding
        if request.query_text is not None and self.embedder is not None:
            return self.embedder(request.query_text)
        return None

    def _fuse_with_rrf(
        self,
        *,
        vector_hits: list[VectorSearchHit],
        traversal: TraversalResult,
        require_lakehouse_ref: bool,
    ) -> list[
        tuple[EntityNode, float, int, int, float | None, int | None, list[list[str]]]
    ]:
        vec_by_id = {hit.entity_id: hit for hit in vector_hits}

        min_hop: dict[str, int] = {sid: 0 for sid in traversal.seed_ids}
        supporting_paths: dict[str, list[list[str]]] = defaultdict(list)
        for path in traversal.paths:
            for idx, node_id in enumerate(path):
                if node_id not in min_hop or idx < min_hop[node_id]:
                    min_hop[node_id] = idx
                if idx == len(path) - 1 or node_id in traversal.seed_ids:
                    supporting_paths[node_id].append(path)

        candidates = [
            node
            for node in traversal.nodes
            if not require_lakehouse_ref or bool(node.lakehouse_ref)
        ]

        # Assign deterministic graph proximity rank from hop distance & path support.
        def _graph_sort_key(node: EntityNode) -> tuple[int, int, float, str]:
            hop = min_hop.get(node.id, traversal.metrics.depth_reached + 1)
            support = len(supporting_paths.get(node.id, ()))
            vec_score = vec_by_id[node.id].score if node.id in vec_by_id else -1.0
            return (hop, -support, -vec_score, node.id)

        graph_ordered = sorted(candidates, key=_graph_sort_key)
        graph_rank_by_id = {node.id: idx + 1 for idx, node in enumerate(graph_ordered)}

        fused: list[
            tuple[
                EntityNode,
                float,
                int,
                int,
                float | None,
                int | None,
                list[list[str]],
            ]
        ] = []
        for node in candidates:
            g_rank = graph_rank_by_id[node.id]
            hop = min_hop.get(node.id, traversal.metrics.depth_reached + 1)
            vec_hit = vec_by_id.get(node.id)
            v_rank = vec_hit.rank if vec_hit is not None else None
            v_score = vec_hit.score if vec_hit is not None else None

            rrf_score = self.graph_weight / (self.rrf_k + g_rank)
            if v_rank is not None:
                rrf_score += self.vector_weight / (self.rrf_k + v_rank)

            fused.append(
                (
                    node,
                    round(rrf_score, 8),
                    hop,
                    g_rank,
                    v_score,
                    v_rank,
                    supporting_paths.get(node.id, []),
                )
            )

        fused.sort(key=lambda item: (-item[1], item[2], item[0].id))
        return fused

    async def retrieve(self, request: HybridRAGRequest) -> HybridRAGResult:
        """Execute all 5 Hybrid GraphRAG stages and return hydrated ranked citations."""
        total_start = time.perf_counter()
        timings: dict[str, float] = {}

        # Stage 1: Vector similarity seed retrieval.
        t1 = time.perf_counter()
        query_emb = self._resolve_query_embedding(request)
        if query_emb is None and not request.seed_ids:
            raise ValueError(
                "HybridRAGRequest requires query_embedding, (query_text + embedder), or seed_ids."
            )
        vector_hits: list[VectorSearchHit] = (
            self.vector_index.search(
                query_emb,
                top_k=request.seed_k,
                domains=request.domains,
            )
            if query_emb is not None
            else []
        )
        seed_ids = list(
            dict.fromkeys(
                [*(request.seed_ids or []), *(hit.entity_id for hit in vector_hits)]
            )
        )
        timings["vector_seed"] = round((time.perf_counter() - t1) * 1000.0, 3)

        if not seed_ids:
            timings["total"] = round((time.perf_counter() - total_start) * 1000.0, 3)
            return HybridRAGResult(
                query_text=request.query_text,
                seed_ids=[],
                stage_timings_ms=timings,
            )

        # Stage 2: Guarded 1-to-3 hop structural expansion via QueryCoordinator.
        t2 = time.perf_counter()
        traversal = await self.coordinator.traverse(
            TraversalRequest(
                seed_ids=seed_ids,
                max_depth=request.max_depth,
                edge_types=request.edge_types,
                domains=request.domains,
                max_results=request.max_results,
                max_execution_seconds=request.max_execution_seconds,
                max_expanded_nodes=request.max_expanded_nodes,
                include_derived=request.include_derived,
            )
        )
        timings["graph_expand"] = round((time.perf_counter() - t2) * 1000.0, 3)

        # Stage 3: Reciprocal Rank Fusion combining vector and graph proximity ranks.
        t3 = time.perf_counter()
        fused = self._fuse_with_rrf(
            vector_hits=vector_hits,
            traversal=traversal,
            require_lakehouse_ref=request.require_lakehouse_ref,
        )
        top_candidates = fused[: request.top_n]
        timings["rrf_fusion"] = round((time.perf_counter() - t3) * 1000.0, 3)

        # Stage 4: Selective Lakehouse hydration ONLY for the final top-N nodes.
        t4 = time.perf_counter()
        refs_to_hydrate = [
            item[0].lakehouse_ref for item in top_candidates if item[0].lakehouse_ref
        ]
        hydrated_docs = (
            self.lakehouse.hydrate_batch(refs_to_hydrate) if refs_to_hydrate else {}
        )
        timings["lakehouse_hydrate"] = round((time.perf_counter() - t4) * 1000.0, 3)

        # Stage 5: Grounded context & citation assembly.
        t5 = time.perf_counter()
        hits: list[HybridRAGHit] = []
        citations: list[SourceCitation] = []
        for idx, (node, rrf, hop, g_rank, v_score, v_rank, paths) in enumerate(
            top_candidates
        ):
            doc = hydrated_docs.get(node.lakehouse_ref) if node.lakehouse_ref else None
            citation: SourceCitation | None = None
            if node.lakehouse_ref:
                citation = SourceCitation(
                    entity_id=node.id,
                    domain=node.domain,
                    lakehouse_ref=node.lakehouse_ref,
                    source_uri=(
                        doc.source_uri if doc and doc.source_uri else node.lakehouse_ref
                    ),
                    title=doc.title if doc else "",
                    supporting_paths=paths,
                )
                citations.append(citation)
            hits.append(
                HybridRAGHit(
                    node=node,
                    rank=idx + 1,
                    rrf_score=rrf,
                    vector_score=v_score,
                    vector_rank=v_rank,
                    graph_hop_distance=hop,
                    graph_rank=g_rank,
                    document=doc,
                    citation=citation,
                    supporting_paths=paths,
                )
            )
        timings["context_assembly"] = round((time.perf_counter() - t5) * 1000.0, 3)
        timings["total"] = round((time.perf_counter() - total_start) * 1000.0, 3)

        return HybridRAGResult(
            query_text=request.query_text,
            seed_ids=seed_ids,
            hits=hits,
            citations=citations,
            traversal=traversal,
            candidates_considered=len(fused),
            hydrated_count=len(hydrated_docs),
            stage_timings_ms=timings,
        )
