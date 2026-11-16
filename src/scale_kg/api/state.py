"""Shared application state, backend factory, and core API service operations."""

from __future__ import annotations

import hmac
import os
import tempfile
import uuid
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Literal

from scale_kg.adapters.base import (
    GraphStats,
    GraphStore,
    GraphStoreError,
    HealthStatus,
)
from scale_kg.adapters.hugegraph_adapter import HugeGraphStore
from scale_kg.adapters.iceberg_puppygraph import IcebergPuppyGraphStore
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.adapters.neo4j_adapter import Neo4jGraphStore
from scale_kg.adapters.stubs import JanusGraphStore, NebulaGraphStore
from scale_kg.api.models import (
    BenchmarkRunRequest,
    BenchmarkRunResponse,
    DirectoryResolveResponse,
    HealthResponse,
    IngestRequest,
    IngestResponse,
    NeighborItem,
    NeighborsResponse,
    PutDocumentsRequest,
    PutDocumentsResponse,
    RegisterAliasesRequest,
    RegisterAliasesResponse,
    SliceListResponse,
    StatsResponse,
    WriteSliceResponse,
)
from scale_kg.benchmark.report import generate_gate2_adr, rank_engines
from scale_kg.benchmark.workloads import BenchmarkSuite
from scale_kg.config import ScaleKGConfig
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.lakehouse.store import (
    HistoricalGraphSlice,
    LakehouseContentStore,
    LakehouseDocument,
)
from scale_kg.routing.coordinator import QueryCoordinator
from scale_kg.routing.directory import (
    EntityDirectory,
    EntityResolutionError,
    parse_canonical_id,
)
from scale_kg.routing.hybrid_rag import (
    HybridGraphRAGRouter,
    HybridRAGRequest,
    HybridRAGResult,
    InMemoryVectorIndex,
)


def verify_api_key_token(
    expected_api_key: str | None,
    *,
    x_api_key: str | None = None,
    authorization: str | None = None,
) -> bool:
    """Validate an incoming API key against X-API-Key or Authorization Bearer."""
    if not expected_api_key:
        return True
    if x_api_key and hmac.compare_digest(x_api_key, expected_api_key):
        return True
    if authorization:
        parts = authorization.split(None, 1)
        if (
            len(parts) == 2
            and parts[0].lower() == "bearer"
            and hmac.compare_digest(parts[1].strip(), expected_api_key)
        ):
            return True
    return False


def build_graph_store(
    backend: str,
    *,
    warehouse_dir: Path,
    config: ScaleKGConfig | None = None,
) -> GraphStore:
    """Construct a GraphStore adapter instance by backend identifier."""
    cfg = config or ScaleKGConfig()
    enforcer = GuardrailEnforcer(cfg.guardrails)
    normalized = backend.strip().lower()

    if normalized == "in_memory":
        return InMemoryGraphStore(enforcer=enforcer, num_buckets=cfg.num_buckets)
    if normalized == "iceberg_puppygraph":
        puppy_url = os.environ.get("SCALE_KG_PUPPYGRAPH_URL") or None
        return IcebergPuppyGraphStore(
            warehouse_dir / "graph_store",
            puppygraph_url=puppy_url,
            enforcer=enforcer,
            num_buckets=cfg.num_buckets,
        )
    if normalized == "hugegraph":
        base_url = os.environ.get("SCALE_KG_HUGEGRAPH_URL", "http://127.0.0.1:8080")
        return HugeGraphStore(
            base_url=base_url,
            enforcer=enforcer,
            num_buckets=cfg.num_buckets,
        )
    if normalized == "neo4j":
        uri = os.environ.get("SCALE_KG_NEO4J_URI", "http://127.0.0.1:7474")
        return Neo4jGraphStore(
            uri=uri,
            enforcer=enforcer,
            num_buckets=cfg.num_buckets,
        )
    if normalized == "janusgraph":
        return JanusGraphStore(enforcer=enforcer, num_buckets=cfg.num_buckets)
    if normalized == "nebula":
        return NebulaGraphStore(enforcer=enforcer, num_buckets=cfg.num_buckets)

    raise ValueError(
        f"Unsupported GraphStore backend '{backend}'. Expected one of: "
        "in_memory, iceberg_puppygraph, hugegraph, neo4j, janusgraph, nebula."
    )


class ScaleKGApiState:
    """Shared service state backing both FastAPI REST endpoints and gRPC RPCs."""

    def __init__(
        self,
        *,
        store: GraphStore | Mapping[Domain, GraphStore] | None = None,
        backend: str | None = None,
        shard_map: Mapping[Domain, GraphStore] | None = None,
        warehouse_dir: str | Path | None = None,
        api_key: str | None = None,
        config: ScaleKGConfig | None = None,
        embedder: Callable[[str], list[float]] | None = None,
    ) -> None:
        self.config = config or ScaleKGConfig()
        raw_wh = warehouse_dir or os.environ.get("SCALE_KG_WAREHOUSE_DIR")
        if raw_wh:
            self.warehouse_dir = Path(raw_wh)
            self._temp_dir: tempfile.TemporaryDirectory[str] | None = None
        else:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="scale_kg_api_")
            self.warehouse_dir = Path(self._temp_dir.name)
        self.warehouse_dir.mkdir(parents=True, exist_ok=True)

        resolved_backend = (
            backend or os.environ.get("SCALE_KG_BACKEND") or "in_memory"
        ).strip()
        self.backend_name = resolved_backend

        raw_key = api_key if api_key is not None else os.environ.get("SCALE_KG_API_KEY")
        self.api_key: str | None = (
            raw_key.strip() if raw_key and raw_key.strip() else None
        )

        stores_arg: GraphStore | Mapping[Domain, GraphStore]
        if shard_map is not None:
            stores_arg = dict(shard_map)
            if not backend:
                self.backend_name = "federated_shards"
        elif store is not None:
            stores_arg = store
            if isinstance(store, GraphStore) and not backend:
                self.backend_name = store.name
        else:
            stores_arg = build_graph_store(
                resolved_backend,
                warehouse_dir=self.warehouse_dir,
                config=self.config,
            )

        enforcer = GuardrailEnforcer(self.config.guardrails)
        self.directory = EntityDirectory()
        self.coordinator = QueryCoordinator(
            stores_arg,
            directory=self.directory,
            enforcer=enforcer,
        )
        self.vector_index = InMemoryVectorIndex()
        self.lakehouse = LakehouseContentStore(self.warehouse_dir / "lakehouse")
        self.hybrid_router = HybridGraphRAGRouter(
            vector_index=self.vector_index,
            coordinator=self.coordinator,
            lakehouse=self.lakehouse,
            embedder=embedder,
        )
        from scale_kg.api.grpc_service import ScaleKGGrpcServicer

        self.grpc_servicer = ScaleKGGrpcServicer(self)

    def _unique_shards(self) -> list[GraphStore]:
        seen: set[int] = set()
        unique: list[GraphStore] = []
        for domain in Domain:
            try:
                shard = self.coordinator.shard_for(domain)
            except GraphStoreError:
                continue
            if id(shard) not in seen:
                seen.add(id(shard))
                unique.append(shard)
        return unique

    async def check_health(self) -> HealthResponse:
        """Aggregate health across configured domain shards and subsystems."""
        shard_statuses: dict[str, HealthStatus] = {}
        cached_by_id: dict[int, HealthStatus] = {}

        for domain in Domain:
            try:
                shard = self.coordinator.shard_for(domain)
            except GraphStoreError:
                continue
            shard_id = id(shard)
            if shard_id not in cached_by_id:
                try:
                    cached_by_id[shard_id] = await shard.health_check()
                except (GraphStoreError, TimeoutError, OSError) as exc:
                    cached_by_id[shard_id] = HealthStatus(
                        adapter_name=getattr(shard, "name", "unknown"),
                        healthy=False,
                        status="unavailable",
                        details={"error": str(exc)},
                    )
            shard_statuses[domain.value] = cached_by_id[shard_id]

        if not shard_statuses or any(
            not s.healthy or s.status == "unavailable" for s in shard_statuses.values()
        ):
            overall_healthy = False
            overall_status: Literal["healthy", "degraded", "unavailable"] = (
                "unavailable"
            )
        elif any(s.status == "degraded" for s in shard_statuses.values()):
            overall_healthy = True
            overall_status = "degraded"
        else:
            overall_healthy = True
            overall_status = "healthy"

        return HealthResponse(
            status=overall_status,
            healthy=overall_healthy,
            backend=self.backend_name,
            shards=shard_statuses,
            coordinator_stats=self.coordinator.stats(),
            directory_stats=self.directory.stats(),
            lakehouse_stats=self.lakehouse.stats(),
        )

    async def ingest(self, request: IngestRequest) -> IngestResponse:
        """Validate and ingest nodes, edges, aliases, lakehouse docs, and vectors."""
        nodes_upserted = await self.coordinator.upsert_nodes(
            request.nodes, aliases=request.aliases
        )
        if request.aliases and not request.nodes:
            for target_id, alias_list in request.aliases.items():
                for alias in alias_list:
                    self.directory.register_alias(alias, target_id)

        edges_upserted = await self.coordinator.upsert_edges(request.edges)
        docs_stored = (
            self.lakehouse.put_documents(request.documents) if request.documents else 0
        )

        embeddings_indexed = 0
        for entity_id, embedding in request.embeddings.items():
            domain = self.directory.try_resolve_domain(entity_id)
            self.vector_index.upsert(entity_id, embedding, domain=domain)
            embeddings_indexed += 1

        return IngestResponse(
            nodes_upserted=nodes_upserted,
            edges_upserted=edges_upserted,
            documents_stored=docs_stored,
            embeddings_indexed=embeddings_indexed,
        )

    async def traverse(self, request: TraversalRequest) -> TraversalResult:
        """Execute a guardrail-enforced federated k-hop traversal."""
        return await self.coordinator.traverse(request)

    async def hybrid_retrieve(self, request: HybridRAGRequest) -> HybridRAGResult:
        """Execute the 5-stage Seed-Then-Expand Hybrid GraphRAG pipeline."""
        return await self.hybrid_router.retrieve(request)

    async def get_node(self, node_id: str) -> EntityNode:
        """Fetch an EntityNode by ID or registered alias, or raise EntityResolutionError."""
        node = await self.coordinator.get_node(node_id)
        if node is None:
            raise EntityResolutionError(f"Entity node '{node_id}' not found.")
        return node

    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> NeighborsResponse:
        """Return logical neighbors for ``node_id`` across domain shards."""
        canonical_id = self.directory.canonicalize(node_id)
        domain = self.directory.try_resolve_domain(canonical_id)
        if domain is None:
            node = await self.coordinator.get_node(canonical_id)
            if node is None:
                raise EntityResolutionError(f"Entity node '{node_id}' not found.")
            domain = node.domain

        raw_pairs: list[tuple[RelationshipEdge, EntityNode]] = []
        if direction in ("in", "both"):
            for shard in self._unique_shards():
                raw_pairs.extend(
                    await shard.get_neighbors(
                        canonical_id,
                        edge_types=edge_types,
                        domains=domains,
                        direction=direction,
                        limit=limit,
                    )
                )
        else:
            shard = self.coordinator.shard_for(domain)
            raw_pairs.extend(
                await shard.get_neighbors(
                    canonical_id,
                    edge_types=edge_types,
                    domains=domains,
                    direction=direction,
                    limit=limit,
                )
            )

        deduped: list[NeighborItem] = []
        seen_edges: set[tuple[str, str, str]] = set()
        for edge, neighbor in raw_pairs:
            key = (edge.source_id, edge.target_id, edge.edge_type)
            if key in seen_edges:
                continue
            seen_edges.add(key)

            resolved_node = neighbor
            if neighbor.is_proxy and neighbor.target_domain is not None:
                real = await self.coordinator.shard_for(
                    neighbor.target_domain
                ).get_node(neighbor.id)
                resolved_node = (
                    real
                    if real is not None and not real.is_proxy
                    else neighbor.model_copy(
                        update={"domain": neighbor.target_domain, "is_proxy": False}
                    )
                )
            deduped.append(NeighborItem(edge=edge, node=resolved_node))
            if len(deduped) >= limit:
                break

        if not deduped and await self.coordinator.get_node(canonical_id) is None:
            raise EntityResolutionError(f"Entity node '{node_id}' not found.")

        return NeighborsResponse(node_id=canonical_id, neighbors=deduped)

    async def stats(self) -> StatsResponse:
        """Aggregate graph, coordinator, directory, and lakehouse counters."""
        shards = self._unique_shards()
        if len(shards) == 1:
            graph_stats = await shards[0].count_stats()
        else:
            total_nodes = 0
            total_edges = 0
            total_buckets = 0
            total_proxies = 0
            nodes_by_domain: dict[str, int] = {}
            edges_by_domain: dict[str, int] = {}
            for shard in shards:
                st = await shard.count_stats()
                total_nodes += st.node_count
                total_edges += st.edge_count
                total_buckets += st.bucket_node_count
                total_proxies += st.proxy_node_count
                for d_name, count in st.nodes_by_domain.items():
                    nodes_by_domain[d_name] = nodes_by_domain.get(d_name, 0) + count
                for d_name, count in st.edges_by_domain.items():
                    edges_by_domain[d_name] = edges_by_domain.get(d_name, 0) + count
            graph_stats = GraphStats(
                node_count=total_nodes,
                edge_count=total_edges,
                bucket_node_count=total_buckets,
                proxy_node_count=total_proxies,
                nodes_by_domain=nodes_by_domain,
                edges_by_domain=edges_by_domain,
            )

        return StatsResponse(
            graph=graph_stats,
            coordinator=self.coordinator.stats(),
            directory=self.directory.stats(),
            lakehouse=self.lakehouse.stats(),
        )

    def put_documents(self, request: PutDocumentsRequest) -> PutDocumentsResponse:
        """Persist documents into the LakehouseContentStore."""
        count = self.lakehouse.put_documents(request.documents)
        return PutDocumentsResponse(documents_stored=count)

    def get_document(self, lakehouse_ref: str) -> LakehouseDocument:
        """Retrieve a document from LakehouseContentStore or raise EntityResolutionError."""
        doc = self.lakehouse.get_document(lakehouse_ref)
        if doc is None:
            raise EntityResolutionError(
                f"Lakehouse document '{lakehouse_ref}' not found."
            )
        return doc

    def write_slice(self, graph_slice: HistoricalGraphSlice) -> WriteSliceResponse:
        """Write a historical graph slice into Lakehouse Parquet tables."""
        path = self.lakehouse.write_historical_slice(graph_slice)
        return WriteSliceResponse(
            slice_id=graph_slice.slice_id,
            domain=graph_slice.domain,
            path=path,
        )

    def list_slices(self, *, domain: Domain | None = None) -> SliceListResponse:
        """List stored historical graph slice IDs."""
        return SliceListResponse(slices=self.lakehouse.list_slices(domain=domain))

    def load_slice(self, slice_id: str) -> HistoricalGraphSlice:
        """Load a historical graph slice by ID or raise EntityResolutionError."""
        loaded = self.lakehouse.load_historical_slice(slice_id)
        if loaded is None:
            raise EntityResolutionError(f"Historical slice '{slice_id}' not found.")
        return loaded

    def register_aliases(
        self, request: RegisterAliasesRequest
    ) -> RegisterAliasesResponse:
        """Register explicit entity domains and legacy aliases in EntityDirectory."""
        for entity_id, domain in request.entities.items():
            self.directory.register(entity_id, domain)
        for alias, target_id in request.aliases.items():
            self.directory.register_alias(alias, target_id)
        return RegisterAliasesResponse(
            aliases_registered=len(request.aliases),
            entities_registered=len(request.entities),
            stats=self.directory.stats(),
        )

    def resolve_entity(self, entity_id: str) -> DirectoryResolveResponse:
        """Canonicalize an entity ID/alias and resolve its owning Domain shard."""
        canonical_id = self.directory.canonicalize(entity_id)
        domain = self.directory.resolve_domain(entity_id)
        parsed = parse_canonical_id(canonical_id)
        return DirectoryResolveResponse(
            raw_id=entity_id,
            canonical_id=canonical_id,
            domain=domain,
            parsed=parsed,
            stats=self.directory.stats(),
        )

    async def run_benchmark(self, request: BenchmarkRunRequest) -> BenchmarkRunResponse:
        """Execute the 7-workload Gate 2 BenchmarkSuite and optional ADR report."""
        if not request.adapters:
            raise ValueError("BenchmarkRunRequest.adapters must be non-empty.")

        run_root = self.warehouse_dir / "benchmarks" / uuid.uuid4().hex
        run_root.mkdir(parents=True, exist_ok=True)
        cfg = self.config

        def _make_factory(adapter_name: str) -> Callable[[], GraphStore]:
            def _factory() -> GraphStore:
                return build_graph_store(
                    adapter_name,
                    warehouse_dir=run_root / f"{adapter_name}_{uuid.uuid4().hex[:8]}",
                    config=cfg,
                )

            return _factory

        factories: dict[str, Callable[[], GraphStore]] = {
            adapter_name: _make_factory(adapter_name)
            for adapter_name in dict.fromkeys(request.adapters)
        }
        suite = BenchmarkSuite(
            preset=request.preset,
            cross_domain_edge_share=request.cross_domain_edge_share,
            seed=request.seed,
            budgets=self.config.latency_budgets,
            enforcer=GuardrailEnforcer(self.config.guardrails),
        )
        suite_result = await suite.run(
            factories, run_golden_parity=request.run_golden_parity
        )
        scorecards = rank_engines(suite_result, budgets=self.config.latency_budgets)
        adr_markdown = (
            generate_gate2_adr(suite_result, budgets=self.config.latency_budgets)
            if request.generate_adr
            else None
        )
        return BenchmarkRunResponse(
            suite_result=suite_result,
            scorecards=scorecards,
            adr_markdown=adr_markdown,
        )
