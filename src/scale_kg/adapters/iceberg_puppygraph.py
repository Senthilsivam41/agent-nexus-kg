"""Zero-ETL Apache Iceberg (PyArrow Parquet) + PuppyGraph GraphStore adapter."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from scale_kg.adapters._traversal import (
    _unwrap_edge,
    candidate_bucket_ids,
    finalize_remote_traversal,
    resolve_logical_neighbors,
    run_guarded_bfs,
)
from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    FaultState,
    GraphStats,
    GraphStore,
    GraphStoreError,
    HealthStatus,
    validate_edge_types,
)
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.core.supernode import (
    BUCKET_EDGE_TYPE,
    SupernodeBucketer,
    compute_bucket_id,
)

NODE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("entity_type", pa.string()),
        pa.field("routing_props_json", pa.string()),
        pa.field("lakehouse_ref", pa.string()),
        pa.field("is_proxy", pa.bool_()),
        pa.field("target_domain", pa.string()),
        pa.field("is_bucket", pa.bool_()),
        pa.field("bucket_16", pa.int32()),
    ]
)

EDGE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("source_id", pa.string()),
        pa.field("target_id", pa.string()),
        pa.field("edge_type", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("target_domain", pa.string()),
        pa.field("is_derived", pa.bool_()),
        pa.field("materialized_hot_path", pa.bool_()),
        pa.field("routing_props_json", pa.string()),
        pa.field("source_bucket_16", pa.int32()),
        pa.field("target_bucket_16", pa.int32()),
    ]
)


class PartitionScanStats(BaseModel):
    """Telemetry tracking Iceberg/Parquet partition pruning efficiency."""

    total_partitions: int = 0
    partitions_scanned: int = 0
    partitions_pruned: int = 0
    pruning_ratio: float = 0.0


class IcebergPuppyGraphStore(GraphStore):
    """Zero-ETL adapter writing domain/bucket_16 Parquet tables and PuppyGraph schemas."""

    name = "iceberg_puppygraph"

    def __init__(
        self,
        warehouse_path: str | Path,
        *,
        puppygraph_url: str | None = None,
        query_protocol: Literal["cypher", "gremlin"] = "cypher",
        catalog_name: str = "iceberg_scale_kg",
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
        transport: httpx.AsyncBaseTransport | None = None,
        http_client: httpx.AsyncClient | None = None,
        cache_partitions: bool = False,
    ) -> None:
        self.warehouse_path = Path(warehouse_path)
        self.nodes_dir = self.warehouse_path / "nodes"
        self.edges_dir = self.warehouse_path / "edges"
        self.metadata_dir = self.warehouse_path / "metadata"
        self.schema_path = self.warehouse_path / "puppygraph_schema.json"

        self.puppygraph_url = puppygraph_url.rstrip("/") if puppygraph_url else None
        self.query_protocol = query_protocol
        self.catalog_name = catalog_name
        self._enforcer = enforcer or GuardrailEnforcer()
        self._num_buckets = num_buckets
        self._fault = FaultState()
        self._transport = transport
        self._external_client = http_client
        self._cache_partitions = cache_partitions

        self._last_scan_stats = PartitionScanStats()
        self._cumulative_total_partitions = 0
        self._cumulative_scanned_partitions = 0
        self._snapshot_seq = 0
        self._has_buckets = False

        self._ensure_layout()

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="iceberg_puppygraph",
            tier="tier1",
            storage_backend="Apache Iceberg / PyArrow Parquet (domain + bucket_16 partitioned)",
            query_languages=["cypher", "gremlin", "pyarrow-scan"],
            supports_zero_etl=True,
            supports_cross_shard_edges=True,
            estimated_cost_per_billion_edges_usd_month=420.0,
            operational_complexity="low",
            notes="Zero-ETL lakehouse graph engine avoiding duplicate cluster storage.",
        )

    @property
    def last_scan_stats(self) -> PartitionScanStats:
        return self._last_scan_stats

    @property
    def cumulative_scan_stats(self) -> PartitionScanStats:
        total = self._cumulative_total_partitions
        scanned = self._cumulative_scanned_partitions
        pruned = max(0, total - scanned)
        ratio = round(pruned / total, 4) if total > 0 else 0.0
        return PartitionScanStats(
            total_partitions=total,
            partitions_scanned=scanned,
            partitions_pruned=pruned,
            pruning_ratio=ratio,
        )

    def _ensure_layout(self) -> None:
        self.nodes_dir.mkdir(parents=True, exist_ok=True)
        self.edges_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.write_puppygraph_schema()
        self._write_iceberg_metadata()

    def generate_puppygraph_schema(self) -> dict[str, Any]:
        """Build PuppyGraph JSON mapping over domain/bucket_16 Iceberg tables."""
        return {
            "version": "1.0",
            "catalogs": [
                {
                    "name": self.catalog_name,
                    "type": "iceberg",
                    "metastore": {
                        "type": "hadoop",
                        "warehouse": str(self.warehouse_path),
                    },
                }
            ],
            "graph": {
                "vertices": [
                    {
                        "label": "Entity",
                        "mappedTableSource": {
                            "catalog": self.catalog_name,
                            "schema": "scale_kg",
                            "table": "nodes",
                            "metaFields": [
                                {"field": "domain", "type": "String"},
                                {"field": "bucket_16", "type": "Int"},
                            ],
                        },
                        "idFields": [{"field": "id", "type": "String"}],
                        "attributes": [
                            {"field": "domain", "type": "String"},
                            {"field": "entity_type", "type": "String"},
                            {"field": "lakehouse_ref", "type": "String"},
                            {"field": "is_proxy", "type": "Boolean"},
                            {"field": "target_domain", "type": "String"},
                            {"field": "is_bucket", "type": "Boolean"},
                            {"field": "bucket_16", "type": "Int"},
                            {"field": "routing_props_json", "type": "String"},
                        ],
                    }
                ],
                "edges": [
                    {
                        "label": "RELATIONSHIP",
                        "mappedTableSource": {
                            "catalog": self.catalog_name,
                            "schema": "scale_kg",
                            "table": "edges",
                            "metaFields": [
                                {"field": "domain", "type": "String"},
                                {"field": "source_bucket_16", "type": "Int"},
                            ],
                        },
                        "fromVertex": {
                            "label": "Entity",
                            "idFields": [{"field": "source_id", "type": "String"}],
                        },
                        "toVertex": {
                            "label": "Entity",
                            "idFields": [{"field": "target_id", "type": "String"}],
                        },
                        "attributes": [
                            {"field": "edge_type", "type": "String"},
                            {"field": "domain", "type": "String"},
                            {"field": "target_domain", "type": "String"},
                            {"field": "is_derived", "type": "Boolean"},
                            {"field": "materialized_hot_path", "type": "Boolean"},
                            {"field": "source_bucket_16", "type": "Int"},
                            {"field": "target_bucket_16", "type": "Int"},
                            {"field": "routing_props_json", "type": "String"},
                        ],
                    }
                ],
            },
        }

    def write_puppygraph_schema(self) -> Path:
        """Write puppygraph_schema.json into the warehouse root."""
        schema = self.generate_puppygraph_schema()
        self.schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
        return self.schema_path

    def _write_iceberg_metadata(self) -> None:
        metadata = {
            "format-version": 2,
            "table-uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, str(self.warehouse_path))),
            "location": str(self.warehouse_path),
            "last-sequence-number": self._snapshot_seq,
            "partition-spec": [
                {"name": "domain", "transform": "identity"},
                {"name": "bucket_16", "transform": f"bucket[{self._num_buckets}]"},
            ],
            "tables": {
                "nodes": {
                    "location": str(self.nodes_dir),
                    "fields": [f.name for f in NODE_ARROW_SCHEMA],
                },
                "edges": {
                    "location": str(self.edges_dir),
                    "fields": [f.name for f in EDGE_ARROW_SCHEMA],
                },
            },
        }
        (self.metadata_dir / "v1.metadata.json").write_text(
            json.dumps(metadata, indent=2), encoding="utf-8"
        )

    def _node_partition_file(self, domain: Domain, bucket_16: int) -> Path:
        return (
            self.nodes_dir
            / f"domain={domain.value}"
            / f"bucket_16={bucket_16}"
            / "part-00000.parquet"
        )

    def _edge_partition_file(self, domain: Domain, bucket_16: int) -> Path:
        return (
            self.edges_dir
            / f"domain={domain.value}"
            / f"bucket_16={bucket_16}"
            / "part-00000.parquet"
        )

    @staticmethod
    def _list_partition_files(base_dir: Path) -> list[Path]:
        if not base_dir.exists():
            return []
        return sorted(base_dir.glob("domain=*/bucket_16=*/*.parquet"))

    @staticmethod
    def _node_to_row(node: EntityNode, bucket_16: int) -> dict[str, Any]:
        return {
            "id": node.id,
            "domain": node.domain.value,
            "entity_type": node.entity_type,
            "routing_props_json": json.dumps(node.routing_props, sort_keys=True),
            "lakehouse_ref": node.lakehouse_ref,
            "is_proxy": node.is_proxy,
            "target_domain": node.target_domain.value if node.target_domain else None,
            "is_bucket": node.is_bucket,
            "bucket_16": bucket_16,
        }

    @staticmethod
    def _row_to_node(row: dict[str, Any]) -> EntityNode:
        return EntityNode(
            id=row["id"],
            domain=Domain(row["domain"]),
            entity_type=row["entity_type"],
            routing_props=json.loads(row["routing_props_json"] or "{}"),
            lakehouse_ref=row["lakehouse_ref"],
            is_proxy=bool(row["is_proxy"]),
            target_domain=Domain(row["target_domain"])
            if row["target_domain"]
            else None,
            is_bucket=bool(row["is_bucket"]),
        )

    @staticmethod
    def _edge_to_row(
        edge: RelationshipEdge, src_bucket: int, dst_bucket: int
    ) -> dict[str, Any]:
        return {
            "source_id": edge.source_id,
            "target_id": edge.target_id,
            "edge_type": edge.edge_type,
            "domain": edge.domain.value,
            "target_domain": edge.target_domain.value if edge.target_domain else None,
            "is_derived": edge.is_derived,
            "materialized_hot_path": edge.materialized_hot_path,
            "routing_props_json": json.dumps(edge.routing_props, sort_keys=True),
            "source_bucket_16": src_bucket,
            "target_bucket_16": dst_bucket,
        }

    @staticmethod
    def _row_to_edge(row: dict[str, Any]) -> RelationshipEdge:
        return RelationshipEdge(
            source_id=row["source_id"],
            target_id=row["target_id"],
            edge_type=row["edge_type"],
            domain=Domain(row["domain"]),
            target_domain=Domain(row["target_domain"])
            if row["target_domain"]
            else None,
            is_derived=bool(row["is_derived"]),
            materialized_hot_path=bool(row["materialized_hot_path"]),
            routing_props=json.loads(row["routing_props_json"] or "{}"),
        )

    @staticmethod
    def _write_rows(path: Path, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        columns = {field.name: [row[field.name] for row in rows] for field in schema}
        table = pa.Table.from_pydict(columns, schema=schema)
        pq.write_table(table, path)

    def _record_scan(self, total_partitions: int, scanned_partitions: int) -> None:
        pruned = max(0, total_partitions - scanned_partitions)
        ratio = round(pruned / total_partitions, 4) if total_partitions > 0 else 0.0
        self._last_scan_stats = PartitionScanStats(
            total_partitions=total_partitions,
            partitions_scanned=scanned_partitions,
            partitions_pruned=pruned,
            pruning_ratio=ratio,
        )
        self._cumulative_total_partitions += total_partitions
        self._cumulative_scanned_partitions += scanned_partitions

    async def upsert_nodes(self, nodes: list[EntityNode]) -> int:
        for node in nodes:
            self._enforcer.validate_node_write(node)
        grouped: dict[tuple[Domain, int], dict[str, EntityNode]] = {}
        for node in nodes:
            self._fault.check(node.domain)
            if node.is_bucket or SupernodeBucketer.is_bucket_node_id(node.id):
                self._has_buckets = True
            bucket = compute_bucket_id(node.id, self._num_buckets)
            grouped.setdefault((node.domain, bucket), {})[node.id] = node

        for (domain, bucket), batch_by_id in grouped.items():
            part_file = self._node_partition_file(domain, bucket)
            merged: dict[str, dict[str, Any]] = {}
            if part_file.exists():
                existing = pq.read_table(part_file).to_pylist()
                for row in existing:
                    merged[row["id"]] = row
            for node_id, node in batch_by_id.items():
                merged[node_id] = self._node_to_row(node, bucket)
            self._write_rows(part_file, list(merged.values()), NODE_ARROW_SCHEMA)

        if nodes:
            self._snapshot_seq += 1
            self._write_iceberg_metadata()
        return len(nodes)

    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        for edge in edges:
            self._enforcer.validate_edge_write(edge)
        grouped: dict[
            tuple[Domain, int], dict[tuple[str, str, str], RelationshipEdge]
        ] = {}
        for edge in edges:
            self._fault.check(edge.domain)
            if (
                edge.edge_type == BUCKET_EDGE_TYPE
                or SupernodeBucketer.is_bucket_node_id(edge.source_id)
                or SupernodeBucketer.is_bucket_node_id(edge.target_id)
            ):
                self._has_buckets = True
            src_bucket = compute_bucket_id(edge.source_id, self._num_buckets)
            key = (edge.source_id, edge.target_id, edge.edge_type)
            grouped.setdefault((edge.domain, src_bucket), {})[key] = edge

        for (domain, src_bucket), batch_by_key in grouped.items():
            part_file = self._edge_partition_file(domain, src_bucket)
            merged: dict[tuple[str, str, str], dict[str, Any]] = {}
            if part_file.exists():
                existing = pq.read_table(part_file).to_pylist()
                for row in existing:
                    k = (row["source_id"], row["target_id"], row["edge_type"])
                    merged[k] = row
            for key, edge in batch_by_key.items():
                dst_bucket = compute_bucket_id(edge.target_id, self._num_buckets)
                merged[key] = self._edge_to_row(edge, src_bucket, dst_bucket)
            self._write_rows(part_file, list(merged.values()), EDGE_ARROW_SCHEMA)

        if edges:
            self._snapshot_seq += 1
            self._write_iceberg_metadata()
        return len(edges)

    def _read_node_local(
        self,
        node_id: str,
        scanned_tracker: set[Path] | None = None,
        *,
        all_files: list[Path] | None = None,
        partition_cache: dict[Path, dict[str, EntityNode]] | None = None,
    ) -> EntityNode | None:
        bucket = compute_bucket_id(node_id, self._num_buckets)
        files = (
            all_files
            if all_files is not None
            else self._list_partition_files(self.nodes_dir)
        )
        candidate_files = [p for p in files if p.parent.name == f"bucket_16={bucket}"]
        if scanned_tracker is not None:
            scanned_tracker.update(candidate_files)
        else:
            self._record_scan(len(files), len(candidate_files))

        for part_file in candidate_files:
            if partition_cache is not None:
                if part_file not in partition_cache:
                    rows = pq.read_table(part_file).to_pylist()
                    partition_cache[part_file] = {
                        r["id"]: self._row_to_node(r) for r in rows
                    }
                found = partition_cache[part_file].get(node_id)
                if found is not None:
                    return found
            else:
                table = pq.read_table(part_file, filters=[("id", "=", node_id)])
                if table.num_rows > 0:
                    return self._row_to_node(table.to_pylist()[0])
        return None

    def _read_out_edges_local(
        self,
        node_id: str,
        scanned_tracker: set[Path] | None = None,
        *,
        all_files: list[Path] | None = None,
        partition_cache: dict[Path, dict[str, list[RelationshipEdge]]] | None = None,
    ) -> list[RelationshipEdge]:
        bucket = compute_bucket_id(node_id, self._num_buckets)
        files = (
            all_files
            if all_files is not None
            else self._list_partition_files(self.edges_dir)
        )
        candidate_files = [p for p in files if p.parent.name == f"bucket_16={bucket}"]
        if scanned_tracker is not None:
            scanned_tracker.update(candidate_files)
        else:
            self._record_scan(len(files), len(candidate_files))

        edges: list[RelationshipEdge] = []
        for part_file in candidate_files:
            if partition_cache is not None:
                if part_file not in partition_cache:
                    by_src: dict[str, list[RelationshipEdge]] = {}
                    for row in pq.read_table(part_file).to_pylist():
                        edge = self._row_to_edge(row)
                        by_src.setdefault(edge.source_id, []).append(edge)
                    partition_cache[part_file] = by_src
                edges.extend(partition_cache[part_file].get(node_id, ()))
            else:
                table = pq.read_table(part_file, filters=[("source_id", "=", node_id)])
                for row in table.to_pylist():
                    edges.append(self._row_to_edge(row))
        return edges

    def _read_in_edges_local(
        self,
        candidate_ids: Sequence[str],
        scanned_tracker: set[Path] | None = None,
        *,
        all_files: list[Path] | None = None,
        partition_cache: dict[Path, dict[str, list[RelationshipEdge]]] | None = None,
    ) -> list[RelationshipEdge]:
        target_ids = list(dict.fromkeys(candidate_ids))
        if not target_ids:
            return []

        files = (
            all_files
            if all_files is not None
            else self._list_partition_files(self.edges_dir)
        )
        if scanned_tracker is not None:
            scanned_tracker.update(files)
        else:
            self._record_scan(len(files), len(files))

        edges: list[RelationshipEdge] = []
        for part_file in files:
            if partition_cache is not None:
                if part_file not in partition_cache:
                    by_dst: dict[str, list[RelationshipEdge]] = {}
                    for row in pq.read_table(part_file).to_pylist():
                        edge = self._row_to_edge(row)
                        by_dst.setdefault(edge.target_id, []).append(edge)
                    partition_cache[part_file] = by_dst
                by_dst = partition_cache[part_file]
                for tid in target_ids:
                    edges.extend(by_dst.get(tid, ()))
            else:
                table = pq.read_table(
                    part_file, filters=[("target_id", "in", target_ids)]
                )
                for row in table.to_pylist():
                    edges.append(self._row_to_edge(row))
        return edges

    def build_cypher_query(
        self, request: TraversalRequest, *, bucketed: bool | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded OpenCypher query for PuppyGraph."""
        use_buckets = self._has_buckets if bucketed is None else bucketed
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        physical_depth = depth * 3 if use_buckets else depth
        limit = min(request.max_results, self._enforcer.config.max_results)
        edge_types = validate_edge_types(request.edge_types)
        if edge_types and use_buckets and BUCKET_EDGE_TYPE not in edge_types:
            edge_types = [*edge_types, BUCKET_EDGE_TYPE]
        rel_filter = ":" + "|".join(edge_types) if edge_types else ""
        domain_clause = (
            " AND ALL(n IN nodes(p) WHERE n.domain IN $domains)"
            if request.domains
            else ""
        )
        derived_clause = (
            ""
            if request.include_derived
            else " AND ALL(rel IN relationships(p) WHERE coalesce(rel.is_derived, false) = false)"
        )
        cypher = (
            f"MATCH p = (s:Entity)-[r{rel_filter}*1..{physical_depth}]->(t:Entity) "
            f"WHERE s.id IN $seed_ids{domain_clause}{derived_clause} "
            f"RETURN p LIMIT {limit}"
        )
        params: dict[str, Any] = {
            "seed_ids": list(request.seed_ids),
            "domains": [d.value for d in request.domains] if request.domains else [],
            "max_depth": depth,
            "limit": limit,
        }
        return cypher, params

    def build_gremlin_query(
        self, request: TraversalRequest, *, bucketed: bool | None = None
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded Gremlin query for PuppyGraph."""
        use_buckets = self._has_buckets if bucketed is None else bucketed
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        physical_depth = depth * 3 if use_buckets else depth
        limit = min(request.max_results, self._enforcer.config.max_results)
        edge_types = validate_edge_types(request.edge_types)
        if edge_types and use_buckets and BUCKET_EDGE_TYPE not in edge_types:
            edge_types = [*edge_types, BUCKET_EDGE_TYPE]
        edge_arg = ", ".join(f"'{et}'" for et in edge_types) if edge_types else ""
        gremlin = (
            "g.V().has('id', within(seed_ids))"
            f".repeat(outE({edge_arg}).inV()).times({physical_depth}).emit()"
            f".path().limit({limit})"
        )
        bindings: dict[str, Any] = {
            "seed_ids": list(request.seed_ids),
            "domains": [d.value for d in request.domains] if request.domains else [],
            "limit": limit,
        }
        return gremlin, bindings

    async def _post_puppygraph(
        self, path: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if not self.puppygraph_url:
            raise GraphStoreError("puppygraph_url is not configured.")
        url = f"{self.puppygraph_url}{path}"
        headers = {"x-puppygraph-warehouse": str(self.warehouse_path)}
        if self._external_client is not None:
            resp = await self._external_client.post(url, json=payload, headers=headers)
        else:
            async with httpx.AsyncClient(
                transport=self._transport, timeout=5.0
            ) as client:
                resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code >= 400:
            raise GraphStoreError(
                f"PuppyGraph endpoint {path} failed with HTTP {resp.status_code}: {resp.text}"
            )
        return resp.json()

    async def get_node(self, node_id: str) -> EntityNode | None:
        self._fault.check(None)
        if self.puppygraph_url is not None:
            data = await self._post_puppygraph(
                "/cypher",
                {
                    "query": "MATCH (n:Entity {id: $id}) RETURN n LIMIT 1",
                    "parameters": {"id": node_id},
                },
            )
            nodes = data.get("nodes", [])
            return EntityNode.model_validate(nodes[0]) if nodes else None
        return self._read_node_local(node_id)

    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> list[tuple[RelationshipEdge, EntityNode]]:
        self._fault.check(None)
        if self.puppygraph_url is not None:
            candidate_ids = candidate_bucket_ids(node_id, self._num_buckets)
            data = await self._post_puppygraph(
                "/cypher",
                {
                    "query": (
                        "MATCH (s:Entity)-[r]-(t:Entity) "
                        "WHERE s.id IN $ids "
                        "RETURN r, t LIMIT $limit"
                    ),
                    "parameters": {
                        "id": node_id,
                        "ids": candidate_ids,
                        "edge_types": edge_types or [],
                        "domains": [d.value for d in domains] if domains else [],
                        "direction": direction,
                        "limit": limit,
                    },
                },
            )
            allowed_types = set(edge_types) if edge_types else None
            allowed_domains = set(domains) if domains else None
            pairs: list[tuple[RelationshipEdge, EntityNode]] = []
            seen_keys: set[tuple[str, str, str]] = set()
            for item in data.get("neighbors", []):
                edge = RelationshipEdge.model_validate(item["edge"])
                if edge.edge_type == BUCKET_EDGE_TYPE:
                    continue
                if allowed_types and edge.edge_type not in allowed_types:
                    continue
                edge = _unwrap_edge(edge)
                neighbor_id = (
                    edge.target_id if edge.source_id == node_id else edge.source_id
                )
                if neighbor_id == node_id:
                    continue
                key = (edge.source_id, edge.target_id, edge.edge_type)
                if key in seen_keys:
                    continue
                node = EntityNode.model_validate(item["node"])
                if (
                    node.is_bucket
                    or SupernodeBucketer.is_bucket_node_id(node.id)
                    or node.id != neighbor_id
                ):
                    resolved = await self.get_node(neighbor_id)
                    if resolved is None:
                        continue
                    node = resolved
                if allowed_domains and node.domain not in allowed_domains:
                    continue
                seen_keys.add(key)
                pairs.append((edge, node))
                if len(pairs) >= limit:
                    break
            return pairs

        scanned_edge_files: set[Path] = set()
        all_node_files = self._list_partition_files(self.nodes_dir)
        all_edge_files = self._list_partition_files(self.edges_dir)
        node_cache: dict[Path, dict[str, EntityNode]] | None = (
            {} if self._cache_partitions else None
        )
        out_cache: dict[Path, dict[str, list[RelationshipEdge]]] | None = (
            {} if self._cache_partitions else None
        )
        in_cache: dict[Path, dict[str, list[RelationshipEdge]]] | None = (
            {} if self._cache_partitions else None
        )

        async def _fetch_node(nid: str) -> EntityNode | None:
            return self._read_node_local(
                nid,
                scanned_tracker=set(),
                all_files=all_node_files,
                partition_cache=node_cache,
            )

        async def _fetch_out(nid: str) -> list[RelationshipEdge]:
            return self._read_out_edges_local(
                nid,
                scanned_tracker=scanned_edge_files,
                all_files=all_edge_files,
                partition_cache=out_cache,
            )

        async def _fetch_in(cids: Sequence[str]) -> list[RelationshipEdge]:
            return self._read_in_edges_local(
                cids,
                scanned_tracker=scanned_edge_files,
                all_files=all_edge_files,
                partition_cache=in_cache,
            )

        neighbors = await resolve_logical_neighbors(
            node_id,
            fetch_node=_fetch_node,
            fetch_out_edges=_fetch_out,
            fetch_in_edges=_fetch_in,
            edge_types=edge_types,
            domains=domains,
            direction=direction,
            limit=limit,
            num_buckets=self._num_buckets,
        )
        self._record_scan(len(all_edge_files), len(scanned_edge_files))
        return neighbors

    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        for d in request.domains or []:
            self._fault.check(d)
        if not request.domains:
            self._fault.check(None)

        if self.puppygraph_url is not None:
            return await self._traverse_remote_puppygraph(request)

        scanned_node_files: set[Path] = set()
        scanned_edge_files: set[Path] = set()
        all_node_files = self._list_partition_files(self.nodes_dir)
        all_edge_files = self._list_partition_files(self.edges_dir)
        node_cache: dict[Path, dict[str, EntityNode]] | None = (
            {} if self._cache_partitions else None
        )
        out_cache: dict[Path, dict[str, list[RelationshipEdge]]] | None = (
            {} if self._cache_partitions else None
        )

        async def _fetch_node(nid: str) -> EntityNode | None:
            return self._read_node_local(
                nid,
                scanned_tracker=scanned_node_files,
                all_files=all_node_files,
                partition_cache=node_cache,
            )

        async def _fetch_out(nid: str) -> list[RelationshipEdge]:
            return self._read_out_edges_local(
                nid,
                scanned_tracker=scanned_edge_files,
                all_files=all_edge_files,
                partition_cache=out_cache,
            )

        result = await run_guarded_bfs(
            request,
            enforcer=self._enforcer,
            fetch_node=_fetch_node,
            fetch_out_edges=_fetch_out,
        )
        self._record_scan(
            len(all_node_files) + len(all_edge_files),
            len(scanned_node_files) + len(scanned_edge_files),
        )
        return result

    async def _traverse_remote_puppygraph(
        self, request: TraversalRequest
    ) -> TraversalResult:
        tracker = self._enforcer.create_runtime_tracker(request)
        effective_max_depth = min(request.max_depth, self._enforcer.config.max_depth)
        started = time.monotonic()
        if self.query_protocol == "gremlin":
            query, params = self.build_gremlin_query(request, bucketed=True)
            data = await self._post_puppygraph(
                "/gremlin", {"gremlin": query, "bindings": params}
            )
        else:
            query, params = self.build_cypher_query(request, bucketed=True)
            data = await self._post_puppygraph(
                "/cypher", {"query": query, "parameters": params}
            )

        raw_nodes = [
            EntityNode.model_validate(n)
            for n in data.get("nodes", data.get("vertices", []))
        ]
        raw_edges = [RelationshipEdge.model_validate(e) for e in data.get("edges", [])]
        raw_paths: list[list[str]] = data.get("paths", [])
        elapsed_ms = round((time.monotonic() - started) * 1000.0, 3)

        return finalize_remote_traversal(
            request,
            tracker=tracker,
            effective_max_depth=effective_max_depth,
            raw_nodes=raw_nodes,
            raw_edges=raw_edges,
            raw_paths=raw_paths,
            nodes_expanded_override=(
                int(data["nodes_expanded"]) if "nodes_expanded" in data else None
            ),
            depth_reached_override=(
                int(data["depth_reached"]) if "depth_reached" in data else None
            ),
            bucket_hops_override=(
                int(data["bucket_hops_unwrapped"])
                if "bucket_hops_unwrapped" in data
                else None
            ),
            execution_time_ms=elapsed_ms,
        )

    async def set_fault_mode(
        self,
        mode: str | FaultMode,
        *,
        target_domain: Domain | None = None,
        latency_ms: float = 0.0,
        recover_after_calls: int | None = None,
    ) -> None:
        self._fault.configure(
            mode,
            target_domain=target_domain,
            latency_ms=latency_ms,
            recover_after_calls=recover_after_calls,
        )

    async def health_check(self) -> HealthStatus:
        healthy, status = self._fault.health_state()
        mode_label = "remote_puppygraph" if self.puppygraph_url else "local_pyarrow"
        if self.puppygraph_url and healthy:
            try:
                if self._external_client is not None:
                    resp = await self._external_client.get(
                        f"{self.puppygraph_url}/health"
                    )
                else:
                    async with httpx.AsyncClient(
                        transport=self._transport, timeout=2.0
                    ) as client:
                        resp = await client.get(f"{self.puppygraph_url}/health")
                if resp.status_code >= 400:
                    healthy = False
                    status = "unavailable"
            except httpx.HTTPError:
                healthy = False
                status = "unavailable"

        return HealthStatus(
            adapter_name=self.name,
            healthy=healthy,
            status=status,
            fault_mode=self._fault.mode.value,
            details={
                "execution_mode": mode_label,
                "schema_exists": self.schema_path.exists(),
                "last_pruning_ratio": self._last_scan_stats.pruning_ratio,
                "cumulative_pruning_ratio": self.cumulative_scan_stats.pruning_ratio,
            },
        )

    async def count_stats(self) -> GraphStats:
        nodes_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        edges_by_domain: dict[str, int] = {d.value: 0 for d in Domain}
        bucket_nodes = 0
        proxy_nodes = 0

        node_files = self._list_partition_files(self.nodes_dir)
        for pf in node_files:
            table = pq.read_table(pf, columns=["domain", "is_bucket", "is_proxy"])
            for row in table.to_pylist():
                nodes_by_domain[row["domain"]] = (
                    nodes_by_domain.get(row["domain"], 0) + 1
                )
                if row["is_bucket"]:
                    bucket_nodes += 1
                if row["is_proxy"]:
                    proxy_nodes += 1

        edge_files = self._list_partition_files(self.edges_dir)
        for pf in edge_files:
            table = pq.read_table(pf, columns=["domain"])
            for row in table.to_pylist():
                edges_by_domain[row["domain"]] = (
                    edges_by_domain.get(row["domain"], 0) + 1
                )

        cum = self.cumulative_scan_stats
        return GraphStats(
            node_count=sum(nodes_by_domain.values()),
            edge_count=sum(edges_by_domain.values()),
            bucket_node_count=bucket_nodes,
            proxy_node_count=proxy_nodes,
            nodes_by_domain=nodes_by_domain,
            edges_by_domain=edges_by_domain,
            extra={
                "node_partitions": len(node_files),
                "edge_partitions": len(edge_files),
                "pruning_ratio": cum.pruning_ratio,
            },
        )

    async def clear(self) -> None:
        if self.nodes_dir.exists():
            shutil.rmtree(self.nodes_dir)
        if self.edges_dir.exists():
            shutil.rmtree(self.edges_dir)
        self._last_scan_stats = PartitionScanStats()
        self._cumulative_total_partitions = 0
        self._cumulative_scanned_partitions = 0
        self._snapshot_seq = 0
        self._has_buckets = False
        self._fault.configure(FaultMode.NONE)
        self._ensure_layout()
