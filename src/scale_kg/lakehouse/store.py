"""Authoritative Apache Parquet/Iceberg Lakehouse store for payloads and historical slices."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from scale_kg.core.models import Domain, EntityNode, RelationshipEdge, RoutingPropValue

DOCUMENT_ARROW_SCHEMA = pa.schema(
    [
        pa.field("lakehouse_ref", pa.string()),
        pa.field("entity_id", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("title", pa.string()),
        pa.field("content", pa.string()),
        pa.field("source_uri", pa.string()),
        pa.field("metadata_json", pa.string()),
        pa.field("updated_at", pa.string()),
    ]
)

SLICE_NODE_SCHEMA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("entity_type", pa.string()),
        pa.field("routing_props_json", pa.string()),
        pa.field("lakehouse_ref", pa.string()),
        pa.field("is_proxy", pa.bool_()),
        pa.field("target_domain", pa.string()),
        pa.field("is_bucket", pa.bool_()),
    ]
)

SLICE_EDGE_SCHEMA = pa.schema(
    [
        pa.field("source_id", pa.string()),
        pa.field("target_id", pa.string()),
        pa.field("edge_type", pa.string()),
        pa.field("domain", pa.string()),
        pa.field("target_domain", pa.string()),
        pa.field("is_derived", pa.bool_()),
        pa.field("materialized_hot_path", pa.bool_()),
        pa.field("routing_props_json", pa.string()),
    ]
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class LakehouseDocument(BaseModel):
    """Authoritative out-of-graph content record referenced by ``lakehouse_ref``."""

    lakehouse_ref: str = Field(min_length=1)
    entity_id: str = Field(min_length=1)
    domain: Domain
    title: str = ""
    content: str
    source_uri: str = ""
    metadata: dict[str, RoutingPropValue] = Field(default_factory=dict)
    updated_at: str = Field(default_factory=_utc_now_iso)


class HistoricalGraphSlice(BaseModel):
    """Point-in-time historical graph slice stored in the Lakehouse tier."""

    slice_id: str = Field(min_length=1)
    as_of: str = Field(default_factory=_utc_now_iso)
    domain: Domain
    nodes: list[EntityNode] = Field(default_factory=list)
    edges: list[RelationshipEdge] = Field(default_factory=list)
    metadata: dict[str, RoutingPropValue] = Field(default_factory=dict)


class LakehouseStats(BaseModel):
    """Telemetry tracking stored payloads, slices, and selective hydration."""

    document_count: int = 0
    slice_count: int = 0
    hydration_calls: int = 0
    hydrated_doc_count: int = 0


class LakehouseContentStore:
    """Parquet-backed authoritative store for document payloads and historical graph slices."""

    def __init__(self, warehouse_path: str | Path) -> None:
        self.warehouse_path = Path(warehouse_path)
        self.documents_dir = self.warehouse_path / "documents"
        self.slices_dir = self.warehouse_path / "slices"
        self._ref_domain_index: dict[str, Domain] = {}
        self._hydration_calls = 0
        self._hydrated_doc_count = 0
        self._ensure_layout()
        self._rebuild_ref_index()

    def _ensure_layout(self) -> None:
        self.documents_dir.mkdir(parents=True, exist_ok=True)
        self.slices_dir.mkdir(parents=True, exist_ok=True)

    def _domain_doc_path(self, domain: Domain) -> Path:
        return self.documents_dir / f"domain={domain.value}" / "documents.parquet"

    def _rebuild_ref_index(self) -> None:
        self._ref_domain_index.clear()
        for domain in Domain:
            path = self._domain_doc_path(domain)
            if not path.exists():
                continue
            table = pq.read_table(path, columns=["lakehouse_ref"])
            for ref in table.column("lakehouse_ref").to_pylist():
                if ref:
                    self._ref_domain_index[str(ref)] = domain

    @staticmethod
    def _doc_to_row(doc: LakehouseDocument) -> dict[str, str]:
        return {
            "lakehouse_ref": doc.lakehouse_ref,
            "entity_id": doc.entity_id,
            "domain": doc.domain.value,
            "title": doc.title,
            "content": doc.content,
            "source_uri": doc.source_uri or doc.lakehouse_ref,
            "metadata_json": json.dumps(doc.metadata, sort_keys=True),
            "updated_at": doc.updated_at,
        }

    @staticmethod
    def _row_to_doc(row: dict[str, object]) -> LakehouseDocument:
        raw_meta = row.get("metadata_json")
        metadata = (
            json.loads(raw_meta) if isinstance(raw_meta, str) and raw_meta else {}
        )
        return LakehouseDocument(
            lakehouse_ref=str(row["lakehouse_ref"]),
            entity_id=str(row["entity_id"]),
            domain=Domain(str(row["domain"])),
            title=str(row.get("title") or ""),
            content=str(row.get("content") or ""),
            source_uri=str(row.get("source_uri") or ""),
            metadata=metadata,
            updated_at=str(row.get("updated_at") or _utc_now_iso()),
        )

    def _read_domain_docs(self, domain: Domain) -> dict[str, LakehouseDocument]:
        path = self._domain_doc_path(domain)
        if not path.exists():
            return {}
        table = pq.read_table(path)
        docs: dict[str, LakehouseDocument] = {}
        for row in table.to_pylist():
            doc = self._row_to_doc(row)
            docs[doc.lakehouse_ref] = doc
        return docs

    def _write_domain_docs(
        self, domain: Domain, docs: dict[str, LakehouseDocument]
    ) -> None:
        path = self._domain_doc_path(domain)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [self._doc_to_row(docs[ref]) for ref in sorted(docs)]
        table = pa.Table.from_pylist(rows, schema=DOCUMENT_ARROW_SCHEMA)
        pq.write_table(table, path)

    def put_documents(self, docs: list[LakehouseDocument]) -> int:
        """Idempotently persist authoritative documents partitioned by domain."""
        if not docs:
            return 0
        by_domain: dict[Domain, list[LakehouseDocument]] = {}
        for doc in docs:
            by_domain.setdefault(doc.domain, []).append(doc)

        for domain, batch in by_domain.items():
            existing = self._read_domain_docs(domain)
            for doc in batch:
                existing[doc.lakehouse_ref] = doc
                self._ref_domain_index[doc.lakehouse_ref] = domain
            self._write_domain_docs(domain, existing)
        return len(docs)

    def get_document(self, lakehouse_ref: str) -> LakehouseDocument | None:
        """Fetch a single document payload by ``lakehouse_ref``."""
        domain = self._ref_domain_index.get(lakehouse_ref)
        domains_to_scan = [domain] if domain is not None else list(Domain)
        for d in domains_to_scan:
            docs = self._read_domain_docs(d)
            if lakehouse_ref in docs:
                self._ref_domain_index[lakehouse_ref] = d
                return docs[lakehouse_ref]
        return None

    def hydrate_batch(self, lakehouse_refs: list[str]) -> dict[str, LakehouseDocument]:
        """Selectively hydrate payloads for the requested ``lakehouse_refs``."""
        self._hydration_calls += 1
        unique_refs = [ref for ref in dict.fromkeys(lakehouse_refs) if ref]
        if not unique_refs:
            return {}

        target_domains: set[Domain] = set()
        has_unknown = False
        for ref in unique_refs:
            domain = self._ref_domain_index.get(ref)
            if domain is not None:
                target_domains.add(domain)
            else:
                has_unknown = True

        scan_domains = (
            list(Domain)
            if has_unknown
            else sorted(target_domains, key=lambda d: d.value)
        )
        loaded: dict[str, LakehouseDocument] = {}
        ref_set = set(unique_refs)
        for domain in scan_domains:
            path = self._domain_doc_path(domain)
            if not path.exists():
                continue
            table = pq.read_table(path)
            for row in table.to_pylist():
                ref = str(row["lakehouse_ref"])
                self._ref_domain_index[ref] = domain
                if ref in ref_set:
                    loaded[ref] = self._row_to_doc(row)

        ordered = {ref: loaded[ref] for ref in unique_refs if ref in loaded}
        self._hydrated_doc_count += len(ordered)
        return ordered

    def hydrate_nodes(self, nodes: list[EntityNode]) -> dict[str, LakehouseDocument]:
        """Hydrate payloads only for nodes carrying a non-empty ``lakehouse_ref``."""
        refs = [node.lakehouse_ref for node in nodes if node.lakehouse_ref]
        return self.hydrate_batch(refs)

    def _slice_dir(self, domain: Domain, slice_id: str) -> Path:
        safe_id = slice_id.replace("/", "_")
        return self.slices_dir / f"domain={domain.value}" / f"slice={safe_id}"

    def write_historical_slice(self, graph_slice: HistoricalGraphSlice) -> str:
        """Persist a historical graph slice as Parquet tables + metadata JSON."""
        target_dir = self._slice_dir(graph_slice.domain, graph_slice.slice_id)
        target_dir.mkdir(parents=True, exist_ok=True)

        node_rows = [
            {
                "id": n.id,
                "domain": n.domain.value,
                "entity_type": n.entity_type,
                "routing_props_json": json.dumps(n.routing_props, sort_keys=True),
                "lakehouse_ref": n.lakehouse_ref,
                "is_proxy": n.is_proxy,
                "target_domain": n.target_domain.value if n.target_domain else None,
                "is_bucket": n.is_bucket,
            }
            for n in graph_slice.nodes
        ]
        edge_rows = [
            {
                "source_id": e.source_id,
                "target_id": e.target_id,
                "edge_type": e.edge_type,
                "domain": e.domain.value,
                "target_domain": e.target_domain.value if e.target_domain else None,
                "is_derived": e.is_derived,
                "materialized_hot_path": e.materialized_hot_path,
                "routing_props_json": json.dumps(e.routing_props, sort_keys=True),
            }
            for e in graph_slice.edges
        ]
        pq.write_table(
            pa.Table.from_pylist(node_rows, schema=SLICE_NODE_SCHEMA),
            target_dir / "nodes.parquet",
        )
        pq.write_table(
            pa.Table.from_pylist(edge_rows, schema=SLICE_EDGE_SCHEMA),
            target_dir / "edges.parquet",
        )
        meta_payload = {
            "slice_id": graph_slice.slice_id,
            "as_of": graph_slice.as_of,
            "domain": graph_slice.domain.value,
            "metadata": graph_slice.metadata,
        }
        (target_dir / "slice_meta.json").write_text(
            json.dumps(meta_payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        return str(target_dir)

    def load_historical_slice(self, slice_id: str) -> HistoricalGraphSlice | None:
        """Reload a previously persisted historical graph slice by ``slice_id``."""
        for domain in Domain:
            target_dir = self._slice_dir(domain, slice_id)
            meta_path = target_dir / "slice_meta.json"
            if not meta_path.exists():
                continue
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            nodes_table = pq.read_table(target_dir / "nodes.parquet")
            edges_table = pq.read_table(target_dir / "edges.parquet")
            nodes = [
                EntityNode(
                    id=str(row["id"]),
                    domain=Domain(str(row["domain"])),
                    entity_type=str(row["entity_type"]),
                    routing_props=json.loads(str(row["routing_props_json"] or "{}")),
                    lakehouse_ref=row.get("lakehouse_ref"),
                    is_proxy=bool(row.get("is_proxy", False)),
                    target_domain=Domain(str(row["target_domain"]))
                    if row.get("target_domain")
                    else None,
                    is_bucket=bool(row.get("is_bucket", False)),
                )
                for row in nodes_table.to_pylist()
            ]
            edges = [
                RelationshipEdge(
                    source_id=str(row["source_id"]),
                    target_id=str(row["target_id"]),
                    edge_type=str(row["edge_type"]),
                    domain=Domain(str(row["domain"])),
                    target_domain=Domain(str(row["target_domain"]))
                    if row.get("target_domain")
                    else None,
                    is_derived=bool(row.get("is_derived", False)),
                    materialized_hot_path=bool(row.get("materialized_hot_path", False)),
                    routing_props=json.loads(str(row["routing_props_json"] or "{}")),
                )
                for row in edges_table.to_pylist()
            ]
            return HistoricalGraphSlice(
                slice_id=str(meta["slice_id"]),
                as_of=str(meta["as_of"]),
                domain=Domain(str(meta["domain"])),
                nodes=nodes,
                edges=edges,
                metadata=meta.get("metadata", {}),
            )
        return None

    def list_slices(self, domain: Domain | None = None) -> list[str]:
        """List available historical slice IDs, optionally filtered by Domain."""
        domains = [domain] if domain is not None else list(Domain)
        slice_ids: list[str] = []
        for d in domains:
            domain_dir = self.slices_dir / f"domain={d.value}"
            if not domain_dir.exists():
                continue
            for entry in sorted(domain_dir.iterdir()):
                meta_path = entry / "slice_meta.json"
                if entry.is_dir() and meta_path.exists():
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    slice_ids.append(str(meta["slice_id"]))
        return slice_ids

    def stats(self) -> LakehouseStats:
        """Return document, slice, and hydration telemetry."""
        return LakehouseStats(
            document_count=len(self._ref_domain_index),
            slice_count=len(self.list_slices()),
            hydration_calls=self._hydration_calls,
            hydrated_doc_count=self._hydrated_doc_count,
        )

    def clear(self) -> None:
        """Delete all stored documents and slices and reset telemetry."""
        if self.documents_dir.exists():
            shutil.rmtree(self.documents_dir)
        if self.slices_dir.exists():
            shutil.rmtree(self.slices_dir)
        self._ref_domain_index.clear()
        self._hydration_calls = 0
        self._hydrated_doc_count = 0
        self._ensure_layout()
