"""Containerized HTTP server endpoints for Neo4j, HugeGraph, and PuppyGraph."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

from scale_kg.adapters.hugegraph_adapter import HugeGraphStore
from scale_kg.adapters.iceberg_puppygraph import IcebergPuppyGraphStore
from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.adapters.neo4j_adapter import Neo4jGraphStore
from scale_kg.core.models import Domain, EntityNode, TraversalRequest
from scale_kg.core.supernode import BUCKET_EDGE_TYPE, SupernodeBucketer

_QUOTED_TOKEN_RE = re.compile(r"'([A-Za-z_][A-Za-z0-9_]*)'")


class ContainerApp(Protocol):
    """Protocol for containerized graph service HTTP handlers."""

    async def handle(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, Any]]: ...


def _extract_inline_edge_types(query: str) -> list[str] | None:
    """Extract non-bucket edge type literals from inline Gremlin or Cypher filters."""
    if ".outE(" in query:
        segment = query.split(".outE(", 1)[1].split(")", 1)[0]
        tokens = [t for t in _QUOTED_TOKEN_RE.findall(segment) if t != BUCKET_EDGE_TYPE]
        return tokens or None
    if "rel.edge_type IN [" in query:
        segment = query.split("rel.edge_type IN [", 1)[1].split("]", 1)[0]
        tokens = [t for t in _QUOTED_TOKEN_RE.findall(segment) if t != BUCKET_EDGE_TYPE]
        return tokens or None
    if "[r:" in query and "*1.." in query:
        segment = query.split("[r:", 1)[1].split("*1..", 1)[0]
        tokens = [
            t.strip()
            for t in segment.split("|")
            if t.strip() and t.strip() not in {"REL", BUCKET_EDGE_TYPE}
        ]
        return tokens or None
    return None


class Neo4jContainerApp:
    """HTTP Cypher shard service implementing Neo4j's transactional endpoint."""

    def __init__(self) -> None:
        self.shards: dict[str, InMemoryGraphStore] = {
            f"kg_{d.value}": InMemoryGraphStore() for d in Domain
        }
        self.shards["composite"] = InMemoryGraphStore()
        self.shards["neo4j"] = self.shards["composite"]

    def _shard(self, db_name: str) -> InMemoryGraphStore:
        if db_name not in self.shards:
            self.shards[db_name] = InMemoryGraphStore()
        return self.shards[db_name]

    async def handle(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(raw_path)
        path = parsed.path

        if method == "GET" and path in ("/", "/health"):
            return 200, {
                "status": "ok",
                "engine": "neo4j",
                "databases": sorted(self.shards.keys()),
            }

        if method == "POST" and path.startswith("/db/") and path.endswith("/tx/commit"):
            db_name = path.split("/db/", 1)[1].rsplit("/tx/commit", 1)[0]
            store = self._shard(db_name)
            payload = json.loads(body.decode("utf-8")) if body else {}
            query = str(payload.get("query", ""))
            params: dict[str, Any] = payload.get("parameters") or {}
            rows = await self._execute_cypher(store, query, params)
            return 200, {"rows": rows}

        return 404, {"error": f"Unhandled Neo4j route {method} {path}"}

    async def _execute_cypher(
        self,
        store: InMemoryGraphStore,
        query: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        if "UNWIND $nodes AS n" in query:
            nodes = [Neo4jGraphStore._deserialize_node(n) for n in params["nodes"]]
            await store.upsert_nodes(nodes)
            return [{"upserted": len(nodes)}]

        if "UNWIND $edges AS e" in query:
            edges = [Neo4jGraphStore._deserialize_edge(e) for e in params["edges"]]
            await store.upsert_edges(edges)
            return [{"upserted": len(edges)}]

        if "MATCH (n:Entity {id: $id})" in query:
            node = await store.get_node(str(params["id"]))
            if node is None or node.is_proxy:
                return []
            return [Neo4jGraphStore._serialize_node(node)]

        if "RETURN r, t LIMIT" in query:
            direction: Any = "both"
            if "-[r:REL]->" in query:
                direction = "out"
            elif "<-[r:REL]-" in query:
                direction = "in"
            edge_types = params.get("edge_types") or None
            domains = (
                [Domain(d) for d in params["domains"]]
                if params.get("domains")
                else None
            )
            neighbors = await store.get_neighbors(
                str(params["node_id"]),
                edge_types=edge_types,
                domains=domains,
                direction=direction,
            )
            return [
                {
                    "r": Neo4jGraphStore._serialize_edge(e),
                    "t": Neo4jGraphStore._serialize_node(n),
                }
                for e, n in neighbors
            ]

        if "MATCH p = (s:Entity)-[r:REL*1.." in query:
            raw_depth = int(
                params.get("max_depth") or query.split("*1..", 1)[1].split("]", 1)[0]
            )
            depth = min(max(raw_depth, 1), 3)
            edge_types = params.get("edge_types") or None
            domains = (
                [Domain(d) for d in params["domains"]]
                if params.get("domains")
                else None
            )
            res = await store.traverse_k_hop(
                TraversalRequest(
                    seed_ids=list(params["seed_ids"]),
                    max_depth=depth,
                    edge_types=edge_types,
                    domains=domains,
                    max_results=int(params.get("limit", 5000)),
                    include_derived=bool(params.get("include_derived", False)),
                )
            )
            if not res.nodes:
                return []
            nodes_by_id: dict[str, EntityNode] = {}
            for n in res.nodes:
                nodes_by_id[n.id] = n
                nodes_by_id.setdefault(SupernodeBucketer.unwrap_bucket_id(n.id), n)
            rows: list[dict[str, Any]] = []
            for p in res.paths:
                p_nodes = [
                    Neo4jGraphStore._serialize_node(nodes_by_id[nid])
                    for nid in p
                    if nid in nodes_by_id
                ]
                p_edges = [
                    Neo4jGraphStore._serialize_edge(e)
                    for e in res.edges
                    if e.source_id in p and e.target_id in p
                ]
                rows.append({"nodes": p_nodes, "edges": p_edges, "path": p})
            if not rows:
                rows.append(
                    {
                        "nodes": [
                            Neo4jGraphStore._serialize_node(n) for n in res.nodes
                        ],
                        "edges": [
                            Neo4jGraphStore._serialize_edge(e) for e in res.edges
                        ],
                        "path": [n.id for n in res.nodes],
                    }
                )
            return rows

        if "RETURN count(n) AS total" in query:
            stats = await store.count_stats()
            return [
                {
                    "total": stats.node_count,
                    "buckets": stats.bucket_node_count,
                    "proxies": stats.proxy_node_count,
                }
            ]

        if "MATCH ()-[r:REL]->() RETURN count(r) AS total" in query:
            stats = await store.count_stats()
            return [{"total": stats.edge_count}]

        if "DETACH DELETE n" in query:
            await store.clear()
            return [{"deleted": True}]

        return []


class HugeGraphContainerApp:
    """HTTP service implementing HugeGraph REST, Gremlin, and OpenCypher endpoints."""

    def __init__(self) -> None:
        self.backing = InMemoryGraphStore()
        self.serializer = HugeGraphStore(base_url="http://localhost:8080")

    async def handle(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(raw_path)
        path = parsed.path
        query_params = {k: v[0] for k, v in parse_qs(parsed.query).items() if v}
        payload: Any = json.loads(body.decode("utf-8")) if body else None

        if method == "GET" and path in ("/versions", "/health"):
            return 200, {
                "versions": {"version": "1.3.0", "api": "1.5.0"},
                "status": "ok",
            }

        if method == "POST" and path.endswith("/graph/vertices/batch"):
            nodes = [HugeGraphStore._deserialize_vertex(item) for item in payload]
            await self.backing.upsert_nodes(nodes)
            return 201, {"count": len(nodes)}

        if method == "POST" and path.endswith("/graph/edges/batch"):
            edges = [HugeGraphStore._deserialize_edge(item) for item in payload]
            await self.backing.upsert_edges(edges)
            return 201, {"count": len(edges)}

        if method == "GET" and "/graph/vertices/" in path:
            node_id = unquote(path.rsplit("/graph/vertices/", 1)[1])
            node = await self.backing.get_node(node_id)
            if node is None:
                return 404, {"message": "Vertex not found"}
            return 200, self.serializer._serialize_vertex(node)

        if method == "GET" and path.endswith(("/traversers/edges", "/graph/edges")):
            vid = unquote(query_params.get("vertex_id", ""))
            direction: Any = query_params.get("direction", "OUT").lower()
            raw_labels = query_params.get("labels") or query_params.get("label")
            edge_types = raw_labels.split(",") if raw_labels else None
            domains = (
                [Domain(d) for d in query_params["domains"].split(",")]
                if query_params.get("domains")
                else None
            )
            limit = int(query_params.get("limit", "5000"))
            neighbors = await self.backing.get_neighbors(
                vid,
                edge_types=edge_types,
                domains=domains,
                direction=direction,
                limit=limit,
            )
            return 200, {
                "edges": [e.model_dump(mode="json") for e, _ in neighbors],
                "vertices": [n.model_dump(mode="json") for _, n in neighbors],
            }

        if method == "POST" and path.endswith(
            ("/gremlin", "/cypher", "/traversers/kneighbor")
        ):
            if path.endswith("/gremlin"):
                params = payload["bindings"]
                seed_ids = params["seed_ids"]
                depth = min(max(int(params["max_depth"]), 1), 3)
                limit = int(params["limit"])
                domains = (
                    [Domain(d) for d in params["domains"]]
                    if params.get("domains")
                    else None
                )
                gremlin_q = str(payload.get("gremlin", ""))
                edge_types = _extract_inline_edge_types(gremlin_q)
                include_derived = ".has('is_derived', false)" not in gremlin_q
            elif path.endswith("/cypher"):
                params = payload["parameters"]
                seed_ids = params["seed_ids"]
                depth = min(max(int(params["max_depth"]), 1), 3)
                limit = int(params["limit"])
                domains = (
                    [Domain(d) for d in params["domains"]]
                    if params.get("domains")
                    else None
                )
                cypher_q = str(payload.get("query", ""))
                edge_types = _extract_inline_edge_types(cypher_q)
                include_derived = "is_derived = false" not in cypher_q
            else:
                seed_ids = payload["sources"]["ids"]
                raw_depth = int(
                    payload.get("logical_max_depth", min(payload["max_depth"], 3))
                )
                depth = min(max(raw_depth, 1), 3)
                limit = int(payload["limit"])
                domains = (
                    [Domain(d) for d in payload["domains"]]
                    if payload.get("domains")
                    else None
                )
                step_labels = [
                    str(lbl)
                    for lbl in (payload["steps"]["labels"] or [])
                    if lbl != BUCKET_EDGE_TYPE
                ]
                edge_types = step_labels or None
                include_derived = not bool(
                    payload["steps"].get("exclude_derived", False)
                )

            res = await self.backing.traverse_k_hop(
                TraversalRequest(
                    seed_ids=seed_ids,
                    max_depth=depth,
                    edge_types=edge_types,
                    domains=domains,
                    max_results=limit,
                    include_derived=include_derived,
                )
            )
            return 200, {
                "vertices": [n.model_dump(mode="json") for n in res.nodes],
                "edges": [e.model_dump(mode="json") for e in res.edges],
                "paths": res.paths,
                "nodes_expanded": res.metrics.nodes_expanded,
                "bucket_hops_unwrapped": res.metrics.bucket_hops_unwrapped,
            }

        if method == "GET" and path.endswith("/traversers/count"):
            stats = await self.backing.count_stats()
            return 200, stats.model_dump(mode="json")

        if method == "DELETE" and path.endswith("/clear"):
            await self.backing.clear()
            return 200, {"cleared": True}

        return 404, {"error": f"Unhandled HugeGraph route {method} {path}"}


class PuppyGraphContainerApp:
    """Zero-ETL HTTP service querying Iceberg/Parquet warehouse partitions."""

    def __init__(self, default_warehouse: str | Path | None = None) -> None:
        self.default_warehouse = Path(
            default_warehouse
            or os.environ.get("PUPPYGRAPH_WAREHOUSE_DIR", "/data/warehouse")
        )
        self.registered_schemas: list[dict[str, Any]] = []

    def _resolve_store(self, headers: dict[str, str]) -> IcebergPuppyGraphStore:
        header_wh = headers.get("x-puppygraph-warehouse")
        if header_wh:
            candidate = Path(header_wh)
            if candidate.exists():
                return IcebergPuppyGraphStore(candidate, cache_partitions=True)
            if ".docker-data/warehouse" in header_wh:
                rel = header_wh.split(".docker-data/warehouse", 1)[1].lstrip("/")
                mapped = self.default_warehouse / rel if rel else self.default_warehouse
                return IcebergPuppyGraphStore(mapped, cache_partitions=True)
        return IcebergPuppyGraphStore(self.default_warehouse, cache_partitions=True)

    async def handle(
        self,
        method: str,
        raw_path: str,
        headers: dict[str, str],
        body: bytes,
    ) -> tuple[int, dict[str, Any]]:
        parsed = urlsplit(raw_path)
        path = parsed.path
        payload: dict[str, Any] = json.loads(body.decode("utf-8")) if body else {}

        if method == "GET" and path == "/health":
            return 200, {
                "status": "ok",
                "engine": "puppygraph",
                "warehouse": str(self.default_warehouse),
            }

        if method == "POST" and path == "/schema":
            self.registered_schemas.append(payload)
            return 200, {"status": "registered", "catalog": payload.get("catalog")}

        if method == "DELETE" and path == "/cache":
            return 200, {"cleared": True}

        if method == "POST" and path in ("/cypher", "/gremlin"):
            store = self._resolve_store(headers)
            if path == "/cypher":
                query = str(payload.get("query", ""))
                params: dict[str, Any] = payload.get("parameters") or {}

                if "MATCH (n:Entity {id: $id})" in query:
                    node = await store.get_node(str(params["id"]))
                    return 200, {
                        "nodes": [node.model_dump(mode="json")] if node else []
                    }

                if "WHERE s.id IN $ids" in query:
                    node_id = str(params["id"])
                    direction: Any = params.get("direction", "out")
                    edge_types = params.get("edge_types") or None
                    domains = (
                        [Domain(d) for d in params["domains"]]
                        if params.get("domains")
                        else None
                    )
                    limit = int(params.get("limit", 5000))
                    pairs = await store.get_neighbors(
                        node_id,
                        edge_types=edge_types,
                        domains=domains,
                        direction=direction,
                        limit=limit,
                    )
                    return 200, {
                        "neighbors": [
                            {
                                "edge": e.model_dump(mode="json"),
                                "node": n.model_dump(mode="json"),
                            }
                            for e, n in pairs
                        ]
                    }

                seed_ids = list(params.get("seed_ids", []))
                raw_depth = int(params.get("max_depth", 2))
                depth = min(max(raw_depth, 1), 3)
                limit = int(params.get("limit", 5000))
                domains = (
                    [Domain(d) for d in params["domains"]]
                    if params.get("domains")
                    else None
                )
                edge_types = _extract_inline_edge_types(query)
                include_derived = "is_derived = false" not in query
            else:
                query = str(payload.get("gremlin") or payload.get("query", ""))
                bindings: dict[str, Any] = payload.get("bindings") or {}
                seed_ids = list(bindings.get("seed_ids", []))
                raw_times = (
                    int(query.split(".times(", 1)[1].split(")", 1)[0])
                    if ".times(" in query
                    else 2
                )
                depth = min(max(raw_times // 3 if raw_times > 3 else raw_times, 1), 3)
                limit = int(bindings.get("limit", 5000))
                domains = (
                    [Domain(d) for d in bindings["domains"]]
                    if bindings.get("domains")
                    else None
                )
                edge_types = _extract_inline_edge_types(query)
                include_derived = ".has('is_derived', false)" not in query

            res = await store.traverse_k_hop(
                TraversalRequest(
                    seed_ids=seed_ids,
                    max_depth=depth,
                    edge_types=edge_types,
                    domains=domains,
                    max_results=limit,
                    include_derived=include_derived,
                )
            )
            return 200, {
                "nodes": [n.model_dump(mode="json") for n in res.nodes],
                "edges": [e.model_dump(mode="json") for e in res.edges],
                "paths": res.paths,
                "nodes_expanded": res.metrics.nodes_expanded,
                "depth_reached": res.metrics.depth_reached,
                "bucket_hops_unwrapped": res.metrics.bucket_hops_unwrapped,
            }

        return 404, {"error": f"Unhandled PuppyGraph route {method} {path}"}


class ContainerTransport(httpx.AsyncBaseTransport):
    """In-process httpx transport wrapping a ContainerApp for unit tests."""

    def __init__(self, app: ContainerApp) -> None:
        self.app = app

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        raw_path = request.url.raw_path.decode("ascii")
        headers = {k.lower(): v for k, v in request.headers.items()}
        status, payload = await self.app.handle(
            request.method, raw_path, headers, request.content
        )
        return httpx.Response(status, json=payload)


_REASON_PHRASES = {
    200: "OK",
    201: "Created",
    400: "Bad Request",
    404: "Not Found",
    500: "Internal Server Error",
}


async def _handle_client(
    app: ContainerApp,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while True:
            request_line = await reader.readline()
            if not request_line:
                return
            parts = request_line.decode("latin-1").strip().split(" ")
            if len(parts) < 2:
                return
            method, raw_path = parts[0], parts[1]

            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if not line or line in (b"\r\n", b"\n"):
                    break
                decoded = line.decode("latin-1").strip()
                if ":" in decoded:
                    k, v = decoded.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            content_length = int(headers.get("content-length", "0"))
            body = (
                await reader.readexactly(content_length) if content_length > 0 else b""
            )

            try:
                status, payload = await app.handle(method, raw_path, headers, body)
            except Exception as exc:  # noqa: BLE001
                status, payload = 500, {"error": str(exc)}

            resp_bytes = json.dumps(payload).encode("utf-8")
            reason = _REASON_PHRASES.get(status, "OK")
            should_close = headers.get("connection", "").lower() == "close"
            conn_hdr = "close" if should_close else "keep-alive"
            header_block = (
                f"HTTP/1.1 {status} {reason}\r\n"
                "Content-Type: application/json\r\n"
                f"Content-Length: {len(resp_bytes)}\r\n"
                f"Connection: {conn_hdr}\r\n\r\n"
            ).encode("latin-1")
            writer.write(header_block + resp_bytes)
            await writer.drain()
            if should_close:
                return
    except (asyncio.IncompleteReadError, ConnectionError):
        return
    finally:
        writer.close()
        await writer.wait_closed()


def create_app(engine: str, *, warehouse_dir: str | Path | None = None) -> ContainerApp:
    """Create a ContainerApp for ``neo4j``, ``hugegraph``, or ``puppygraph``."""
    normalized = engine.lower().strip()
    if normalized == "neo4j":
        return Neo4jContainerApp()
    if normalized == "hugegraph":
        return HugeGraphContainerApp()
    if normalized == "puppygraph":
        return PuppyGraphContainerApp(default_warehouse=warehouse_dir)
    raise ValueError(
        f"Unsupported engine '{engine}'; expected neo4j, hugegraph, or puppygraph."
    )


async def serve_app(app: ContainerApp, host: str, port: int) -> None:
    """Run the asyncio HTTP server until cancelled."""
    server = await asyncio.start_server(
        lambda r, w: _handle_client(app, r, w), host=host, port=port
    )
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run scale-kg graph container service."
    )
    parser.add_argument(
        "--engine",
        choices=["neo4j", "hugegraph", "puppygraph"],
        required=True,
    )
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--warehouse-dir", default=None)
    args = parser.parse_args()

    app = create_app(args.engine, warehouse_dir=args.warehouse_dir)
    asyncio.run(serve_app(app, args.host, args.port))


if __name__ == "__main__":
    main()
