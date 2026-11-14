"""Apache HugeGraph distributed adapter with PD + Raft HStore placement hints."""

from __future__ import annotations

import json
import time
from typing import Any, Literal
from urllib.parse import quote

import httpx

from scale_kg.adapters.base import (
    AdapterCostProfile,
    FaultMode,
    FaultState,
    GraphStats,
    GraphStore,
    GraphStoreError,
    GraphStoreUnavailableError,
    HealthStatus,
    canonical_domain_from_id,
)
from scale_kg.core.guardrails import GuardrailEnforcer
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalMetrics,
    TraversalRequest,
    TraversalResult,
)
from scale_kg.core.supernode import BUCKET_EDGE_TYPE, SupernodeBucketer, compute_bucket_id


class HugeGraphStore(GraphStore):
    """Primary 1B-10B+ edge async adapter for Apache HugeGraph (PD + HStore)."""

    name = "hugegraph"

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        graph_name: str = "scale_kg",
        pd_endpoints: list[str] | None = None,
        tenant_id: str = "default",
        query_mode: Literal["gremlin", "opencypher", "rest"] = "gremlin",
        enforcer: GuardrailEnforcer | None = None,
        num_buckets: int = 16,
        transport: httpx.AsyncBaseTransport | None = None,
        http_client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.graph_name = graph_name
        self.pd_endpoints = pd_endpoints or ["http://localhost:8686"]
        self.tenant_id = tenant_id
        self.query_mode = query_mode
        self._enforcer = enforcer or GuardrailEnforcer()
        self._num_buckets = num_buckets
        self._fault = FaultState()
        self._transport = transport
        self._external_client = http_client
        self._timeout_seconds = timeout_seconds

    def cost_profile(self) -> AdapterCostProfile:
        return AdapterCostProfile(
            engine="hugegraph",
            tier="tier1",
            storage_backend="HugeGraph Placement Driver (PD) + Raft-replicated HStore",
            query_languages=["gremlin", "opencypher", "rest"],
            supports_zero_etl=False,
            supports_cross_shard_edges=True,
            estimated_cost_per_billion_edges_usd_month=890.0,
            operational_complexity="medium",
            notes="Primary 1B-10B+ edge distributed engine with native PD/HStore partitioning.",
        )

    def build_partition_headers(self, domain: Domain | None = None) -> dict[str, str]:
        """Construct HugeGraph PD locality headers for domain and tenant routing."""
        return {
            "X-HugeGraph-Tenant": self.tenant_id,
            "X-HugeGraph-Partition-Domain": domain.value if domain else "all",
            "X-HugeGraph-PD-Endpoints": ",".join(self.pd_endpoints),
        }

    def partition_hint(self, entity_id: str, domain: Domain) -> str:
        """Compute deterministic PD/HStore placement hint for an entity."""
        bucket = compute_bucket_id(entity_id, self._num_buckets)
        return f"{domain.value}:{self.tenant_id}:{bucket}"

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any = None,
        params: dict[str, Any] | None = None,
        domain: Domain | None = None,
        allow_404: bool = False,
    ) -> Any:
        self._fault.check(domain)
        url = f"{self.base_url}{path}"
        headers = self.build_partition_headers(domain)
        try:
            if self._external_client is not None:
                resp = await self._external_client.request(
                    method, url, json=json_body, params=params, headers=headers
                )
            else:
                async with httpx.AsyncClient(
                    transport=self._transport, timeout=self._timeout_seconds
                ) as client:
                    resp = await client.request(
                        method, url, json=json_body, params=params, headers=headers
                    )
        except httpx.TimeoutException as exc:
            raise TimeoutError(f"HugeGraph request timed out: {method} {path}") from exc
        except httpx.HTTPError as exc:
            raise GraphStoreUnavailableError(
                f"HugeGraph transport failure on {method} {path}: {exc}"
            ) from exc

        if allow_404 and resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            raise GraphStoreError(
                f"HugeGraph {method} {path} failed with HTTP {resp.status_code}: {resp.text}"
            )
        if not resp.content:
            return {}
        return resp.json()

    def _serialize_vertex(self, node: EntityNode) -> dict[str, Any]:
        return {
            "id": node.id,
            "label": node.entity_type,
            "properties": {
                "domain": node.domain.value,
                "tenant": self.tenant_id,
                "partition_hint": self.partition_hint(node.id, node.domain),
                "entity_type": node.entity_type,
                "lakehouse_ref": node.lakehouse_ref,
                "is_proxy": node.is_proxy,
                "target_domain": node.target_domain.value if node.target_domain else None,
                "is_bucket": node.is_bucket,
                "routing_props_json": json.dumps(node.routing_props, sort_keys=True),
            },
        }

    @staticmethod
    def _deserialize_vertex(payload: dict[str, Any]) -> EntityNode:
        if "domain" in payload and "entity_type" in payload and "properties" not in payload:
            return EntityNode.model_validate(payload)
        props = payload.get("properties", {})
        node_id = str(payload["id"])
        domain_raw = props.get("domain") or canonical_domain_from_id(node_id) or Domain.TECHNOLOGY
        routing_raw = props.get("routing_props_json")
        routing_props = (
            json.loads(routing_raw) if isinstance(routing_raw, str) else props.get("routing_props", {})
        )
        return EntityNode(
            id=node_id,
            domain=Domain(domain_raw),
            entity_type=str(props.get("entity_type") or payload.get("label") or "Entity"),
            routing_props=routing_props,
            lakehouse_ref=props.get("lakehouse_ref"),
            is_proxy=bool(props.get("is_proxy", False)),
            target_domain=Domain(props["target_domain"]) if props.get("target_domain") else None,
            is_bucket=bool(props.get("is_bucket", False)),
        )

    def _serialize_edge(self, edge: RelationshipEdge) -> dict[str, Any]:
        return {
            "label": edge.edge_type,
            "outV": edge.source_id,
            "inV": edge.target_id,
            "outVLabel": "Entity",
            "inVLabel": "Entity",
            "properties": {
                "domain": edge.domain.value,
                "tenant": self.tenant_id,
                "partition_hint": self.partition_hint(edge.source_id, edge.domain),
                "target_domain": edge.target_domain.value if edge.target_domain else None,
                "is_derived": edge.is_derived,
                "materialized_hot_path": edge.materialized_hot_path,
                "routing_props_json": json.dumps(edge.routing_props, sort_keys=True),
            },
        }

    @staticmethod
    def _deserialize_edge(payload: dict[str, Any]) -> RelationshipEdge:
        if "source_id" in payload and "edge_type" in payload and "properties" not in payload:
            return RelationshipEdge.model_validate(payload)
        props = payload.get("properties", {})
        src = str(payload.get("outV") or payload["source_id"])
        dst = str(payload.get("inV") or payload["target_id"])
        domain_raw = props.get("domain") or canonical_domain_from_id(src) or Domain.TECHNOLOGY
        routing_raw = props.get("routing_props_json")
        routing_props = (
            json.loads(routing_raw) if isinstance(routing_raw, str) else props.get("routing_props", {})
        )
        return RelationshipEdge(
            source_id=src,
            target_id=dst,
            edge_type=str(payload.get("label") or payload["edge_type"]),
            domain=Domain(domain_raw),
            target_domain=Domain(props["target_domain"]) if props.get("target_domain") else None,
            is_derived=bool(props.get("is_derived", False)),
            materialized_hot_path=bool(props.get("materialized_hot_path", False)),
            routing_props=routing_props,
        )

    async def upsert_nodes(self, nodes: list[EntityNode]) -> int:
        for node in nodes:
            self._enforcer.validate_node_write(node)
        if not nodes:
            return 0
        by_domain: dict[Domain, list[dict[str, Any]]] = {}
        for node in nodes:
            by_domain.setdefault(node.domain, []).append(self._serialize_vertex(node))
        for domain, batch in by_domain.items():
            await self._request(
                "POST",
                f"/graphs/{self.graph_name}/graph/vertices/batch",
                json_body=batch,
                domain=domain,
            )
        return len(nodes)

    async def upsert_edges(self, edges: list[RelationshipEdge]) -> int:
        for edge in edges:
            self._enforcer.validate_edge_write(edge)
        if not edges:
            return 0
        by_domain: dict[Domain, list[dict[str, Any]]] = {}
        for edge in edges:
            by_domain.setdefault(edge.domain, []).append(self._serialize_edge(edge))
        for domain, batch in by_domain.items():
            await self._request(
                "POST",
                f"/graphs/{self.graph_name}/graph/edges/batch",
                json_body=batch,
                domain=domain,
            )
        return len(edges)

    async def get_node(self, node_id: str) -> EntityNode | None:
        encoded = quote(node_id, safe="")
        data = await self._request(
            "GET",
            f"/graphs/{self.graph_name}/graph/vertices/{encoded}",
            domain=canonical_domain_from_id(node_id),
            allow_404=True,
        )
        if data is None:
            return None
        return self._deserialize_vertex(data)

    async def get_neighbors(
        self,
        node_id: str,
        *,
        edge_types: list[str] | None = None,
        domains: list[Domain] | None = None,
        direction: Literal["out", "in", "both"] = "out",
        limit: int = 5000,
    ) -> list[tuple[RelationshipEdge, EntityNode]]:
        data = await self._request(
            "GET",
            f"/graphs/{self.graph_name}/traversers/edges",
            params={
                "vertex_id": node_id,
                "direction": direction.upper(),
                "limit": limit,
                "tenant": self.tenant_id,
                **({"labels": ",".join(edge_types)} if edge_types else {}),
                **({"domains": ",".join(d.value for d in domains)} if domains else {}),
            },
            domain=canonical_domain_from_id(node_id),
        )
        raw_edges = data.get("edges", [])
        raw_vertices = {
            str(v["id"]): self._deserialize_vertex(v) for v in data.get("vertices", [])
        }

        allowed_domains = set(domains) if domains else None
        allowed_types = set(edge_types) if edge_types else None
        results: list[tuple[RelationshipEdge, EntityNode]] = []
        seen_keys: set[tuple[str, str, str]] = set()

        for raw_edge in raw_edges:
            edge = self._deserialize_edge(raw_edge)
            if edge.edge_type == BUCKET_EDGE_TYPE:
                continue
            if allowed_types and edge.edge_type not in allowed_types:
                continue
            logical_src = SupernodeBucketer.unwrap_bucket_id(edge.source_id)
            logical_dst = SupernodeBucketer.unwrap_bucket_id(edge.target_id)
            if logical_src != edge.source_id or logical_dst != edge.target_id:
                edge = edge.model_copy(
                    update={"source_id": logical_src, "target_id": logical_dst}
                )
            neighbor_id = edge.target_id if edge.source_id == node_id else edge.source_id
            key = (edge.source_id, edge.target_id, edge.edge_type)
            if key in seen_keys:
                continue
            neighbor = raw_vertices.get(neighbor_id) or await self.get_node(neighbor_id)
            if neighbor is None:
                continue
            if allowed_domains and neighbor.domain not in allowed_domains:
                continue
            seen_keys.add(key)
            results.append((edge, neighbor))
            if len(results) >= limit:
                break
        return results

    def build_gremlin_traversal(
        self, request: TraversalRequest
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded Gremlin traversal script with tenant/domain bindings."""
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        limit = min(request.max_results, self._enforcer.config.max_results)
        edge_filter = (
            ", ".join(f"'{et}'" for et in request.edge_types) if request.edge_types else ""
        )
        script = (
            "g.V(seed_ids).has('tenant', tenant)"
            f".repeat(outE({edge_filter}).inV()).times({depth}).emit()"
            f".path().limit({limit})"
        )
        bindings: dict[str, Any] = {
            "seed_ids": list(request.seed_ids),
            "tenant": self.tenant_id,
            "domains": [d.value for d in request.domains] if request.domains else [],
            "max_depth": depth,
            "limit": limit,
        }
        return script, bindings

    def build_opencypher_traversal(
        self, request: TraversalRequest
    ) -> tuple[str, dict[str, Any]]:
        """Build a bounded OpenCypher traversal query for HugeGraph."""
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        limit = min(request.max_results, self._enforcer.config.max_results)
        rel_clause = ":" + "|".join(request.edge_types) if request.edge_types else ""
        domain_clause = (
            " AND ALL(n IN nodes(p) WHERE n.domain IN $domains)"
            if request.domains
            else ""
        )
        cypher = (
            f"MATCH p=(s)-[r{rel_clause}*1..{depth}]->(t) "
            f"WHERE s.id IN $seed_ids AND s.tenant = $tenant{domain_clause} "
            f"RETURN p LIMIT {limit}"
        )
        params: dict[str, Any] = {
            "seed_ids": list(request.seed_ids),
            "tenant": self.tenant_id,
            "domains": [d.value for d in request.domains] if request.domains else [],
            "max_depth": depth,
            "limit": limit,
        }
        return cypher, params

    def build_rest_kneighbor_payload(
        self, request: TraversalRequest
    ) -> dict[str, Any]:
        """Build a HugeGraph REST /traversers/kneighbor request payload."""
        depth = min(request.max_depth, self._enforcer.config.max_depth)
        limit = min(request.max_results, self._enforcer.config.max_results)
        return {
            "sources": {"ids": list(request.seed_ids)},
            "steps": {
                "direction": "OUT",
                "labels": list(request.edge_types) if request.edge_types else [],
                "properties": {"tenant": self.tenant_id},
            },
            "max_depth": depth,
            "limit": limit,
            "domains": [d.value for d in request.domains] if request.domains else [],
            "with_vertex": True,
            "with_edge": True,
            "with_path": True,
        }

    async def traverse_k_hop(self, request: TraversalRequest) -> TraversalResult:
        tracker = self._enforcer.create_runtime_tracker(request)
        domain_hint = request.domains[0] if request.domains and len(request.domains) == 1 else None
        started = time.monotonic()

        if self.query_mode == "gremlin":
            script, bindings = self.build_gremlin_traversal(request)
            data = await self._request(
                "POST",
                f"/graphs/{self.graph_name}/gremlin",
                json_body={"gremlin": script, "bindings": bindings},
                domain=domain_hint,
            )
        elif self.query_mode == "opencypher":
            cypher, params = self.build_opencypher_traversal(request)
            data = await self._request(
                "POST",
                f"/graphs/{self.graph_name}/cypher",
                json_body={"query": cypher, "parameters": params},
                domain=domain_hint,
            )
        else:
            payload = self.build_rest_kneighbor_payload(request)
            data = await self._request(
                "POST",
                f"/graphs/{self.graph_name}/traversers/kneighbor",
                json_body=payload,
                domain=domain_hint,
            )

        raw_nodes = [self._deserialize_vertex(v) for v in data.get("vertices", data.get("nodes", []))]
        nodes = [n for n in raw_nodes if not n.is_bucket]

        raw_edges = [self._deserialize_edge(e) for e in data.get("edges", [])]
        edges: list[RelationshipEdge] = []
        seen_edge_keys: set[tuple[str, str, str]] = set()
        for edge in raw_edges:
            if edge.edge_type == BUCKET_EDGE_TYPE:
                continue
            if edge.is_derived and not request.include_derived:
                continue
            unwrapped = edge.model_copy(
                update={
                    "source_id": SupernodeBucketer.unwrap_bucket_id(edge.source_id),
                    "target_id": SupernodeBucketer.unwrap_bucket_id(edge.target_id),
                }
            )
            key = (unwrapped.source_id, unwrapped.target_id, unwrapped.edge_type)
            if key not in seen_edge_keys:
                seen_edge_keys.add(key)
                edges.append(unwrapped)

        logical_paths: list[list[str]] = []
        seen_paths: set[tuple[str, ...]] = set()
        unwrapped_hops_total = 0
        depth_reached = 0
        for raw_path in data.get("paths", []):
            lp, unwrapped = SupernodeBucketer.unwrap_path([str(v) for v in raw_path])
            unwrapped_hops_total += unwrapped
            if len(lp) > 1 and tuple(lp) not in seen_paths:
                seen_paths.add(tuple(lp))
                logical_paths.append(lp)
                depth_reached = max(depth_reached, len(lp) - 1)

        nodes_expanded = int(data.get("nodes_expanded", len(raw_nodes)))
        violation = tracker.check_step(
            depth=depth_reached,
            expanded_nodes=nodes_expanded,
            results_count=len(nodes),
        )
        cross_domain = sum(
            1
            for e in edges
            if e.target_domain is not None and e.target_domain != e.domain
        )
        locality = (
            round((len(edges) - cross_domain) / len(edges), 4) if edges else 1.0
        )
        if violation is not None:
            return tracker.build_aborted_result(
                violation=violation,
                nodes=nodes[: request.max_results],
                edges=edges,
                paths=logical_paths,
                depth_reached=depth_reached,
                expanded_nodes=nodes_expanded,
                cross_domain_hops=cross_domain,
                single_domain_locality_ratio=locality,
                bucket_hops_unwrapped=unwrapped_hops_total,
            )

        elapsed_ms = round((time.monotonic() - started) * 1000.0, 3)
        return TraversalResult(
            seed_ids=list(request.seed_ids),
            nodes=nodes,
            edges=edges,
            paths=logical_paths,
            metrics=TraversalMetrics(
                depth_reached=depth_reached,
                nodes_expanded=nodes_expanded,
                results_returned=len(nodes),
                execution_time_ms=elapsed_ms,
                cross_domain_hops=cross_domain,
                single_domain_locality_ratio=locality,
                bucket_hops_unwrapped=unwrapped_hops_total,
                aborted=False,
                violation=None,
            ),
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
        remote_version = ""
        if healthy:
            try:
                data = await self._request("GET", "/versions")
                remote_version = str(data.get("versions", {}).get("api", "ok"))
            except (GraphStoreError, TimeoutError):
                healthy = False
                status = "unavailable"
        return HealthStatus(
            adapter_name=self.name,
            healthy=healthy,
            status=status,
            fault_mode=self._fault.mode.value,
            details={
                "graph_name": self.graph_name,
                "tenant_id": self.tenant_id,
                "query_mode": self.query_mode,
                "pd_count": len(self.pd_endpoints),
                "api_version": remote_version,
            },
        )

    async def count_stats(self) -> GraphStats:
        data = await self._request(
            "GET", f"/graphs/{self.graph_name}/traversers/count"
        )
        return GraphStats(
            node_count=int(data.get("node_count", 0)),
            edge_count=int(data.get("edge_count", 0)),
            bucket_node_count=int(data.get("bucket_node_count", 0)),
            proxy_node_count=int(data.get("proxy_node_count", 0)),
            nodes_by_domain=dict(data.get("nodes_by_domain", {})),
            edges_by_domain=dict(data.get("edges_by_domain", {})),
            extra={"tenant_id": self.tenant_id, "pd_count": len(self.pd_endpoints)},
        )

    async def clear(self) -> None:
        await self._request(
            "DELETE",
            f"/graphs/{self.graph_name}/clear",
            json_body={"confirm_message": "I'm sure to delete all data"},
        )
        self._fault.configure(FaultMode.NONE)
