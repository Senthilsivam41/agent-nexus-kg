"""Canonical global entity ID parser, EntityDirectory, and cross-shard proxy tracking."""

from __future__ import annotations

from collections.abc import Iterable

from pydantic import BaseModel

from scale_kg.core.models import Domain, EntityNode, RelationshipEdge
from scale_kg.core.supernode import SupernodeBucketer


class CanonicalEntityId(BaseModel):
    """Parsed components of a canonical ``<domain>:<entity_type>:<slug>`` identifier."""

    domain: Domain
    entity_type: str
    slug: str
    raw_id: str


class EntityResolutionError(KeyError):
    """Raised when an entity ID cannot be resolved to an owning Domain shard."""


def build_canonical_id(domain: Domain, entity_type: str, slug: str) -> str:
    """Construct a canonical ``<domain>:<entity_type>:<slug>`` identifier."""
    clean_type = entity_type.strip()
    clean_slug = slug.strip()
    if not clean_type or not clean_slug:
        raise ValueError("entity_type and slug must be non-empty.")
    return f"{domain.value}:{clean_type}:{clean_slug}"


def parse_canonical_id(entity_id: str) -> CanonicalEntityId | None:
    """Parse ``<domain>:<entity_type>:<slug>`` (or bucketed variant) in O(1) time.

    Returns ``None`` for legacy or opaque identifiers that lack a valid domain prefix
    and ``<domain>:<entity_type>:<slug>`` structure.
    """
    unwrapped = SupernodeBucketer.unwrap_bucket_id(entity_id)
    parts = unwrapped.split(":", 2)
    if len(parts) != 3:
        return None
    domain_str, entity_type, slug = parts
    if not entity_type or not slug:
        return None
    try:
        domain = Domain(domain_str)
    except ValueError:
        return None
    return CanonicalEntityId(
        domain=domain,
        entity_type=entity_type,
        slug=slug,
        raw_id=entity_id,
    )


class DirectoryStats(BaseModel):
    """Lookup and registration telemetry for EntityDirectory."""

    registered_entities: int = 0
    registered_aliases: int = 0
    registered_proxies: int = 0
    canonical_hits: int = 0
    directory_hits: int = 0
    proxy_hints_used: int = 0


class EntityDirectory:
    """Resolves entity IDs and aliases to owning Domain shards and manages proxy nodes."""

    def __init__(self) -> None:
        self._id_to_domain: dict[str, Domain] = {}
        self._aliases: dict[str, str] = {}
        self._proxies: dict[tuple[Domain, str], EntityNode] = {}
        self._canonical_hits = 0
        self._directory_hits = 0
        self._proxy_hints_used = 0

    def register(
        self,
        entity_id: str,
        domain: Domain,
        *,
        aliases: Iterable[str] | None = None,
    ) -> None:
        """Register an entity ID and optional legacy aliases under an owning Domain."""
        self._id_to_domain[entity_id] = domain
        if aliases:
            for alias in aliases:
                self.register_alias(alias, entity_id)

    def register_node(
        self,
        node: EntityNode,
        *,
        aliases: Iterable[str] | None = None,
    ) -> None:
        """Register an EntityNode (or its proxy hint if ``is_proxy=True``)."""
        if node.is_proxy:
            if node.target_domain is not None:
                self._proxies[(node.domain, node.id)] = node
            return
        self.register(node.id, node.domain, aliases=aliases)

    def register_alias(self, alias: str, target_id: str) -> None:
        """Map a legacy or alternate identifier to a canonical/primary entity ID."""
        if not alias or not target_id:
            raise ValueError("alias and target_id must be non-empty.")
        self._aliases[alias] = target_id

    def canonicalize(self, entity_id: str) -> str:
        """Resolve an alias to its primary entity ID, or return ``entity_id`` unchanged."""
        seen: set[str] = set()
        curr = entity_id
        while curr in self._aliases and curr not in seen:
            seen.add(curr)
            curr = self._aliases[curr]
        return curr

    def try_resolve_domain(self, entity_id: str) -> Domain | None:
        """Resolve an entity ID to its owning Domain shard without raising if missing.

        Canonical ``<domain>:<entity_type>:<slug>`` IDs resolve in O(1) without
        consulting the directory mapping table.
        """
        resolved_id = self.canonicalize(entity_id)
        unwrapped = SupernodeBucketer.unwrap_bucket_id(resolved_id)
        parsed = parse_canonical_id(unwrapped)
        if parsed is not None:
            self._canonical_hits += 1
            return parsed.domain

        domain = self._id_to_domain.get(unwrapped) or self._id_to_domain.get(
            resolved_id
        )
        if domain is not None:
            self._directory_hits += 1
            return domain
        return None

    def resolve_domain(self, entity_id: str) -> Domain:
        """Resolve an entity ID to its owning Domain shard or raise EntityResolutionError."""
        domain = self.try_resolve_domain(entity_id)
        if domain is None:
            raise EntityResolutionError(
                f"Cannot resolve owning domain for entity '{entity_id}'."
            )
        return domain

    def register_proxy(
        self,
        target_id: str,
        *,
        owning_domain: Domain,
        target_domain: Domain,
        entity_type: str = "Proxy",
    ) -> EntityNode:
        """Create and register an in-shard ghost/proxy vertex for a cross-domain edge."""
        proxy = EntityNode(
            id=target_id,
            domain=owning_domain,
            entity_type=entity_type,
            routing_props={"proxy_target_domain": target_domain.value},
            is_proxy=True,
            target_domain=target_domain,
        )
        self._proxies[(owning_domain, target_id)] = proxy
        return proxy

    def register_cross_domain_edge(self, edge: RelationshipEdge) -> EntityNode | None:
        """Register a proxy node in ``edge.domain`` if ``edge`` crosses domain boundaries."""
        target_domain = edge.target_domain or self.try_resolve_domain(edge.target_id)
        if target_domain is None or target_domain == edge.domain:
            return None
        return self.register_proxy(
            edge.target_id,
            owning_domain=edge.domain,
            target_domain=target_domain,
        )

    def get_proxy(self, owning_domain: Domain, target_id: str) -> EntityNode | None:
        """Return the registered proxy node stored in ``owning_domain`` for ``target_id``."""
        return self._proxies.get((owning_domain, target_id))

    def proxies_for_domain(self, owning_domain: Domain) -> list[EntityNode]:
        """List all ghost/proxy vertices registered inside ``owning_domain``."""
        return [
            proxy
            for (domain, _), proxy in self._proxies.items()
            if domain == owning_domain
        ]

    def resolve_next_hop_domain(
        self,
        *,
        neighbor: EntityNode | None = None,
        edge: RelationshipEdge | None = None,
        target_id: str | None = None,
    ) -> Domain | None:
        """Determine the owning shard for the next hop, preferring proxy/edge hints."""
        if neighbor is not None and neighbor.is_proxy and neighbor.target_domain:
            self._proxy_hints_used += 1
            return neighbor.target_domain
        if edge is not None and edge.target_domain is not None:
            self._proxy_hints_used += 1
            return edge.target_domain
        if neighbor is not None:
            return neighbor.domain
        if target_id is not None:
            return self.try_resolve_domain(target_id)
        return None

    def stats(self) -> DirectoryStats:
        """Return directory registration and lookup telemetry."""
        return DirectoryStats(
            registered_entities=len(self._id_to_domain),
            registered_aliases=len(self._aliases),
            registered_proxies=len(self._proxies),
            canonical_hits=self._canonical_hits,
            directory_hits=self._directory_hits,
            proxy_hints_used=self._proxy_hints_used,
        )

    def clear(self) -> None:
        """Clear all registered mappings, aliases, proxies, and counters."""
        self._id_to_domain.clear()
        self._aliases.clear()
        self._proxies.clear()
        self._canonical_hits = 0
        self._directory_hits = 0
        self._proxy_hints_used = 0
