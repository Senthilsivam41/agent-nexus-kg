"""Deterministic local demo of the in-memory GraphStore. No network or API keys."""

from __future__ import annotations

import asyncio
import os

from scale_kg.adapters.in_memory import InMemoryGraphStore
from scale_kg.core.models import Domain, EntityNode, RelationshipEdge, TraversalRequest


def _fixture() -> tuple[list[EntityNode], list[RelationshipEdge]]:
    nodes = [
        EntityNode(
            id="customer:org:acme",
            domain=Domain.CUSTOMER,
            entity_type="Organization",
        ),
        EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
        ),
        EntityNode(
            id="product:sdk:scale-kg",
            domain=Domain.PRODUCT,
            entity_type="Product",
        ),
    ]
    edges = [
        RelationshipEdge(
            source_id="customer:org:acme",
            target_id="technology:lang:python",
            edge_type="USES",
            domain=Domain.CUSTOMER,
            target_domain=Domain.TECHNOLOGY,
        ),
        RelationshipEdge(
            source_id="technology:lang:python",
            target_id="product:sdk:scale-kg",
            edge_type="POWERS",
            domain=Domain.TECHNOLOGY,
            target_domain=Domain.PRODUCT,
        ),
    ]
    return nodes, edges


async def run_demo() -> str:
    """Load a fixed graph and return one bounded traversal."""
    os.environ.setdefault("MOCK_MODE", "true")
    store = InMemoryGraphStore()
    nodes, edges = _fixture()
    await store.upsert_nodes(nodes)
    await store.upsert_edges(edges)
    result = await store.traverse_k_hop(
        TraversalRequest(seed_ids=["customer:org:acme"], max_depth=2)
    )
    path = next(
        (item for item in result.paths if item and item[-1] == "product:sdk:scale-kg"),
        result.paths[0] if result.paths else [],
    )
    rendered = " -> ".join(path)
    return (
        "scale-kg demo\n"
        "adapter=in_memory\n"
        f"nodes={len(nodes)} edges={len(edges)}\n"
        f"path={rendered}\n"
    )


def main() -> None:
    print(asyncio.run(run_demo()), end="")


if __name__ == "__main__":
    main()
