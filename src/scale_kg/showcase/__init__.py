"""Interactive showcase engine, 50-user concurrency layer, and static exporter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scale_kg.showcase.concurrency import ShowcaseService
from scale_kg.showcase.introspect import introspect_system
from scale_kg.showcase.router import create_showcase_router
from scale_kg.showcase.snapshot import (
    CommitTrendEntry,
    ShowcaseSnapshot,
    SystemIntrospection,
)

if TYPE_CHECKING:
    from scale_kg.showcase.build import build_showcase_site

__all__ = [
    "CommitTrendEntry",
    "ShowcaseService",
    "ShowcaseSnapshot",
    "SystemIntrospection",
    "build_showcase_site",
    "create_showcase_router",
    "introspect_system",
]


def __getattr__(name: str) -> Any:
    if name == "build_showcase_site":
        from scale_kg.showcase.build import build_showcase_site

        return build_showcase_site
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
