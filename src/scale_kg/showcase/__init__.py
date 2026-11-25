"""Interactive showcase engine, 50-user concurrency layer, and static exporter."""

from scale_kg.showcase.concurrency import ShowcaseService
from scale_kg.showcase.introspect import introspect_system
from scale_kg.showcase.router import create_showcase_router
from scale_kg.showcase.snapshot import (
    CommitTrendEntry,
    ShowcaseSnapshot,
    SystemIntrospection,
)

__all__ = [
    "CommitTrendEntry",
    "ShowcaseService",
    "ShowcaseSnapshot",
    "SystemIntrospection",
    "create_showcase_router",
    "introspect_system",
]
