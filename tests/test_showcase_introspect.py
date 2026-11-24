"""Tests for zero-drift runtime introspection of scale_kg components."""

from __future__ import annotations

from scale_kg.adapters import (
    GraphStore,
    HugeGraphStore,
    IcebergPuppyGraphStore,
    InMemoryGraphStore,
    JanusGraphStore,
    NebulaGraphStore,
    Neo4jGraphStore,
)
from scale_kg.benchmark.generator import SCALE_PRESETS
from scale_kg.benchmark.golden_queries import FROZEN_GOLDEN_QUERIES
from scale_kg.config import GuardrailConfig, LatencyBudgetConfig, ScaleKGConfig
from scale_kg.core.models import Domain
from scale_kg.showcase.introspect import introspect_system


def test_introspect_captures_all_live_configs_domains_adapters_and_presets() -> None:
    info = introspect_system(probe_remote=False)

    guardrail_names = {item.name for item in info.guardrails}
    assert guardrail_names == set(GuardrailConfig.model_fields.keys())

    budget_names = {item.name for item in info.latency_budgets}
    assert budget_names == set(LatencyBudgetConfig.model_fields.keys())

    assert info.domains == [d.value for d in Domain]
    assert {p.preset for p in info.presets} == {p.value for p in SCALE_PRESETS}
    assert {q.query_id for q in info.golden_queries} == {
        q.query_id for q in FROZEN_GOLDEN_QUERIES
    }

    expected_adapters = {
        InMemoryGraphStore,
        IcebergPuppyGraphStore,
        HugeGraphStore,
        Neo4jGraphStore,
        JanusGraphStore,
        NebulaGraphStore,
    }
    discovered_classes = {entry.class_name for entry in info.adapters}
    assert discovered_classes == {cls.__name__ for cls in expected_adapters}
    assert all(issubclass(cls, GraphStore) for cls in expected_adapters)
    assert len(info.switch_rules) >= 4
    assert len(info.config_fingerprint) == 16


def test_introspect_fingerprint_updates_when_config_changes() -> None:
    base = introspect_system(config=ScaleKGConfig(), probe_remote=False)
    modified = introspect_system(
        config=ScaleKGConfig(supernode_degree_threshold=250, num_buckets=8),
        probe_remote=False,
    )

    assert base.config_fingerprint != modified.config_fingerprint
    assert modified.supernode_degree_threshold == 250
    assert modified.num_buckets == 8
    assert any("250" in rule.threshold for rule in modified.switch_rules)
