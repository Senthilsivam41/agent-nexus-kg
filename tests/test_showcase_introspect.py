"""Tests for zero-drift runtime introspection of scale_kg components."""

from __future__ import annotations

import pytest

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
from scale_kg.showcase.introspect import _probe_http_endpoint, introspect_system


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


def test_probe_http_endpoint_rejects_malformed_and_non_http_urls() -> None:
    assert _probe_http_endpoint(None) is False
    assert _probe_http_endpoint("") is False
    assert _probe_http_endpoint("not-a-url") is False
    assert _probe_http_endpoint("ftp://localhost:8080") is False
    assert _probe_http_endpoint("http://[invalid-ipv6") is False


def test_introspect_detects_canonical_remote_env_vars_and_upgrades_puppygraph(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for var in (
        "SCALE_KG_HUGEGRAPH_URL",
        "HUGEGRAPH_URL",
        "SCALE_KG_NEO4J_URI",
        "NEO4J_HTTP_URL",
        "SCALE_KG_PUPPYGRAPH_URL",
        "PUPPYGRAPH_URL",
    ):
        monkeypatch.delenv(var, raising=False)

    probed_urls: list[str | None] = []

    def fake_probe(url: str | None) -> bool:
        probed_urls.append(url)
        return url in {
            "http://canonical-hugegraph:8080",
            "http://canonical-neo4j:7474",
            "http://canonical-puppygraph:8081",
        }

    monkeypatch.setattr("scale_kg.showcase.introspect._probe_http_endpoint", fake_probe)

    # When unset, iceberg_puppygraph stays verified_local and remote adapters stay offline_remote.
    unset_info = introspect_system(probe_remote=True)
    unset_by_key = {entry.key: entry.availability for entry in unset_info.adapters}
    assert unset_by_key["iceberg_puppygraph"] == "verified_local"
    assert unset_by_key["hugegraph"] == "offline_remote"
    assert unset_by_key["neo4j"] == "offline_remote"

    # Canonical SCALE_KG_* env vars take precedence over fallback env vars and upgrade to live_remote.
    probed_urls.clear()
    monkeypatch.setenv("SCALE_KG_HUGEGRAPH_URL", "http://canonical-hugegraph:8080")
    monkeypatch.setenv("HUGEGRAPH_URL", "http://fallback-hugegraph:8080")
    monkeypatch.setenv("SCALE_KG_NEO4J_URI", "http://canonical-neo4j:7474")
    monkeypatch.setenv("NEO4J_HTTP_URL", "http://fallback-neo4j:7474")
    monkeypatch.setenv("SCALE_KG_PUPPYGRAPH_URL", "http://canonical-puppygraph:8081")
    monkeypatch.setenv("PUPPYGRAPH_URL", "http://fallback-puppygraph:8081")

    live_info = introspect_system(probe_remote=True)
    live_by_key = {entry.key: entry.availability for entry in live_info.adapters}
    assert live_by_key["hugegraph"] == "live_remote"
    assert live_by_key["neo4j"] == "live_remote"
    assert live_by_key["iceberg_puppygraph"] == "live_remote"
    assert "http://canonical-hugegraph:8080" in probed_urls
    assert "http://canonical-neo4j:7474" in probed_urls
    assert "http://canonical-puppygraph:8081" in probed_urls
    assert "http://fallback-hugegraph:8080" not in probed_urls
    assert "http://fallback-puppygraph:8081" not in probed_urls
