"""Tests for core domain models, config budgets, and pre-flight/runtime/write guardrails."""

from __future__ import annotations

import pytest

from scale_kg.config import GuardrailConfig, LatencyBudgetConfig, ScaleKGConfig
from scale_kg.core.guardrails import (
    GuardrailEnforcer,
    GuardrailViolationCode,
    GuardrailViolationError,
)
from scale_kg.core.models import (
    Domain,
    EntityNode,
    RelationshipEdge,
    TraversalMetrics,
    TraversalRequest,
    TraversalResult,
)


class TestDomainAndModels:
    def test_domain_enum_covers_four_canonical_domains(self) -> None:
        assert [d.value for d in Domain] == [
            "customer",
            "knowledge",
            "technology",
            "product",
        ]

    def test_entity_node_and_relationship_edge_defaults(self) -> None:
        node = EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
            routing_props={"active": True, "rank": 1},
        )
        assert node.lakehouse_ref is None
        assert node.is_proxy is False
        assert node.target_domain is None
        assert node.is_bucket is False

        edge = RelationshipEdge(
            source_id="customer:org:acme",
            target_id="technology:lang:python",
            edge_type="USES",
            domain=Domain.CUSTOMER,
            target_domain=Domain.TECHNOLOGY,
        )
        assert edge.is_derived is False
        assert edge.materialized_hot_path is False
        assert edge.routing_props == {}


class TestConfigAndLatencyBudgets:
    def test_guardrail_defaults_match_proposal(self) -> None:
        cfg = GuardrailConfig()
        assert cfg.max_depth == 3
        assert cfg.max_results == 5000
        assert cfg.max_execution_seconds == 2.0
        assert cfg.max_expanded_nodes == 50000

    def test_latency_budget_gate2_p95_targets(self) -> None:
        budgets = LatencyBudgetConfig()
        assert budgets.hop_1_p95_ms == 25.0
        assert budgets.hop_2_p95_ms == 100.0
        assert budgets.hop_3_p95_ms == 300.0
        assert budgets.hard_timeout_ms == 2000.0

        assert budgets.budget_for_depth(1) == 25.0
        assert budgets.budget_for_depth(2) == 100.0
        assert budgets.budget_for_depth(3) == 300.0
        assert budgets.is_within_budget(1, 24.9) is True
        assert budgets.is_within_budget(1, 25.1) is False
        assert budgets.is_within_budget(2, 100.0) is True
        assert budgets.is_within_budget(3, 300.1) is False

        with pytest.raises(ValueError):
            budgets.budget_for_depth(4)

    def test_scale_kg_config_combines_subconfigs(self) -> None:
        cfg = ScaleKGConfig()
        assert cfg.guardrails.max_depth == 3
        assert cfg.latency_budgets.hop_2_p95_ms == 100.0
        assert cfg.num_buckets == 16


class TestLeanGraphWriteDiscipline:
    @pytest.mark.parametrize(
        "forbidden_key",
        ["content", "raw_text", "embedding", "history_blob", "Content", "RAW_TEXT"],
    )
    def test_rejects_heavy_payload_keys_on_node_write(self, forbidden_key: str) -> None:
        enforcer = GuardrailEnforcer()
        node = EntityNode(
            id="knowledge:doc:1",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            routing_props={forbidden_key: "heavy-payload"},
            lakehouse_ref="s3://lakehouse/docs/1.parquet",
        )
        with pytest.raises(GuardrailViolationError) as exc_info:
            enforcer.validate_node_write(node)

        v = exc_info.value.violation
        assert v.code == GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD
        assert v.phase == "write"
        assert v.entity_id == "knowledge:doc:1"

    def test_requires_lakehouse_ref_for_out_of_graph_content_nodes(self) -> None:
        enforcer = GuardrailEnforcer()
        doc_without_ref = EntityNode(
            id="knowledge:doc:missing-ref",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            routing_props={"active": True},
        )
        with pytest.raises(GuardrailViolationError) as exc_info:
            enforcer.validate_node_write(doc_without_ref)
        assert exc_info.value.violation.code == GuardrailViolationCode.LEAN_GRAPH_MISSING_LAKEHOUSE_REF

        flagged_without_ref = EntityNode(
            id="product:item:42",
            domain=Domain.PRODUCT,
            entity_type="Product",
            routing_props={"has_content": True},
            lakehouse_ref="   ",
        )
        with pytest.raises(GuardrailViolationError) as exc_info2:
            enforcer.validate_node_write(flagged_without_ref)
        assert exc_info2.value.violation.code == GuardrailViolationCode.LEAN_GRAPH_MISSING_LAKEHOUSE_REF

        valid_doc = EntityNode(
            id="knowledge:doc:ok",
            domain=Domain.KNOWLEDGE,
            entity_type="Document",
            routing_props={"active": True},
            lakehouse_ref="s3://lakehouse/docs/ok.parquet",
        )
        enforcer.validate_node_write(valid_doc)

    def test_allows_structural_nodes_without_lakehouse_ref(self) -> None:
        enforcer = GuardrailEnforcer()
        node = EntityNode(
            id="technology:lang:python",
            domain=Domain.TECHNOLOGY,
            entity_type="Language",
            routing_props={"active": True, "weight": 1.0},
        )
        enforcer.validate_node_write(node)

    def test_rejects_unmaterialized_derived_edges(self) -> None:
        enforcer = GuardrailEnforcer()
        derived_unmaterialized = RelationshipEdge(
            source_id="customer:org:1",
            target_id="customer:org:2",
            edge_type="SIMILAR_TO",
            domain=Domain.CUSTOMER,
            is_derived=True,
            materialized_hot_path=False,
        )
        with pytest.raises(GuardrailViolationError) as exc_info:
            enforcer.validate_edge_write(derived_unmaterialized)
        assert exc_info.value.violation.code == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE
        assert exc_info.value.violation.phase == "write"

        # Even if caller omits is_derived=True on a known derived edge type
        implicit_derived = RelationshipEdge(
            source_id="customer:org:1",
            target_id="technology:lib:fastapi",
            edge_type="INDIRECTLY_USES",
            domain=Domain.CUSTOMER,
            target_domain=Domain.TECHNOLOGY,
            is_derived=False,
            materialized_hot_path=False,
        )
        with pytest.raises(GuardrailViolationError) as exc_info2:
            enforcer.validate_edge_write(implicit_derived)
        assert exc_info2.value.violation.code == GuardrailViolationCode.UNMATERIALIZED_DERIVED_EDGE

        # Materialized hot-path derived edges are allowed
        materialized = RelationshipEdge(
            source_id="customer:org:1",
            target_id="customer:org:2",
            edge_type="SIMILAR_TO",
            domain=Domain.CUSTOMER,
            is_derived=True,
            materialized_hot_path=True,
        )
        enforcer.validate_edge_write(materialized)

    def test_rejects_heavy_payload_keys_on_edge_write(self) -> None:
        enforcer = GuardrailEnforcer()
        edge = RelationshipEdge(
            source_id="a",
            target_id="b",
            edge_type="USES",
            domain=Domain.TECHNOLOGY,
            routing_props={"raw_text": "bad"},
        )
        with pytest.raises(GuardrailViolationError) as exc_info:
            enforcer.validate_edge_write(edge)
        assert exc_info.value.violation.code == GuardrailViolationCode.LEAN_GRAPH_FORBIDDEN_PAYLOAD


class TestPreFlightAndRuntimeGuardrails:
    def test_preflight_accepts_valid_request(self) -> None:
        enforcer = GuardrailEnforcer()
        req = TraversalRequest(
            seed_ids=["customer:org:1"],
            max_depth=3,
            max_results=5000,
            max_execution_seconds=2.0,
            max_expanded_nodes=50000,
        )
        enforcer.validate_request(req)

    def test_preflight_rejects_empty_seeds_and_invalid_depth(self) -> None:
        enforcer = GuardrailEnforcer()
        with pytest.raises(GuardrailViolationError) as exc_empty:
            enforcer.validate_request(TraversalRequest(seed_ids=[], max_depth=2))
        assert exc_empty.value.violation.code == GuardrailViolationCode.EMPTY_SEED_IDS
        assert exc_empty.value.violation.phase == "preflight"

        with pytest.raises(GuardrailViolationError) as exc_zero:
            enforcer.validate_request(TraversalRequest(seed_ids=["a"], max_depth=0))
        assert exc_zero.value.violation.code == GuardrailViolationCode.INVALID_DEPTH

        with pytest.raises(GuardrailViolationError) as exc_deep:
            enforcer.validate_request(TraversalRequest(seed_ids=["a"], max_depth=4))
        v = exc_deep.value.violation
        assert v.code == GuardrailViolationCode.MAX_DEPTH_EXCEEDED
        assert v.limit == 3
        assert v.observed == 4

    @pytest.mark.parametrize(
        ("field", "value", "expected_code"),
        [
            ("max_results", 5001, GuardrailViolationCode.MAX_RESULTS_EXCEEDED),
            ("max_execution_seconds", 2.5, GuardrailViolationCode.MAX_EXECUTION_TIME_EXCEEDED),
            ("max_expanded_nodes", 50001, GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED),
        ],
    )
    def test_preflight_rejects_budget_overrides_above_global_caps(
        self, field: str, value: int | float, expected_code: GuardrailViolationCode
    ) -> None:
        enforcer = GuardrailEnforcer()
        req = TraversalRequest(seed_ids=["customer:org:1"], **{field: value})
        with pytest.raises(GuardrailViolationError) as exc_info:
            enforcer.validate_request(req)
        assert exc_info.value.violation.code == expected_code
        assert exc_info.value.violation.phase == "preflight"

    def test_runtime_tracker_aborts_cleanly_with_structured_metadata(self) -> None:
        now = [100.0]
        enforcer = GuardrailEnforcer(clock=lambda: now[0])
        req = TraversalRequest(
            seed_ids=["customer:org:1"],
            max_depth=2,
            max_results=10,
            max_execution_seconds=0.5,
            max_expanded_nodes=100,
        )
        tracker = enforcer.create_runtime_tracker(req)

        # Within budget
        assert tracker.check_step(depth=1, expanded_nodes=25, results_count=5) is None

        # Exceeds expanded nodes
        v_nodes = tracker.check_step(depth=2, expanded_nodes=101, results_count=5)
        assert v_nodes is not None
        assert v_nodes.code == GuardrailViolationCode.MAX_EXPANDED_NODES_EXCEEDED
        assert v_nodes.phase == "runtime"
        assert v_nodes.limit == 100
        assert v_nodes.observed == 101

        # Exceeds results
        v_results = tracker.check_step(depth=2, expanded_nodes=50, results_count=11)
        assert v_results is not None
        assert v_results.code == GuardrailViolationCode.MAX_RESULTS_EXCEEDED

        # Exceeds execution time
        now[0] = 100.6
        v_time = tracker.check_step(depth=1, expanded_nodes=10, results_count=1)
        assert v_time is not None
        assert v_time.code == GuardrailViolationCode.MAX_EXECUTION_TIME_EXCEEDED
        assert v_time.limit == 0.5

        # Clean abort result builder preserves structured metadata
        aborted = tracker.build_aborted_result(
            violation=v_time,
            nodes=[],
            edges=[],
            paths=[],
            depth_reached=1,
            expanded_nodes=10,
        )
        assert isinstance(aborted, TraversalResult)
        assert isinstance(aborted.metrics, TraversalMetrics)
        assert aborted.metrics.aborted is True
        assert aborted.metrics.violation == v_time
