"""Configuration models for query guardrails, Gate 2 latency SLAs, and supernode bucketing."""

from __future__ import annotations

from pydantic import BaseModel, Field


class GuardrailConfig(BaseModel):
    """Fixed safety budgets enforced on every graph query."""

    max_depth: int = Field(default=3, ge=1, le=3)
    max_results: int = Field(default=5000, ge=1, le=5000)
    max_execution_seconds: float = Field(default=2.0, gt=0.0, le=2.0)
    max_expanded_nodes: int = Field(default=50000, ge=1, le=50000)


class LatencyBudgetConfig(BaseModel):
    """Gate 2 P95 latency SLA budgets (ms) across hop depths."""

    hop_1_p95_ms: float = Field(default=25.0, gt=0.0)
    hop_2_p95_ms: float = Field(default=100.0, gt=0.0)
    hop_3_p95_ms: float = Field(default=300.0, gt=0.0)
    hard_timeout_ms: float = Field(default=2000.0, gt=0.0)

    def budget_for_depth(self, depth: int) -> float:
        """Return the Gate 2 P95 latency budget in ms for hop depth 1..3."""
        mapping = {
            1: self.hop_1_p95_ms,
            2: self.hop_2_p95_ms,
            3: self.hop_3_p95_ms,
        }
        if depth not in mapping:
            raise ValueError(f"Unsupported hop depth {depth}; expected 1, 2, or 3.")
        return mapping[depth]

    def is_within_budget(self, depth: int, observed_p95_ms: float) -> bool:
        """Check whether an observed P95 latency satisfies the Gate 2 budget."""
        return observed_p95_ms <= self.budget_for_depth(depth)


class ScaleKGConfig(BaseModel):
    """Top-level configuration combining guardrails, latency SLAs, and supernode settings."""

    guardrails: GuardrailConfig = Field(default_factory=GuardrailConfig)
    latency_budgets: LatencyBudgetConfig = Field(default_factory=LatencyBudgetConfig)
    supernode_degree_threshold: int = Field(default=1000, ge=1)
    num_buckets: int = Field(default=16, ge=2)
