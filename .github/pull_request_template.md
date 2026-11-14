## Change and governing controls

Describe the problem and resulting behavior. Cite applicable policy (`AGENTS.md`), ADR (`docs/decisions/`), design spec (`docs/DESIGN.md`), and approved standard IDs/links; identify proposed controls as **proposed**. State `no architecture change` with the governing ADRs/spec when applicable.

## Decisions and conflicts

Link owner approval for new or superseding decisions, or state `none needed`.
Record assumptions, conflicts, compliant alternatives, and unresolved deviations.
An unapproved assumption must not be presented as an accepted decision.

## Verification

Give commands (e.g., `make check`, `make demo`), results, and environment. Distinguish local tests (`MOCK_MODE=true` / in-memory adapters), CI (`.github/workflows/ci.yml`), and live provider or graph-engine evidence (e.g., `ALLOW_LIVE_AI_TESTS=true`, Neo4j, HugeGraph, Iceberg/PuppyGraph). Link changed interface contracts (`src/scale_kg/adapters/base.py`, `src/scale_kg/core/models.py`, `.env.example`, `portfolio.yaml`) and corresponding tests (`tests/`).

## Checklist

- [ ] Change follows applicable active controls (`AGENTS.md`); any required approvals are linked.
- [ ] Affected contracts, documentation (`README.md`, `docs/DESIGN.md`, `docs/decisions/`), and architectural checks are updated.
- [ ] Required checks (`make check`) pass; skipped or unverified work (including live engine or live AI tests) is explicitly stated.
- [ ] No credentials (`.env`, API keys), runtime artifacts, or personal-memory contents are included.

Use a focused branch. If stacked, link the prerequisite PR and explain why.
An implementation PR must not establish approval for its own architectural change.
