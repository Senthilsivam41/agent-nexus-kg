# Epic E1 (`SEN-61`): Core Domain Model, Lean Graph Guardrails & Supernode Mitigation

- [x] Project scaffolding (`pyproject.toml`, `scale_kg` package, `config.py`)
- [x] `[E1.1]` `SEN-66`: Domain ontology (`Domain`, `EntityNode`, `RelationshipEdge`) & Lean Graph write discipline (`validate_node_write`, `validate_edge_write`)
- [x] `[E1.2]` `SEN-67`: Pre-flight & runtime query guardrail enforcer (`max_depth=3`, `max_results=5000`, `max_execution_seconds=2.0`, `max_expanded_nodes=50000`), `LatencyBudgetConfig`, structured violation metadata
- [x] `[E1.3]` `SEN-68`: Deterministic supernode bucketing (`compute_bucket_id(node_id, 16)`), `SupernodeRegistry` (seeded hubs + auto-promotion), `SupernodeBucketer` (split, runtime promotion, hop collapse & path unwrapping)
- [x] Verify all E1 unit tests pass via `pytest -v` (29/29 passed)
