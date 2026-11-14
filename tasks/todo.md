# Epic E1 (`SEN-61`): Core Domain Model, Lean Graph Guardrails & Supernode Mitigation

- [x] Project scaffolding (`pyproject.toml`, `scale_kg` package, `config.py`)
- [x] `[E1.1]` `SEN-66`: Domain ontology (`Domain`, `EntityNode`, `RelationshipEdge`) & Lean Graph write discipline (`validate_node_write`, `validate_edge_write`)
- [x] `[E1.2]` `SEN-67`: Pre-flight & runtime query guardrail enforcer (`max_depth=3`, `max_results=5000`, `max_execution_seconds=2.0`, `max_expanded_nodes=50000`), `LatencyBudgetConfig`, structured violation metadata
- [x] `[E1.3]` `SEN-68`: Deterministic supernode bucketing (`compute_bucket_id(node_id, 16)`), `SupernodeRegistry` (seeded hubs + auto-promotion), `SupernodeBucketer` (split, runtime promotion, hop collapse & path unwrapping)
- [x] Verify all E1 unit tests pass via `pytest -v` (29/29 passed)

# Epic E2 (`SEN-62`): Pluggable GraphStore Abstraction & Multi-Engine Adapters

- [x] `[E2.1]` `SEN-69`: Async `GraphStore` ABC (`base.py`), shared guarded BFS & supernode bucket collapse (`_traversal.py`), `InMemoryGraphStore` with fault injection (`in_memory.py`)
- [x] `[E2.2]` `SEN-70`: `IcebergPuppyGraphStore` (`iceberg_puppygraph.py`) with `domain`/`bucket_16` Parquet partitions, Iceberg metadata, `puppygraph_schema.json`, partition-pruned PyArrow scans (`>=50%` I/O reduction), and remote Cypher/Gremlin execution
- [x] `[E2.3]` `SEN-71`: `HugeGraphStore` (`hugegraph_adapter.py`) async REST/Gremlin/OpenCypher client with PD + HStore `domain`/`tenant` placement hints
- [x] `[E2.4]` `SEN-72`: `Neo4jGraphStore` (`neo4j_adapter.py`) async Cypher Composite adapter with cross-shard proxy nodes + `JanusGraphStore` & `NebulaGraphStore` contract stubs (`stubs.py`)
- [x] Verify all E1 + E2 tests pass via `.venv/bin/pytest -v` (103/103 passed, incl. `test_e2_validation.py`)
