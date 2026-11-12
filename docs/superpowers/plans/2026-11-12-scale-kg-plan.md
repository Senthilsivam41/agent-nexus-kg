# Scale-KG (Kinetic-V Semantic Knowledge Graph) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Phase 0 `GraphStore` SDK (guardrails, supernode bucketing, semantic domain routing, hybrid Vector+KG+Lakehouse retrieval) and Phase 1 PoC Benchmark Suite (power-law generator, Tier-1 engine adapters including Iceberg+PuppyGraph, 7 benchmark workloads, FastAPI service, and Docker Compose profiles).

**Architecture:** A 4-layer modular Python monorepo (`scale_kg`): Layer 1 FastAPI & Hybrid GraphRAG Router; Layer 2 Query Coordinator with fixed guardrails (`depth≤3`, `results≤5000`, `timeout≤2.0s`, `expanded_nodes≤50000`), `SupernodeBucketer` (`bucket(node_id, 16)`), and `EntityDirectory` across 4 semantic domains (`customer`, `knowledge`, `technology`, `product`); Layer 3 async `GraphStore` adapters (`InMemory`, `Neo4j`, `HugeGraph`, `IcebergPuppyGraph` zero-ETL with local PyArrow/Parquet execution + remote Cypher/Gremlin, plus `JanusGraph` and `NebulaGraph` stubs); Layer 4 Lakehouse authoritative content store and 7-workload benchmark runner generating Gate 2 ADR reports.

**Tech Stack:** Python 3.11+, `pydantic>=2.0`, `fastapi`, `uvicorn`, `httpx`, `pyarrow`, `neo4j`, `pytest`, `pytest-asyncio`, Docker Compose (per-engine profiles).

## Global Constraints

- Semantic domains fixed to: `customer`, `knowledge`, `technology`, `product`.
- Query guardrails enforced on every query: `max_depth=3`, `max_results=5000`, `max_execution_seconds=2.0`, `max_expanded_nodes=50000`.
- Gate 2 latency budgets: 1-hop P95 ≤ 25.0 ms, 2-hop P95 ≤ 100.0 ms, 3-hop P95 ≤ 300.0 ms.
- Lean Graph Rule: Reject non-routing payload fields (`content`, `raw_text`, `embedding`, `history_blob`) on `EntityNode` and reject unmaterialized derived edges (`is_derived=True` without `materialized_hot_path=True`) on write.
- Supernode bucketing uses deterministic `bucket(id, 16)` (`hash(id) % 16`) intermediate bucket nodes (`<hub_id>::bucket::<0..15>`).

---

### Task 1: Project Scaffolding, Configuration & Core Domain Models

**Files:**
- Create: `pyproject.toml`
- Create: `src/scale_kg/__init__.py`
- Create: `src/scale_kg/config.py`
- Create: `src/scale_kg/core/__init__.py`
- Create: `src/scale_kg/core/models.py`
- Create: `src/scale_kg/core/guardrails.py`
- Test: `tests/test_models_and_guardrails.py`

**Interfaces:**
- Produces:
  - `Domain` (`Enum`: `CUSTOMER`, `KNOWLEDGE`, `TECHNOLOGY`, `PRODUCT`)
  - `EntityNode(id: str, domain: Domain, entity_type: str, routing_props: dict[str, str | int | float | bool], lakehouse_ref: str | None)`
  - `RelationshipEdge(source_id: str, target_id: str, edge_type: str, domain: Domain, target_domain: Domain | None, is_derived: bool, materialized_hot_path: bool, routing_props: dict[str, str | int | float | bool])`
  - `GuardrailConfig`, `LatencyBudgetConfig`, `TraversalRequest`, `TraversalResult`, `TraversalMetrics`
  - `GuardrailEnforcer.validate_request(req: TraversalRequest) -> None`
  - `GuardrailEnforcer.validate_node_write(node: EntityNode) -> None`
  - `GuardrailEnforcer.validate_edge_write(edge: RelationshipEdge) -> None`

- [ ] **Step 1: Write failing tests for models and guardrails** (`tests/test_models_and_guardrails.py`)
- [ ] **Step 2: Run `pytest tests/test_models_and_guardrails.py -v` to confirm failure**
- [ ] **Step 3: Implement `pyproject.toml`, `config.py`, `core/models.py`, and `core/guardrails.py`**
- [ ] **Step 4: Run `pytest tests/test_models_and_guardrails.py -v` and verify PASS**

---

### Task 2: Deterministic Supernode Bucketing (`bucket(node_id, 16)`)

**Files:**
- Create: `src/scale_kg/core/supernode.py`
- Test: `tests/test_supernode_bucketing.py`

**Interfaces:**
- Consumes: `EntityNode`, `RelationshipEdge`, `Domain` from `scale_kg.core.models`
- Produces:
  - `compute_bucket_id(node_id: str, num_buckets: int = 16) -> int` (deterministic CRC32/MD5 mod 16)
  - `SupernodeBucketer(degree_threshold: int = 100, num_buckets: int = 16)`
  - `SupernodeBucketer.transform_Dataset(nodes: list[EntityNode], edges: list[RelationshipEdge]) -> tuple[list[EntityNode], list[RelationshipEdge], set[str]]`
  - `SupernodeBucketer.is_bucket_node_id(node_id: str) -> bool`
  - `SupernodeBucketer.unwrap_bucket_id(node_id: str) -> str`

- [ ] **Step 1: Write failing tests for supernode bucket splitting and unwrapping** (`tests/test_supernode_bucketing.py`)
- [ ] **Step 2: Run `pytest tests/test_supernode_bucketing.py -v` to confirm failure**
- [ ] **Step 3: Implement `src/scale_kg/core/supernode.py`**
- [ ] **Step 4: Run `pytest tests/test_supernode_bucketing.py -v` and verify PASS**

---

### Task 3: `GraphStore` Abstract Interface, `InMemoryGraphStore` & Tier-1/Tier-2 Adapters

**Files:**
- Create: `src/scale_kg/adapters/__init__.py`
- Create: `src/scale_kg/adapters/base.py`
- Create: `src/scale_kg/adapters/in_memory.py`
- Create: `src/scale_kg/adapters/neo4j_adapter.py`
- Create: `src/scale_kg/adapters/hugegraph_adapter.py`
- Create: `src/scale_kg/adapters/iceberg_puppygraph.py`
- Create: `src/scale_kg/adapters/stubs.py`
- Test: `tests/test_adapters_and_golden_queries.py`
- Test: `tests/test_iceberg_puppygraph.py`

**Interfaces:**
- Consumes: `EntityNode`, `RelationshipEdge`, `Domain`, `TraversalRequest`, `TraversalResult`, `GuardrailConfig`
- Produces:
  - `GraphStore` ABC (`upsert_nodes`, `upsert_edges`, `get_node`, `get_neighbors`, `traverse_k_hop`, `set_fault_mode`, `health_check`, `count_Stats`, `clear`)
  - `InMemoryGraphStore(GraphStore)` (with transparent bucket-node hop collapses & fault simulation)
  - `IcebergPuppyGraphStore(GraphStore)` (writes PyArrow Parquet/Iceberg-layout tables partitioned by `domain` and `bucket_16`, generates PuppyGraph JSON schema mapping `puppygraph_schema.json`, executes partition-pruned zero-ETL graph traversal locally over PyArrow or delegates to PuppyGraph Cypher/Gremlin endpoint when configured)
  - `Neo4jGraphStore(GraphStore)` (Cypher generation, proxy node cross-shard linking, async driver integration + mockable transport)
  - `HugeGraphStore(GraphStore)` (HugeGraph REST/Gremlin/Cypher client + mockable transport)
  - `JanusGraphStore(GraphStore)` and `NebulaGraphStore(GraphStore)` contract stubs

- [ ] **Step 1: Write failing tests for `InMemoryGraphStore`, `IcebergPuppyGraphStore` (partition pruning + PuppyGraph schema generation), `Neo4jGraphStore`, `HugeGraphStore`, and stubs**
- [ ] **Step 2: Run `pytest tests/test_adapters_and_golden_queries.py tests/test_iceberg_puppygraph.py -v` to confirm failure**
- [ ] **Step 3: Implement `base.py`, `in_memory.py`, `iceberg_puppygraph.py`, `neo4j_adapter.py`, `hugegraph_adapter.py`, and `stubs.py`**
- [ ] **Step 4: Run `pytest tests/test_adapters_and_golden_queries.py tests/test_iceberg_puppygraph.py -v` and verify PASS**

---

### Task 4: Entity Directory, Federated Query Coordinator & Hybrid GraphRAG + Lakehouse Store

**Files:**
- Create: `src/scale_kg/routing/__init__.py`
- Create: `src/scale_kg/routing/directory.py`
- Create: `src/scale_kg/routing/coordinator.py`
- Create: `src/scale_kg/routing/hybrid_rag.py`
- Create: `src/scale_kg/lakehouse/__init__.py`
- Create: `src/scale_kg/lakehouse/store.py`
- Test: `tests/test_routing_and_coordinator.py`
- Test: `tests/test_hybrid_rag_and_lakehouse.py`

**Interfaces:**
- Produces:
  - `EntityDirectory`: registers/resolves `entity_id -> Domain`, tracks cross-domain proxy links.
  - `QueryCoordinator`: enforces `GuardrailEnforcer`, routes hops by `EntityDirectory`, computes on-the-fly derived similarity/transitive edges at query time, tracks `cross_domain_hops` and `single_domain_locality_ratio`.
  - `LakehouseContentStore`: stores & retrieves authoritative document text and historical graph slices via PyArrow Parquet (`lakehouse_ref`).
  - `HybridGraphRAGRouter`: combines cosine vector similarity seed retrieval + `QueryCoordinator` 1-to-3 hop structural expansion + `LakehouseContentStore` hydration.

- [ ] **Step 1: Write failing tests in `tests/test_routing_and_coordinator.py` and `tests/test_hybrid_rag_and_lakehouse.py`**
- [ ] **Step 2: Run `pytest tests/test_routing_and_coordinator.py tests/test_hybrid_rag_and_lakehouse.py -v` to confirm failure**
- [ ] **Step 3: Implement `directory.py`, `coordinator.py`, `lakehouse/store.py`, and `hybrid_rag.py`**
- [ ] **Step 4: Run `pytest tests/test_routing_and_coordinator.py tests/test_hybrid_rag_and_lakehouse.py -v` and verify PASS**

---

### Task 5: Synthetic Power-Law Graph Generator, Golden Queries, 7-Workload Benchmark Runner & Gate 2 ADR Generator

**Files:**
- Create: `src/scale_kg/benchmark/__init__.py`
- Create: `src/scale_kg/benchmark/generator.py`
- Create: `src/scale_kg/benchmark/golden_queries.py`
- Create: `src/scale_kg/benchmark/workloads.py`
- Create: `src/scale_kg/benchmark/report.py`
- Test: `tests/test_generator_and_benchmark.py`

**Interfaces:**
- Produces:
  - `PowerLawGraphGenerator`: generates synthetic power-law graphs across the 4 domains with deliberate supernodes (`tech::Python`, `loc::India`), configurable `cross_domain_edge_share` (`0.05`, `0.15`, `0.30`), and optional derived edges to test write discipline.
  - `GoldenQuerySuite`: runs identical GraphRAG retrieval queries across `InMemoryGraphStore` and `IcebergPuppyGraphStore` to verify 100% deterministic parity (`engine_independence`).
  - `BenchmarkSuite`: executes all 7 proposal workloads (1/2/3-hop latency, supernode traversal, concurrent ramp, bulk/stream ingestion, cross-partition cost ratio, HA node-failure recovery, cost-per-billion-edges) and outputs a Gate 2 ADR markdown document (`generate_gate2_adr`).

- [ ] **Step 1: Write failing tests in `tests/test_generator_and_benchmark.py`**
- [ ] **Step 2: Run `pytest tests/test_generator_and_benchmark.py -v` to confirm failure**
- [ ] **Step 3: Implement `generator.py`, `golden_queries.py`, `workloads.py`, and `report.py`**
- [ ] **Step 4: Run `pytest tests/test_generator_and_benchmark.py -v` and verify PASS**

---

### Task 6: FastAPI Service, CLI Runner & Per-Engine Docker Compose Profiles

**Files:**
- Create: `src/scale_kg/api/__init__.py`
- Create: `src/scale_kg/api/app.py`
- Create: `docker-compose.yml`
- Test: `tests/test_api.py`

**Interfaces:**
- Produces:
  - FastAPI application (`create_app()`) with endpoints:
    - `GET /v1/health`
    - `POST /v1/ingest`
    - `POST /v1/traverse`
    - `POST /v1/hybrid-retrieve`
    - `POST /v1/benchmark/run`
  - `docker-compose.yml` with isolated profiles: `core`, `neo4j`, `hugegraph`, `puppygraph`, `janusgraph`, `nebula`.

- [ ] **Step 1: Write failing API tests in `tests/test_api.py`**
- [ ] **Step 2: Run `pytest tests/test_api.py -v` to confirm failure**
- [ ] **Step 3: Implement `src/scale_kg/api/app.py` and `docker-compose.yml`**
- [ ] **Step 4: Run full test suite `pytest -v` and verify 100% PASS**
