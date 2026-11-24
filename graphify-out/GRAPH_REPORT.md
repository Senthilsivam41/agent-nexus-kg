# Graph Report - scale-kg  (2026-11-14)

## Corpus Check
- Corpus is ~23,134 words - fits in a single context window. You may not need a graph.

## Summary
- 583 nodes · 1513 edges · 28 communities (16 shown, 12 thin omitted)
- Extraction: 82% EXTRACTED · 18% INFERRED · 0% AMBIGUOUS · INFERRED: 265 edges (avg confidence: 0.9)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- Iceberg & Traversal Interface
- HugeGraph & PyArrow I/O
- Guarded BFS & Bucket Collapse
- InMemory & Tier-2 Stubs
- Fault Injection & Cost Profiles
- Adapter Contract & Parquet Tests
- Neo4j Composite & Proxy Adapter
- Core Config, Guardrails & Supernodes
- Portfolio Manifest Metadata
- GitHub Actions CI Workflow
- GraphStore Abstract Protocol
- Async Cypher Driver Protocols
- Multi-Engine Adapter Spec & E2
- E1 Guardrails & Supernode Spec
- Benchmark & Gate 2 ADR Plan
- Hybrid GraphRAG & API Roadmap
- Lean Semantic Graph Architecture
- Proposal & Design Documents
- Cross-Shard Federation & Routing
- Roadmap & NebulaGraph Evaluation
- Supernode Query Service Design
- Repository Agent Instructions
- Core Domain Ontology Schema
- Production SLOs & K8s Topology
- Python Package Configuration
- Hybrid Bulk & Stream Ingestion
- Epic E1 & E2 Task Tracker

## God Nodes (most connected - your core abstractions)
1. `EntityNode` - 67 edges
2. `TraversalRequest` - 66 edges
3. `RelationshipEdge` - 64 edges
4. `IcebergPuppyGraphStore` - 59 edges
5. `Domain` - 59 edges
6. `GuardrailEnforcer` - 49 edges
7. `Neo4jGraphStore` - 46 edges
8. `HugeGraphStore` - 44 edges
9. `InMemoryGraphStore` - 39 edges
10. `SupernodeBucketer` - 33 edges

## Surprising Connections (you probably didn't know these)
- `_build_adapter()` --calls--> `HugeGraphStore`  [INFERRED]
  tests/test_adapters_and_golden_queries.py → src/scale_kg/adapters/hugegraph_adapter.py
- `_build_adapter()` --calls--> `IcebergPuppyGraphStore`  [INFERRED]
  tests/test_adapters_and_golden_queries.py → src/scale_kg/adapters/iceberg_puppygraph.py
- `_build_adapter()` --calls--> `Neo4jGraphStore`  [INFERRED]
  tests/test_adapters_and_golden_queries.py → src/scale_kg/adapters/neo4j_adapter.py
- `_build_adapter()` --calls--> `JanusGraphStore`  [INFERRED]
  tests/test_adapters_and_golden_queries.py → src/scale_kg/adapters/stubs.py
- `_build_adapter()` --calls--> `NebulaGraphStore`  [INFERRED]
  tests/test_adapters_and_golden_queries.py → src/scale_kg/adapters/stubs.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Candidate GraphStore Storage Engines** — readme_apache_hugegraph, readme_janusgraph_scylladb, readme_neo4j, docs_design_nebulagraph, docs_superpowers_specs_2026_11_12_scale_kg_design_iceberg_puppygraph_adapter [INFERRED 0.95]
- **4-Layer Scale-KG Runtime Pipeline** — docs_superpowers_plans_2026_11_12_scale_kg_plan_fastapi_service_and_docker_compose, docs_superpowers_plans_2026_11_12_scale_kg_plan_query_coordinator_and_hybrid_rag, docs_superpowers_plans_2026_11_12_scale_kg_plan_graphstore_adapters, docs_superpowers_plans_2026_11_12_scale_kg_plan_lakehouse_content_store [EXTRACTED 1.00]
- **Gate 2 PoC Benchmark & Engine Selection Flow** — docs_proposal_scaling_the_semantic_knowledge_graph_project_proposal_seven_poc_workloads, docs_proposal_scaling_the_semantic_knowledge_graph_project_proposal_gate2_engine_decision, docs_superpowers_plans_2026_11_12_scale_kg_plan_benchmark_suite_and_generator [INFERRED 0.95]

## Communities (28 total, 12 thin omitted)

### Community 0 - "Iceberg & Traversal Interface"
Cohesion: 0.05
Nodes (14): IcebergPuppyGraphStore, _fetch_in(), _fetch_node(), _fetch_out(), _fetch_node(), _fetch_out(), PartitionScanStats, RuntimeGuardrailTracker (+6 more)

### Community 1 - "HugeGraph & PyArrow I/O"
Cohesion: 0.06
Nodes (8): canonical_domain_from_id(), FaultMode, FaultState, GraphStats, GraphStoreError, GraphStoreUnavailableError, HugeGraphStore, Domain

### Community 2 - "Guarded BFS & Bucket Collapse"
Cohesion: 0.07
Nodes (14): resolve_logical_neighbors(), _collect_from_edges(), run_guarded_bfs(), _abort(), _locality_ratio(), _unwrap_edge(), RelationshipEdge, SupernodeBucketer (+6 more)

### Community 3 - "InMemory & Tier-2 Stubs"
Cohesion: 0.06
Nodes (8): GuardrailEnforcer, GuardrailViolationError, TraversalRequest, TestE21GraphStoreProtocolAndInMemory, TestE22IcebergPuppyGraphValidation, TestE23HugeGraphValidation, TestLeanGraphWriteDiscipline, TestPreFlightAndRuntimeGuardrails

### Community 4 - "Fault Injection & Cost Profiles"
Cohesion: 0.07
Nodes (9): AdapterCostProfile, HealthStatus, InMemoryGraphStore, _ContractStubBase, JanusGraphStore, NebulaGraphStore, _fixture(), main() (+1 more)

### Community 5 - "Adapter Contract & Parquet Tests"
Cohesion: 0.06
Nodes (12): _build_adapter(), _HugeGraphMockTransport, _Neo4jMockDriver, _Neo4jMockSession, _sample_graph(), TestHugeGraphAdapterSpecifics, TestInMemoryAdapterSpecifics, TestNeo4jAndStubsSpecifics (+4 more)

### Community 6 - "Neo4j Composite & Proxy Adapter"
Cohesion: 0.12
Nodes (4): Neo4jGraphStore, EntityNode, _Neo4jMockResult, TestDomainAndModels

### Community 7 - "Core Config, Guardrails & Supernodes"
Cohesion: 0.10
Nodes (5): GuardrailConfig, LatencyBudgetConfig, ScaleKGConfig, GuardrailViolationCode, TestConfigAndLatencyBudgets

### Community 8 - "Portfolio Manifest Metadata"
Cohesion: 0.07
Nodes (26): check, demo, dev, setup, architecture, configuration, quickstart, consumes (+18 more)

### Community 9 - "GitHub Actions CI Workflow"
Cohesion: 0.10
Nodes (19): env, runs-on, steps, timeout-minutes, CI, jobs, check, name (+11 more)

### Community 11 - "Async Cypher Driver Protocols"
Cohesion: 0.13
Nodes (3): AsyncCypherDriver, AsyncCypherResult, AsyncCypherSession

### Community 12 - "Multi-Engine Adapter Spec & E2"
Cohesion: 0.20
Nodes (9): GraphStore Protocol (DESIGN.md), GraphStore ABC & Multi-Engine Adapters (Task 3), Iceberg + PuppyGraph Zero-ETL Adapter, Apache HugeGraph (Primary), Pluggable GraphStore Interface, JanusGraph + ScyllaDB (Secondary), Kinetic-V Multi-Hop GraphRAG, Neo4j (Baseline) (+1 more)

### Community 13 - "E1 Guardrails & Supernode Spec"
Cohesion: 0.33
Nodes (4): GuardrailEnforcer & Core Domain Models (Task 1), SupernodeBucketer (Task 2), Query Guardrails & Gate 2 Latency Budgets, Epic E1 (SEN-61): Core Domain Model, Guardrails & Supernode Mitigation

### Community 14 - "Benchmark & Gate 2 ADR Plan"
Cohesion: 0.50
Nodes (4): Engine Feasibility Validation (Oct 2026), Gate 2 Engine Decision (ADR), 7-Workload Benchmark & PoC Plan, PowerLawGraphGenerator, GoldenQuerySuite & BenchmarkSuite (Task 5)

### Community 15 - "Hybrid GraphRAG & API Roadmap"
Cohesion: 0.40
Nodes (4): Five-Stage GraphRAG Retrieval Flow, FastAPI Service & Per-Engine Docker Compose (Task 6), LakehouseContentStore (Task 4), EntityDirectory, QueryCoordinator & HybridGraphRAGRouter (Task 4)

### Community 17 - "Proposal & Design Documents"
Cohesion: 0.67
Nodes (3): Scaling the Semantic Knowledge Graph Project Proposal, Scale-KG Implementation Plan, Scale-KG Phase 0 & Phase 1 Design Spec

## Knowledge Gaps
- **22 isolated node(s):** `scale-kg`, `AGENTS.md Repository Instructions`, `Supernode-Safe Query Service`, `Hybrid Bulk & Streaming Ingestion`, `Kinetic-V Multi-Hop GraphRAG` (+17 more)
  These have ≤1 connection - possible missing edges or undocumented components. (Counts symbols only; 223 node(s) total have ≤1 connection when file, concept and rationale nodes are included.)
- **12 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `TraversalRequest` connect `InMemory & Tier-2 Stubs` to `Iceberg & Traversal Interface`, `HugeGraph & PyArrow I/O`, `Guarded BFS & Bucket Collapse`, `Fault Injection & Cost Profiles`, `Adapter Contract & Parquet Tests`, `Neo4j Composite & Proxy Adapter`, `Core Config, Guardrails & Supernodes`, `GraphStore Abstract Protocol`?**
  _High betweenness centrality (0.121) - this node is a cross-community bridge._
- **Are the 18 inferred relationships involving `EntityNode` (e.g. with `GraphStore` and `HugeGraphStore`) actually correct?**
  _`EntityNode` has 18 INFERRED edges - model-reasoned connections that need verification._
- **What connects `scale-kg`, `AGENTS.md Repository Instructions`, `Supernode-Safe Query Service` to the rest of the system?**
  _22 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Iceberg & Traversal Interface` be split into smaller, more focused modules?**
  _Cohesion score 0.05042016806722689 - nodes in this community are weakly interconnected._
- **Why does `EntityNode` connect `Neo4j Composite & Proxy Adapter` to `Iceberg & Traversal Interface`, `HugeGraph & PyArrow I/O`, `Guarded BFS & Bucket Collapse`, `InMemory & Tier-2 Stubs`, `Fault Injection & Cost Profiles`, `Adapter Contract & Parquet Tests`, `Core Config, Guardrails & Supernodes`, `GraphStore Abstract Protocol`?**
  _High betweenness centrality (0.084) - this node is a cross-community bridge._
- **Are the 31 inferred relationships involving `TraversalRequest` (e.g. with `GraphStore` and `HugeGraphStore`) actually correct?**
  _`TraversalRequest` has 31 INFERRED edges - model-reasoned connections that need verification._
- **Should `HugeGraph & PyArrow I/O` be split into smaller, more focused modules?**
  _Cohesion score 0.06151742993848257 - nodes in this community are weakly interconnected._