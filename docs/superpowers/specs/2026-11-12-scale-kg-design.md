# Scaling the Semantic Knowledge Graph (Kinetic-V GraphRAG) — Design Specification

**Date:** 2026-11-12  
**Source Proposal:** `docs/proposal/Scaling the Semantic Knowledge Graph Project Proposal.md`  
**NotebookLM Reference:** *Architecting Scalable Semantic GraphRAG Systems* (`Iceberg + PuppyGraph Zero-ETL` & `Hybrid Vector/KG RAG`)

---

## 1. Goal & Scope

Build the **Phase 0 Foundations** and **Phase 1 Benchmark Proof-of-Concept (PoC)** platform (`scale_kg`) to scale the Kinetic-V Semantic Knowledge Graph from 100M to 10B+ relationships while keeping 1-to-3 hop GraphRAG retrieval within strict latency budgets.

### Locked Decisions (Validated via `/grill-me`)
1. **Deliverable Scope:** Phase 0 (`GraphStore` SDK, Query Guardrails, Supernode Bucketing, Semantic Domain Router, Entity Directory, Hybrid Vector+KG+Lakehouse Coordinator) + Phase 1 (Power-law Synthetic Graph Generator, Golden Query Regression Suite, 7-Workload Benchmark Runner, Gate 2 ADR Generator).
2. **Cross-Shard Federation (Branch 1.1):** Ghost/Proxy Nodes stored inside each owning shard (`is_proxy=True`, `target_domain`) so local edge scans immediately inform the `QueryCoordinator` which remote domain shard owns the next hop without querying a central directory per edge.
3. **Global Entity ID & Hop-0 Routing (Branch 1.2):** Domain-prefixed canonical IDs (`<domain>:<entity_type>:<slug>`, e.g., `technology:lang:python`) for O(1) zero-lookup Hop-0 routing, backed by `EntityDirectory` fallback for opaque/legacy IDs and aliases.
4. **Supernode Mechanics (Branch 2.1):** Uniform transparent intermediate bucket vertices (`<hub_id>::bucket::0..15`) across all engines. Bucket hops do **not** count toward the logical 3-hop cap and are unwrapped before returning paths.
5. **Supernode Promotion (Branch 2.2):** `SupernodeRegistry` seeded with known high-cardinality types/hubs (`Language`, `Country`, `technology:lang:python`, `customer:region:india`) plus automatic runtime promotion when node degree crosses `supernode_degree_threshold` (rewriting existing edges into `::bucket::0..15`).
6. **Derived Edges & Hot-Path Materialization (Branch 3):** Declarative derived-edge rules in `QueryCoordinator` (`SIMILAR_TO` via shared neighbors, `INDIRECTLY_USES` via 2-hop composition) computed on-the-fly at query time; unmaterialized derived edges are rejected at write time until query hit frequency promotes them to `materialized_hot_path=True`.
7. **Iceberg + PuppyGraph Zero-ETL Adapter (Branch 4):** Dual-mode execution — writes real partitioned Parquet/Iceberg tables (`domain=<d>/bucket_16=<b>`) and `puppygraph_schema.json`; delegates to live PuppyGraph Cypher/Gremlin endpoint when configured, or executes partition-pruned PyArrow scans locally (tracking scanned vs pruned partitions to verify ≥50% I/O reduction).
8. **Hybrid Vector + KG RAG Pipeline (Branch 5):** Seed-Then-Expand + Reciprocal Rank Fusion (RRF) — vector similarity retrieves top-$K$ seed IDs, `QueryCoordinator` runs guarded 1–3 hop expansion, RRF fuses vector + graph proximity scores, and `LakehouseContentStore` hydrates only the final top-$N$ payloads.
9. **Benchmark Scale Presets & Extrapolator (Branch 6):** Parameterized presets (`micro`, `local`, `cluster`) + empirical 3-step scaling & cost-per-billion-edges extrapolator producing the Gate 2 ADR markdown report.
10. **Semantic Domains & Latency Gates:**
    - 4 shards: `customer`, `knowledge`, `technology`, `product`
    - Latency gates: 1-hop P95 ≤ 25 ms, 2-hop P95 ≤ 100 ms, 3-hop P95 ≤ 300 ms, hard timeout 2,000 ms
    - Guardrails: max depth `3`, max results `5,000`, max execution time `2.0s`, max expanded nodes `50,000`
11. **Adapters Included:**
    - **Tier-1 (Full Implementation):** `InMemoryGraphStore`, `Neo4jGraphStore`, `HugeGraphStore`, `IcebergPuppyGraphStore`
    - **Tier-2 (Contract Stubs):** `JanusGraphStore` (ScyllaDB), `NebulaGraphStore`

---

## 2. System Architecture (4 Layers)

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Layer 1: Reasoning & API Layer (FastAPI / HybridGraphRAGRouter)         │
│  • Vector Seed Lookup  • Query-Time Derived Edges  • Lakehouse Hydrate  │
└───────────────────────────────────┬─────────────────────────────────────┘
                                    │
┌───────────────────────────────────▼─────────────────────────────────────┐
│ Layer 2: Query Coordinator, Guardrails & Semantic Domain Router         │
│  • Guardrail Enforcer (depth≤3, results≤5000, time≤2s, expanded≤50000)  │
│  • SupernodeBucketer (bucket(node_id, 16) hierarchy split)              │
│  • EntityDirectory (Global ID → Domain Shard: customer/knowledge/...)   │
└───────┬───────────────────┬───────────────────────┬─────────────────────┘
        │                   │                       │
┌───────▼───────┐   ┌───────▼───────┐       ┌───────▼─────────────────────┐
│ Layer 3:      │   │ Neo4j /       │       │ Iceberg + PuppyGraph        │
│ GraphStore    │   │ HugeGraph     │       │ Zero-ETL Adapter            │
│ Adapters      │   │ Adapters      │       │ (bucket(id,16) pruning)     │
└───────┬───────┘   └───────┬───────┘       └───────┬─────────────────────┘
        └───────────────────┼───────────────────────┘
                            │ Reference IDs Only (Lean Graph Rule)
┌───────────────────────────▼─────────────────────────────────────────────┐
│ Layer 4: Lakehouse Authoritative Store (Apache Iceberg / Parquet)       │
│  • Document Content Payloads  • Historical Graph Slices  • Analytics    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Three Enforced Design Rules
1. **Lean Graph Rule:** Only properties that decide traversal routing (`domain`, `entity_type`, `weight`, `active`) stay in the graph. Full text/payloads live in the Lakehouse and are fetched by `lakehouse_ref` after traversal.
2. **Core Edges Only:** Derived edges (`SIMILAR_TO`, `INDIRECTLY_USES`) are rejected at write time unless explicitly flagged as materialized hot-paths; similarity and transitive inference run at query time in the Coordinator.
3. **Supernode Bucketing:** High-degree nodes exceeding `supernode_degree_threshold` (default 1,000 in dev, 50,000 in prod) are automatically sharded via deterministic hash buckets `bucket(target_id, 16)` (`<hub_id>::bucket::<0..15>`) so no single adjacency list grows unbounded.

---

## 3. Repository & Module Structure

```
scale-kg/
├── pyproject.toml
├── docker-compose.yml
├── src/
│   └── scale_kg/
│       ├── __init__.py
│       ├── config.py                    # Guardrails, SLA gates, domain & adapter settings
│       ├── core/
│       │   ├── __init__.py
│       │   ├── models.py                # Domain, EntityNode, RelationshipEdge, TraversalRequest/Result
│       │   ├── guardrails.py            # Pre-flight + runtime budget enforcer & lean-graph validator
│       │   └── supernode.py             # Deterministic bucket(node_id, 16) supernode transformer
│       ├── routing/
│       │   ├── __init__.py
│       │   ├── directory.py             # EntityDirectory (ID -> Domain Shard + proxy mapping)
│       │   ├── coordinator.py           # Federated multi-hop QueryCoordinator + locality metrics
│       │   └── hybrid_rag.py            # Vector + GraphStore + Lakehouse content hydration pipeline
│       ├── adapters/
│       │   ├── __init__.py
│       │   ├── base.py                  # Abstract async GraphStore interface
│       │   ├── in_memory.py             # Full in-memory partitioned graph + fault injection
│       │   ├── neo4j_adapter.py         # Async Cypher Composite/Shard adapter with proxy nodes
│       │   ├── hugegraph_adapter.py     # Async HugeGraph REST/Gremlin/Cypher adapter (PD+HStore)
│       │   ├── iceberg_puppygraph.py    # Zero-ETL PyArrow/Iceberg tables + PuppyGraph schema & query
│       │   └── stubs.py                 # JanusGraph (ScyllaDB) & NebulaGraph contract stubs
│       ├── lakehouse/
│       │   ├── __init__.py
│       │   └── store.py                 # Authoritative content & historical slice Parquet/Iceberg store
│       ├── benchmark/
│       │   ├── __init__.py
│       │   ├── generator.py             # Power-law + supernode + cross-domain synthetic generator
│       │   ├── golden_queries.py        # Frozen regression query set across adapters
│       │   ├── workloads.py             # 7 PoC workloads (latency, supernode, concurrency, ingest, etc.)
│       │   └── report.py                # Gate 2 ADR & Markdown benchmark comparison table generator
│       └── api/
│           ├── __init__.py
│           └── app.py                   # FastAPI server + CLI entrypoints
└── tests/
    ├── test_models_and_guardrails.py
    ├── test_supernode_bucketing.py
    ├── test_routing_and_coordinator.py
    ├── test_adapters_and_golden_queries.py
    ├── test_iceberg_puppygraph.py
    ├── test_hybrid_rag_and_lakehouse.py
    ├── test_generator_and_benchmark.py
    └── test_api.py
```

---

## 4. Acceptance Criteria Verification Mapping

| Proposal Criterion | Implementation Component | Verification Test |
| --- | --- | --- |
| 1-, 2-, 3-hop P95/P99 latency vs Gate 2 budget | `benchmark/workloads.py::run_traversal_latency_workload` | `test_generator_and_benchmark.py` |
| Query guardrails (depth≤3, results≤5000, 2s, expanded≤50000) | `core/guardrails.py::GuardrailEnforcer` | `test_models_and_guardrails.py` |
| Locality & cross-domain tracking | `routing/coordinator.py::QueryCoordinator` | `test_routing_and_coordinator.py` |
| Supernode mitigation (`bucket(node_id, 16)`) | `core/supernode.py::SupernodeBucketer` | `test_supernode_bucketing.py` |
| Graph size discipline (reject derived edges, lean props) | `core/guardrails.py::validate_edge_write` | `test_models_and_guardrails.py` |
| Engine independence (Golden Query Set on ≥2 adapters) | `benchmark/golden_queries.py` on `InMemory` + `IcebergPuppyGraph` | `test_adapters_and_golden_queries.py` |
| High availability fault recovery | `benchmark/workloads.py::run_ha_failure_workload` | `test_generator_and_benchmark.py` |
| Cost per billion edges & Gate 2 ADR | `benchmark/report.py::generate_gate2_adr` | `test_generator_and_benchmark.py` |
