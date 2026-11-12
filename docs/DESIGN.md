# System Design Specification: Agent Nexus KG (`agent-nexus-kg`)

**Target Platform:** Scalable Semantic Knowledge Graph Platform for Kinetic-V GraphRAG  
**Scale Trajectory:** 100M $\rightarrow$ 1B $\rightarrow$ 10B+ Relationships  
**License Standard:** Open Source (Apache 2.0)

---

## 1. Architectural Principles

### 1.1 Lean Semantic Graph

The semantic graph serves as a high-speed topological navigation layer rather than a general-purpose document store:

* **In-Graph Data:** Entities, typed relationships, and traversal-critical routing/filter attributes (`id`, `type`, `domain`, `tenant`, `status`, `effective_date`, `uri`, `weight`).
* **Out-of-Graph Data:** Full documents, source files, analytical tables, and dense vector embeddings remain in authoritative stores (lakehouse, object store, data products, vector index), linked via immutable reference IDs/URIs.

```text
(Customer:123) -PURCHASED-> (Product:456) -DESCRIBED_BY-> (Document:789)
Document:789 = { id: "789", uri: "s3://enterprise-kb/docs/789.pdf", type: "PDF" }
```

### 1.2 Pluggable GraphStore Abstraction

Application and GraphRAG reasoning layers never bind directly to vendor query drivers or proprietary sharding logic. All graph operations execute through an engine-neutral `GraphStore` protocol:

```python
from typing import Protocol


class GraphStore(Protocol):
    def upsert_vertices(self, vertices: list[Vertex]) -> None: ...
    def upsert_edges(self, edges: list[Edge]) -> None: ...
    def neighbors(self, vid: str, labels: list[str] | None, direction: str, limit: int) -> list[Vertex]: ...
    def traverse(self, seed_ids: list[str], pattern: TraversalSpec, max_hops: int) -> Subgraph: ...
    def find(self, label: str, filters: dict, limit: int) -> list[Vertex]: ...
    def shortest_path(self, src: str, dst: str, max_hops: int) -> Path | None: ...
    def health(self) -> HealthStatus: ...
```

---

## 2. Logical Architecture & Storage Adapters

```text
          Kinetic-V Agents / GraphRAG / Reasoning Layer
                                │
                    Semantic Graph API (REST / gRPC)
                                │
     Query Service (query planning, hop/fan-out limits, caching, auth)
                                │
                       GraphStore Interface
                ┌───────────────┼───────────────┐
                ▼               ▼               ▼
        HugeGraph Adapter  JanusGraph Adapter  Neo4j Adapter
         (Primary 10B+)     (Secondary 10B+)   (Small/Medium)
                │               │               │
         HugeGraph Server   JanusGraph +       Neo4j DB
          + PD + HStore    ScyllaDB + OpenSearch
                                │
                                ▼  reference IDs / URIs
        ────── Authoritative Data & Knowledge Stores ──────
        Lakehouse │ Object Store │ Data Products │ Vector Store
```

| Engine Candidate | Role | Storage & Partitioning | Query Languages | Operational Profile |
| :--- | :--- | :--- | :--- | :--- |
| **Apache HugeGraph** | Primary (1B–10B+ edges) | Built-in Placement Driver (PD) + Raft-replicated distributed HStore | Gremlin, OpenCypher | Medium complexity; native distributed partitioning and HugeGraph-AI tooling |
| **JanusGraph + ScyllaDB** | Secondary (1B–10B+ edges) | ScyllaDB/Cassandra storage backend + OpenSearch/Elasticsearch mixed indexes | Gremlin (limited OpenCypher) | High complexity (3 distributed clusters); mitigated cross-node hops via caching |
| **Neo4j** | Baseline (Small/Medium) | Single-node or Enterprise Composite federation | Cypher | Low complexity; no cross-shard edges in Community/Composite without Infinigraph |
| **NebulaGraph** | Optional benchmark | Distributed storage + meta service | nGQL / OpenCypher | Evaluated during Week 7–12 benchmark gate |

---

## 3. Data Model, Partitioning & Supernode Mitigation

### 3.1 Core Ontology Schema

* **Vertex Labels:** `Customer`, `Product`, `Category`, `Document`, `Asset`, `Concept`, `CodeModule`, `DataProduct`
* **Edge Labels:** `PURCHASED`, `BELONGS_TO`, `DESCRIBED_BY`, `RELATES_TO`, `DEPENDS_ON`, `PART_OF`
* **Core Traversal Properties:** `id`, `type`, `domain`, `tenant`, `status`, `effective_date`, `uri`, `weight`

### 3.2 Partitioning & Locality

* **Partition Hints:** Apply `domain` and `tenant` partition hints where supported (Apache HugeGraph PD) to co-locate high-affinity subgraphs.
* **Cross-Domain Edges:** Model cross-domain relationships using deterministic global IDs to preserve adapter portability.

### 3.3 Supernode Handling

High-degree vertices (e.g., global categories or core shared libraries) are guarded at both ingestion and query time:

* **Ingestion Degree Detection:** Flag vertices exceeding degree thresholds during ETL/stream loading.
* **Edge Bucketing & Time Slicing:** Partition high-fan-out edges into time-sliced or hash-bucketed intermediate hub nodes.
* **Vertex-Centric Indexes & Query Guards:** Enforce strict per-hop fan-out caps, depth limits (1–3 hops), and result caching in the Query Service.

---

## 4. Ingestion & GraphRAG Execution Pipelines

### 4.1 Hybrid Ingestion Pipeline

* **Bulk Load:** Engine-native high-throughput loaders (`HugeGraph-Loader`, JanusGraph Spark bulk loader) targeting $\ge 50\text{K}$ edges/sec.
* **Streaming Upserts:** Apache Kafka consumers performing idempotent vertex/edge upserts keyed by global ID, preserving source system lineage and reference URIs.

### 4.2 Five-Stage GraphRAG Retrieval Flow

1. **Entry-Point Discovery:** Vector similarity or mixed full-text/range index lookup identifies seed entities.
2. **Bounded Graph Expansion:** Query Service executes bounded 1–3 hop filtered traversals with supernode fan-out guards.
3. **Authoritative Context Assembly:** Reference IDs/URIs on traversed nodes resolve full text chunks, records, or code artifacts from authoritative stores.
4. **Grounded LLM Synthesis:** Assembled graph paths and source passages are passed to the reasoning model with strict source citations.
5. **End-to-End Observability:** OpenTelemetry and Langfuse traces link retrieval subgraphs and latency spans directly to generated answers.

---

## 5. Non-Functional SLOs & Kubernetes Deployment

| Dimension | Target SLO | Implementation Controls |
| :--- | :--- | :--- |
| **Traversal Latency** | 2-hop P95 < 100 ms; 3-hop P95 < 500 ms | Query Service hop limits, traversal templates, distributed cache |
| **Concurrency** | 500+ concurrent GraphRAG retrievals | Stateless REST/gRPC API & Query Service horizontal pod autoscaling |
| **Ingestion Throughput** | 50,000+ edges/sec sustained bulk load | Parallel partition-aware loaders + Kafka consumer groups |
| **Availability** | 99.9%+ with node failure tolerance | Multi-AZ Kubernetes StatefulSets, anti-affinity, 3x Raft/backend replication |
| **Security & Governance** | Zero-trust access & auditability | TLS in transit, encryption at rest, OIDC / Entra ID AuthN, RBAC, Vault secrets |
| **Observability** | Full metric & trace correlation | Prometheus, Grafana, OpenTelemetry, Langfuse, structured slow-query logs |

### Minimum Production Topology (Apache HugeGraph)

* **Placement Driver (PD):** 3 pods across availability zones
* **Distributed HStore:** 3+ StatefulSet nodes with 3-way Raft replication and scheduled object-store snapshots
* **Graph / Query Servers:** 2+ stateless pods behind an internal L7 load balancer

---

## 6. Benchmark Gate & Delivery Roadmap (24 Weeks)

**Decision Gate Criteria (100M & 1B edge runs):** Performance (35%), Scalability & Resilience (25%), Cost per Billion Edges (15%), Operability (15%), GraphRAG Ecosystem Fit (10%).

**Phases:**

* **Weeks 1–2 (Mobilize):** NFR sign-off, representative benchmark dataset selection.
* **Weeks 3–6 (Foundation):** `GraphStore` protocol, Semantic Graph API spec, Neo4j baseline adapter.
* **Weeks 7–12 (Benchmark):** HugeGraph & JanusGraph adapters, 100M/1B scale harness, Week-12 engine decision.
* **Weeks 13–18 (Production Build):** Multi-AZ Kubernetes Helm/operator deployment, Kafka ingestion, OIDC/TLS hardening.
* **Weeks 19–22 (GraphRAG Integration):** Kinetic-V agent retrieval integration and groundedness/relevance evaluation.
* **Weeks 23–24 (Hardening & Go-Live):** Chaos/failover testing, backup restore drills, operational runbooks.
