# Agent Nexus KG (`agent-nexus-kg`)

> Distributed, engine-agnostic semantic knowledge graph platform (100M–10B+ edges) with pluggable GraphStore adapters for multi-hop Kinetic-V GraphRAG.

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python 3.11+](https://img.shields.io/badge/Python-3.11+-3776AB.svg)](https://www.python.org/)
[![Kubernetes Native](https://img.shields.io/badge/Kubernetes-Native-326CE5.svg)](https://kubernetes.io/)
![Architecture: Lean Semantic GraphRAG](https://img.shields.io/badge/Architecture-Lean_Semantic_GraphRAG-10B981.svg)

**Agent Nexus KG** provides the structural memory and multi-hop reasoning backbone for Kinetic-V agents. Designed to scale seamlessly from 100 million to 10 billion+ relationships without vendor lock-in, it separates lightweight topological graph navigation from heavy content storage and abstracts physical graph engines behind a unified `GraphStore` interface.

## Key Capabilities

* **Lean Semantic Graph Architecture:** Stores only entity nodes, typed relationships, and traversal-relevant properties in the graph while linking full documents, data products, and embeddings in authoritative stores via reference URIs.
* **Pluggable `GraphStore` Abstraction:** Swap or benchmark backends without changing agent or GraphRAG retrieval code:
  * **Apache HugeGraph (Primary):** Built-in Placement Driver (PD) + Raft-replicated distributed HStore for native 1B–10B+ edge partitioning.
  * **JanusGraph + ScyllaDB (Secondary):** High-scale distributed storage paired with OpenSearch/Elasticsearch mixed indexes.
  * **Neo4j (Baseline):** Fast local development and small-to-medium single-cluster deployments.
* **Supernode-Safe Query Service:** Enforces hop depth bounds (1–3 hops), per-hop fan-out caps, time-sliced edge bucketing, and query result caching to prevent high-degree vertex latency spikes.
* **Hybrid Bulk & Streaming Ingestion:** Combines high-throughput bulk loaders ($\ge 50\text{K}$ edges/sec) with idempotent Apache Kafka streaming upserts and full source lineage tracking.
* **Production-Grade Observability & Security:** Instrumented with OpenTelemetry, Prometheus, Grafana, and Langfuse retrieval tracing, secured via TLS, OIDC/Entra ID, and RBAC.

## Architecture

<img width="1024" height="1024" alt="image" src="https://github.com/user-attachments/assets/171612d1-1628-475a-b945-596f9d459639" />


For full architectural specifications, ontology schemas, supernode mitigation strategies, and Kubernetes deployment topologies, see [`docs/DESIGN.md`](docs/DESIGN.md).

## Core Tech Stack & Skills

* **Graph Engines & Query Languages:** Apache HugeGraph (PD + HStore), JanusGraph, Neo4j, Apache TinkerPop Gremlin, OpenCypher
* **Distributed Storage & Search:** ScyllaDB / Apache Cassandra, OpenSearch / Elasticsearch, S3-compatible Object Storage
* **Backend & Ingestion:** Python 3.11+, REST / gRPC, Apache Kafka (CDC & idempotent stream upserts)
* **AI & GraphRAG Orchestration:** Multi-hop GraphRAG, Hybrid Vector + Graph Retrieval, Agentic Reasoning, Langfuse
* **Cloud & Observability:** Kubernetes (StatefulSets, Helm/Operators), OpenTelemetry, Prometheus, Grafana, OIDC / Entra ID

## Performance & Reliability Targets (SLOs)

| Metric | Target |
| :--- | :--- |
| **Graph Scale** | 100M $\rightarrow$ 1B $\rightarrow$ 10B+ relationships |
| **Traversal Latency** | 2-hop P95 < 100 ms \| 3-hop P95 < 500 ms |
| **Query Concurrency** | 500+ concurrent GraphRAG retrievals |
| **Bulk Ingestion Rate** | 50,000+ edges/sec sustained |
| **Availability** | 99.9%+ multi-AZ replica fault tolerance |

## Planned Repository Structure

<img width="1024" height="1024" alt="image" src="https://github.com/user-attachments/assets/29ea952f-a963-4c9f-b099-faba72cddae1" />


## Quickstart

```bash
# 1. Clone the repository
git clone https://github.com/Senthilsivam41/agent-nexus-kg.git
cd agent-nexus-kg

# 2. Set up Python environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Review system architecture & implementation phases
cat docs/DESIGN.md
```

## Delivery Roadmap

* **Phase 0–1 (Weeks 1–6):** `GraphStore` protocol, Semantic Graph API specification, lean ontology schema, and Neo4j baseline adapter.
* **Phase 2 (Weeks 7–12):** Apache HugeGraph and JanusGraph + ScyllaDB adapters + 100M/1B edge benchmark decision gate.
* **Phase 3 (Weeks 13–18):** Multi-AZ Kubernetes deployment, Kafka ingestion pipelines, TLS/OIDC security, and OpenTelemetry monitoring.
* **Phase 4–5 (Weeks 19–24):** Kinetic-V GraphRAG integration, groundedness evaluation, failover testing, and production go-live.

## License

Licensed under the [Apache License 2.0](https://opensource.org/licenses/Apache-2.0).
