# Scaling the Semantic Knowledge Graph: Project Proposal

Oct 8, 2026 · @Sendil Sadasivam

## Executive summary

We propose an evidence-first path to scale the semantic graph from about 100 million to 10 billion or more relationships: keep the graph lean, shard it by semantic domain, and pick the engine by benchmark rather than vendor claims.

- **Feasible.** All four candidate engines are open source and document distributed operation. Two statements from the design discussion were corrected after checking current sources: Infinigraph licensing and the production maturity of HugeGraph's distributed store.
- **Recommended path.** Evaluate HugeGraph first and JanusGraph on ScyllaDB second, with Neo4j as the baseline and NebulaGraph as a challenger, all behind a `GraphStore` interface so the engine can change without touching the reasoning layer.
- **Plan.** Four phases with three gates; the engine decision is made at Gate 2 from measured latency, ingestion, failure recovery and cost per billion edges.
- **Decision requested.** Approve Phases 0 and 1: foundations and the benchmark proof of concept.

## Background and problem

The semantic graph behind the Kinetic-V GraphRAG platform has to grow from about 100 million relationships toward 10 billion or more while keeping 1 to 3 hop retrieval fast. Storage is not the hard part, because documents and raw data already live outside the graph and are referenced by ID. The hard parts are traversal latency, ingestion throughput and cost per billion edges.

Four properties of graphs make this different from scaling a relational database:

- **Hash partitioning breaks traversal.** Spreading nodes by `hash(id)` turns a 4-hop query into about five network calls, so partitions must follow traversal locality.
- **Traversal fans out fast.** At an average degree of 20, hops 1 to 5 touch 20, 400, 8,000, 160,000 and 3.2 million nodes.
- **Supernodes concentrate load.** One node such as `Python` or `India` can hold hundreds of millions of edges and dominate a single partition.
- **Derived edges multiply.** Persisting every inferred relationship (similarity, indirect use) can add billions of edges that need not be stored.

Requirements carried into this proposal: open source, runs on Kubernetes, horizontal scaling, high availability, Gremlin or Cypher access, and a GraphRAG-friendly retrieval path.

## Feasibility validation

The approach is feasible: every engine in the shortlist is open source and documents distributed operation, and the architecture rules (lean graph, depth caps, supernode handling) are standard practice. No engine has yet been shown to meet our latency budget, so feasibility of the engine choice rests on the PoC. Two statements from the design discussion needed correction after checking current sources.

| Claim from the design discussion | Verdict | What the sources say |
| --- | --- | --- |
| Neo4j composite databases federate and shard, but a relationship cannot span constituent graphs; cross-shard links use proxy nodes | Confirmed | [Composite databases](https://neo4j.com/docs/operations-manual/current/composite-databases/) states relationships cannot span graphs and describes the proxy-node pattern. |
| Composite databases and sharding are Enterprise-only; Community has no distributed sharding | Unverified | The pages opened do not name editions. Confirm with Neo4j licensing before relying on it. |
| Infinigraph property sharding keeps structure in a graph shard and scales properties out | Confirmed, with a caveat | [Scaling with Neo4j](https://neo4j.com/docs/operations-manual/current/scalability/scaling-with-neo4j/) lists up to 100TB with reads scaling horizontally, but write concurrency limited to a single instance. |
| Infinigraph needs an extra subscription and is not in Enterprise or Aura | Corrected | The [launch announcement](https://neo4j.com/press-releases/neo4j-launches-infinigraph/) (September 4, 2025) says it is available in Enterprise Edition, self-managed, and coming to AuraDB. It names no add-on subscription. |
| JanusGraph is Apache 2.0 and scales to hundreds of billions of edges on Cassandra, ScyllaDB, HBase or Bigtable | Confirmed | [JanusGraph](https://janusgraph.org/) lists these backends and the scale claim. |
| JanusGraph cannot place vertices explicitly | Confirmed, with nuance | [Partitioning](https://docs.janusgraph.org/advanced-topics/partitioning) says explicit placement is unsupported, but a custom `IDPlacementStrategy` can be registered. Vertex cuts for hot vertices are called high-overhead, not production-ready and discouraged. |
| HugeGraph provides built-in distributed storage (PD plus HStore), Gremlin and OpenCypher under Apache 2.0 | Confirmed | The [HugeGraph README](https://apache.googlesource.com/hugegraph/+show/HEAD/README.md) lists PD and HStore on Raft, both query languages, and a stated scale above 10 billion vertices and edges. |
| HugeGraph's distributed mode is production-ready | Unverified | The README recommends distributed mode for production but gives HStore no maturity label, requires Java 17, and the [1.5.0 release notes](https://hugegraph.apache.org/docs/changelog/hugegraph-1.5.0-release-notes/) page is marked work in progress. Treat as a PoC gate. |
| HugeGraph-AI supports GraphRAG | Confirmed | It is a separate ecosystem repository for graph, LLM and knowledge-graph integration, not part of the core server. |
| NebulaGraph is a viable benchmark candidate | Confirmed, with a caveat | Apache 2.0 with openCypher support per its [repository](https://github.com/vesoft-inc/nebula). The [architecture paper](https://ar5iv.labs.arxiv.org/html/2206.07278) shows placement by static vertex-ID hash, not graph-aware, and reports a 2-hop query at about 328 ms on roughly 5.27 billion edges across three VMs. |
| A 100-billion-edge graph splits into customer, knowledge, technology and product domains of 25B, 35B, 20B and 20B edges | Unverified | Illustrative sizing from the discussion. It is not derived from our data and is not used in this proposal. |

## Proposed solution

Scale the knowledge system rather than one graph database: keep the graph lean, split it by semantic domain, and place the engine behind an interface so it can be swapped.

&#91;embedded content: target architecture · 4 layers, 3 engine adapters\]

The coordinator checks each query against the guardrails, asks the entity directory which shard owns each ID, and runs every hop through the engine adapter. Shards hold structure and reference IDs only; content stays in the lakehouse.

The levers are applied in this order, and sharding comes late because it multiplies the cost of a poorly modelled graph:

1. **Reduce edges.** Persist core factual relationships such as `Project USES Technology`; compute derived ones such as similarity at query time.
2. **Cap traversal depth.** Enforce depth 3, 5,000 results, 2 seconds and 50,000 expanded nodes in the graph API.
3. **Fix supernodes.** Insert hierarchy or bucket nodes so no adjacency list grows without bound.
4. **Add indexes** on the properties that decide where a traversal goes.
5. **Add read replicas.** GraphRAG is read-heavy, so replicas raise capacity without changing the partitioning.
6. **Partition by semantic locality.** One shard per domain, stable global entity IDs, and an entity directory that routes each ID to its shard.
7. **Federate.** A query coordinator runs each hop on the owning domain and merges the results.

Three design rules follow from the separation of graph and storage:

- A property stays in the graph only if it decides where to traverse; content is fetched from the authoritative store after the entity is found.
- The graph holds current relationships. History lives in the lakehouse and is reloaded as a graph slice when needed.
- Heavy graph analytics (community detection, PageRank) run on the lakehouse, not on the operational graph.

## Technology options

Evaluate HugeGraph first and JanusGraph on ScyllaDB second, keep Neo4j as the baseline for small and medium deployments, and include NebulaGraph as a challenger. The deciding differences are who owns partition placement and how much infrastructure each option adds.

| Option | Distribution model | Query language | Placement and locality | Supernode handling | Main risk |
| --- | --- | --- | --- | --- | --- |
| HugeGraph with PD and HStore | Built-in distributed store; PD schedules partitions, Raft replicas | Gremlin, OpenCypher | PD manages partitions; control over traversal locality to be verified in the PoC | To test; model bucket nodes as a fallback | HStore maturity is unlabeled; Java 17 only |
| JanusGraph on ScyllaDB | Graph layer over a distributed wide-column store; separate index backend | Gremlin | Random by default; custom `IDPlacementStrategy` possible | Vertex cuts are discouraged, so bucket or hierarchy nodes are modelled by hand | Three systems to operate: graph, storage, index |
| Neo4j Enterprise composite databases | One database per shard, queried through a composite database | Cypher | Full control: the shard is chosen by domain design | Modelled by hand; Infinigraph adds property scale-out only | Relationships cannot cross shards, so proxy nodes and licensing cost |
| NebulaGraph | Static hash partitions with Raft replicas | openCypher, nGQL | Hash of vertex ID; not graph-aware | To test | Poor locality; ACID isolation not guaranteed per the paper |

The graph layer is placed behind a `GraphStore` interface with Neo4j, HugeGraph and JanusGraph adapters, so the GraphRAG reasoning layer does not depend on one engine and the choice can follow the benchmark.

## Benchmark and proof-of-concept plan

The PoC decides the engine by measuring the workloads GraphRAG actually runs, not by vendor scale claims. Four candidates run the same dataset and query set on the same Kubernetes cluster shape: Neo4j, HugeGraph with HStore, JanusGraph on ScyllaDB, and NebulaGraph.

**Dataset.** A synthetic graph with a power-law degree distribution and deliberate supernodes, generated at 100 million, 1 billion and 10 billion edges, plus a sampled slice of the real Kinetic-V graph. Cross-domain edge share is varied at three levels to expose cross-partition cost.

| Workload | What it measures | Metric |
| --- | --- | --- |
| 1-, 2- and 3-hop traversal from a seed entity | Core GraphRAG retrieval | P50, P95, P99 latency |
| Traversal through a supernode | Hot-partition and adjacency-list behavior | P99 latency, expanded nodes |
| Concurrent retrieval, ramped to peak load | Read scaling and replica behavior | Throughput at the latency budget |
| Bulk and streaming ingestion | Write path and index maintenance | Edges per second, lag behind source |
| Cross-partition traversal | Cost of crossing shards or domains | Latency ratio against single-partition |
| Node and replica failure during load | High availability | Recovery time, failed queries |
| Full cost run | Infrastructure per scale step | Cost per billion edges |

**Fixed guardrails for every engine:** maximum depth 3, 5,000 results, 2 seconds execution and 50,000 expanded nodes per query.

**Pass criteria.** Each candidate must meet the retrieval latency budget at 1 billion edges and degrade gracefully at 10 billion. The numeric budget is set at kickoff from the end-to-end GraphRAG response target, so the gate is agreed before any result is seen.

## Phased roadmap

Delivery runs in four phases, and a gate between phases blocks scale-up until the evidence is in.

&#91;embedded content: phased roadmap · 4 phases, 3 gates\]

Durations are set once the team is sized. Gate 2 is the engine decision, recorded as an ADR with the benchmark results; Gate 3 uses the acceptance criteria in this proposal.

## Team, resources and cost drivers

The PoC needs five roles and a Kubernetes cluster large enough to hold the 10-billion-edge run for four engines. Headcount and budget are not set here; they are an open question at the end.

| Role | Responsibility |
| --- | --- |
| Solution architect | Owns the architecture, the `GraphStore` interface and the decision record |
| Graph platform engineer | Deploys and tunes each engine; builds the adapters |
| Data engineer | Dataset generation, bulk and streaming ingestion, lakehouse references |
| SRE | Kubernetes operators, failure tests, cost measurement |
| GraphRAG engineer | Retrieval router, golden query set, evaluation against the application |

Cost drivers, in the order they are likely to matter:

- Compute and storage for the benchmark clusters at 100 million, 1 billion and 10 billion edges, with three replicas per shard.
- Neo4j Enterprise licensing if composite databases are in scope.
- Operations effort to run JanusGraph with ScyllaDB and an index backend, compared with HugeGraph's integrated stack.
- Streaming and lakehouse capacity for ingestion and for the historical tier.

## Risks and mitigations

The largest risks are engine maturity and cross-partition latency, and both are tested in the PoC before any commitment.

| Risk | Impact | Mitigation |
| --- | --- | --- |
| HugeGraph HStore proves immature under our load | Primary candidate drops out | Run node-failure and rebalance tests in the PoC; keep JanusGraph on ScyllaDB as the fallback behind the `GraphStore` interface |
| Cross-partition traversals dominate latency | Retrieval misses the latency budget | Partition by semantic domain, keep an entity directory, cap depth at 3, and track the cross-domain share of queries |
| Supernodes overload one partition | Hot spots and timeouts | Insert hierarchy or bucket nodes, cap expanded nodes per query, avoid JanusGraph vertex cuts |
| Derived edges inflate the graph | Cost and ingestion time grow by orders of magnitude | Persist only core factual edges, compute derived edges at query time, materialize only paths that are queried repeatedly |
| Sharding a poorly modelled graph | A distributed graph that is harder to operate | Follow the lever order in the proposal: reduce edges, cap depth, fix supernodes, add indexes and replicas, then partition |
| Operational load of JanusGraph, ScyllaDB and an index backend | Slower delivery, on-call burden | Count operations effort in the cost comparison; use Kubernetes operators and written runbooks |
| Neo4j edition and licensing limits | Sharding unavailable or costly | Confirm edition terms with Neo4j in the first week of Phase 0 |
| Benchmark does not reflect real queries | Wrong engine chosen | Mix a sampled slice of the real graph with synthetic data; freeze a golden query set and re-run it as a regression gate |

## Success metrics and acceptance criteria

The project is accepted when one engine meets the latency budget at 1 billion edges, degrades gracefully at 10 billion, and the reasoning layer runs unchanged on a second engine. Numeric budgets marked "set at kickoff" are agreed before the first benchmark run.

| Metric | Target | Measured by |
| --- | --- | --- |
| 1-, 2- and 3-hop P95 and P99 latency at 1 billion edges | Within the budget set at kickoff | PoC traversal workload |
| Latency growth from 1 billion to 10 billion edges | Within the degradation limit set at kickoff | PoC scale ladder |
| Query guardrails | Depth 3, 5,000 results, 2 seconds, 50,000 expanded nodes enforced on every query | Graph API tests |
| Locality | Most GraphRAG queries complete inside one domain; share set at kickoff | Query logs against the entity directory |
| High availability | Recovery time and failed-query count within limits set at kickoff | Node and replica failure test |
| Graph size discipline | Persistent edges limited to core relationships; derived-to-core ratio reported | Schema review and edge counts |
| Engine independence | Reasoning layer passes the golden query set on at least two adapters | Regression gate in CI |
| Cost | Cost per billion edges reported for every engine | Cost run |

## Decisions, open questions and next steps

Approve Phases 0 and 1 so the engine decision is made on benchmark evidence.

Decisions requested:

- [ ] Approve Phase 0 (foundations) and Phase 1 (benchmark proof of concept)
- [ ] Confirm the shortlist: HugeGraph, JanusGraph on ScyllaDB, Neo4j and NebulaGraph
- [ ] Adopt the `GraphStore` interface as the rule for how the reasoning layer reaches the graph

Open questions:

- What end-to-end GraphRAG latency budget sets the traversal gate?
- What are the current and 12-month edge counts of the Kinetic-V graph, and into which domains does it split?
- Is a Neo4j Enterprise license available for the proof of concept?
- What team size and budget apply, so phase durations can be set?

## Sources

Pages opened for the feasibility check on 2026-10-08:

- [Neo4j: Scaling with Neo4j](https://neo4j.com/docs/operations-manual/current/scalability/scaling-with-neo4j/)
- [Neo4j: Composite databases](https://neo4j.com/docs/operations-manual/current/composite-databases/)
- [Neo4j: Infinigraph architecture](https://neo4j.com/blog/graph-database/infinigraph-scalable-architecture/)
- [Neo4j: Infinigraph launch announcement](https://neo4j.com/press-releases/neo4j-launches-infinigraph/)
- [JanusGraph: home](https://janusgraph.org/)
- [JanusGraph: graph partitioning](https://docs.janusgraph.org/advanced-topics/partitioning)
- [Apache HugeGraph: README](https://apache.googlesource.com/hugegraph/+show/HEAD/README.md)
- [Apache HugeGraph: 1.5.0 release notes](https://hugegraph.apache.org/docs/changelog/hugegraph-1.5.0-release-notes/)
- [NebulaGraph: repository](https://github.com/vesoft-inc/nebula)
- [NebulaGraph: architecture paper](https://ar5iv.labs.arxiv.org/html/2206.07278)
- [Scaling a Knowledge Graph: design discussion](https://chatgpt.com/share/6ac68212-fa14-83ec-a3e5-755ec60e8375), the source of the claims checked above
