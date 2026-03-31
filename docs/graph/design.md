# Property Graph Engine for zvec

**Status:** Draft
**Date:** 2026-03-27
**Audience:** Principal/Staff Engineers, Architects

---

## 1. Motivation

zvec is a high-performance, in-process vector database. It provides dense/sparse vector search, scalar filtering, and persistent storage via RocksDB + Arrow + Proxima index files.

This design adds a **property graph layer** to zvec, enabling GraphRAG workloads:

```
High-level business question
  -> Vector search on node embeddings -> find candidate entities
    -> Traverse 2-5 hops to gather related context (columns, relationships, lineage)
      -> Feed subgraph to agent -> generate SQL / answer question
```

The primary consumer is **AI agents** — potentially hundreds or thousands concurrently in an enterprise setting. The API, response format, and performance model are optimized for this.

---

## 2. Design Principles

1. **General-purpose property graph.** Not tied to any domain. Data catalogs, knowledge graphs, org charts, dependency graphs — all valid.
2. **Schema as contract.** A schema layer sits between the domain and the generic graph engine. It defines legal node types, edge types, properties, and constraints.
3. **Embeddings are a first-class concern.** Both nodes and edges can carry vector fields. What gets embedded is a domain decision — the engine has no opinion.
4. **Agent-optimized.** Structured responses, tool-shaped APIs, high concurrency, low latency.
5. **Storage abstraction from day one.** The graph engine talks to a storage interface, not directly to zvec internals. Today: in-process zvec collections. Tomorrow: cloud/remote store.
6. **C++ core, thin Python API.** Graph logic (traversal, mutation, atomicity, concurrency) lives in C++. Python is a thin binding layer for API ergonomics and agent integration.

---

## 3. Data Model

This is a **property graph** in the classic sense:

- **Nodes** have a type and arbitrary key-value properties
- **Edges** have a type, direction, and arbitrary key-value properties
- Properties are typed, filterable, and indexable
- Both nodes and edges can carry vector fields

### 3.1 Node

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Unique node identifier |
| `node_type` | `str` | Schema-defined type (e.g., `"table"`, `"column"`) |
| `properties` | `dict[str, Any]` | Schema-defined typed properties |
| `vectors` | `dict[str, list]` | Schema-defined vector fields (optional) |
| `neighbor_ids` | `ARRAY_STRING` | Adjacency list — IDs of connected nodes |
| `neighbor_edge_ids` | `ARRAY_STRING` | Parallel array — edge ID for each neighbor |

### 3.2 Edge

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Deterministic: `"{source_id}:{edge_type}:{target_id}"` |
| `source_id` | `str` | Origin node ID |
| `target_id` | `str` | Destination node ID |
| `edge_type` | `str` | Schema-defined type (e.g., `"foreign_key"`, `"learned"`) |
| `directed` | `bool` | Directional or bidirectional |
| `properties` | `dict[str, Any]` | Schema-defined typed properties |
| `vectors` | `dict[str, list]` | Schema-defined vector fields (optional) |

### 3.3 Property Types

Same as zvec's scalar types — no new type system:

| Category | Types |
|----------|-------|
| Integer | `INT32`, `INT64`, `UINT32`, `UINT64` |
| Float | `FLOAT`, `DOUBLE` |
| Text | `STRING` |
| Boolean | `BOOL` |
| Arrays | `ARRAY_INT32`, `ARRAY_INT64`, `ARRAY_UINT32`, `ARRAY_UINT64`, `ARRAY_FLOAT`, `ARRAY_DOUBLE`, `ARRAY_STRING`, `ARRAY_BOOL` |

### 3.4 Adjacency List Design

- `neighbor_ids` and `neighbor_edge_ids` are parallel arrays — index `i` in both refers to the same relationship
- **Undirected edges:** both endpoints get a neighbor entry
- **Directed edges:** both endpoints still get the entry (enabling traversal in either direction), but the edge document records the actual direction via `source_id`/`target_id`
- This is the **hybrid model (C)**: adjacency on nodes for fast traversal, edge documents for properties/embeddings/metadata

---

## 4. Storage Mapping

The graph engine maps the property graph onto **two zvec collections** internally:

| Graph Concept | zvec Mapping |
|---------------|-------------|
| Node properties | Scalar fields on nodes collection |
| Node vectors | Vector fields on nodes collection |
| Edge properties | Scalar fields on edges collection |
| Edge vectors | Vector fields on edges collection |
| Adjacency lists | `ARRAY_STRING` fields on node documents |
| Structural lookups | Auto-created inverted indexes (see 4.1) |

### 4.1 Indexing Strategy

**Auto-created at graph initialization** (structural — required for traversal):

| Collection | Field | Reason |
|------------|-------|--------|
| Nodes | `node_type` | Filter by type during search/traversal |
| Edges | `source_id` | Outgoing traversal |
| Edges | `target_id` | Incoming traversal |
| Edges | `edge_type` | Filter by relationship type |

**User-created on demand** (property fields and vectors):

No property fields or vector fields are indexed by default. Users create indexes when needed, same as zvec today:

```python
graph.create_index("nodes", "department", InvertIndexParam())
graph.create_index("edges", "confidence", InvertIndexParam(enable_range_optimization=True))
graph.create_index("nodes", "description_emb", HnswIndexParam(ef_construction=200))
```

This keeps the default footprint small and lets the domain decide what's worth indexing.

---

## 5. Graph Schema

The schema layer validates all mutations and defines the structure of the graph.

### 5.1 Schema Definition

```python
schema = GraphSchema(
    name="data_catalog",
    node_types=[
        NodeType(
            name="table",
            properties=[
                PropertyDef("database", DataType.STRING),
                PropertyDef("row_count", DataType.INT64, nullable=True),
            ],
            vectors=[
                VectorDef("description_emb", DataType.VECTOR_FP32, dimension=768),
            ],
        ),
        NodeType(
            name="column",
            properties=[
                PropertyDef("data_type", DataType.STRING),
                PropertyDef("nullable", DataType.BOOL),
            ],
            vectors=[
                VectorDef("description_emb", DataType.VECTOR_FP32, dimension=768),
            ],
        ),
    ],
    edge_types=[
        EdgeType(
            name="has_column",
            directed=True,
            properties=[],
        ),
        EdgeType(
            name="foreign_key",
            directed=True,
            properties=[
                PropertyDef("on_column", DataType.STRING),
            ],
        ),
        EdgeType(
            name="learned",
            directed=False,
            properties=[
                PropertyDef("confidence", DataType.DOUBLE),
                PropertyDef("source", DataType.STRING),
            ],
            vectors=[
                VectorDef("relationship_emb", DataType.VECTOR_FP32, dimension=768),
            ],
        ),
    ],
    edge_constraints=[
        EdgeConstraint("has_column", source="table", target="column"),
        EdgeConstraint("foreign_key", source="table", target="table"),
        # "learned" intentionally unconstrained — any node to any node
    ],
)
```

### 5.2 Schema Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Validation in C++** | Every `add_node`, `add_edge`, `update` validates against schema before touching storage. Invalid mutations rejected with clear errors. |
| **Edge constraints are optional** | If defined, the engine enforces which node types an edge can connect. If omitted, that edge type is unconstrained. Some domains want strict typing, others want flexibility. |
| **Schema evolution supported** | Adding new node types, edge types, or properties to existing types works without rebuild. Removing or changing types requires migration (same as zvec's `alter_column`). |
| **Schema persisted as protobuf** | Stored alongside graph data in graph metadata, same pattern as zvec's collection schema. |

---

## 6. C++ Module Architecture

### 6.1 Directory Structure

```
src/
├── db/                              # existing zvec core (UNCHANGED)
│   ├── collection.cc
│   ├── index/
│   ├── sqlengine/
│   └── ...
├── graph/                           # NEW: graph module
│   ├── graph_engine.h/cc            # top-level API, lifecycle management
│   ├── graph_schema.h/cc            # schema definition, validation, persistence
│   ├── graph_store.h/cc             # orchestrates storage operations
│   ├── traversal.h/cc               # multi-hop expand, subgraph extraction
│   ├── mutation.h/cc                # atomic add/remove/update for nodes and edges
│   ├── storage/
│   │   ├── storage_interface.h      # abstract storage interface
│   │   └── zvec_storage.h/cc        # concrete impl using zvec collections
│   └── proto/
│       └── graph.proto              # schema, metadata, subgraph serialization
├── binding/python/
│   ├── python_collection.cc         # existing (unchanged)
│   └── python_graph.cc              # NEW: pybind11 bindings for graph engine
```

### 6.2 Class Hierarchy

```
GraphEngine                          # top-level: create, open, destroy, repair
  |-- GraphSchema                    # defines and validates node/edge types
  |-- GraphStore                     # owns the two zvec collections
  |     \-- StorageInterface         # abstract -- zvec today, cloud tomorrow
  |-- MutationEngine                 # add/remove/update with atomic batches
  \-- TraversalEngine                # multi-hop expand, subgraph extraction
        \-- Subgraph                 # result object: nodes + edges + metadata
```

### 6.3 Storage Interface

The abstraction that enables future storage separation:

```cpp
class StorageInterface {
 public:
  virtual ~StorageInterface() = default;

  // Node operations
  virtual Status UpsertNodes(const std::vector<Document>& docs) = 0;
  virtual Status DeleteNodes(const std::vector<std::string>& ids) = 0;
  virtual std::vector<Document> FetchNodes(const std::vector<std::string>& ids) = 0;
  virtual std::vector<Document> QueryNodes(const VectorQuery& query) = 0;
  virtual std::vector<Document> FilterNodes(const std::string& filter) = 0;

  // Edge operations
  virtual Status UpsertEdges(const std::vector<Document>& docs) = 0;
  virtual Status DeleteEdges(const std::vector<std::string>& ids) = 0;
  virtual std::vector<Document> FetchEdges(const std::vector<std::string>& ids) = 0;
  virtual std::vector<Document> FilterEdges(const std::string& filter) = 0;

  // Atomic batch (nodes + edges in one write)
  virtual Status AtomicBatch(const std::vector<Mutation>& mutations) = 0;

  // Index management
  virtual Status CreateIndex(Target target, const std::string& field,
                             const IndexParam& param) = 0;
  virtual Status DropIndex(Target target, const std::string& field) = 0;
};
```

`ZvecStorage` implements this using two `zvec::Collection` instances. A future `CloudStorage` would implement the same interface against a remote store — no changes to `GraphEngine`, `MutationEngine`, or `TraversalEngine`.

### 6.4 Concurrency Model

| Aspect | Design |
|--------|--------|
| Thread safety | `GraphEngine` is thread-safe — multiple agents call concurrently |
| Reads | Lock-free — zvec collections handle concurrent reads internally |
| Writes | Per-graph mutex for adjacency list consistency |
| Python GIL | Released at pybind11 boundary — all C++ work runs without Python lock |
| Scaling | N agents → N concurrent C++ threads, no Python serialization |

### 6.5 Atomic Mutations via RocksDB WriteBatch

`ZvecStorage::AtomicBatch` collects all document mutations into a single RocksDB `WriteBatch` — all-or-nothing at the storage level.

**Example: adding an edge**

```
add_edge("orders", "foreign_key", "customers")

MutationEngine builds 3 operations:
  1. Upsert edge doc  {id: "orders:foreign_key:customers", ...}
  2. Append to orders.neighbor_ids + orders.neighbor_edge_ids
  3. Append to customers.neighbor_ids + customers.neighbor_edge_ids

StorageInterface::AtomicBatch([1, 2, 3])
  -> ZvecStorage maps to single RocksDB WriteBatch
  -> All-or-nothing commit
```

No partial writes. No repair needed for crash recovery during mutations.

---

## 7. Traversal Engine

### 7.1 Core Operation: Subgraph Extraction

The primary traversal operation is "given seed nodes, expand N hops with filters, return the subgraph." This is the operation agents use most — find seeds via vector search, expand to gather context.

```
traverse(start_ids, depth, edge_filter, node_filter)
  -> returns Subgraph { nodes, edges, metadata }
```

### 7.2 Traversal Algorithm

```
Input:  seed node IDs, max_depth, edge_filter, node_filter
Output: Subgraph (all discovered nodes + edges)

visited_nodes = set(seed_ids)
visited_edges = set()
frontier = seed_ids

for hop in 1..max_depth:
    # Use adjacency lists on frontier nodes to get candidate neighbors + edge IDs
    candidates = []
    for node in frontier:
        for (neighbor_id, edge_id) in zip(node.neighbor_ids, node.neighbor_edge_ids):
            if neighbor_id not in visited_nodes:
                candidates.append((neighbor_id, edge_id))

    # Batch fetch candidate edge docs, apply edge_filter
    edge_docs = storage.FetchEdges([c.edge_id for c in candidates])
    filtered_edges = apply_filter(edge_docs, edge_filter)

    # Batch fetch target nodes, apply node_filter
    target_ids = [e.target_id for e in filtered_edges]
    target_nodes = storage.FetchNodes(target_ids)
    filtered_nodes = apply_filter(target_nodes, node_filter)

    # Update state
    visited_nodes.update(filtered_nodes.ids)
    visited_edges.update(filtered_edges.ids)
    frontier = filtered_nodes.ids

return Subgraph(
    nodes = storage.FetchNodes(visited_nodes),
    edges = storage.FetchEdges(visited_edges),
)
```

**Batch-oriented:** Each hop is 2 collection calls (batch fetch edges, batch fetch nodes) regardless of fan-out. For 3 hops: ~6 collection calls total. All in C++, no Python round-trips.

### 7.3 Subgraph Result Object

```cpp
struct Subgraph {
  std::vector<Node> nodes;
  std::vector<Edge> edges;

  // Convenience accessors
  std::vector<Node> nodes_of_type(const std::string& type) const;
  std::vector<Edge> edges_of_type(const std::string& type) const;
  std::vector<Edge> edges_from(const std::string& node_id) const;
  std::vector<Edge> edges_to(const std::string& node_id) const;
  std::vector<Node> neighbors(const std::string& node_id) const;

  // Serialization for agent consumption
  std::string to_json() const;
  std::string to_text() const;    // human/LLM-readable summary
};
```

The `Subgraph` is the primary response type for agents. It carries all the context needed — nodes with their properties, edges with their properties, and convenience methods to navigate the result.

---

## 8. Python API

Thin layer — type conversions, ergonomic wrappers, agent-friendly formatting. No graph logic.

### 8.1 Graph Lifecycle

```python
import zvec

# Define schema
schema = zvec.GraphSchema(
    name="data_catalog",
    node_types=[...],
    edge_types=[...],
    edge_constraints=[...],
)

# Create and open
graph = zvec.create_graph(path="./my_graph", schema=schema)

# Open existing
graph = zvec.open_graph(path="./my_graph")

# Destroy
graph.destroy()
```

### 8.2 Mutations

```python
# Add nodes
graph.add_node(id="orders", node_type="table", properties={
    "database": "analytics",
    "row_count": 1_000_000,
}, vectors={
    "description_emb": [0.1, 0.2, ...],
})

# Add edges
graph.add_edge(
    source="orders",
    target="customers",
    edge_type="foreign_key",
    properties={"on_column": "customer_id"},
)

# Update properties
graph.update_node("orders", properties={"row_count": 1_500_000})
graph.update_edge("orders:foreign_key:customers", properties={"on_column": "cust_id"})

# Delete
graph.delete_node("orders")        # also removes all connected edges
graph.delete_edge("orders:foreign_key:customers")
```

### 8.3 Query and Traversal

```python
# Vector search on nodes
seeds = graph.search_nodes(
    vector=query_embedding,
    vector_field="description_emb",
    topk=5,
    filter="node_type = 'table'",
)

# Vector search on edges
edges = graph.search_edges(
    vector=relationship_embedding,
    vector_field="relationship_emb",
    topk=10,
    filter="edge_type = 'learned' AND confidence > 0.8",
)

# Subgraph extraction
subgraph = graph.traverse(
    start=[node.id for node in seeds],
    depth=3,
    edge_filter="edge_type IN ('has_column', 'foreign_key')",
    node_filter="node_type IN ('table', 'column')",
)

# Access results
subgraph.nodes          # list[Node]
subgraph.edges          # list[Edge]
subgraph.to_json()      # JSON serialization
subgraph.to_text()      # LLM-friendly text representation

# Convenience
tables = subgraph.nodes_of_type("table")
columns = subgraph.nodes_of_type("column")
fks = subgraph.edges_of_type("foreign_key")
```

### 8.4 Index Management

```python
# Create indexes on property fields (on demand)
graph.create_index("nodes", "department", zvec.InvertIndexParam())
graph.create_index("edges", "confidence", zvec.InvertIndexParam(
    enable_range_optimization=True
))

# Create vector indexes (on demand)
graph.create_index("nodes", "description_emb", zvec.HnswIndexParam(
    ef_construction=200, m=16
))

# Drop indexes
graph.drop_index("nodes", "department")
```

### 8.5 Repair

```python
# Reconcile adjacency lists with edges collection
# Run after suspected inconsistency or on startup if desired
graph.repair()
```

---

## 9. Agent Optimization

### 9.1 Response Design

All responses are structured for direct consumption by LLM agents:

| Method | Returns | Agent Use |
|--------|---------|-----------|
| `search_nodes()` | `list[Node]` | Seed entity discovery |
| `search_edges()` | `list[Edge]` | Relationship discovery |
| `traverse()` | `Subgraph` | Context gathering for generation |
| `fetch_node()` | `Node` | Point lookup |
| `fetch_edge()` | `Edge` | Point lookup |

`Subgraph.to_json()` returns a structured JSON that agents can parse. `Subgraph.to_text()` returns a human/LLM-readable summary suitable for prompt injection.

### 9.2 Text Representation Example

```
Subgraph: 4 nodes, 5 edges

Nodes:
  [table] orders (database=analytics, row_count=1000000)
  [table] customers (database=analytics, row_count=50000)
  [column] orders.customer_id (data_type=INT64, nullable=false)
  [column] orders.amount (data_type=DOUBLE, nullable=false)

Edges:
  orders --has_column--> orders.customer_id
  orders --has_column--> orders.amount
  orders --foreign_key--> customers (on_column=customer_id)
  customers --learned--> orders (confidence=0.92, source=agent_v2)
```

### 9.3 Performance Targets

| Operation | Target Latency | Notes |
|-----------|---------------|-------|
| Vector search (nodes or edges) | < 10ms | Same as zvec baseline |
| Single-hop traversal | < 5ms | Batch fetch from adjacency |
| 3-hop subgraph extraction | < 20ms | 6 batch operations |
| Node/edge point lookup | < 1ms | Direct ID fetch |
| Add node | < 2ms | Single doc upsert |
| Add edge | < 5ms | Atomic batch (3 writes) |

These are in-process latencies. For context: an LLM API call is 200-2000ms. The graph operations should be negligible in an agent loop.

### 9.4 Concurrency

Designed for N concurrent agents:
- All read operations are lock-free
- Write operations use per-graph mutex (not per-node — keeps it simple)
- GIL released at pybind11 boundary
- No Python-side bottleneck

---

## 10. End-to-End Example: Data Catalog GraphRAG

```python
import zvec

# 1. Initialize
zvec.init(query_threads=8)

# 2. Define schema
schema = zvec.GraphSchema(
    name="data_catalog",
    node_types=[
        zvec.NodeType("catalog", properties=[
            zvec.PropertyDef("owner", zvec.DataType.STRING),
        ]),
        zvec.NodeType("schema", properties=[
            zvec.PropertyDef("database", zvec.DataType.STRING),
        ]),
        zvec.NodeType("table", properties=[
            zvec.PropertyDef("database", zvec.DataType.STRING),
            zvec.PropertyDef("row_count", zvec.DataType.INT64, nullable=True),
        ], vectors=[
            zvec.VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, 768),
        ]),
        zvec.NodeType("column", properties=[
            zvec.PropertyDef("data_type", zvec.DataType.STRING),
            zvec.PropertyDef("nullable", zvec.DataType.BOOL),
        ], vectors=[
            zvec.VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, 768),
        ]),
    ],
    edge_types=[
        zvec.EdgeType("contains", directed=True),
        zvec.EdgeType("has_column", directed=True),
        zvec.EdgeType("foreign_key", directed=True, properties=[
            zvec.PropertyDef("on_column", zvec.DataType.STRING),
        ]),
        zvec.EdgeType("learned", directed=False, properties=[
            zvec.PropertyDef("confidence", zvec.DataType.DOUBLE),
            zvec.PropertyDef("source", zvec.DataType.STRING),
        ]),
    ],
    edge_constraints=[
        zvec.EdgeConstraint("contains", source="catalog", target="schema"),
        zvec.EdgeConstraint("contains", source="schema", target="table"),
        zvec.EdgeConstraint("has_column", source="table", target="column"),
        zvec.EdgeConstraint("foreign_key", source="table", target="table"),
    ],
)

# 3. Create graph
graph = zvec.create_graph(path="./catalog_graph", schema=schema)

# 4. Create vector indexes
graph.create_index("nodes", "desc_emb", zvec.HnswIndexParam(ef_construction=200))

# 5. Populate (bulk load)
graph.add_node("orders", "table", {"database": "analytics", "row_count": 1_000_000},
               vectors={"desc_emb": embed("Orders table with customer purchases")})
graph.add_node("customers", "table", {"database": "analytics", "row_count": 50_000},
               vectors={"desc_emb": embed("Customer master data")})
graph.add_node("orders.customer_id", "column", {"data_type": "INT64", "nullable": False},
               vectors={"desc_emb": embed("Foreign key to customers table")})
graph.add_node("orders.amount", "column", {"data_type": "DOUBLE", "nullable": False},
               vectors={"desc_emb": embed("Order amount in dollars")})

graph.add_edge("orders", "customers", "foreign_key", {"on_column": "customer_id"})
graph.add_edge("orders", "orders.customer_id", "has_column")
graph.add_edge("orders", "orders.amount", "has_column")

# 6. Agent query flow
query = "How is customer churn affecting quarterly revenue?"
query_vec = embed(query)

# Step 1: Vector search for candidate tables
seeds = graph.search_nodes(
    vector=query_vec,
    vector_field="desc_emb",
    topk=5,
    filter="node_type = 'table'",
)

# Step 2: Expand to get full context
subgraph = graph.traverse(
    start=[n.id for n in seeds],
    depth=3,
    edge_filter="edge_type IN ('has_column', 'foreign_key', 'learned')",
)

# Step 3: Feed to LLM
context = subgraph.to_text()
sql = llm.generate(f"Given this schema context:\n{context}\n\nWrite SQL for: {query}")
```

---

## 11. Scale Considerations (100K Agent Velocity)

The sections above describe the core engine. The following concerns apply when this engine is serving tens or hundreds of thousands of concurrent agents in an enterprise setting. These are captured here as design constraints — some are addressed in the current architecture, others are deferred to future iterations.

### 11.1 Graph Integrity

**Tombstone deletes, not hard deletes.** When a node or edge is deleted, it is marked with a tombstone (`_deleted: true`, `_deleted_at: timestamp`) rather than physically removed. Agents mid-traversal will encounter the tombstone and skip it gracefully rather than hitting a missing document or a dangling adjacency pointer.

- Tombstoned nodes: traversal skips them, `search_nodes` excludes them via filter
- Tombstoned edges: traversal skips them, adjacency lists retain the ID (cleaned up lazily)
- A background compaction process hard-deletes tombstoned entities after a configurable TTL (e.g., 24h)

**Continuous reconciliation.** The on-demand `repair()` is insufficient at scale. A background reconciliation thread should:
- Periodically scan adjacency lists and verify referenced edge IDs exist in the edges collection
- Detect orphan edges (edges whose source/target nodes are tombstoned or missing)
- Emit metrics on inconsistency count and reconciliation lag
- Run at low priority — should not compete with agent read/write paths

This is the biggest failure mode at scale: adjacency lists drifting from edge documents. The atomic `WriteBatch` prevents it during normal operation, but crash recovery, partial restores, and storage-level issues can cause drift over time.

### 11.2 Traversal Control

**Node budget.** Depth alone is not sufficient — `depth=3` on a highly connected FK graph can explode to 10K+ nodes. The `traverse()` API must accept a `max_nodes` parameter:

```python
subgraph = graph.traverse(
    start=seed_ids,
    depth=3,
    max_nodes=500,           # hard cap on total nodes in result
    edge_filter="...",
    node_filter="...",
)
```

When the budget is exhausted mid-traversal, the engine stops expanding and returns what it has, with a `truncated: true` flag on the `Subgraph`.

**Cycle detection.** Non-negotiable. The traversal engine already tracks `visited_nodes` (Section 7.2), which naturally prevents cycles. This must be preserved in all traversal variants — no node is visited twice.

**Beam pruning.** At each hop, rather than expanding all neighbors, score candidates and keep only the top-N per hop. Scoring can be:
- Edge property-based (e.g., highest confidence learned edges first)
- Vector similarity-based (re-score neighbors against the original query vector)
- Configurable via a `beam_width` parameter

```python
subgraph = graph.traverse(
    start=seed_ids,
    depth=3,
    max_nodes=500,
    beam_width=20,           # keep top 20 candidates per hop
    edge_filter="...",
)
```

### 11.3 Staleness and Versioning

Catalogs change — tables get dropped, FKs added, columns renamed. Agents need to reason about the freshness of the context they receive.

**Node/edge versioning.** Every node and edge carries:
- `_version: uint64` — monotonically increasing, bumped on every mutation
- `_updated_at: uint64` — timestamp of last mutation

These are system-managed fields (not schema-defined), automatically set by the `MutationEngine`.

**Subgraph staleness metadata.** The `Subgraph` result includes:
- `oldest_node_updated_at` — the least-recently-updated node in the subgraph
- `newest_node_updated_at` — the most-recently-updated node
- Per-node `_updated_at` accessible on each node object

This lets agents decide: "this subgraph was last touched 6 months ago — I should flag low confidence" or "this was updated 2 minutes ago — high confidence."

### 11.4 Write Contention

The per-graph mutex (Section 9.4) becomes a bottleneck when many agents are concurrently writing `learned` edges. Structural edges (FK, has_column) are written by admin/ETL processes — low contention. Learned edges are written by agents — high contention.

**Tiered write paths:**

| Edge Category | Write Path | Consistency |
|---------------|-----------|-------------|
| Structural edges (FK, has_column, contains) | Synchronous, per-graph mutex, atomic `WriteBatch` | Strong — immediately visible |
| Learned edges | Async write queue, batched flush | Eventually consistent — visible after flush interval |

Learned edge writes:
- Agent calls `add_edge(type="learned")` — returns immediately, edge is queued
- Background writer batches queued edges and flushes at configurable interval (e.g., 100ms or 100 edges, whichever comes first)
- Agents reading learned edges may see slight lag — acceptable since learned edges are probabilistic by nature
- The queue is per-graph, in-memory, bounded (backpressure if queue is full)

This separates the hot write path (learned edges from many agents) from the structural write path (admin mutations that need strong consistency).

### 11.5 Observability

**Node hotness tracking.** Track access counts per node (read and write) in a fixed-size in-memory counter (e.g., Count-Min Sketch). Exposes:
- Top-N hottest nodes (cache candidates — relates to Open Question 3)
- Hot node alerts (a single table being hit by 10K agents simultaneously may indicate a problem)

**Traversal telemetry.** Each `traverse()` call produces a lightweight trace:

```json
{
  "trace_id": "abc-123",
  "start_nodes": ["orders", "customers"],
  "depth_reached": 3,
  "nodes_visited": 47,
  "nodes_returned": 47,
  "edges_traversed": 82,
  "truncated": false,
  "latency_ms": 12,
  "hops": [
    {"depth": 1, "candidates": 15, "after_filter": 12, "after_beam": 12},
    {"depth": 2, "candidates": 38, "after_filter": 30, "after_beam": 20},
    {"depth": 3, "candidates": 22, "after_filter": 15, "after_beam": 15}
  ]
}
```

This is critical for debugging "why did the agent choose the wrong tables?" — you can trace the traversal path and see where the wrong branch was taken or the right branch was pruned.

**Metrics exposed:**
- Traversal latency histogram (p50, p95, p99)
- Nodes/edges per traversal histogram
- Mutation throughput (structural vs learned)
- Reconciliation lag and inconsistency count
- Write queue depth (for async learned edges)

---

## 12. Traversal Algorithm Analysis

### 12.1 Current Implementation

The v0.1 traversal engine uses **basic BFS with per-node storage lookups**:

```
For each hop (1..max_depth):
  For each node in frontier:
    Read adjacency list from storage (RocksDB point lookup)
    Batch-fetch edge documents, apply in-memory filter
    Batch-fetch neighbor nodes, apply in-memory filter
    Check max_nodes budget, update visited set
```

- **Complexity:** O(V + E) per traversal, with per-node I/O
- **Parallelism:** None — single-threaded BFS
- **Filter execution:** Naive string parsing per node (`"field = 'value'"`)
- **Cycle detection:** Hash set of visited node IDs

This is correct and sufficient for the GraphRAG use case: shallow traversals (2-5 hops) over small neighborhoods (< 1000 nodes). A typical query (3 seeds, 2-hop, max_nodes=100) touches ~200 nodes with ~200 RocksDB reads, completing in **< 10ms**. The LLM inference that follows takes 500ms-5s, so traversal is not the bottleneck.

### 12.2 State-of-the-Art Comparison

| Approach | Key Insight | Performance |
|----------|------------|-------------|
| **SuiteSparse:GraphBLAS + LAGraph** | Graph ops = sparse linear algebra. BFS = sparse matrix-vector multiply (SpMV). Visited set = complement mask. | Billions of edges in seconds, single machine |
| **Ligra / Julienne** | Direction-optimized BFS: push (sparse frontier) vs pull (dense frontier) switching based on frontier/graph ratio | Near-optimal work for any frontier size |
| **Gunrock** | GPU-accelerated graph analytics via massive parallelism | Billions of edges/second on GPU |
| **KuzuDB** | Worst-case optimal joins for multi-hop pattern matching, compiled to vectorized relational operators | Embedded, competitive with Neo4j |
| **Neo4j / Memgraph** | Morsel-driven parallelism, columnar adjacency, JIT-compiled queries | Production-grade, millions of edges |

The GraphBLAS insight is particularly elegant: a k-hop BFS is just `frontier = A * frontier` repeated k times, where A is the adjacency matrix in CSR format and the multiply uses a semiring (AND/OR for reachability, min/+ for shortest path, etc.). The visited set becomes a complement mask on SpMV — zero-copy, cache-friendly, SIMD-vectorized.

### 12.3 Gap Analysis

| Aspect | v0.1 BFS | SOTA |
|--------|----------|------|
| Traversal depth | 2-5 hops | Arbitrary |
| Graph size | < 100K nodes | Billions of edges |
| Parallelism | None | Thread/GPU parallel |
| Memory layout | RocksDB point lookups | CSR/columnar, cache-aware |
| Filter execution | String parsing per node | Compiled/vectorized predicates |
| Direction optimization | No | Push/pull switching |

### 12.4 When SOTA Becomes Necessary

The v0.1 approach breaks down in these scenarios:

1. **Deep traversals (10+ hops)** — per-node I/O model degrades; need bulk adjacency loading or GraphBLAS-style SpMV
2. **Large dense neighborhoods (10K+ edges per node)** — exhaustive expansion is wasteful; need beam pruning or probabilistic sampling
3. **Complex filter predicates** — string parsing per node is O(n) in predicate length; need compiled filter expressions
4. **Analytical workloads** — PageRank, community detection, etc. require full-graph iteration, not local BFS

### 12.5 Incremental Optimization Path

These can be adopted independently as scale demands:

1. **Batch adjacency loading** — `RocksDB::MultiGet` for the entire frontier instead of per-node `Get`. Expected: 3-5x throughput improvement on large frontiers.
2. **Compiled filters** — pre-parse filter expressions into a predicate tree at query construction time, evaluate without string allocation per node.
3. **Parallel frontier expansion** — partition frontier across threads, each expands independently, merge results. Lock-free with per-thread visited sets merged via atomic union.
4. **GraphBLAS backend** — for analytical workloads, maintain a CSR adjacency matrix alongside the property store. Use GraphBLAS SpMV for structural traversal, point lookups for property hydration. The two representations stay in sync via the mutation engine.
5. **Direction-optimized BFS** — when frontier exceeds ~10% of graph size, switch from push (iterate frontier, check neighbors) to pull (iterate all vertices, check if any frontier neighbor exists). Requires maintaining both CSR and CSC formats.

---

## 13. Concurrency Model

### 13.1 Current Implementation

The v0.1 concurrency model is simple:

- **MutationEngine:** A single `std::mutex` serializes all write operations. This exists because edge mutation is a 3-write operation (edge doc + source adjacency + target adjacency). Without the lock, concurrent mutations to the same node trigger a read-modify-write race: both threads read the adjacency list, append their entry, and write back — losing one update.

- **TraversalEngine:** No locking. Reads are naturally concurrent. Multiple agents can traverse simultaneously without contention.

- **RocksDB layer:** Provides snapshot isolation for reads, so a traversal always sees a consistent state even if a mutation is in flight.

### 13.2 Scaling Limitations

At 100K concurrent agents:

- **Write throughput:** All mutations funnel through one mutex. If agents are frequently adding learned edges or updating confidence scores, they queue up. With ~50μs per write and the lock held for ~150μs per edge mutation (3 writes), theoretical max is ~6,600 edge mutations/second.

- **Write-read latency:** Traversals aren't blocked by the mutex, but they might read slightly stale adjacency lists (snapshot isolation means they see the state at read-start, not real-time). This is acceptable for GraphRAG — the next traversal will see the updated state.

### 13.3 Future Concurrency Approaches

In order of implementation complexity:

1. **Per-node locking** — Replace the single mutex with a lock table keyed by node ID. Mutations only lock the nodes they touch. Two mutations to different parts of the graph proceed in parallel. Estimated effort: moderate. Expected improvement: proportional to graph size / working set.

2. **Write batching with WAL** — Queue mutations in a write-ahead log and flush periodically (e.g., every 10ms or every 100 mutations). Amortizes the lock acquisition cost. Good for bursty writes from many agents. Estimated effort: moderate.

3. **Lock-free adjacency with CAS** — Use compare-and-swap on adjacency list updates. Read current list, compute new list, CAS-write. Retry on conflict. Requires adjacency lists stored as immutable versioned snapshots. Estimated effort: high. Only needed if per-node locking is insufficient.

4. **Sharded graph engines** — Partition the graph across N `GraphEngine` instances, each with its own storage and mutex. Route mutations by source node hash. Cross-shard edges require a 2PC protocol. Estimated effort: high. This is the path to distributed graph.

### 13.4 Recommended Approach for 100K Agents

For the typical GraphRAG workload (bulk load at build time, infrequent learned edge writes, read-heavy traversals):

- The single mutex is sufficient through ~10K agents
- Per-node locking + write batching gets to ~100K agents
- Sharded engines only needed if write volume exceeds what a single RocksDB instance can handle (~100K writes/second)

---

## 14. Open Questions

1. **Bulk loading API.** Should there be a batch `add_nodes` / `add_edges` that optimizes for initial graph population (skip adjacency updates until flush)?
2. **Schema migration tooling.** What does `alter_node_type` or `add_property_to_existing_type` look like operationally?
3. **Subgraph caching.** For hot subgraphs that many agents request, should there be an in-memory cache layer? Node hotness tracking (11.5) provides the signal — the caching strategy is TBD.
4. **Graph-level embeddings.** Should the engine support computing aggregate embeddings over subgraphs (e.g., a table's embedding = f(table embedding, column embeddings))?
5. **MCP tool integration.** Should the graph API expose itself as an MCP server for direct agent tool calling?
6. **Learned edge conflict resolution.** If two agents write conflicting learned edges (e.g., different confidence scores for the same relationship), what wins? Last-write-wins, highest confidence, or merge?
7. **Multi-graph queries.** Should an agent be able to traverse across multiple graphs (e.g., a data catalog graph + an access control graph)?
8. **Tombstone TTL configuration.** Per-graph? Per-node-type? Global default with overrides?
