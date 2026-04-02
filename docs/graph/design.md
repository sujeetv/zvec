# Property Graph Engine for zvec

**Status:** Active
**Date:** 2026-03-31
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
3. **Embeddings are separate.** Vector search lives in standard zvec Collections. The graph engine stores structure and properties; embedding algorithms create companion collections independently.
4. **Agent-optimized.** Structured responses, tool-shaped APIs, high concurrency, low latency.
5. **KV-native storage.** The graph engine uses a direct RocksDB KV store optimized for point lookups, traversals, and concurrent agent reads. No Arrow IPC, no segment-level locks.
6. **C++ core, thin Python API.** Graph logic (traversal, mutation, atomicity, concurrency) lives in C++. Python is a thin binding layer for API ergonomics and agent integration.

---

## 3. Data Model

This is a **property graph** in the classic sense:

- **Nodes** have a type and arbitrary key-value properties
- **Edges** have a type, direction, and arbitrary key-value properties
- Properties are typed, filterable, and indexable

### 3.1 Node

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Unique node identifier |
| `node_type` | `str` | Schema-defined type (e.g., `"table"`, `"column"`) |
| `properties` | `dict[str, str]` | Schema-defined typed properties |
| `neighbor_ids` | `list[str]` | Adjacency list — IDs of connected nodes |
| `neighbor_edge_ids` | `list[str]` | Parallel array — edge ID for each neighbor |
| `version` | `uint64` | Monotonically increasing, bumped on every mutation |
| `updated_at` | `uint64` | Timestamp of last mutation (epoch ms) |

### 3.2 Edge

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Deterministic: `"{source_id}--{edge_type}--{target_id}"` |
| `source_id` | `str` | Origin node ID |
| `target_id` | `str` | Destination node ID |
| `edge_type` | `str` | Schema-defined type (e.g., `"foreign_key"`, `"learned"`) |
| `directed` | `bool` | Directional or bidirectional |
| `properties` | `dict[str, str]` | Schema-defined typed properties |
| `version` | `uint64` | Monotonically increasing |
| `updated_at` | `uint64` | Timestamp of last mutation (epoch ms) |

### 3.3 Property Types

Same as zvec's scalar types — no new type system:

| Category | Types |
|----------|-------|
| Integer | `INT32`, `INT64`, `UINT32`, `UINT64` |
| Float | `FLOAT`, `DOUBLE` |
| Text | `STRING` |
| Boolean | `BOOL` |

### 3.4 Adjacency List Design

- `neighbor_ids` and `neighbor_edge_ids` are parallel arrays — index `i` in both refers to the same relationship
- **Undirected edges:** both endpoints get a neighbor entry
- **Directed edges:** both endpoints still get the entry (enabling traversal in either direction), but the edge document records the actual direction via `source_id`/`target_id`
- This is the **hybrid model**: adjacency on nodes for fast traversal, edge documents for properties/embeddings/metadata

---

## 4. Storage Architecture

### 4.1 Graph Collection with KV-Backed Storage

The graph engine uses a **Graph Collection** as a new collection type in zvec, backed by a direct RocksDB KV store optimized for point lookups, traversals, and concurrent agent reads.

```
GraphCollection (public API)
  |
  +-- graph_meta.pb              schema + metadata (protobuf file)
  |
  +-- GraphKVStore               ALL graph data (primary store)
  |     |-- RocksDB instance
  |     |     |-- CF "nodes"     key=node_id   val=GraphNodeProto
  |     |     |-- CF "edges"     key=edge_id   val=GraphEdgeProto
  |     |     |-- CF "idx:..."   secondary indexes (inverted)
  |     |     +-- CF "default"   RocksDB internal
  |     +-- supports: MultiGet, WriteBatch, concurrent lock-free reads
  |
  +-- MutationEngine             writes to GraphKVStore
  +-- TraversalEngine            reads from GraphKVStore
  +-- GraphSchema                validates types + constraints (loaded from graph_meta.pb)
```

When an embedding algorithm (e.g., GraphSAGE) later produces vectors, it creates a standard zvec Collection separately and stores vectors there. **No vectors in the graph store.** The agent orchestrates the two-phase lookup: vector search for seed IDs, then graph traversal.

### 4.2 Why RocksDB (not zvec Collections)

The graph engine previously used zvec Collections (Arrow IPC columnar files) for storage. This was a poor fit:

| Threads | Throughput (1-hop) | p50 latency | Scaling factor |
|---------|--------------------|-------------|----------------|
| 1       | 50 trav/s          | 20ms        | 1.00x          |
| 4       | 39 trav/s          | 77ms        | 0.20x          |
| 16      | 52 trav/s          | 262ms       | 0.07x          |
| 32      | 50 trav/s          | 541ms       | 0.03x          |

**Root cause**: `SegmentImpl::Fetch()` acquires an exclusive `std::mutex` (`seg_mtx_`), serializing all concurrent reads within a segment. Since a 10K-node graph fits in a single segment, throughput is capped regardless of thread count.

RocksDB provides lock-free concurrent reads via internal MVCC snapshots, true atomic writes via WriteBatch, and native secondary indexes via column families + merge operators.

### 4.3 Protobuf Serialization

Data is serialized as protobuf for RocksDB value encoding:

```protobuf
message GraphNodeProto {
  string id = 1;
  string node_type = 2;
  map<string, string> properties = 3;
  repeated string neighbor_ids = 4;
  repeated string neighbor_edge_ids = 5;
  uint64 version = 6;
  uint64 updated_at = 7;
}

message GraphEdgeProto {
  string id = 1;
  string source_id = 2;
  string target_id = 3;
  string edge_type = 4;
  bool directed = 5;
  map<string, string> properties = 6;
  uint64 version = 7;
  uint64 updated_at = 8;
}

message IndexEntry {
  repeated string ids = 1;
}
```

**Why protobuf?** Schema evolution. When we add fields later (edge weights, labels), protobuf handles forward/backward compatibility. Serialization cost (~1us per node) is negligible vs RocksDB read (~10-50us).

### 4.4 Disk Layout

```
graph_path/
  graph_meta.pb              (schema + metadata protobuf)
  kv/                        (GraphKVStore — single RocksDB instance)
    LOCK
    CURRENT
    MANIFEST-*
    OPTIONS-*
    *.sst                    (all CFs: nodes, edges, indexes)
    *.log                    (WAL files)
```

### 4.5 RocksDB Configuration

```
Column families: "default", "nodes", "edges", "idx:nodes:node_type",
                 "idx:edges:source_id", "idx:edges:target_id",
                 "idx:edges:edge_type", plus user-created indexes
Bloom filter:    10 bits/key
Compression:     LZ4
Block cache:     64MB shared LRU
WAL:             enabled
Merge operator:  custom IndexMergeOperator for index CFs
```

---

## 5. Secondary Indexes

### 5.1 Design

Secondary indexes are inverted: `field_value → list of entity IDs`.

Stored in a dedicated RocksDB column family per index:

```
CF "idx:nodes:node_type"
  key = "merchant"      → IndexEntry{ids: ["m1", "m2", "m3", ...]}
  key = "customer"      → IndexEntry{ids: ["c1", "c2", ...]}

CF "idx:edges:source_id"
  key = "merchant_42"   → IndexEntry{ids: ["e1", "e2", "e3"]}

CF "idx:edges:edge_type"
  key = "settled_at"    → IndexEntry{ids: ["e1", "e5", ...]}

CF "idx:nodes:name"     (user-created)
  key = "neodb.customers" → IndexEntry{ids: ["t1"]}
```

### 5.2 Auto-created indexes (always present)

| Collection | Field       | Purpose                              |
|------------|-------------|--------------------------------------|
| nodes      | node_type   | Filter by type                       |
| edges      | source_id   | Find edges from a source node        |
| edges      | target_id   | Find edges to a target node          |
| edges      | edge_type   | Filter edges by type                 |

### 5.3 User-created indexes

Users can add indexes on any property field:

```cpp
engine.CreateIndex("nodes", "name");
engine.CreateIndex("nodes", "risk_score");
engine.CreateIndex("edges", "currency");
```

### 5.4 Filter execution

```
FilterNodes("node_type = 'table' AND name = 'neodb.customers'")

  1. Lookup CF "idx:nodes:node_type", key "table"
     → ids_a = {"t1", "t2", "t3", ...}

  2. Lookup CF "idx:nodes:name", key "neodb.customers"
     → ids_b = {"t1"}

  3. Intersect: result = ids_a ∩ ids_b = {"t1"}

  4. Fetch full nodes: kv_store_->GetNodes({"t1"})
     → RocksDB MultiGet on "nodes" CF
```

For non-indexed fields, fall back to scan + filter over the relevant CF.

### 5.5 Index maintenance

Indexes are updated synchronously within the same `WriteBatch` as the primary data. Using RocksDB `Merge` operator for index updates avoids read-modify-write:

```
WriteBatch:
  Put(nodes_cf, "merchant_42", serialized_proto)           // primary
  Merge(idx_node_type_cf, "merchant", add("merchant_42"))  // index update
  Merge(idx_name_cf, "Acme Corp", add("merchant_42"))      // index update
```

The `IndexMergeOperator` encodes operands as `[1 byte op_type][entity_id]` — kOpAdd (0x01) or kOpRemove (0x02). On compaction, it deserializes the existing `IndexEntry`, applies all ADD/REMOVE operands, and serializes back.

---

## 6. Graph Schema

The schema layer validates all mutations and defines the structure of the graph.

### 6.1 Schema Definition

```python
schema = GraphSchema(
    name="data_catalog",
    node_types=[
        NodeType(
            name="table",
            properties=[
                PropertyDef("database", "string"),
                PropertyDef("row_count", "int64", nullable=True),
            ],
        ),
        NodeType(
            name="column",
            properties=[
                PropertyDef("data_type", "string"),
                PropertyDef("nullable", "bool"),
            ],
        ),
    ],
    edge_types=[
        EdgeType(name="has_column", directed=True),
        EdgeType(
            name="foreign_key",
            directed=True,
            properties=[PropertyDef("on_column", "string")],
        ),
        EdgeType(
            name="learned",
            directed=False,
            properties=[PropertyDef("confidence", "double")],
        ),
    ],
    edge_constraints=[
        EdgeConstraint("has_column", source="table", target="column"),
        EdgeConstraint("foreign_key", source="table", target="table"),
        # "learned" intentionally unconstrained — any node to any node
    ],
)
```

### 6.2 Schema Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Validation in C++** | Every `add_node`, `add_edge`, `update` validates against schema before touching storage. Invalid mutations rejected with clear errors. |
| **Edge constraints are optional** | If defined, the engine enforces which node types an edge can connect. If omitted, that edge type is unconstrained. |
| **Schema persisted as protobuf** | Stored in `graph_meta.pb` alongside graph data. |

---

## 7. C++ Module Architecture

### 7.1 Directory Structure

```
src/
├── db/                              # existing zvec core (UNCHANGED)
│   ├── collection.cc
│   ├── index/
│   ├── sqlengine/
│   └── ...
├── graph/                           # graph module
│   ├── graph_collection.h/cc        # top-level API, lifecycle management
│   ├── graph_schema.h/cc            # schema definition, validation, persistence
│   ├── graph_node.h                 # GraphNode struct
│   ├── graph_edge.h                 # GraphEdge struct
│   ├── subgraph.h/cc                # Subgraph result with helpers
│   ├── traversal.h/cc               # multi-hop BFS, subgraph extraction
│   ├── mutation_engine.h/cc         # atomic add/remove/update for nodes and edges
│   ├── storage/
│   │   ├── storage_interface.h      # abstract storage interface
│   │   ├── graph_kv_store.h/cc      # RocksDB KV-backed implementation
│   │   └── index_merge_operator.h/cc# RocksDB merge operator for indexes
│   └── proto/
│       └── graph.proto              # schema, metadata, data instance protos
├── binding/python/
│   ├── python_collection.cc         # existing (unchanged)
│   └── python_graph.cc              # pybind11 bindings for graph engine
```

### 7.2 Class Hierarchy

```
GraphCollection                      # top-level: create, open, destroy, flush
  |-- GraphSchema                    # defines and validates node/edge types
  |-- GraphKVStore                   # RocksDB KV store (implements StorageInterface)
  |     \-- IndexMergeOperator       # merge operator for secondary index CFs
  |-- MutationEngine                 # add/remove/update with atomic batches
  \-- TraversalEngine                # multi-hop expand, subgraph extraction
        \-- Subgraph                 # result object: nodes + edges + metadata
```

### 7.3 GraphCollection (public API)

```cpp
class GraphCollection {
 public:
  static std::unique_ptr<GraphCollection> Create(
      const std::string& path, const GraphSchema& schema);
  static std::unique_ptr<GraphCollection> Open(const std::string& path);
  void Destroy();
  Status Flush();
  const GraphSchema& GetSchema() const;

  // Node operations
  Status AddNode(const GraphNode& node);
  Status RemoveNode(const std::string& node_id);
  Status UpdateNode(const std::string& node_id,
                    const std::unordered_map<std::string, std::string>& props);
  std::vector<GraphNode> FetchNodes(const std::vector<std::string>& ids);
  std::vector<GraphNode> FilterNodes(const std::string& filter_expr,
                                     int limit = 1000);

  // Edge operations
  Status AddEdge(const std::string& source_id, const std::string& target_id,
                 const std::string& edge_type,
                 const std::unordered_map<std::string, std::string>& props);
  Status RemoveEdge(const std::string& edge_id);
  Status UpdateEdge(const std::string& edge_id,
                    const std::unordered_map<std::string, std::string>& props);
  std::vector<GraphEdge> FetchEdges(const std::vector<std::string>& ids);
  std::vector<GraphEdge> FilterEdges(const std::string& filter_expr,
                                     int limit = 1000);

  // Traversal
  Subgraph Traverse(const TraversalParams& params);

  // Index operations
  Status CreateIndex(const std::string& entity, const std::string& field);
  Status DropIndex(const std::string& entity, const std::string& field);
};
```

### 7.4 StorageInterface

The abstract interface enables future storage backends:

```cpp
class StorageInterface {
 public:
  virtual ~StorageInterface() = default;

  // Node operations
  virtual Status UpsertNodes(const std::vector<GraphNode>& nodes) = 0;
  virtual Status DeleteNodes(const std::vector<std::string>& ids) = 0;
  virtual Result<std::vector<GraphNode>> FetchNodes(
      const std::vector<std::string>& ids) = 0;
  virtual Result<std::vector<GraphNode>> FetchNodesLite(
      const std::vector<std::string>& ids) = 0;
  virtual Result<std::vector<GraphNode>> FilterNodes(
      const std::string& filter, int limit = 1000) = 0;

  // Edge operations
  virtual Status UpsertEdges(const std::vector<GraphEdge>& edges) = 0;
  virtual Status DeleteEdges(const std::vector<std::string>& ids) = 0;
  virtual Result<std::vector<GraphEdge>> FetchEdges(
      const std::vector<std::string>& ids) = 0;
  virtual Result<std::vector<GraphEdge>> FilterEdges(
      const std::string& filter, int limit = 1000) = 0;

  // Atomic batch (nodes + edges in one write)
  virtual Status AtomicBatch(const std::vector<Mutation>& mutations) = 0;

  // Index management
  virtual Status CreateIndex(const std::string& entity,
                             const std::string& field) = 0;
  virtual Status DropIndex(const std::string& entity,
                           const std::string& field) = 0;
};
```

`GraphKVStore` implements this using RocksDB column families, MultiGet, and WriteBatch.

### 7.5 Concurrency Model

| Aspect | Design |
|--------|--------|
| Thread safety | `GraphCollection` is thread-safe — multiple agents call concurrently |
| Reads | Lock-free — RocksDB MultiGet with internal MVCC snapshots |
| Writes | Per-graph mutex for adjacency list consistency |
| Python GIL | Released at pybind11 boundary — all C++ work runs without Python lock |
| Scaling | N agents → N concurrent C++ threads, no Python serialization |

---

## 8. Read Flows

### 8.1 Graph Traversal (agent hot path)

```
Agent: engine.Traverse({start_ids, max_depth=3, max_nodes=200})

TraversalEngine::Traverse()
  |
  |  1. Fetch seed nodes (lite — adjacency only)
  |     kv_store_->FetchNodesLite(start_ids)
  |       → RocksDB MultiGet on "nodes" CF
  |       → deserialize GraphNodeProto (skip properties)
  |       → return nodes with adjacency lists
  |
  |  2. BFS hop: collect neighbor edge IDs from adjacency
  |     kv_store_->FetchEdges(candidate_edge_ids)
  |       → RocksDB MultiGet on "edges" CF
  |       → deserialize GraphEdgeProto
  |       → apply edge filter
  |
  |  3. Fetch target nodes (lite)
  |     kv_store_->FetchNodesLite(target_node_ids)
  |       → RocksDB MultiGet on "nodes" CF
  |       → apply node filter, budget check
  |
  |  4. Repeat hops 2-3 until max_depth
  |
  +→ Return Subgraph{nodes, edges}
```

**Properties:**
- Zero zvec Collection involvement — no Arrow, no IPC, no seg_mtx_
- All reads via RocksDB MultiGet — lock-free, concurrent
- Protobuf deserialization (~1us/node) replaces Arrow scalar reconstruction
- Multiple agents traverse concurrently with no contention

### 8.2 Direct Fetch (by ID)

```
Agent: engine.FetchNodes(["merchant_42", "acct_1"])

  kv_store_->FetchNodes(ids)
    → RocksDB MultiGet on "nodes" CF
    → return full GraphNode with all properties
```

### 8.3 Filter Query

```
Agent: engine.FilterNodes("node_type = 'table' AND name = 'neodb.customers'")

  kv_store_->FilterNodes(expr, limit)
    |
    |  1. Parse filter into (field, op, value) pairs
    |
    |  2. For each indexed field:
    |     lookup CF "idx:nodes:{field}", key = value
    |     → get candidate ID set
    |
    |  3. Intersect all candidate sets
    |
    |  4. Fetch matching nodes via MultiGet
    |
    +→ Return vector<GraphNode>
```

### 8.4 GraphRAG Query (vector search → graph traversal)

```
Agent workflow:
  |
  |  1. Vector search (zvec collection, managed separately)
  |     vec_col = Collection.open("path/to/entity_embeddings")
  |     results = vec_col.query(vector=query_vec, topk=10)
  |       → HNSW search → returns node IDs with similarity scores
  |
  |  2. Fetch seed nodes from graph collection
  |     graph.FetchNodes(seed_ids)
  |       → RocksDB MultiGet → full nodes with properties
  |
  |  3. Traverse from seeds
  |     graph.Traverse({seed_ids, max_depth=2})
  |       → BFS via RocksDB MultiGet
  |
  |  4. Assemble subgraph context for LLM
  |
  +→ LLM call with graph context
```

The vector collection is a standard zvec Collection, created and managed by the embedding algorithm. The graph collection has no knowledge of it.

---

## 9. Write Flows

### 9.1 AddNode

```
MutationEngine::AddNode(node)
  |
  |  1. Validate against schema
  |
  |  2. Set system fields (version=1, updated_at=now)
  |
  |  3. Atomic write to KV store (single WriteBatch)
  |     rocksdb::WriteBatch batch;
  |     batch.Put(nodes_cf, node.id, GraphNodeProto.Serialize())
  |     batch.Merge(idx_node_type_cf, node.node_type, add(node.id))
  |     // for each indexed property:
  |     batch.Merge(idx_{prop}_cf, prop_value, add(node.id))
  |     db_->Write(write_opts, &batch)
  |
  +→ Return OK
```

### 9.2 AddEdge

```
MutationEngine::AddEdge(source_id, target_id, edge_type, properties)
  |
  |  1. Fetch source + target from KV store
  |
  |  2. Validate edge against schema
  |
  |  3. Build edge, update adjacency lists on both nodes
  |
  |  4. Single atomic WriteBatch:
  |     batch.Put(edges_cf, edge.id, GraphEdgeProto.Serialize())
  |     batch.Put(nodes_cf, source.id, updated_source_proto)
  |     batch.Put(nodes_cf, target.id, updated_target_proto)
  |     batch.Merge(idx_source_id_cf, source_id, add(edge.id))
  |     batch.Merge(idx_target_id_cf, target_id, add(edge.id))
  |     batch.Merge(idx_edge_type_cf, edge_type, add(edge.id))
  |     db_->Write(write_opts, &batch)
  |     → truly atomic: edge + both adjacency updates + all indexes
  |
  +→ Return OK
```

### 9.3 RemoveNode (cascade delete)

```
MutationEngine::RemoveNode(node_id)
  |
  |  1. Fetch node from KV store
  |  2. Fetch all connected edges (batch MultiGet)
  |  3. Compute all adjacency updates on neighbor nodes
  |
  |  4. Single atomic WriteBatch:
  |     batch.Delete(nodes_cf, node_id)
  |     batch.Merge(idx_node_type_cf, node.node_type, remove(node_id))
  |     for each connected edge:
  |       batch.Delete(edges_cf, edge.id)
  |       batch.Merge(idx_*_cf, ..., remove(edge.id))
  |     for each neighbor node:
  |       batch.Put(nodes_cf, neighbor.id, updated_neighbor_proto)
  |     db_->Write(write_opts, &batch)
  |     → truly atomic cascade delete
  |
  +→ Return OK
```

---

## 10. Traversal Engine

### 10.1 Core Operation: Subgraph Extraction

The primary traversal operation is "given seed nodes, expand N hops with filters, return the subgraph."

```
traverse(start_ids, depth, edge_filter, node_filter, max_nodes)
  -> returns Subgraph { nodes, edges, truncated }
```

### 10.2 Traversal Algorithm

```
Input:  seed node IDs, max_depth, edge_filter, node_filter, max_nodes
Output: Subgraph (all discovered nodes + edges)

visited_nodes = set(seed_ids)
visited_edges = set()
frontier = seed_ids

for hop in 1..max_depth:
    # Use adjacency lists on frontier nodes to get candidate edge IDs
    candidate_edge_ids = []
    for node in frontier:
        for (neighbor_id, edge_id) in zip(node.neighbor_ids, node.neighbor_edge_ids):
            if neighbor_id not in visited_nodes and edge_id not in visited_edges:
                candidate_edge_ids.append(edge_id)

    # Batch fetch edges, apply edge_filter
    edges = storage.FetchEdges(candidate_edge_ids)
    filtered_edges = apply_filter(edges, edge_filter)

    # Batch fetch target nodes, apply node_filter + budget
    target_ids = unique_targets(filtered_edges) - visited_nodes
    target_nodes = storage.FetchNodes(target_ids)
    filtered_nodes = apply_filter(target_nodes, node_filter)

    # Budget check
    if max_nodes > 0 and visited_nodes.size() + filtered_nodes.size() > max_nodes:
        truncate and set truncated = true
        break

    visited_nodes.update(filtered_nodes.ids)
    visited_edges.update(filtered_edges.ids)
    frontier = filtered_nodes

return Subgraph(nodes, edges, truncated)
```

**Batch-oriented:** Each hop is 2 storage calls (batch fetch edges, batch fetch nodes) regardless of fan-out. For 3 hops: ~6 calls total. All in C++, no Python round-trips.

### 10.3 Subgraph Result Object

```cpp
struct Subgraph {
  std::vector<GraphNode> nodes;
  std::vector<GraphEdge> edges;
  bool truncated = false;

  // Convenience accessors
  std::vector<const GraphNode*> NodesOfType(const std::string& type) const;
  std::vector<const GraphEdge*> EdgesOfType(const std::string& type) const;
  std::vector<const GraphEdge*> EdgesFrom(const std::string& node_id) const;
  std::vector<const GraphEdge*> EdgesTo(const std::string& node_id) const;
  std::vector<const GraphNode*> Neighbors(const std::string& node_id) const;

  // Serialization for agent consumption
  std::string ToJson() const;
  std::string ToText() const;
};
```

---

## 11. Concurrent Read Scaling

### 11.1 Why RocksDB scales and zvec Collections don't

```
zvec Collection::Fetch (previous approach):
  ┌─────────────────────────────┐
  │  shared_lock(schema_mtx)    │  ← shared, OK
  │  for each pk:               │
  │    id_map_->has(pk)         │  ← RocksDB Get (lock-free) OK
  │    segment->Fetch(id)       │  ← exclusive seg_mtx_ ← BOTTLENECK
  │      seg_mtx_ is std::mutex │     serializes ALL concurrent reads
  │      └─ Arrow IPC read      │     within the same segment
  └─────────────────────────────┘

GraphKVStore::FetchNodes (current):
  ┌─────────────────────────────┐
  │  db_->MultiGet(             │  ← single RocksDB call
  │    read_opts,               │     lock-free (internal MVCC snapshots)
  │    nodes_cf,                │     parallelizes I/O across keys
  │    N keys,                  │     block cache is thread-safe
  │    values, statuses)        │
  │                             │
  │  for each value:            │
  │    proto.ParseFromString()  │  ← CPU-only, no locks, no I/O
  └─────────────────────────────┘
```

**Expected scaling**: near-linear up to 16-32 threads for in-memory workloads (block cache hits), gradual saturation beyond from CPU/memory bandwidth.

---

## 12. Python API

Thin layer — type conversions, ergonomic wrappers, agent-friendly formatting. No graph logic.

### 12.1 Graph Lifecycle

```python
from zvec.graph import Graph, GraphSchema, NodeType, EdgeType, EdgeConstraint, PropertyDef

schema = GraphSchema(name="my_graph", node_types=[...], edge_types=[...])

# Create a new graph
graph = Graph.create(path="/data/my_graph", schema=schema)

# Open existing
graph = Graph.open(path="/data/my_graph")

# Flush / destroy
graph.flush()
graph.destroy()
```

### 12.2 Mutations

```python
# Add nodes
graph.add_node(id="orders", node_type="table", properties={
    "database": "analytics",
    "row_count": "1000000",
})

# Add edges
graph.add_edge(
    source="orders",
    target="customers",
    edge_type="foreign_key",
    properties={"on_column": "customer_id"},
)

# Update properties
graph.update_node("orders", properties={"row_count": "1500000"})

# Delete
graph.remove_node("orders")       # cascade-deletes all connected edges
graph.remove_edge("orders--foreign_key--customers")
```

### 12.3 Query and Traversal

```python
# Fetch by ID
node = graph.fetch_node("orders")
edge = graph.fetch_edge("orders--has_column--orders.customer_id")

# Filter
tables = graph.filter_nodes("node_type = 'table'")
fk_edges = graph.filter_edges("edge_type = 'foreign_key'")

# Subgraph traversal
subgraph = graph.traverse(
    start=["orders", "customers"],
    depth=2,
    max_nodes=100,
    edge_filter="edge_type = 'has_column'",
)

# Access results
subgraph.nodes          # list[GraphNode]
subgraph.edges          # list[GraphEdge]
subgraph.to_json()      # JSON serialization
subgraph.to_text()      # LLM-friendly text
```

### 12.4 Index Management

```python
graph.create_index("nodes", "database")
graph.create_index("edges", "on_column")
graph.drop_index("nodes", "database")
```

---

## 13. Agent Optimization

### 13.1 Response Design

All responses are structured for direct consumption by LLM agents:

| Method | Returns | Agent Use |
|--------|---------|-----------|
| `traverse()` | `Subgraph` | Context gathering for generation |
| `filter_nodes()` | `list[GraphNode]` | Find nodes by type/property |
| `filter_edges()` | `list[GraphEdge]` | Find edges by type/property |
| `fetch_node()` | `GraphNode` | Point lookup |
| `fetch_edge()` | `GraphEdge` | Point lookup |

### 13.2 Text Representation Example

```
Subgraph: 4 nodes, 5 edges

Nodes:
  [table] orders {database: analytics, row_count: 1000000}
  [table] customers {database: analytics, row_count: 50000}
  [column] orders.customer_id {data_type: INT64}
  [column] orders.amount {data_type: DOUBLE}

Edges:
  orders --has_column--> orders.customer_id
  orders --has_column--> orders.amount
  orders --foreign_key--> customers {on_column: customer_id}
  customers --learned-- orders {confidence: 0.92}
```

### 13.3 Performance Targets

| Operation | Target Latency | Notes |
|-----------|---------------|-------|
| Single-hop traversal | < 5ms | Batch fetch from adjacency |
| 3-hop subgraph extraction | < 20ms | 6 batch operations |
| Node/edge point lookup | < 1ms | Direct RocksDB MultiGet |
| Add node | < 2ms | Single WriteBatch |
| Add edge | < 5ms | Atomic WriteBatch (3 writes) |
| Filter query (indexed) | < 5ms | Index lookup + MultiGet |

---

## 14. Scale Considerations (100K Agent Velocity)

### 14.1 Traversal Control

**Node budget.** Depth alone is not sufficient — `depth=3` on a highly connected graph can explode to 10K+ nodes. The `traverse()` API accepts a `max_nodes` parameter. When the budget is exhausted mid-traversal, the engine stops expanding and returns what it has, with a `truncated: true` flag on the `Subgraph`.

**Cycle detection.** The traversal engine tracks `visited_nodes` which naturally prevents cycles. No node is visited twice.

**Beam pruning.** At each hop, rather than expanding all neighbors, score candidates and keep only the top-N per hop. Configurable via a `beam_width` parameter.

### 14.2 Write Contention

The per-graph mutex serializes all writes. This is sufficient for typical GraphRAG workloads (bulk load at build time, read-heavy traversals). For higher write throughput:

1. **Per-node locking** — Replace single mutex with lock table keyed by node ID
2. **Write batching with WAL** — Queue mutations, flush periodically
3. **Lock-free adjacency with CAS** — Compare-and-swap on adjacency updates

### 14.3 Observability

**Node/edge versioning.** Every node and edge carries `version` and `updated_at` fields, automatically managed by the MutationEngine.

**Traversal telemetry.** Each `traverse()` call can produce a lightweight trace of hops, candidates, and filter results.

---

## 15. Traversal Algorithm Analysis

### 15.1 Current Implementation

The v0.1 traversal engine uses **BFS with batched storage lookups**:

```
For each hop (1..max_depth):
  Collect edge IDs from frontier adjacency lists
  Batch-fetch edge documents, apply in-memory filter
  Batch-fetch neighbor nodes, apply in-memory filter
  Check max_nodes budget, update visited set
```

- **Complexity:** O(V + E) per traversal
- **Parallelism:** None — single-threaded BFS
- **Filter execution:** Simple string parsing (`"field = 'value'"`)
- **Cycle detection:** Hash set of visited node IDs

This is correct and sufficient for GraphRAG: shallow traversals (2-5 hops) over small neighborhoods (< 1000 nodes). A typical query completes in < 10ms. The LLM inference that follows takes 500ms-5s.

### 15.2 Incremental Optimization Path

1. **Pre-allocated vectors** — `reserve()` before push_back loops
2. **Node cache in Traverse()** — method-local cache eliminates redundant RocksDB fetches
3. **Skip vector deserialization** — `FetchNodesLite()` skips properties during traversal
4. **Batch edge fetches** — single MultiGet instead of per-edge fetches in mutation paths
5. **Compiled filters** — pre-parse filter expressions into predicate tree
6. **Parallel frontier expansion** — partition frontier across threads
7. **GraphBLAS backend** — for analytical workloads requiring full-graph iteration

---

## 16. Open Questions

1. **Bulk loading API.** Should there be a batch `add_nodes` / `add_edges` that optimizes for initial graph population?
2. **Schema migration tooling.** What does `alter_node_type` or `add_property_to_existing_type` look like operationally?
3. **Subgraph caching.** For hot subgraphs that many agents request, should there be an in-memory cache layer?
4. **MCP tool integration.** Should the graph API expose itself as an MCP server for direct agent tool calling?
5. **Multi-graph queries.** Should an agent be able to traverse across multiple graphs?
6. **Index storage format.** For high-cardinality fields, protobuf lists are fine. For low-cardinality fields mapping to millions of IDs, consider roaring bitmaps.
