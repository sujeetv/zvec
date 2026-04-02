# zvec Graph Engine — Usage Guide

This guide covers the Python API for creating and working with property
graphs in zvec. All examples use the current `Graph` API backed by the
RocksDB KV store.

---

## 1. Define a Schema

A graph schema declares the types of nodes and edges your graph supports,
along with their properties, vector fields, and structural constraints.

```python
from zvec.graph import (
    GraphSchema,
    NodeType,
    EdgeType,
    EdgeConstraint,
    PropertyDef,
)

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
            name="learned_similarity",
            directed=False,
            properties=[PropertyDef("confidence", "double")],
        ),
    ],
    edge_constraints=[
        EdgeConstraint("has_column", source="table", target="column"),
        EdgeConstraint("foreign_key", source="table", target="table"),
        # No constraint on learned_similarity — any node pair allowed
    ],
)
```

**Supported data types:** `string`, `bool`, `int32`, `int64`, `uint32`,
`uint64`, `float`, `double`, `vector_fp32`, `vector_fp64`, `vector_fp16`,
`vector_int8`.

---

## 2. Create and Open a Graph

```python
from zvec.graph import Graph

# Create a new graph on disk
graph = Graph.create(path="/data/my_catalog", schema=schema)

# Later, reopen the same graph (schema is restored from disk)
graph = Graph.open(path="/data/my_catalog")
```

---

## 3. Add Nodes

```python
# Node with scalar properties
graph.add_node(
    id="orders",
    node_type="table",
    properties={"database": "analytics", "row_count": "1000000"},
)

# Node with an embedding vector
graph.add_node(
    id="customers",
    node_type="table",
    properties={"database": "analytics"},
    vectors={"desc_emb": [0.12, -0.45, 0.78, ...]},  # 768-dim
)

# Column node
graph.add_node(
    id="orders.customer_id",
    node_type="column",
    properties={"data_type": "INT64", "nullable": "false"},
)
```

**Schema validation:** If you try to add a node with an unregistered type,
a `ValueError` is raised immediately.

```python
graph.add_node(id="x", node_type="nonexistent")
# ValueError: Unknown node type: nonexistent
```

---

## 4. Add Edges

Edges connect two existing nodes. The engine validates both the edge type
and any constraints defined in the schema.

```python
# Directed edge: table -> column
graph.add_edge(
    source="orders",
    target="orders.customer_id",
    edge_type="has_column",
)

# Directed edge with properties
graph.add_edge(
    source="orders",
    target="customers",
    edge_type="foreign_key",
    properties={"on_column": "customer_id"},
)

# Undirected edge between any node types (no constraint)
graph.add_edge(
    source="orders",
    target="customers",
    edge_type="learned_similarity",
    properties={"confidence": "0.92"},
)
```

**Edge IDs** are deterministic: `source--edge_type--target`
(e.g., `orders--has_column--orders.customer_id`).

**Adding the same edge twice is idempotent** — it succeeds without
creating duplicates.

**Constraint enforcement:**

```python
graph.add_edge(source="col1", target="col2", edge_type="has_column")
# ValueError: constraint violation — has_column requires table -> column
```

---

## 5. Update Nodes

Updates merge new properties into existing ones and bump the version.

```python
graph.update_node("orders", properties={"row_count": "2000000"})

node = graph.fetch_node("orders")
print(node.properties["row_count"])  # "2000000"
print(node.version)                  # 2
```

---

## 6. Remove Nodes and Edges

Removing an edge cleans up adjacency lists on both connected nodes.

```python
graph.remove_edge("orders--has_column--orders.customer_id")

# Verify adjacency was cleaned
node = graph.fetch_node("orders")
assert "orders.customer_id" not in node.neighbor_ids
```

Removing a node **cascade-deletes** all connected edges.

```python
graph.remove_node("orders")

# The node is gone
assert graph.fetch_node("orders") is None

# All edges to/from orders are also gone
assert graph.fetch_edge("orders--foreign_key--customers") is None

# The other end's adjacency is cleaned up
customers = graph.fetch_node("customers")
assert "orders" not in customers.neighbor_ids
```

---

## 7. Create Secondary Indexes

The graph engine automatically creates indexes on `node_type`,
`source_id`, `target_id`, and `edge_type`. You can create additional
indexes on any property field to speed up filter queries.

```python
# Index a node property
graph.create_index("nodes", "database")

# Index an edge property
graph.create_index("edges", "on_column")

# Drop an index when no longer needed
graph.drop_index("nodes", "database")
```

Indexes are backed by RocksDB column families with merge operators for
efficient maintenance. Creating an index backfills all existing data.

---

## 8. Fetch Nodes and Edges

```python
# Fetch a single node by ID
node = graph.fetch_node("orders")
if node:
    print(node.id)              # "orders"
    print(node.node_type)       # "table"
    print(node.properties)      # {"database": "analytics", "row_count": "1000000"}
    print(node.neighbor_ids)    # ["orders.customer_id", "customers", ...]
    print(node.version)         # 1
    print(node.updated_at)      # epoch milliseconds

# Returns None if not found
node = graph.fetch_node("nonexistent")  # None

# Fetch an edge by its deterministic ID
edge = graph.fetch_edge("orders--has_column--orders.customer_id")
if edge:
    print(edge.source_id)   # "orders"
    print(edge.target_id)   # "orders.customer_id"
    print(edge.edge_type)   # "has_column"
    print(edge.directed)    # True
    print(edge.properties)  # {}
```

---

## 9. Filter Nodes and Edges

Filter queries use the format `field = 'value'`. Multiple conditions
can be combined with `AND`.

```python
# Filter nodes by type
tables = graph.filter_nodes("node_type = 'table'")

# Filter nodes by property (faster with an index on "database")
analytics_tables = graph.filter_nodes("node_type = 'table' AND database = 'analytics'")

# Filter edges by type
fk_edges = graph.filter_edges("edge_type = 'foreign_key'")

# Filter edges by source
edges_from_orders = graph.filter_edges("source_id = 'orders'")

# Limit results
top_10 = graph.filter_nodes("node_type = 'table'", limit=10)
```

Filters on indexed fields (auto or user-created) use index lookups.
Non-indexed fields fall back to a full scan.

---

## 10. Traverse the Graph

Traversal performs a multi-hop BFS from seed nodes, collecting the
neighborhood into a `Subgraph`.

```python
# 1-hop: get orders and its direct neighbors
sg = graph.traverse(start="orders", depth=1)
print(f"{len(sg.nodes)} nodes, {len(sg.edges)} edges")

# 2-hop: reach columns of related tables
sg = graph.traverse(start="orders", depth=2)

# Multiple seeds
sg = graph.traverse(start=["orders", "customers"], depth=1)
```

### Edge Filtering

Only follow certain edge types during traversal.

```python
sg = graph.traverse(
    start="orders",
    depth=2,
    edge_filter="edge_type = 'has_column'",
)
# Result: orders + its columns only, no foreign key traversal
```

### Node Filtering

Only include certain node types in results (seed nodes always included).

```python
sg = graph.traverse(
    start="orders",
    depth=1,
    node_filter="node_type = 'column'",
)
# Result: orders (seed, always kept) + column neighbors only
```

### Budget Control

Cap the result size to prevent overwhelming LLM context windows.

```python
sg = graph.traverse(start="orders", depth=3, max_nodes=50)
if sg.truncated:
    print("Result was truncated — increase max_nodes for more context")
```

---

## 11. Inspect Subgraph Results

`Subgraph` provides convenience methods for filtering and serialization.

```python
sg = graph.traverse(start="orders", depth=2)

# Filter by type
tables = sg.nodes_of_type("table")
columns = sg.nodes_of_type("column")
fk_edges = sg.edges_of_type("foreign_key")

# Neighborhood queries within the subgraph
edges_out = sg.edges_from("orders")
edges_in = sg.edges_to("orders.customer_id")
neighbors = sg.neighbors("orders")

# Check truncation
print(sg.truncated)  # False
```

---

## 12. Serialize for Agent Consumption

Two output formats optimized for different agent workflows.

### JSON (for programmatic parsing)

```python
json_str = sg.to_json()
# {"nodes":[{"id":"orders","node_type":"table","properties":{...}}, ...],
#  "edges":[{"id":"orders--has_column--orders.customer_id", ...}],
#  "truncated":false}
```

### Text (for LLM context injection)

```python
text = sg.to_text()
# Subgraph: 5 nodes, 4 edges
# Nodes:
#   [table] orders {database: analytics, row_count: 1000000}
#   [table] customers {database: analytics}
#   [column] orders.customer_id {data_type: INT64}
#   [column] orders.amount {data_type: DOUBLE}
#   [column] customers.id {data_type: INT64}
# Edges:
#   orders --has_column--> orders.customer_id
#   orders --has_column--> orders.amount
#   orders --foreign_key--> customers
#   customers --has_column--> customers.id
```

---

## 13. GraphRAG Workflow (End-to-End)

The full pattern: natural language question -> vector search for seed
entities -> graph traversal for context -> feed to LLM.

```python
from zvec.graph import Graph

# Assume graph is already created and populated
graph = Graph.open("/data/my_catalog")

# Step 1: Embed the user's question
query_embedding = your_embedding_model.encode(
    "What is the total revenue by customer segment?"
)

# Step 2: Vector search for relevant tables
# (uses a separate zvec Collection with HNSW index)
import zvec
vec_col = zvec.Collection.open("path/to/table_embeddings")
results = vec_col.query(vector=query_embedding, topk=5)
seed_ids = [r.id for r in results]
# e.g., ["orders", "customers", "segments"]

# Step 3: Traverse to gather schema context
subgraph = graph.traverse(
    start=seed_ids,
    depth=2,
    max_nodes=100,
)

# Step 4: Feed to LLM as context
context = subgraph.to_text()
prompt = f"""Given the following database schema context:

{context}

Write a SQL query to answer: What is the total revenue by customer segment?
"""
response = your_llm.generate(prompt)
```

---

## 14. Graph Lifecycle

```python
# Flush pending writes to disk
graph.flush()

# Destroy a graph permanently (irreversible!)
graph.destroy()
```

---

## 15. Data Type Reference

| Python string   | Proto enum        | Use case                    |
|-----------------|-------------------|-----------------------------|
| `"string"`      | `DT_STRING`       | Text properties             |
| `"bool"`        | `DT_BOOL`         | Boolean flags               |
| `"int32"`       | `DT_INT32`        | Small integers              |
| `"int64"`       | `DT_INT64`        | Large integers, row counts  |
| `"uint32"`      | `DT_UINT32`       | Unsigned integers           |
| `"uint64"`      | `DT_UINT64`       | Timestamps, versions        |
| `"float"`       | `DT_FLOAT`        | Single-precision floats     |
| `"double"`      | `DT_DOUBLE`       | Confidence scores           |
| `"vector_fp32"` | `DT_VECTOR_FP32`  | Standard embeddings         |
| `"vector_fp64"` | `DT_VECTOR_FP64`  | High-precision vectors      |
| `"vector_fp16"` | `DT_VECTOR_FP16`  | Memory-efficient vectors    |
| `"vector_int8"` | `DT_VECTOR_INT8`  | Quantized vectors           |
