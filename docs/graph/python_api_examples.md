# zvec Graph Engine -- Python API Examples

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
    VectorDef,
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
            vectors=[
                VectorDef("desc_emb", "vector_fp32", dimension=768),
            ],
        ),
        NodeType(
            name="column",
            properties=[
                PropertyDef("data_type", "string"),
                PropertyDef("nullable", "bool"),
            ],
            vectors=[
                VectorDef("desc_emb", "vector_fp32", dimension=768),
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
        # No constraint on learned_similarity -- any node pair allowed
    ],
)
```

**Supported data types:** `string`, `bool`, `int32`, `int64`, `uint32`,
`uint64`, `float`, `double`, `vector_fp32`, `vector_fp64`, `vector_fp16`,
`vector_int8`.

---

## 2. Create and Open a Graph

```python
import zvec

# Create a new graph on disk
graph = zvec.create_graph(path="/data/my_catalog", schema=schema)

# Later, reopen the same graph (schema is restored from disk)
graph = zvec.open_graph(path="/data/my_catalog")
```

---

## 3. Add Nodes

```python
# Simple node with scalar properties
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

**Adding the same edge twice is idempotent** -- it succeeds without
creating duplicates.

**Constraint enforcement:**

```python
graph.add_edge(source="col1", target="col2", edge_type="has_column")
# ValueError: constraint violation -- has_column requires table -> column
```

---

## 5. Fetch Nodes and Edges

```python
# Fetch a single node
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

## 6. Update Nodes

Updates merge new properties into existing ones and bump the version.

```python
graph.update_node("orders", properties={"row_count": "2000000"})

node = graph.fetch_node("orders")
print(node.properties["row_count"])  # "2000000"
print(node.version)                  # 2
```

---

## 7. Remove Nodes and Edges

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

## 8. Traverse the Graph

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
# Only follow has_column edges, skip foreign_key
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
    print("Result was truncated -- increase max_nodes for more context")
```

---

## 9. Inspect Subgraph Results

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

## 10. Serialize for Agent Consumption

Two output formats optimized for different agent workflows.

### JSON (for programmatic parsing)

```python
json_str = sg.to_json()
# {"nodes":[{"id":"orders","node_type":"table","properties":{"database":"analytics"}}, ...],
#  "edges":[{"id":"orders--has_column--orders.customer_id","source_id":"orders", ...}],
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

## 11. GraphRAG Workflow (End-to-End)

The full pattern: natural language question -> vector search for seed
entities -> graph traversal for context -> feed to LLM.

```python
import zvec
from zvec.graph import *

# Assume graph is already created and populated with embeddings

graph = zvec.open_graph("/data/my_catalog")

# Step 1: Embed the user's question
query_embedding = your_embedding_model.encode(
    "What is the total revenue by customer segment?"
)

# Step 2: Vector search for relevant tables
# (requires HNSW index on desc_emb -- see below)
seeds = graph.search_nodes(
    vector=query_embedding,
    vector_field="desc_emb",
    topk=3,
    filter="node_type = 'table'",
)
seed_ids = [n.id for n in seeds]
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

## 12. Graph Lifecycle

```python
# Repair orphaned adjacency references (after crash recovery)
graph.repair()

# Destroy a graph permanently (irreversible)
graph.destroy()
```

---

## 13. Data Type Reference

| Python string | Proto enum    | Use case                    |
|---------------|---------------|-----------------------------|
| `"string"`    | `DT_STRING`   | Text properties             |
| `"bool"`      | `DT_BOOL`     | Boolean flags               |
| `"int32"`     | `DT_INT32`    | Small integers              |
| `"int64"`     | `DT_INT64`    | Large integers, row counts  |
| `"uint32"`    | `DT_UINT32`   | Unsigned integers           |
| `"uint64"`    | `DT_UINT64`   | Timestamps, versions        |
| `"float"`     | `DT_FLOAT`    | Single-precision floats     |
| `"double"`    | `DT_DOUBLE`   | Confidence scores           |
| `"vector_fp32"` | `DT_VECTOR_FP32` | Standard embeddings    |
| `"vector_fp64"` | `DT_VECTOR_FP64` | High-precision vectors |
| `"vector_fp16"` | `DT_VECTOR_FP16` | Memory-efficient vectors |
| `"vector_int8"` | `DT_VECTOR_INT8` | Quantized vectors      |
