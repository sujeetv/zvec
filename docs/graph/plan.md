# Property Graph Engine — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a general-purpose property graph engine to zvec, enabling GraphRAG workloads with C++ core and thin Python API.

**Architecture:** Two zvec collections (nodes + edges) managed by a C++ `GraphEngine` class. Schema validation, atomic mutations, and multi-hop traversal all live in C++. Python is a thin pybind11 wrapper. Storage abstraction interface enables future cloud backends.

**Tech Stack:** C++17 (core), GoogleTest (C++ tests), pybind11 (bindings), protobuf (schema persistence), pytest (Python tests)

**Spec:** `docs/graph/design.md`

---

## File Structure

### C++ Source (`src/graph/`)

| File | Responsibility |
|------|---------------|
| `src/graph/proto/graph.proto` | Protobuf definitions: `GraphMeta`, `NodeTypeDef`, `EdgeTypeDef`, `EdgeConstraintDef` |
| `src/graph/graph_schema.h` / `.cc` | `GraphSchema` class: parse schema, validate nodes/edges against it, serialize to/from protobuf |
| `src/graph/graph_node.h` | `GraphNode` struct: id, node_type, properties, vectors, adjacency lists |
| `src/graph/graph_edge.h` | `GraphEdge` struct: id, source_id, target_id, edge_type, directed, properties, vectors |
| `src/graph/subgraph.h` / `.cc` | `Subgraph` struct: nodes + edges + metadata, convenience accessors, `to_json()`, `to_text()` |
| `src/graph/storage/storage_interface.h` | Abstract `StorageInterface`: node/edge CRUD, atomic batch, index management |
| `src/graph/storage/zvec_storage.h` / `.cc` | `ZvecStorage`: concrete implementation using two `zvec::Collection` instances |
| `src/graph/mutation.h` / `.cc` | `MutationEngine`: add/remove/update nodes and edges with atomic batches, schema validation |
| `src/graph/traversal.h` / `.cc` | `TraversalEngine`: multi-hop BFS with edge/node filters, max_nodes budget, cycle detection |
| `src/graph/graph_engine.h` / `.cc` | `GraphEngine`: top-level lifecycle (create, open, destroy), owns schema + storage + mutation + traversal |
| `src/graph/CMakeLists.txt` | Build config for graph module |

### C++ Tests (`tests/graph/`)

| File | Tests |
|------|-------|
| `tests/graph/graph_schema_test.cc` | Schema construction, validation, serialization round-trip |
| `tests/graph/subgraph_test.cc` | Subgraph accessors, `to_json()`, `to_text()` |
| `tests/graph/zvec_storage_test.cc` | Storage CRUD via ZvecStorage, atomic batch |
| `tests/graph/mutation_test.cc` | Add/remove/update nodes and edges, schema validation rejection |
| `tests/graph/traversal_test.cc` | Multi-hop BFS, filters, max_nodes, cycle detection |
| `tests/graph/graph_engine_test.cc` | Lifecycle: create, open, destroy, end-to-end flows |
| `tests/graph/CMakeLists.txt` | Build config for graph tests |

### Pybind11 Bindings (`src/binding/python/`)

| File | Responsibility |
|------|---------------|
| `src/binding/python/include/python_graph.h` | `ZVecPyGraph` class declaration |
| `src/binding/python/model/python_graph.cc` | Bind `GraphEngine`, `GraphSchema`, `Subgraph`, etc. to Python |
| `src/binding/python/binding.cc` | **Modify:** add `ZVecPyGraph::Initialize(m)` call |

### Python API (`python/zvec/`)

| File | Responsibility |
|------|---------------|
| `python/zvec/graph/__init__.py` | Package exports: `Graph`, `GraphSchema`, `NodeType`, `EdgeType`, etc. |
| `python/zvec/graph/schema.py` | `GraphSchema`, `NodeType`, `EdgeType`, `EdgeConstraint`, `PropertyDef`, `VectorDef` |
| `python/zvec/graph/graph.py` | `Graph` class: thin wrapper over C++ `GraphEngine` |
| `python/zvec/graph/types.py` | `GraphNode`, `GraphEdge`, `Subgraph` Python wrappers |
| `python/zvec/__init__.py` | **Modify:** re-export graph types |

### Python Tests (`python/tests/`)

| File | Tests |
|------|-------|
| `python/tests/test_graph_schema.py` | Schema construction, validation errors |
| `python/tests/test_graph_mutations.py` | Add/remove/update nodes and edges via Python API |
| `python/tests/test_graph_traversal.py` | Traverse, search_nodes, search_edges, subgraph serialization |
| `python/tests/test_graph_e2e.py` | End-to-end: schema → populate → index → vector search → traverse → serialize |

### Build Config

| File | Change |
|------|--------|
| `src/CMakeLists.txt` | **Modify:** add `cc_directory(graph)` |
| `tests/CMakeLists.txt` | **Modify:** add `cc_directory(graph)` |
| `src/binding/python/CMakeLists.txt` | **Modify:** add `model/python_graph.cc` to `SRC_LISTS`, link `zvec_graph` |

---

## Task 1: Protobuf Definitions

**Files:**
- Create: `src/graph/proto/graph.proto`
- Create: `src/graph/CMakeLists.txt`
- Modify: `src/CMakeLists.txt`

This task defines the protobuf schema for graph metadata persistence. No tests yet — protobuf compilation is validated by the build itself.

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p src/graph/proto
mkdir -p src/graph/storage
```

- [ ] **Step 2: Write `graph.proto`**

Create `src/graph/proto/graph.proto`:

```protobuf
syntax = "proto3";

package zvec.graph.proto;

option cc_enable_arenas = true;

import "db/proto/zvec.proto";

// Property definition within a node or edge type
message PropertyDef {
  string name = 1;
  zvec.proto.DataType data_type = 2;
  bool nullable = 3;
}

// Vector field definition within a node or edge type
message VectorDef {
  string name = 1;
  zvec.proto.DataType data_type = 2;
  uint32 dimension = 3;
}

// Definition of a node type in the graph schema
message NodeTypeDef {
  string name = 1;
  repeated PropertyDef properties = 2;
  repeated VectorDef vectors = 3;
}

// Definition of an edge type in the graph schema
message EdgeTypeDef {
  string name = 1;
  bool directed = 2;
  repeated PropertyDef properties = 3;
  repeated VectorDef vectors = 4;
}

// Constraint on which node types an edge type can connect
message EdgeConstraintDef {
  string edge_type = 1;
  string source_node_type = 2;
  string target_node_type = 3;
}

// Top-level graph schema
message GraphSchemaDef {
  string name = 1;
  repeated NodeTypeDef node_types = 2;
  repeated EdgeTypeDef edge_types = 3;
  repeated EdgeConstraintDef edge_constraints = 4;
}

// Persisted graph metadata
message GraphMeta {
  GraphSchemaDef schema = 1;
  string nodes_collection_path = 2;
  string edges_collection_path = 3;
  uint64 version = 4;
  uint64 created_at = 5;
  uint64 updated_at = 6;
}
```

- [ ] **Step 3: Write `src/graph/CMakeLists.txt`**

```cmake
include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)
include(${PROJECT_ROOT_DIR}/cmake/option.cmake)

# Compile protobuf
cc_proto_library(
  NAME zvec_graph_proto STATIC
  SRCS proto/*.proto
  PROTOROOT ${PROJECT_ROOT_DIR}/src
)
```

This is a minimal CMakeLists.txt — we'll add the `cc_library` for the graph module in Task 2 once we have source files.

- [ ] **Step 4: Add graph module to `src/CMakeLists.txt`**

Add `cc_directory(graph)` after the existing `cc_directory(db)` line.

- [ ] **Step 5: Verify protobuf compiles**

```bash
cd build && cmake .. -DBUILD_PYTHON_BINDINGS=OFF && cmake --build . --target zvec_graph_proto
```

Expected: Successful compilation. `graph.pb.h` and `graph.pb.cc` generated in build directory.

- [ ] **Step 6: Commit**

```bash
git add src/graph/ src/CMakeLists.txt
git commit -m "feat(graph): add protobuf definitions for graph schema and metadata"
```

---

## Task 2: GraphSchema — C++ Class

**Files:**
- Create: `src/graph/graph_schema.h`
- Create: `src/graph/graph_schema.cc`
- Create: `tests/graph/graph_schema_test.cc`
- Create: `tests/graph/CMakeLists.txt`
- Modify: `tests/CMakeLists.txt`
- Modify: `src/graph/CMakeLists.txt`

`GraphSchema` owns the in-memory representation of node types, edge types, properties, and constraints. It validates that nodes/edges conform to the schema and serializes to/from protobuf.

- [ ] **Step 1: Write the failing test — schema construction**

Create `tests/graph/graph_schema_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/graph_schema.h"

class GraphSchemaTest : public testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(GraphSchemaTest, ConstructEmptySchema) {
  zvec::graph::GraphSchema schema("test_graph");
  EXPECT_EQ(schema.Name(), "test_graph");
  EXPECT_EQ(schema.NodeTypeCount(), 0);
  EXPECT_EQ(schema.EdgeTypeCount(), 0);
}

TEST_F(GraphSchemaTest, AddNodeType) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  builder.AddProperty("database", zvec::proto::DT_STRING, false);
  builder.AddProperty("row_count", zvec::proto::DT_INT64, true);
  builder.AddVector("desc_emb", zvec::proto::DT_VECTOR_FP32, 768);

  auto status = schema.AddNodeType(builder.Build());
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(schema.NodeTypeCount(), 1);

  auto node_type = schema.GetNodeType("table");
  ASSERT_NE(node_type, nullptr);
  EXPECT_EQ(node_type->name(), "table");
  EXPECT_EQ(node_type->properties_size(), 2);
  EXPECT_EQ(node_type->vectors_size(), 1);
}

TEST_F(GraphSchemaTest, AddEdgeType) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::EdgeTypeBuilder builder("foreign_key", /*directed=*/true);
  builder.AddProperty("on_column", zvec::proto::DT_STRING, false);

  auto status = schema.AddEdgeType(builder.Build());
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(schema.EdgeTypeCount(), 1);
}

TEST_F(GraphSchemaTest, DuplicateNodeTypeRejected) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  EXPECT_TRUE(schema.AddNodeType(builder.Build()).ok());
  EXPECT_FALSE(schema.AddNodeType(builder.Build()).ok());
}

TEST_F(GraphSchemaTest, AddEdgeConstraint) {
  zvec::graph::GraphSchema schema("test_graph");

  schema.AddNodeType(zvec::graph::NodeTypeBuilder("table").Build());
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("column").Build());
  schema.AddEdgeType(
      zvec::graph::EdgeTypeBuilder("has_column", true).Build());

  auto status =
      schema.AddEdgeConstraint("has_column", "table", "column");
  EXPECT_TRUE(status.ok());
}

TEST_F(GraphSchemaTest, EdgeConstraintInvalidNodeTypeRejected) {
  zvec::graph::GraphSchema schema("test_graph");
  schema.AddEdgeType(
      zvec::graph::EdgeTypeBuilder("has_column", true).Build());

  // "table" node type doesn't exist
  auto status =
      schema.AddEdgeConstraint("has_column", "table", "column");
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateNodeAcceptsConformingNode) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder builder("table");
  builder.AddProperty("database", zvec::proto::DT_STRING, false);
  schema.AddNodeType(builder.Build());

  std::unordered_map<std::string, std::string> props = {
      {"database", "analytics"}};
  auto status = schema.ValidateNode("table", props);
  EXPECT_TRUE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateNodeRejectsUnknownType) {
  zvec::graph::GraphSchema schema("test_graph");
  std::unordered_map<std::string, std::string> props;
  auto status = schema.ValidateNode("unknown_type", props);
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, ValidateEdgeRejectsConstraintViolation) {
  zvec::graph::GraphSchema schema("test_graph");
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("table").Build());
  schema.AddNodeType(zvec::graph::NodeTypeBuilder("column").Build());

  zvec::graph::EdgeTypeBuilder eb("has_column", true);
  schema.AddEdgeType(eb.Build());
  schema.AddEdgeConstraint("has_column", "table", "column");

  // column -> table violates the constraint (should be table -> column)
  auto status = schema.ValidateEdge("has_column", "column", "table",
                                     "col_node", "tbl_node");
  EXPECT_FALSE(status.ok());
}

TEST_F(GraphSchemaTest, SerializeAndDeserializeRoundTrip) {
  zvec::graph::GraphSchema schema("test_graph");

  zvec::graph::NodeTypeBuilder nb("table");
  nb.AddProperty("database", zvec::proto::DT_STRING, false);
  nb.AddVector("desc_emb", zvec::proto::DT_VECTOR_FP32, 768);
  schema.AddNodeType(nb.Build());

  zvec::graph::EdgeTypeBuilder eb("foreign_key", true);
  eb.AddProperty("on_column", zvec::proto::DT_STRING, false);
  schema.AddEdgeType(eb.Build());

  // Serialize
  auto proto = schema.ToProto();
  EXPECT_EQ(proto.name(), "test_graph");

  // Deserialize
  auto restored = zvec::graph::GraphSchema::FromProto(proto);
  EXPECT_EQ(restored.Name(), "test_graph");
  EXPECT_EQ(restored.NodeTypeCount(), 1);
  EXPECT_EQ(restored.EdgeTypeCount(), 1);

  auto nt = restored.GetNodeType("table");
  ASSERT_NE(nt, nullptr);
  EXPECT_EQ(nt->properties_size(), 1);
  EXPECT_EQ(nt->vectors_size(), 1);
}
```

- [ ] **Step 2: Create test CMakeLists.txt**

Create `tests/graph/CMakeLists.txt`:

```cmake
include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)

file(GLOB_RECURSE ALL_TEST_SRCS *_test.cc)

foreach(CC_SRCS ${ALL_TEST_SRCS})
  get_filename_component(CC_TARGET ${CC_SRCS} NAME_WE)
  cc_gtest(
      NAME ${CC_TARGET}
      STRICT
      LIBS zvec_graph zvec_graph_proto zvec_db libprotobuf
      SRCS ${CC_SRCS}
      INCS . ${PROJECT_ROOT_DIR}/src
    )
endforeach()
```

Add `cc_directory(graph)` to `tests/CMakeLists.txt`.

- [ ] **Step 3: Run test to verify it fails**

```bash
cmake --build . && ./bin/graph_schema_test
```

Expected: Linker errors — `GraphSchema`, `NodeTypeBuilder`, `EdgeTypeBuilder` not defined yet.

- [ ] **Step 4: Write `graph_schema.h`**

Create `src/graph/graph_schema.h`:

```cpp
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph/proto/graph.pb.h"
#include "zvec/db/common/status.h"

namespace zvec {
namespace graph {

// Builder for NodeTypeDef protobuf message
class NodeTypeBuilder {
 public:
  explicit NodeTypeBuilder(const std::string& name);
  NodeTypeBuilder& AddProperty(const std::string& name,
                               zvec::proto::DataType data_type,
                               bool nullable);
  NodeTypeBuilder& AddVector(const std::string& name,
                             zvec::proto::DataType data_type,
                             uint32_t dimension);
  zvec::graph::proto::NodeTypeDef Build() const;

 private:
  zvec::graph::proto::NodeTypeDef def_;
};

// Builder for EdgeTypeDef protobuf message
class EdgeTypeBuilder {
 public:
  explicit EdgeTypeBuilder(const std::string& name, bool directed);
  EdgeTypeBuilder& AddProperty(const std::string& name,
                               zvec::proto::DataType data_type,
                               bool nullable);
  EdgeTypeBuilder& AddVector(const std::string& name,
                             zvec::proto::DataType data_type,
                             uint32_t dimension);
  zvec::graph::proto::EdgeTypeDef Build() const;

 private:
  zvec::graph::proto::EdgeTypeDef def_;
};

// In-memory graph schema — validates nodes/edges, serializes to protobuf
class GraphSchema {
 public:
  explicit GraphSchema(const std::string& name);

  const std::string& Name() const;
  size_t NodeTypeCount() const;
  size_t EdgeTypeCount() const;

  Status AddNodeType(const zvec::graph::proto::NodeTypeDef& node_type);
  Status AddEdgeType(const zvec::graph::proto::EdgeTypeDef& edge_type);
  Status AddEdgeConstraint(const std::string& edge_type,
                           const std::string& source_node_type,
                           const std::string& target_node_type);

  const zvec::graph::proto::NodeTypeDef* GetNodeType(
      const std::string& name) const;
  const zvec::graph::proto::EdgeTypeDef* GetEdgeType(
      const std::string& name) const;

  // Validate a node against the schema
  Status ValidateNode(
      const std::string& node_type,
      const std::unordered_map<std::string, std::string>& properties) const;

  // Validate an edge against the schema (type + constraint check)
  Status ValidateEdge(const std::string& edge_type,
                      const std::string& source_node_type,
                      const std::string& target_node_type,
                      const std::string& source_id,
                      const std::string& target_id) const;

  // Serialization
  zvec::graph::proto::GraphSchemaDef ToProto() const;
  static GraphSchema FromProto(
      const zvec::graph::proto::GraphSchemaDef& proto);

  // Generate zvec CollectionSchema for nodes collection
  CollectionSchema BuildNodesCollectionSchema() const;

  // Generate zvec CollectionSchema for edges collection
  CollectionSchema BuildEdgesCollectionSchema() const;

 private:
  std::string name_;
  std::unordered_map<std::string, zvec::graph::proto::NodeTypeDef>
      node_types_;
  std::unordered_map<std::string, zvec::graph::proto::EdgeTypeDef>
      edge_types_;
  // edge_type -> vector of (source_node_type, target_node_type)
  std::unordered_map<
      std::string,
      std::vector<std::pair<std::string, std::string>>>
      edge_constraints_;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 5: Write `graph_schema.cc`**

Create `src/graph/graph_schema.cc` implementing all methods. Key logic:

- `AddNodeType` / `AddEdgeType`: reject duplicates
- `AddEdgeConstraint`: verify referenced node types and edge type exist
- `ValidateNode`: check node_type exists, required (non-nullable) properties present
- `ValidateEdge`: check edge_type exists, then check constraints (if any defined for that edge_type, verify source/target node types match at least one constraint)
- `ToProto` / `FromProto`: straightforward protobuf serialization
- `BuildNodesCollectionSchema`: union all properties and vectors across all node types, plus system fields (`node_type: STRING`, `neighbor_ids: ARRAY_STRING`, `neighbor_edge_ids: ARRAY_STRING`, `_version: UINT64`, `_updated_at: UINT64`, `_deleted: BOOL`)
- `BuildEdgesCollectionSchema`: union all edge properties and vectors, plus system fields (`source_id: STRING`, `target_id: STRING`, `edge_type: STRING`, `directed: BOOL`, `_version: UINT64`, `_updated_at: UINT64`, `_deleted: BOOL`)

- [ ] **Step 6: Update `src/graph/CMakeLists.txt`**

Add the library definition:

```cmake
cc_library(
    NAME zvec_graph STATIC STRICT
    SRCS *.cc
    INCS . ${PROJECT_ROOT_DIR}/src ${CMAKE_CURRENT_BINARY_DIR}
    LIBS zvec_db zvec_graph_proto libprotobuf
    VERSION "${PROXIMA_ZVEC_VERSION}"
    DEPS zvec_graph_proto
)
```

- [ ] **Step 7: Build and run tests**

```bash
cmake --build . && ./bin/graph_schema_test
```

Expected: All tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/graph/ tests/graph/ tests/CMakeLists.txt
git commit -m "feat(graph): add GraphSchema with validation and protobuf serialization"
```

---

## Task 3: GraphNode, GraphEdge, and Subgraph Data Structures

**Files:**
- Create: `src/graph/graph_node.h`
- Create: `src/graph/graph_edge.h`
- Create: `src/graph/subgraph.h`
- Create: `src/graph/subgraph.cc`
- Create: `tests/graph/subgraph_test.cc`

These are the core data structures returned to callers. `Subgraph` also provides `to_json()` and `to_text()` for agent consumption.

- [ ] **Step 1: Write the failing test**

Create `tests/graph/subgraph_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/subgraph.h"

using namespace zvec::graph;

class SubgraphTest : public testing::Test {
 protected:
  Subgraph MakeSampleSubgraph() {
    Subgraph sg;

    GraphNode n1;
    n1.id = "orders";
    n1.node_type = "table";
    n1.properties["database"] = "analytics";
    n1.properties["row_count"] = "1000000";

    GraphNode n2;
    n2.id = "orders.customer_id";
    n2.node_type = "column";
    n2.properties["data_type"] = "INT64";

    GraphEdge e1;
    e1.id = "orders:has_column:orders.customer_id";
    e1.source_id = "orders";
    e1.target_id = "orders.customer_id";
    e1.edge_type = "has_column";
    e1.directed = true;

    sg.nodes.push_back(std::move(n1));
    sg.nodes.push_back(std::move(n2));
    sg.edges.push_back(std::move(e1));
    return sg;
  }
};

TEST_F(SubgraphTest, NodesOfType) {
  auto sg = MakeSampleSubgraph();
  auto tables = sg.NodesOfType("table");
  EXPECT_EQ(tables.size(), 1);
  EXPECT_EQ(tables[0]->id, "orders");

  auto columns = sg.NodesOfType("column");
  EXPECT_EQ(columns.size(), 1);
}

TEST_F(SubgraphTest, EdgesOfType) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesOfType("has_column");
  EXPECT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0]->source_id, "orders");
}

TEST_F(SubgraphTest, EdgesFrom) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesFrom("orders");
  EXPECT_EQ(edges.size(), 1);
}

TEST_F(SubgraphTest, EdgesTo) {
  auto sg = MakeSampleSubgraph();
  auto edges = sg.EdgesTo("orders.customer_id");
  EXPECT_EQ(edges.size(), 1);
}

TEST_F(SubgraphTest, Neighbors) {
  auto sg = MakeSampleSubgraph();
  auto nbrs = sg.Neighbors("orders");
  EXPECT_EQ(nbrs.size(), 1);
  EXPECT_EQ(nbrs[0]->id, "orders.customer_id");
}

TEST_F(SubgraphTest, ToJsonContainsNodesAndEdges) {
  auto sg = MakeSampleSubgraph();
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"orders\""), std::string::npos);
  EXPECT_NE(json.find("\"has_column\""), std::string::npos);
  EXPECT_NE(json.find("\"nodes\""), std::string::npos);
  EXPECT_NE(json.find("\"edges\""), std::string::npos);
}

TEST_F(SubgraphTest, ToTextContainsReadableSummary) {
  auto sg = MakeSampleSubgraph();
  auto text = sg.ToText();
  EXPECT_NE(text.find("2 nodes, 1 edges"), std::string::npos);
  EXPECT_NE(text.find("[table] orders"), std::string::npos);
  EXPECT_NE(text.find("orders --has_column-->"), std::string::npos);
}

TEST_F(SubgraphTest, EmptySubgraph) {
  Subgraph sg;
  EXPECT_EQ(sg.NodesOfType("anything").size(), 0);
  EXPECT_EQ(sg.EdgesOfType("anything").size(), 0);
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"nodes\""), std::string::npos);
}

TEST_F(SubgraphTest, TruncatedFlag) {
  Subgraph sg;
  sg.truncated = true;
  auto json = sg.ToJson();
  EXPECT_NE(json.find("\"truncated\":true"), std::string::npos);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build . && ./bin/subgraph_test
```

Expected: Compile/linker errors — `GraphNode`, `GraphEdge`, `Subgraph` not defined.

- [ ] **Step 3: Write `graph_node.h`**

Create `src/graph/graph_node.h`:

```cpp
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace zvec {
namespace graph {

struct GraphNode {
  std::string id;
  std::string node_type;
  std::unordered_map<std::string, std::string> properties;
  // vectors stored as named float arrays
  std::unordered_map<std::string, std::vector<float>> vectors;
  // adjacency
  std::vector<std::string> neighbor_ids;
  std::vector<std::string> neighbor_edge_ids;
  // system fields
  uint64_t version = 0;
  uint64_t updated_at = 0;
  bool deleted = false;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 4: Write `graph_edge.h`**

Create `src/graph/graph_edge.h`:

```cpp
#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace zvec {
namespace graph {

struct GraphEdge {
  std::string id;
  std::string source_id;
  std::string target_id;
  std::string edge_type;
  bool directed = true;
  std::unordered_map<std::string, std::string> properties;
  std::unordered_map<std::string, std::vector<float>> vectors;
  // system fields
  uint64_t version = 0;
  uint64_t updated_at = 0;
  bool deleted = false;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 5: Write `subgraph.h` and `subgraph.cc`**

`subgraph.h`: declare `Subgraph` struct with:
- `std::vector<GraphNode> nodes`
- `std::vector<GraphEdge> edges`
- `bool truncated = false`
- `NodesOfType()`, `EdgesOfType()`, `EdgesFrom()`, `EdgesTo()`, `Neighbors()` — return `vector<const GraphNode*>` / `vector<const GraphEdge*>`
- `ToJson()` — produce JSON with nodes array, edges array, truncated flag
- `ToText()` — produce agent-readable text (see design.md Section 9.2 for format)

`subgraph.cc`: implement all methods. `ToJson()` can use simple string concatenation (no JSON library dependency needed). `ToText()` formats as specified in design doc.

- [ ] **Step 6: Run tests**

```bash
cmake --build . && ./bin/subgraph_test
```

Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/graph/graph_node.h src/graph/graph_edge.h src/graph/subgraph.h src/graph/subgraph.cc tests/graph/subgraph_test.cc
git commit -m "feat(graph): add GraphNode, GraphEdge, and Subgraph data structures"
```

---

## Task 4: StorageInterface and ZvecStorage

**Files:**
- Create: `src/graph/storage/storage_interface.h`
- Create: `src/graph/storage/zvec_storage.h`
- Create: `src/graph/storage/zvec_storage.cc`
- Create: `tests/graph/zvec_storage_test.cc`

This task implements the storage abstraction and its zvec-backed concrete implementation. This is where graph concepts get mapped to zvec collection operations.

- [ ] **Step 1: Write the failing test**

Create `tests/graph/zvec_storage_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/storage/zvec_storage.h"
#include "graph/graph_schema.h"

using namespace zvec::graph;

class ZvecStorageTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<ZvecStorage> storage_;

  void SetUp() override {
    // Build a simple schema
    GraphSchema schema("test_graph");

    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema.AddEdgeType(eb.Build());

    storage_ = ZvecStorage::Create(test_dir_, schema);
    ASSERT_NE(storage_, nullptr);
  }

  void TearDown() override {
    storage_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string ZvecStorageTest::test_dir_ = "/tmp/zvec_storage_test";

TEST_F(ZvecStorageTest, UpsertAndFetchNode) {
  GraphNode node;
  node.id = "orders";
  node.node_type = "table";
  node.properties["database"] = "analytics";

  auto status = storage_->UpsertNodes({node});
  EXPECT_TRUE(status.ok());

  auto fetched = storage_->FetchNodes({"orders"});
  ASSERT_EQ(fetched.size(), 1);
  EXPECT_EQ(fetched[0].id, "orders");
  EXPECT_EQ(fetched[0].node_type, "table");
  EXPECT_EQ(fetched[0].properties.at("database"), "analytics");
}

TEST_F(ZvecStorageTest, UpsertAndFetchEdge) {
  GraphEdge edge;
  edge.id = "orders:has_column:orders.id";
  edge.source_id = "orders";
  edge.target_id = "orders.id";
  edge.edge_type = "has_column";
  edge.directed = true;

  auto status = storage_->UpsertEdges({edge});
  EXPECT_TRUE(status.ok());

  auto fetched = storage_->FetchEdges({"orders:has_column:orders.id"});
  ASSERT_EQ(fetched.size(), 1);
  EXPECT_EQ(fetched[0].source_id, "orders");
  EXPECT_EQ(fetched[0].edge_type, "has_column");
}

TEST_F(ZvecStorageTest, DeleteNode) {
  GraphNode node;
  node.id = "orders";
  node.node_type = "table";
  storage_->UpsertNodes({node});

  auto status = storage_->DeleteNodes({"orders"});
  EXPECT_TRUE(status.ok());

  auto fetched = storage_->FetchNodes({"orders"});
  EXPECT_EQ(fetched.size(), 0);
}

TEST_F(ZvecStorageTest, FilterEdgesBySourceId) {
  GraphEdge e1;
  e1.id = "orders:has_column:col1";
  e1.source_id = "orders";
  e1.target_id = "col1";
  e1.edge_type = "has_column";
  e1.directed = true;

  GraphEdge e2;
  e2.id = "customers:has_column:col2";
  e2.source_id = "customers";
  e2.target_id = "col2";
  e2.edge_type = "has_column";
  e2.directed = true;

  storage_->UpsertEdges({e1, e2});

  auto filtered = storage_->FilterEdges("source_id = 'orders'");
  ASSERT_EQ(filtered.size(), 1);
  EXPECT_EQ(filtered[0].source_id, "orders");
}

TEST_F(ZvecStorageTest, AtomicBatchWritesAllOrNothing) {
  GraphNode node;
  node.id = "orders";
  node.node_type = "table";

  GraphEdge edge;
  edge.id = "orders:has_column:col1";
  edge.source_id = "orders";
  edge.target_id = "col1";
  edge.edge_type = "has_column";
  edge.directed = true;

  std::vector<Mutation> mutations;
  mutations.push_back(Mutation::UpsertNode(node));
  mutations.push_back(Mutation::UpsertEdge(edge));

  auto status = storage_->AtomicBatch(mutations);
  EXPECT_TRUE(status.ok());

  EXPECT_EQ(storage_->FetchNodes({"orders"}).size(), 1);
  EXPECT_EQ(storage_->FetchEdges({"orders:has_column:col1"}).size(), 1);
}

TEST_F(ZvecStorageTest, FetchNonexistentReturnsEmpty) {
  auto nodes = storage_->FetchNodes({"nonexistent"});
  EXPECT_EQ(nodes.size(), 0);

  auto edges = storage_->FetchEdges({"nonexistent"});
  EXPECT_EQ(edges.size(), 0);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build . && ./bin/zvec_storage_test
```

Expected: Compile errors — `StorageInterface`, `ZvecStorage`, `Mutation` not defined.

- [ ] **Step 3: Write `storage_interface.h`**

Create `src/graph/storage/storage_interface.h`:

```cpp
#pragma once

#include <string>
#include <vector>

#include "graph/graph_node.h"
#include "graph/graph_edge.h"
#include "zvec/db/common/status.h"

namespace zvec {
namespace graph {

enum class MutationType { UPSERT_NODE, DELETE_NODE, UPSERT_EDGE, DELETE_EDGE };

struct Mutation {
  MutationType type;
  GraphNode node;    // for UPSERT_NODE / DELETE_NODE
  GraphEdge edge;    // for UPSERT_EDGE / DELETE_EDGE

  static Mutation UpsertNode(const GraphNode& n);
  static Mutation DeleteNode(const GraphNode& n);
  static Mutation UpsertEdge(const GraphEdge& e);
  static Mutation DeleteEdge(const GraphEdge& e);
};

// Forward declaration for VectorQuery-like params
struct NodeVectorQuery {
  std::string field_name;
  std::vector<float> vector;
  int topk = 10;
  std::string filter;
};

class StorageInterface {
 public:
  virtual ~StorageInterface() = default;

  // Node operations
  virtual Status UpsertNodes(const std::vector<GraphNode>& nodes) = 0;
  virtual Status DeleteNodes(const std::vector<std::string>& ids) = 0;
  virtual std::vector<GraphNode> FetchNodes(
      const std::vector<std::string>& ids) = 0;
  virtual std::vector<GraphNode> QueryNodes(
      const NodeVectorQuery& query) = 0;
  virtual std::vector<GraphNode> FilterNodes(
      const std::string& filter) = 0;

  // Edge operations
  virtual Status UpsertEdges(const std::vector<GraphEdge>& edges) = 0;
  virtual Status DeleteEdges(const std::vector<std::string>& ids) = 0;
  virtual std::vector<GraphEdge> FetchEdges(
      const std::vector<std::string>& ids) = 0;
  virtual std::vector<GraphEdge> FilterEdges(
      const std::string& filter) = 0;

  // Atomic batch
  virtual Status AtomicBatch(const std::vector<Mutation>& mutations) = 0;

  // Index management
  enum class Target { NODES, EDGES };
  virtual Status CreateIndex(Target target, const std::string& field,
                             const void* param) = 0;
  virtual Status DropIndex(Target target,
                           const std::string& field) = 0;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 4: Write `zvec_storage.h` and `zvec_storage.cc`**

`ZvecStorage` manages two zvec `Collection` instances (nodes and edges). Key implementation details:

- `Create()` static method: builds `CollectionSchema` for both collections using `GraphSchema::BuildNodesCollectionSchema()` / `BuildEdgesCollectionSchema()`, calls `Collection::CreateAndOpen()`
- `Open()` static method: opens existing collections from paths
- `UpsertNodes()` / `UpsertEdges()`: convert `GraphNode`/`GraphEdge` to zvec `Doc` and call `collection.Upsert()`
- `FetchNodes()` / `FetchEdges()`: call `collection.Fetch()` and convert back to `GraphNode`/`GraphEdge`
- `FilterEdges()`: use `collection.Query()` with filter string (no vector, filter-only mode using zvec's `NoVectorQueryExecutor`)
- `AtomicBatch()`: group all mutations and write via RocksDB `WriteBatch` (this requires accessing the underlying RocksDB instance — investigate zvec's `Collection` internals for the right hook)
- Auto-create inverted indexes on `node_type`, `source_id`, `target_id`, `edge_type` during `Create()`

- [ ] **Step 5: Run tests**

```bash
cmake --build . && ./bin/zvec_storage_test
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/graph/storage/ tests/graph/zvec_storage_test.cc
git commit -m "feat(graph): add StorageInterface and ZvecStorage implementation"
```

---

## Task 5: MutationEngine

**Files:**
- Create: `src/graph/mutation.h`
- Create: `src/graph/mutation.cc`
- Create: `tests/graph/mutation_test.cc`

`MutationEngine` handles add/remove/update of nodes and edges. It validates against the schema, manages adjacency lists, and uses atomic batches for edge mutations.

- [ ] **Step 1: Write the failing test**

Create `tests/graph/mutation_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/mutation.h"
#include "graph/storage/zvec_storage.h"
#include "graph/graph_schema.h"

using namespace zvec::graph;

class MutationTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<ZvecStorage> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  GraphSchema schema_;

  MutationTest() : schema_("test_graph") {}

  void SetUp() override {
    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema_.AddEdgeType(eb.Build());
    schema_.AddEdgeConstraint("has_column", "table", "column");

    EdgeTypeBuilder eb2("learned", false);
    eb2.AddProperty("confidence", zvec::proto::DT_DOUBLE, false);
    schema_.AddEdgeType(eb2.Build());

    storage_ = ZvecStorage::Create(test_dir_, schema_);
    mutation_ = std::make_unique<MutationEngine>(&schema_, storage_.get());
  }

  void TearDown() override {
    mutation_.reset();
    storage_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string MutationTest::test_dir_ = "/tmp/zvec_mutation_test";

TEST_F(MutationTest, AddNode) {
  GraphNode node;
  node.id = "orders";
  node.node_type = "table";
  node.properties["database"] = "analytics";

  auto status = mutation_->AddNode(node);
  EXPECT_TRUE(status.ok());

  auto fetched = storage_->FetchNodes({"orders"});
  ASSERT_EQ(fetched.size(), 1);
  EXPECT_EQ(fetched[0].node_type, "table");
  EXPECT_GT(fetched[0].version, 0u);
  EXPECT_GT(fetched[0].updated_at, 0u);
}

TEST_F(MutationTest, AddNodeInvalidTypeRejected) {
  GraphNode node;
  node.id = "x";
  node.node_type = "nonexistent_type";

  auto status = mutation_->AddNode(node);
  EXPECT_FALSE(status.ok());
}

TEST_F(MutationTest, AddEdgeUpdatesAdjacencyLists) {
  // Add nodes first
  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  mutation_->AddNode(n1);

  GraphNode n2;
  n2.id = "orders.id";
  n2.node_type = "column";
  n2.properties["data_type"] = "INT64";
  mutation_->AddNode(n2);

  // Add edge
  auto status = mutation_->AddEdge("orders", "orders.id", "has_column", {});
  EXPECT_TRUE(status.ok());

  // Verify adjacency updated on both nodes
  auto source = storage_->FetchNodes({"orders"});
  ASSERT_EQ(source.size(), 1);
  EXPECT_EQ(source[0].neighbor_ids.size(), 1);
  EXPECT_EQ(source[0].neighbor_ids[0], "orders.id");

  auto target = storage_->FetchNodes({"orders.id"});
  ASSERT_EQ(target.size(), 1);
  EXPECT_EQ(target[0].neighbor_ids.size(), 1);
  EXPECT_EQ(target[0].neighbor_ids[0], "orders");

  // Verify edge document exists
  auto edges = storage_->FetchEdges({"orders:has_column:orders.id"});
  ASSERT_EQ(edges.size(), 1);
  EXPECT_EQ(edges[0].source_id, "orders");
  EXPECT_EQ(edges[0].target_id, "orders.id");
  EXPECT_TRUE(edges[0].directed);
}

TEST_F(MutationTest, AddEdgeConstraintViolationRejected) {
  GraphNode n1;
  n1.id = "col1";
  n1.node_type = "column";
  n1.properties["data_type"] = "INT64";
  mutation_->AddNode(n1);

  GraphNode n2;
  n2.id = "col2";
  n2.node_type = "column";
  n2.properties["data_type"] = "STRING";
  mutation_->AddNode(n2);

  // has_column constraint: table -> column. column -> column should fail.
  auto status = mutation_->AddEdge("col1", "col2", "has_column", {});
  EXPECT_FALSE(status.ok());
}

TEST_F(MutationTest, AddEdgeIdempotent) {
  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  mutation_->AddNode(n1);

  GraphNode n2;
  n2.id = "orders.id";
  n2.node_type = "column";
  n2.properties["data_type"] = "INT64";
  mutation_->AddNode(n2);

  mutation_->AddEdge("orders", "orders.id", "has_column", {});
  auto status = mutation_->AddEdge("orders", "orders.id", "has_column", {});
  EXPECT_TRUE(status.ok());

  // Should still have only one neighbor entry
  auto source = storage_->FetchNodes({"orders"});
  EXPECT_EQ(source[0].neighbor_ids.size(), 1);
}

TEST_F(MutationTest, RemoveEdgeCleansUpAdjacency) {
  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  mutation_->AddNode(n1);

  GraphNode n2;
  n2.id = "orders.id";
  n2.node_type = "column";
  n2.properties["data_type"] = "INT64";
  mutation_->AddNode(n2);

  mutation_->AddEdge("orders", "orders.id", "has_column", {});
  auto status = mutation_->RemoveEdge("orders:has_column:orders.id");
  EXPECT_TRUE(status.ok());

  // Adjacency lists should be empty
  auto source = storage_->FetchNodes({"orders"});
  EXPECT_EQ(source[0].neighbor_ids.size(), 0);

  auto target = storage_->FetchNodes({"orders.id"});
  EXPECT_EQ(target[0].neighbor_ids.size(), 0);
}

TEST_F(MutationTest, RemoveNodeCascadesEdges) {
  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  mutation_->AddNode(n1);

  GraphNode n2;
  n2.id = "orders.id";
  n2.node_type = "column";
  n2.properties["data_type"] = "INT64";
  mutation_->AddNode(n2);

  mutation_->AddEdge("orders", "orders.id", "has_column", {});
  auto status = mutation_->RemoveNode("orders");
  EXPECT_TRUE(status.ok());

  // Node gone
  EXPECT_EQ(storage_->FetchNodes({"orders"}).size(), 0);
  // Edge gone
  EXPECT_EQ(
      storage_->FetchEdges({"orders:has_column:orders.id"}).size(), 0);
  // Target node adjacency cleaned up
  auto target = storage_->FetchNodes({"orders.id"});
  EXPECT_EQ(target[0].neighbor_ids.size(), 0);
}

TEST_F(MutationTest, UpdateNodeProperties) {
  GraphNode node;
  node.id = "orders";
  node.node_type = "table";
  node.properties["database"] = "analytics";
  mutation_->AddNode(node);

  auto status = mutation_->UpdateNode("orders",
                                       {{"database", "warehouse"}});
  EXPECT_TRUE(status.ok());

  auto fetched = storage_->FetchNodes({"orders"});
  EXPECT_EQ(fetched[0].properties.at("database"), "warehouse");
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build . && ./bin/mutation_test
```

Expected: Compile errors — `MutationEngine` not defined.

- [ ] **Step 3: Write `mutation.h`**

```cpp
#pragma once

#include "graph/graph_schema.h"
#include "graph/graph_node.h"
#include "graph/graph_edge.h"
#include "graph/storage/storage_interface.h"

#include <mutex>
#include <string>
#include <unordered_map>

namespace zvec {
namespace graph {

class MutationEngine {
 public:
  MutationEngine(const GraphSchema* schema, StorageInterface* storage);

  // Node mutations
  Status AddNode(const GraphNode& node);
  Status RemoveNode(const std::string& node_id);
  Status UpdateNode(
      const std::string& node_id,
      const std::unordered_map<std::string, std::string>& properties);

  // Edge mutations (atomic: edge doc + both adjacency lists)
  Status AddEdge(
      const std::string& source_id, const std::string& target_id,
      const std::string& edge_type,
      const std::unordered_map<std::string, std::string>& properties);
  Status RemoveEdge(const std::string& edge_id);
  Status UpdateEdge(
      const std::string& edge_id,
      const std::unordered_map<std::string, std::string>& properties);

 private:
  const GraphSchema* schema_;
  StorageInterface* storage_;
  std::mutex mutex_;

  // Generate deterministic edge ID
  static std::string MakeEdgeId(const std::string& source,
                                 const std::string& edge_type,
                                 const std::string& target);
  // Parse edge ID back to components
  static bool ParseEdgeId(const std::string& edge_id,
                           std::string& source,
                           std::string& edge_type,
                           std::string& target);
  // Get current timestamp
  static uint64_t Now();
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 4: Write `mutation.cc`**

Key implementation logic:

- `AddNode()`: validate against schema, set `_version=1`, `_updated_at=Now()`, upsert
- `AddEdge()`: validate schema + constraints, check both nodes exist, check idempotency (if edge already exists in adjacency, skip), build atomic batch of 3 mutations (edge doc + 2 node adjacency updates), execute via `storage_->AtomicBatch()`
- `RemoveEdge()`: fetch edge doc, remove from both nodes' adjacency lists, delete edge doc — all via atomic batch
- `RemoveNode()`: find all edges connected to this node (via adjacency list), remove each edge, then delete the node
- `UpdateNode()`: fetch, merge properties, bump version, upsert
- All write methods lock `mutex_`

- [ ] **Step 5: Run tests**

```bash
cmake --build . && ./bin/mutation_test
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/graph/mutation.h src/graph/mutation.cc tests/graph/mutation_test.cc
git commit -m "feat(graph): add MutationEngine with atomic edge writes and schema validation"
```

---

## Task 6: TraversalEngine

**Files:**
- Create: `src/graph/traversal.h`
- Create: `src/graph/traversal.cc`
- Create: `tests/graph/traversal_test.cc`

Multi-hop BFS with edge/node filters, max_nodes budget, and cycle detection.

- [ ] **Step 1: Write the failing test**

Create `tests/graph/traversal_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/traversal.h"
#include "graph/mutation.h"
#include "graph/storage/zvec_storage.h"
#include "graph/graph_schema.h"

using namespace zvec::graph;

class TraversalTest : public testing::Test {
 protected:
  static std::string test_dir_;
  std::unique_ptr<ZvecStorage> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  std::unique_ptr<TraversalEngine> traversal_;
  GraphSchema schema_;

  TraversalTest() : schema_("test_graph") {}

  void SetUp() override {
    // Schema: table -> column (has_column), table -> table (fk)
    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema_.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema_.AddEdgeType(eb.Build());
    schema_.AddEdgeConstraint("has_column", "table", "column");

    EdgeTypeBuilder eb2("foreign_key", true);
    eb2.AddProperty("on_column", zvec::proto::DT_STRING, false);
    schema_.AddEdgeType(eb2.Build());
    schema_.AddEdgeConstraint("foreign_key", "table", "table");

    storage_ = ZvecStorage::Create(test_dir_, schema_);
    mutation_ = std::make_unique<MutationEngine>(&schema_, storage_.get());
    traversal_ = std::make_unique<TraversalEngine>(storage_.get());

    // Build a small graph:
    // orders (table) --has_column--> orders.customer_id (column)
    // orders (table) --has_column--> orders.amount (column)
    // orders (table) --foreign_key--> customers (table)
    // customers (table) --has_column--> customers.id (column)
    // customers (table) --has_column--> customers.name (column)

    auto add_node = [&](const std::string& id, const std::string& type,
                         const std::string& prop_key,
                         const std::string& prop_val) {
      GraphNode n;
      n.id = id;
      n.node_type = type;
      n.properties[prop_key] = prop_val;
      mutation_->AddNode(n);
    };

    add_node("orders", "table", "database", "analytics");
    add_node("customers", "table", "database", "analytics");
    add_node("orders.customer_id", "column", "data_type", "INT64");
    add_node("orders.amount", "column", "data_type", "DOUBLE");
    add_node("customers.id", "column", "data_type", "INT64");
    add_node("customers.name", "column", "data_type", "STRING");

    mutation_->AddEdge("orders", "orders.customer_id", "has_column", {});
    mutation_->AddEdge("orders", "orders.amount", "has_column", {});
    mutation_->AddEdge("orders", "customers", "foreign_key",
                        {{"on_column", "customer_id"}});
    mutation_->AddEdge("customers", "customers.id", "has_column", {});
    mutation_->AddEdge("customers", "customers.name", "has_column", {});
  }

  void TearDown() override {
    traversal_.reset();
    mutation_.reset();
    storage_.reset();
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string TraversalTest::test_dir_ = "/tmp/zvec_traversal_test";

TEST_F(TraversalTest, SingleHopFromOrders) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 1;

  auto sg = traversal_->Traverse(params);

  // orders + its 3 direct neighbors
  EXPECT_EQ(sg.nodes.size(), 4);
  EXPECT_EQ(sg.edges.size(), 3);
  EXPECT_FALSE(sg.truncated);
}

TEST_F(TraversalTest, TwoHopsReachesCustomerColumns) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;

  auto sg = traversal_->Traverse(params);

  // All 6 nodes reachable in 2 hops from orders
  EXPECT_EQ(sg.nodes.size(), 6);
  EXPECT_EQ(sg.edges.size(), 5);
}

TEST_F(TraversalTest, EdgeFilterLimitsTraversal) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;
  params.edge_filter = "edge_type = 'has_column'";

  auto sg = traversal_->Traverse(params);

  // Only follows has_column edges from orders -> 2 columns
  // Does NOT follow foreign_key to customers
  EXPECT_EQ(sg.nodes.size(), 3);  // orders + 2 columns
  EXPECT_EQ(sg.edges.size(), 2);
}

TEST_F(TraversalTest, NodeFilterLimitsResults) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 1;
  params.node_filter = "node_type = 'column'";

  auto sg = traversal_->Traverse(params);

  // Start node (orders) + only column neighbors pass filter
  // Seed node is always included even if it doesn't match node_filter
  auto columns = sg.NodesOfType("column");
  EXPECT_EQ(columns.size(), 2);
}

TEST_F(TraversalTest, MaxNodesBudgetTruncates) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 2;
  params.max_nodes = 3;  // stop after 3 nodes total

  auto sg = traversal_->Traverse(params);

  EXPECT_LE(sg.nodes.size(), 3u);
  EXPECT_TRUE(sg.truncated);
}

TEST_F(TraversalTest, CycleDetection) {
  // Add a cycle: customers -> orders (reverse FK)
  // The "learned" edge type is unconstrained, so use foreign_key
  // Actually, just add another FK edge in reverse
  EdgeTypeBuilder eb3("learned", false);
  schema_.AddEdgeType(eb3.Build());
  mutation_->AddEdge("customers", "orders", "learned", {});

  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 10;  // high depth, but cycle should not cause infinite loop

  auto sg = traversal_->Traverse(params);

  // Should still terminate — all 6 nodes visited once
  EXPECT_EQ(sg.nodes.size(), 6);
}

TEST_F(TraversalTest, EmptyStartIds) {
  TraversalParams params;
  params.start_ids = {};
  params.max_depth = 2;

  auto sg = traversal_->Traverse(params);
  EXPECT_EQ(sg.nodes.size(), 0);
  EXPECT_EQ(sg.edges.size(), 0);
}

TEST_F(TraversalTest, DepthZeroReturnsOnlySeeds) {
  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 0;

  auto sg = traversal_->Traverse(params);
  EXPECT_EQ(sg.nodes.size(), 1);
  EXPECT_EQ(sg.edges.size(), 0);
}

TEST_F(TraversalTest, MultipleSeeds) {
  TraversalParams params;
  params.start_ids = {"orders", "customers"};
  params.max_depth = 1;

  auto sg = traversal_->Traverse(params);

  // Both seeds + all their direct neighbors (deduplicated)
  EXPECT_EQ(sg.nodes.size(), 6);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build . && ./bin/traversal_test
```

Expected: Compile errors — `TraversalEngine`, `TraversalParams` not defined.

- [ ] **Step 3: Write `traversal.h`**

```cpp
#pragma once

#include "graph/subgraph.h"
#include "graph/storage/storage_interface.h"

#include <string>
#include <vector>

namespace zvec {
namespace graph {

struct TraversalParams {
  std::vector<std::string> start_ids;
  int max_depth = 3;
  int max_nodes = 0;       // 0 = unlimited
  int beam_width = 0;      // 0 = unlimited (expand all)
  std::string edge_filter;  // SQL-style filter on edge fields
  std::string node_filter;  // SQL-style filter on node fields
};

class TraversalEngine {
 public:
  explicit TraversalEngine(StorageInterface* storage);

  Subgraph Traverse(const TraversalParams& params) const;

 private:
  StorageInterface* storage_;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 4: Write `traversal.cc`**

Implement the BFS traversal algorithm from design.md Section 7.2:

1. Fetch seed nodes, add to `visited_nodes` set
2. For each hop up to `max_depth`:
   a. For each node in frontier, read `neighbor_ids` / `neighbor_edge_ids`
   b. Collect candidate (neighbor_id, edge_id) pairs where neighbor not in `visited_nodes`
   c. Batch fetch edge docs for candidates
   d. Apply `edge_filter` if provided (use zvec's filter syntax)
   e. Batch fetch target nodes that passed edge filter
   f. Apply `node_filter` if provided
   g. Check `max_nodes` budget — if exceeded, set `truncated=true` and stop
   h. Apply `beam_width` if set — keep only top N candidates (for now, first N; beam scoring is a future enhancement)
   i. Update `visited_nodes`, `visited_edges`, and frontier
3. Return `Subgraph` with all visited nodes and edges

- [ ] **Step 5: Run tests**

```bash
cmake --build . && ./bin/traversal_test
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/graph/traversal.h src/graph/traversal.cc tests/graph/traversal_test.cc
git commit -m "feat(graph): add TraversalEngine with BFS, filters, budget, and cycle detection"
```

---

## Task 7: GraphEngine — Top-Level Lifecycle

**Files:**
- Create: `src/graph/graph_engine.h`
- Create: `src/graph/graph_engine.cc`
- Create: `tests/graph/graph_engine_test.cc`

`GraphEngine` is the top-level C++ API. It owns the schema, storage, mutation engine, and traversal engine. Provides lifecycle management (create, open, destroy) and delegates operations.

- [ ] **Step 1: Write the failing test**

Create `tests/graph/graph_engine_test.cc`:

```cpp
#include <gtest/gtest.h>
#include "graph/graph_engine.h"

using namespace zvec::graph;

class GraphEngineTest : public testing::Test {
 protected:
  static std::string test_dir_;

  GraphSchema MakeTestSchema() {
    GraphSchema schema("test_graph");

    NodeTypeBuilder nb("table");
    nb.AddProperty("database", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb.Build());

    NodeTypeBuilder nb2("column");
    nb2.AddProperty("data_type", zvec::proto::DT_STRING, false);
    schema.AddNodeType(nb2.Build());

    EdgeTypeBuilder eb("has_column", true);
    schema.AddEdgeType(eb.Build());
    schema.AddEdgeConstraint("has_column", "table", "column");

    return schema;
  }

  void TearDown() override {
    system(("rm -rf " + test_dir_).c_str());
  }
};

std::string GraphEngineTest::test_dir_ = "/tmp/zvec_engine_test";

TEST_F(GraphEngineTest, CreateAndOpen) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->GetSchema().Name(), "test_graph");
}

TEST_F(GraphEngineTest, OpenExisting) {
  {
    auto schema = MakeTestSchema();
    auto engine = GraphEngine::Create(test_dir_, schema);
    ASSERT_NE(engine, nullptr);

    GraphNode node;
    node.id = "orders";
    node.node_type = "table";
    node.properties["database"] = "analytics";
    engine->AddNode(node);
  }

  // Reopen
  auto engine = GraphEngine::Open(test_dir_);
  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->GetSchema().Name(), "test_graph");

  // Data persisted
  auto nodes = engine->FetchNodes({"orders"});
  ASSERT_EQ(nodes.size(), 1);
  EXPECT_EQ(nodes[0].node_type, "table");
}

TEST_F(GraphEngineTest, EndToEndTraversal) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);

  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  engine->AddNode(n1);

  GraphNode n2;
  n2.id = "orders.id";
  n2.node_type = "column";
  n2.properties["data_type"] = "INT64";
  engine->AddNode(n2);

  engine->AddEdge("orders", "orders.id", "has_column", {});

  TraversalParams params;
  params.start_ids = {"orders"};
  params.max_depth = 1;

  auto sg = engine->Traverse(params);
  EXPECT_EQ(sg.nodes.size(), 2);
  EXPECT_EQ(sg.edges.size(), 1);

  auto text = sg.ToText();
  EXPECT_NE(text.find("[table] orders"), std::string::npos);
  EXPECT_NE(text.find("has_column"), std::string::npos);
}

TEST_F(GraphEngineTest, Destroy) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);
  ASSERT_NE(engine, nullptr);

  engine->Destroy();

  // Opening destroyed graph should fail
  auto reopened = GraphEngine::Open(test_dir_);
  EXPECT_EQ(reopened, nullptr);
}

TEST_F(GraphEngineTest, Repair) {
  auto schema = MakeTestSchema();
  auto engine = GraphEngine::Create(test_dir_, schema);

  GraphNode n1;
  n1.id = "orders";
  n1.node_type = "table";
  n1.properties["database"] = "analytics";
  engine->AddNode(n1);

  // repair() should complete without error even on clean state
  auto status = engine->Repair();
  EXPECT_TRUE(status.ok());
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cmake --build . && ./bin/graph_engine_test
```

Expected: Compile errors — `GraphEngine` not defined.

- [ ] **Step 3: Write `graph_engine.h`**

```cpp
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "graph/graph_schema.h"
#include "graph/graph_node.h"
#include "graph/graph_edge.h"
#include "graph/subgraph.h"
#include "graph/traversal.h"
#include "graph/mutation.h"
#include "graph/storage/storage_interface.h"

namespace zvec {
namespace graph {

class GraphEngine {
 public:
  // Lifecycle
  static std::unique_ptr<GraphEngine> Create(
      const std::string& path, const GraphSchema& schema);
  static std::unique_ptr<GraphEngine> Open(const std::string& path);
  void Destroy();
  Status Repair();

  // Schema
  const GraphSchema& GetSchema() const;

  // Node operations (delegates to MutationEngine)
  Status AddNode(const GraphNode& node);
  Status RemoveNode(const std::string& node_id);
  Status UpdateNode(
      const std::string& node_id,
      const std::unordered_map<std::string, std::string>& properties);
  std::vector<GraphNode> FetchNodes(
      const std::vector<std::string>& ids);
  std::vector<GraphNode> SearchNodes(const NodeVectorQuery& query);

  // Edge operations (delegates to MutationEngine)
  Status AddEdge(
      const std::string& source_id, const std::string& target_id,
      const std::string& edge_type,
      const std::unordered_map<std::string, std::string>& properties);
  Status RemoveEdge(const std::string& edge_id);
  Status UpdateEdge(
      const std::string& edge_id,
      const std::unordered_map<std::string, std::string>& properties);
  std::vector<GraphEdge> FetchEdges(
      const std::vector<std::string>& ids);

  // Traversal
  Subgraph Traverse(const TraversalParams& params);

  // Index management
  Status CreateIndex(StorageInterface::Target target,
                     const std::string& field, const void* param);
  Status DropIndex(StorageInterface::Target target,
                   const std::string& field);

 private:
  GraphEngine() = default;

  std::string path_;
  GraphSchema schema_;
  std::unique_ptr<StorageInterface> storage_;
  std::unique_ptr<MutationEngine> mutation_;
  std::unique_ptr<TraversalEngine> traversal_;
};

}  // namespace graph
}  // namespace zvec
```

- [ ] **Step 4: Write `graph_engine.cc`**

Key implementation:

- `Create()`: serialize schema to protobuf, write to `{path}/graph_meta.pb`, create `ZvecStorage`, construct `MutationEngine` and `TraversalEngine`
- `Open()`: read `{path}/graph_meta.pb`, deserialize schema, open `ZvecStorage`, construct engines
- `Destroy()`: delete the directory
- `Repair()`: iterate all nodes' adjacency lists, verify edge IDs exist, remove orphan references
- All other methods delegate to the appropriate engine

- [ ] **Step 5: Run tests**

```bash
cmake --build . && ./bin/graph_engine_test
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/graph/graph_engine.h src/graph/graph_engine.cc tests/graph/graph_engine_test.cc
git commit -m "feat(graph): add GraphEngine with lifecycle management and end-to-end integration"
```

---

## Task 8: Pybind11 Bindings

**Files:**
- Create: `src/binding/python/include/python_graph.h`
- Create: `src/binding/python/model/python_graph.cc`
- Modify: `src/binding/python/binding.cc`
- Modify: `src/binding/python/CMakeLists.txt`

Expose the C++ `GraphEngine` to Python via pybind11.

- [ ] **Step 1: Write `python_graph.h`**

```cpp
#pragma once

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace zvec {

class ZVecPyGraph {
 public:
  ZVecPyGraph() = delete;
  static void Initialize(py::module_& m);

 private:
  static void bind_graph_schema(py::module_& m);
  static void bind_graph_engine(py::module_& m);
  static void bind_graph_node(py::module_& m);
  static void bind_graph_edge(py::module_& m);
  static void bind_subgraph(py::module_& m);
  static void bind_traversal_params(py::module_& m);
};

}  // namespace zvec
```

- [ ] **Step 2: Write `python_graph.cc`**

Bind all graph types to Python:

- `_GraphSchema` — wraps `GraphSchema` with builder methods
- `_NodeTypeBuilder`, `_EdgeTypeBuilder` — for schema construction
- `_GraphEngine` — wraps `GraphEngine` (create, open, add_node, add_edge, traverse, etc.)
- `_GraphNode`, `_GraphEdge` — data classes with dict-like property access
- `_Subgraph` — with `nodes`, `edges`, `to_json()`, `to_text()`, `nodes_of_type()`, etc.
- `_TraversalParams` — with all fields

Follow the existing pattern in `python_collection.cc`: use `unwrap_expected()` for error handling, release GIL with `py::call_guard<py::gil_scoped_release>()` on all C++ methods.

- [ ] **Step 3: Register in `binding.cc`**

Add to `src/binding/python/binding.cc`:

```cpp
#include "python_graph.h"

// Inside PYBIND11_MODULE:
ZVecPyGraph::Initialize(m);
```

- [ ] **Step 4: Update `src/binding/python/CMakeLists.txt`**

Add `model/python_graph.cc` to `SRC_LISTS`. Add `zvec_graph` and `zvec_graph_proto` to link targets.

- [ ] **Step 5: Build and verify module loads**

```bash
cmake --build . --target _zvec
python -c "from _zvec import _GraphEngine; print('OK')"
```

Expected: "OK" printed without errors.

- [ ] **Step 6: Commit**

```bash
git add src/binding/python/
git commit -m "feat(graph): add pybind11 bindings for graph engine"
```

---

## Task 9: Python API Layer

**Files:**
- Create: `python/zvec/graph/__init__.py`
- Create: `python/zvec/graph/schema.py`
- Create: `python/zvec/graph/graph.py`
- Create: `python/zvec/graph/types.py`
- Modify: `python/zvec/__init__.py`
- Create: `python/tests/test_graph_schema.py`

Thin Python wrappers over the pybind11 bindings. TDD with pytest.

- [ ] **Step 1: Write the failing test**

Create `python/tests/test_graph_schema.py`:

```python
import pytest
import zvec
from zvec.graph import (
    GraphSchema,
    NodeType,
    EdgeType,
    EdgeConstraint,
    PropertyDef,
    VectorDef,
)


def test_create_empty_schema():
    schema = GraphSchema(name="test")
    assert schema.name == "test"
    assert len(schema.node_types) == 0
    assert len(schema.edge_types) == 0


def test_create_schema_with_node_types():
    schema = GraphSchema(
        name="test",
        node_types=[
            NodeType(
                name="table",
                properties=[
                    PropertyDef("database", zvec.DataType.STRING),
                    PropertyDef("row_count", zvec.DataType.INT64, nullable=True),
                ],
                vectors=[
                    VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, dimension=768),
                ],
            ),
        ],
    )
    assert len(schema.node_types) == 1
    assert schema.node_types[0].name == "table"
    assert len(schema.node_types[0].properties) == 2
    assert len(schema.node_types[0].vectors) == 1


def test_create_schema_with_edge_types():
    schema = GraphSchema(
        name="test",
        node_types=[
            NodeType(name="table"),
            NodeType(name="column"),
        ],
        edge_types=[
            EdgeType(
                name="has_column",
                directed=True,
            ),
            EdgeType(
                name="learned",
                directed=False,
                properties=[
                    PropertyDef("confidence", zvec.DataType.DOUBLE),
                ],
            ),
        ],
    )
    assert len(schema.edge_types) == 2
    assert schema.edge_types[0].directed is True
    assert schema.edge_types[1].directed is False


def test_edge_constraints():
    schema = GraphSchema(
        name="test",
        node_types=[
            NodeType(name="table"),
            NodeType(name="column"),
        ],
        edge_types=[
            EdgeType(name="has_column", directed=True),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", source="table", target="column"),
        ],
    )
    assert len(schema.edge_constraints) == 1


def test_duplicate_node_type_rejected():
    with pytest.raises(ValueError):
        GraphSchema(
            name="test",
            node_types=[
                NodeType(name="table"),
                NodeType(name="table"),
            ],
        )


def test_constraint_invalid_node_type_rejected():
    with pytest.raises(ValueError):
        GraphSchema(
            name="test",
            node_types=[NodeType(name="table")],
            edge_types=[EdgeType(name="has_column", directed=True)],
            edge_constraints=[
                EdgeConstraint("has_column", source="table", target="nonexistent"),
            ],
        )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest python/tests/test_graph_schema.py -v
```

Expected: `ImportError` — `zvec.graph` module doesn't exist.

- [ ] **Step 3: Write Python modules**

`python/zvec/graph/schema.py`:
- `PropertyDef(name, data_type, nullable=False)` — dataclass
- `VectorDef(name, data_type, dimension)` — dataclass
- `NodeType(name, properties=[], vectors=[])` — dataclass
- `EdgeType(name, directed=True, properties=[], vectors=[])` — dataclass
- `EdgeConstraint(edge_type, source, target)` — dataclass
- `GraphSchema(name, node_types=[], edge_types=[], edge_constraints=[])` — wraps C++ `_GraphSchema`, validates in constructor

`python/zvec/graph/types.py`:
- `GraphNode` — wraps C++ `_GraphNode`, exposes `id`, `node_type`, `properties`, `vectors`
- `GraphEdge` — wraps C++ `_GraphEdge`, exposes `id`, `source_id`, `target_id`, `edge_type`, `directed`, `properties`, `vectors`
- `Subgraph` — wraps C++ `_Subgraph`, exposes `nodes`, `edges`, `truncated`, `to_json()`, `to_text()`, `nodes_of_type()`, `edges_of_type()`

`python/zvec/graph/graph.py`:
- `Graph` — wraps C++ `_GraphEngine`. Methods: `add_node()`, `add_edge()`, `remove_node()`, `remove_edge()`, `update_node()`, `update_edge()`, `search_nodes()`, `search_edges()`, `traverse()`, `fetch_node()`, `fetch_edge()`, `create_index()`, `drop_index()`, `repair()`, `destroy()`

`python/zvec/graph/__init__.py`:
- Re-export all public types

`python/zvec/__init__.py`:
- Add `from zvec.graph import *` or selective imports

- [ ] **Step 4: Run tests**

```bash
pytest python/tests/test_graph_schema.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add python/zvec/graph/ python/zvec/__init__.py python/tests/test_graph_schema.py
git commit -m "feat(graph): add Python API layer with GraphSchema, Graph, and type wrappers"
```

---

## Task 10: Python Mutation Tests

**Files:**
- Create: `python/tests/test_graph_mutations.py`

Tests for node/edge CRUD operations through the Python API.

- [ ] **Step 1: Write the failing test**

Create `python/tests/test_graph_mutations.py`:

```python
import pytest
import zvec
from zvec.graph import (
    Graph,
    GraphSchema,
    NodeType,
    EdgeType,
    EdgeConstraint,
    PropertyDef,
)


@pytest.fixture
def graph_schema():
    return GraphSchema(
        name="test",
        node_types=[
            NodeType(
                name="table",
                properties=[PropertyDef("database", zvec.DataType.STRING)],
            ),
            NodeType(
                name="column",
                properties=[PropertyDef("data_type", zvec.DataType.STRING)],
            ),
        ],
        edge_types=[
            EdgeType(name="has_column", directed=True),
            EdgeType(
                name="learned",
                directed=False,
                properties=[PropertyDef("confidence", zvec.DataType.DOUBLE)],
            ),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", source="table", target="column"),
        ],
    )


@pytest.fixture
def graph(tmp_path, graph_schema):
    return zvec.create_graph(path=str(tmp_path / "test_graph"), schema=graph_schema)


class TestNodeMutations:
    def test_add_node(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        node = graph.fetch_node("orders")
        assert node is not None
        assert node.id == "orders"
        assert node.node_type == "table"
        assert node.properties["database"] == "analytics"

    def test_add_node_invalid_type_raises(self, graph):
        with pytest.raises(Exception):
            graph.add_node(id="x", node_type="nonexistent")

    def test_update_node(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.update_node("orders", properties={"database": "warehouse"})
        node = graph.fetch_node("orders")
        assert node.properties["database"] == "warehouse"

    def test_remove_node(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.remove_node("orders")
        node = graph.fetch_node("orders")
        assert node is None


class TestEdgeMutations:
    def test_add_edge(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.add_node(
            id="orders.id", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")

        edge = graph.fetch_edge("orders:has_column:orders.id")
        assert edge is not None
        assert edge.source_id == "orders"
        assert edge.target_id == "orders.id"

    def test_add_edge_updates_adjacency(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.add_node(
            id="orders.id", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")

        node = graph.fetch_node("orders")
        assert "orders.id" in node.neighbor_ids

    def test_add_edge_constraint_violation_raises(self, graph):
        graph.add_node(
            id="col1", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_node(
            id="col2", node_type="column", properties={"data_type": "STRING"}
        )
        with pytest.raises(Exception):
            graph.add_edge(source="col1", target="col2", edge_type="has_column")

    def test_remove_edge(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.add_node(
            id="orders.id", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")
        graph.remove_edge("orders:has_column:orders.id")

        edge = graph.fetch_edge("orders:has_column:orders.id")
        assert edge is None

        node = graph.fetch_node("orders")
        assert "orders.id" not in node.neighbor_ids

    def test_remove_node_cascades_edges(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.add_node(
            id="orders.id", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")
        graph.remove_node("orders")

        edge = graph.fetch_edge("orders:has_column:orders.id")
        assert edge is None

    def test_add_edge_idempotent(self, graph):
        graph.add_node(
            id="orders", node_type="table", properties={"database": "analytics"}
        )
        graph.add_node(
            id="orders.id", node_type="column", properties={"data_type": "INT64"}
        )
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")
        graph.add_edge(source="orders", target="orders.id", edge_type="has_column")

        node = graph.fetch_node("orders")
        assert node.neighbor_ids.count("orders.id") == 1
```

- [ ] **Step 2: Run tests**

```bash
pytest python/tests/test_graph_mutations.py -v
```

Expected: All tests PASS (implementation already done in Tasks 5 + 8 + 9).

- [ ] **Step 3: Commit**

```bash
git add python/tests/test_graph_mutations.py
git commit -m "test(graph): add Python mutation tests for nodes and edges"
```

---

## Task 11: Python Traversal and Search Tests

**Files:**
- Create: `python/tests/test_graph_traversal.py`

Tests for search and traversal through the Python API.

- [ ] **Step 1: Write the failing test**

Create `python/tests/test_graph_traversal.py`:

```python
import pytest
import numpy as np
import zvec
from zvec.graph import (
    Graph,
    GraphSchema,
    NodeType,
    EdgeType,
    EdgeConstraint,
    PropertyDef,
    VectorDef,
)


@pytest.fixture
def graph_schema():
    return GraphSchema(
        name="test",
        node_types=[
            NodeType(
                name="table",
                properties=[PropertyDef("database", zvec.DataType.STRING)],
                vectors=[VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, dimension=4)],
            ),
            NodeType(
                name="column",
                properties=[PropertyDef("data_type", zvec.DataType.STRING)],
                vectors=[VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, dimension=4)],
            ),
        ],
        edge_types=[
            EdgeType(name="has_column", directed=True),
            EdgeType(name="foreign_key", directed=True),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", source="table", target="column"),
            EdgeConstraint("foreign_key", source="table", target="table"),
        ],
    )


@pytest.fixture
def populated_graph(tmp_path, graph_schema):
    graph = zvec.create_graph(
        path=str(tmp_path / "test_graph"), schema=graph_schema
    )

    # Create vector index
    graph.create_index("nodes", "desc_emb", zvec.HnswIndexParam())

    # Add nodes with embeddings
    graph.add_node(
        "orders", "table",
        properties={"database": "analytics"},
        vectors={"desc_emb": [1.0, 0.0, 0.0, 0.0]},
    )
    graph.add_node(
        "customers", "table",
        properties={"database": "analytics"},
        vectors={"desc_emb": [0.0, 1.0, 0.0, 0.0]},
    )
    graph.add_node(
        "orders.customer_id", "column",
        properties={"data_type": "INT64"},
        vectors={"desc_emb": [0.5, 0.5, 0.0, 0.0]},
    )
    graph.add_node(
        "orders.amount", "column",
        properties={"data_type": "DOUBLE"},
        vectors={"desc_emb": [0.0, 0.0, 1.0, 0.0]},
    )
    graph.add_node(
        "customers.id", "column",
        properties={"data_type": "INT64"},
        vectors={"desc_emb": [0.0, 0.0, 0.0, 1.0]},
    )

    graph.add_edge("orders", "orders.customer_id", "has_column")
    graph.add_edge("orders", "orders.amount", "has_column")
    graph.add_edge("orders", "customers", "foreign_key")
    graph.add_edge("customers", "customers.id", "has_column")

    return graph


class TestTraversal:
    def test_single_hop(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=1)
        assert len(sg.nodes) == 4  # orders + 3 neighbors
        assert len(sg.edges) == 3

    def test_two_hops(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=2)
        assert len(sg.nodes) == 5  # all nodes
        assert len(sg.edges) == 4

    def test_edge_filter(self, populated_graph):
        sg = populated_graph.traverse(
            start=["orders"],
            depth=2,
            edge_filter="edge_type = 'has_column'",
        )
        # Only follows has_column, not foreign_key
        assert len(sg.nodes) == 3  # orders + 2 columns
        assert all(
            e.edge_type == "has_column" for e in sg.edges
        )

    def test_max_nodes_truncates(self, populated_graph):
        sg = populated_graph.traverse(
            start=["orders"], depth=2, max_nodes=3
        )
        assert len(sg.nodes) <= 3
        assert sg.truncated is True

    def test_depth_zero_returns_seeds(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=0)
        assert len(sg.nodes) == 1
        assert sg.nodes[0].id == "orders"


class TestSearch:
    def test_search_nodes_by_vector(self, populated_graph):
        results = populated_graph.search_nodes(
            vector=[1.0, 0.0, 0.0, 0.0],
            vector_field="desc_emb",
            topk=2,
        )
        assert len(results) > 0
        # orders should be closest to [1,0,0,0]
        assert results[0].id == "orders"

    def test_search_nodes_with_filter(self, populated_graph):
        results = populated_graph.search_nodes(
            vector=[0.5, 0.5, 0.0, 0.0],
            vector_field="desc_emb",
            topk=5,
            filter="node_type = 'column'",
        )
        assert all(n.node_type == "column" for n in results)


class TestSubgraphSerialization:
    def test_to_json(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=1)
        json_str = sg.to_json()
        assert '"nodes"' in json_str
        assert '"edges"' in json_str
        assert '"orders"' in json_str

    def test_to_text(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=1)
        text = sg.to_text()
        assert "[table] orders" in text
        assert "has_column" in text

    def test_nodes_of_type(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=1)
        tables = sg.nodes_of_type("table")
        columns = sg.nodes_of_type("column")
        assert len(tables) >= 1
        assert len(columns) >= 1

    def test_edges_of_type(self, populated_graph):
        sg = populated_graph.traverse(start=["orders"], depth=1)
        hc = sg.edges_of_type("has_column")
        fk = sg.edges_of_type("foreign_key")
        assert len(hc) == 2
        assert len(fk) == 1
```

- [ ] **Step 2: Run tests**

```bash
pytest python/tests/test_graph_traversal.py -v
```

Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add python/tests/test_graph_traversal.py
git commit -m "test(graph): add Python traversal, search, and serialization tests"
```

---

## Task 12: End-to-End Integration Test

**Files:**
- Create: `python/tests/test_graph_e2e.py`

Full GraphRAG workflow: schema → populate → index → vector search → traverse → serialize.

- [ ] **Step 1: Write the test**

Create `python/tests/test_graph_e2e.py`:

```python
import pytest
import numpy as np
import zvec
from zvec.graph import (
    GraphSchema,
    NodeType,
    EdgeType,
    EdgeConstraint,
    PropertyDef,
    VectorDef,
)


def test_data_catalog_graphrag_workflow(tmp_path):
    """
    End-to-end test matching the design doc Section 10 example.
    Simulates the GraphRAG workflow:
      1. Define schema
      2. Create graph
      3. Index embeddings
      4. Populate nodes and edges
      5. Vector search for seed tables
      6. Traverse to gather context
      7. Serialize subgraph for agent consumption
    """
    DIM = 8

    # 1. Schema
    schema = GraphSchema(
        name="data_catalog",
        node_types=[
            NodeType(
                name="table",
                properties=[
                    PropertyDef("database", zvec.DataType.STRING),
                    PropertyDef("row_count", zvec.DataType.INT64, nullable=True),
                ],
                vectors=[VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, DIM)],
            ),
            NodeType(
                name="column",
                properties=[
                    PropertyDef("data_type", zvec.DataType.STRING),
                    PropertyDef("nullable", zvec.DataType.BOOL),
                ],
                vectors=[VectorDef("desc_emb", zvec.DataType.VECTOR_FP32, DIM)],
            ),
        ],
        edge_types=[
            EdgeType(name="has_column", directed=True),
            EdgeType(
                name="foreign_key",
                directed=True,
                properties=[PropertyDef("on_column", zvec.DataType.STRING)],
            ),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", source="table", target="column"),
            EdgeConstraint("foreign_key", source="table", target="table"),
        ],
    )

    # 2. Create graph
    graph = zvec.create_graph(path=str(tmp_path / "catalog"), schema=schema)

    # 3. Index
    graph.create_index("nodes", "desc_emb", zvec.HnswIndexParam())

    # 4. Populate
    def emb(seed):
        rng = np.random.RandomState(hash(seed) % 2**31)
        return rng.randn(DIM).astype(np.float32).tolist()

    graph.add_node("orders", "table", {"database": "analytics", "row_count": 1000000},
                   vectors={"desc_emb": emb("orders")})
    graph.add_node("customers", "table", {"database": "analytics", "row_count": 50000},
                   vectors={"desc_emb": emb("customers")})
    graph.add_node("products", "table", {"database": "analytics", "row_count": 10000},
                   vectors={"desc_emb": emb("products")})

    graph.add_node("orders.customer_id", "column",
                   {"data_type": "INT64", "nullable": False},
                   vectors={"desc_emb": emb("orders.customer_id")})
    graph.add_node("orders.amount", "column",
                   {"data_type": "DOUBLE", "nullable": False},
                   vectors={"desc_emb": emb("orders.amount")})
    graph.add_node("customers.id", "column",
                   {"data_type": "INT64", "nullable": False},
                   vectors={"desc_emb": emb("customers.id")})
    graph.add_node("customers.name", "column",
                   {"data_type": "STRING", "nullable": False},
                   vectors={"desc_emb": emb("customers.name")})

    graph.add_edge("orders", "orders.customer_id", "has_column")
    graph.add_edge("orders", "orders.amount", "has_column")
    graph.add_edge("orders", "customers", "foreign_key",
                   properties={"on_column": "customer_id"})
    graph.add_edge("customers", "customers.id", "has_column")
    graph.add_edge("customers", "customers.name", "has_column")

    # 5. Vector search
    query_vec = emb("orders")
    seeds = graph.search_nodes(
        vector=query_vec,
        vector_field="desc_emb",
        topk=2,
        filter="node_type = 'table'",
    )
    assert len(seeds) > 0
    seed_ids = [n.id for n in seeds]

    # 6. Traverse
    subgraph = graph.traverse(
        start=seed_ids,
        depth=2,
        edge_filter="edge_type IN ('has_column', 'foreign_key')",
    )
    assert len(subgraph.nodes) > 0
    assert len(subgraph.edges) > 0

    # 7. Serialize
    json_str = subgraph.to_json()
    assert '"nodes"' in json_str

    text = subgraph.to_text()
    assert "nodes" in text.lower()
    assert "edges" in text.lower()

    # Verify we can access typed results
    tables = subgraph.nodes_of_type("table")
    columns = subgraph.nodes_of_type("column")
    assert len(tables) >= 1
    assert len(columns) >= 1

    fks = subgraph.edges_of_type("foreign_key")
    for fk in fks:
        assert fk.properties.get("on_column") is not None


def test_graph_persistence(tmp_path):
    """Test that graph data survives close and reopen."""
    DIM = 4
    path = str(tmp_path / "persist_test")

    schema = GraphSchema(
        name="test",
        node_types=[NodeType(name="item")],
        edge_types=[EdgeType(name="related", directed=False)],
    )

    # Create, populate, close
    graph = zvec.create_graph(path=path, schema=schema)
    graph.add_node("a", "item")
    graph.add_node("b", "item")
    graph.add_edge("a", "b", "related")
    del graph

    # Reopen
    graph = zvec.open_graph(path=path)
    assert graph.schema.name == "test"

    node = graph.fetch_node("a")
    assert node is not None
    assert "b" in node.neighbor_ids

    sg = graph.traverse(start=["a"], depth=1)
    assert len(sg.nodes) == 2
    assert len(sg.edges) == 1


def test_graph_repair(tmp_path):
    """Test that repair completes without error."""
    schema = GraphSchema(
        name="test",
        node_types=[NodeType(name="item")],
        edge_types=[EdgeType(name="related", directed=False)],
    )
    graph = zvec.create_graph(path=str(tmp_path / "repair_test"), schema=schema)
    graph.add_node("a", "item")
    graph.add_node("b", "item")
    graph.add_edge("a", "b", "related")

    # Should not raise
    graph.repair()
```

- [ ] **Step 2: Run tests**

```bash
pytest python/tests/test_graph_e2e.py -v
```

Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add python/tests/test_graph_e2e.py
git commit -m "test(graph): add end-to-end GraphRAG workflow integration test"
```

---

## Task Summary

| # | Task | Type | Dependencies |
|---|------|------|-------------|
| 1 | Protobuf definitions | Infra | None |
| 2 | GraphSchema C++ | Core | 1 |
| 3 | GraphNode, GraphEdge, Subgraph | Core | 2 |
| 4 | StorageInterface + ZvecStorage | Core | 2, 3 |
| 5 | MutationEngine | Core | 2, 3, 4 |
| 6 | TraversalEngine | Core | 3, 4 |
| 7 | GraphEngine lifecycle | Core | 2, 4, 5, 6 |
| 8 | Pybind11 bindings | Binding | 7 |
| 9 | Python API layer + schema tests | Python | 8 |
| 10 | Python mutation tests | Test | 9 |
| 11 | Python traversal + search tests | Test | 9 |
| 12 | End-to-end integration test | Test | 9, 10, 11 |
