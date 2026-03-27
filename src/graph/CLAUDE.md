# CLAUDE.md — Graph Module

## Overview

Property graph engine for zvec. Enables GraphRAG workloads: vector search on nodes/edges, multi-hop traversal, subgraph extraction for agent consumption.

**Design spec:** `docs/graph/design.md`
**Implementation plan:** `docs/graph/plan.md`

## Architecture

```
GraphEngine              # Top-level: create, open, destroy, repair
  |-- GraphSchema        # Validates node/edge types, serializes to protobuf
  |-- GraphStore         # Owns two zvec collections (nodes + edges)
  |     \-- StorageInterface  # Abstract — ZvecStorage today, cloud later
  |-- MutationEngine     # Atomic add/remove/update (RocksDB WriteBatch)
  \-- TraversalEngine    # Multi-hop BFS with filters, budget, cycle detection
        \-- Subgraph     # Result: nodes + edges + to_json() + to_text()
```

## Data Model

Two zvec collections per graph:

- **Nodes:** id, node_type, properties, vectors, neighbor_ids (ARRAY_STRING), neighbor_edge_ids (ARRAY_STRING)
- **Edges:** id (`{source}:{type}:{target}`), source_id, target_id, edge_type, directed, properties, vectors

System fields on both: `_version` (uint64), `_updated_at` (uint64), `_deleted` (bool)

## Auto-Indexed Fields

Created at graph initialization (not user-configurable):
- Nodes: `node_type`
- Edges: `source_id`, `target_id`, `edge_type`

## C++ Conventions (graph-specific)

- Namespace: `zvec::graph`
- Files: `src/graph/*.h`, `src/graph/*.cc`, `src/graph/storage/`, `src/graph/proto/`
- Protobuf: `src/graph/proto/graph.proto` → generates `graph.pb.h` / `graph.pb.cc`
- Library target: `zvec_graph` (depends on `zvec_db`, `zvec_graph_proto`, `libprotobuf`)
- Tests: `tests/graph/*_test.cc` using GoogleTest

## Key Design Decisions

- **Hybrid storage (Model C):** Adjacency lists on nodes for fast traversal + edge collection for properties/embeddings
- **Atomic edge mutations:** Single RocksDB WriteBatch for edge doc + both adjacency updates
- **Schema validation in C++:** All mutations validated before storage writes
- **Edge constraints optional:** If defined, enforced; if omitted, unconstrained
- **Write order:** edge doc first, then source adjacency, then target adjacency
- **Idempotent writes:** Adding an existing edge is a no-op

## Python API

Thin wrappers in `python/zvec/graph/`:
- `schema.py` — GraphSchema, NodeType, EdgeType, EdgeConstraint, PropertyDef, VectorDef
- `graph.py` — Graph class (wraps C++ GraphEngine)
- `types.py` — GraphNode, GraphEdge, Subgraph wrappers

## Testing

```bash
# C++ tests
cmake --build build && ./build/bin/graph_schema_test
cmake --build build && ./build/bin/traversal_test

# Python tests
pytest python/tests/test_graph_schema.py -v
pytest python/tests/test_graph_mutations.py -v
pytest python/tests/test_graph_traversal.py -v
pytest python/tests/test_graph_e2e.py -v
```
