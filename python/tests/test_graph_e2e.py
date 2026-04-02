# Copyright 2025-present the zvec project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""End-to-end integration tests for the graph engine.

Exercises the full GraphRAG workflow: schema definition → graph creation →
node/edge population → traversal → filtering → serialization → mutation →
persistence across reopen → cleanup.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from zvec.graph import (
    EdgeConstraint,
    EdgeType,
    Graph,
    GraphSchema,
    NodeType,
    PropertyDef,
    VectorDef,
)


@pytest.fixture()
def catalog_schema() -> GraphSchema:
    """A realistic data-catalog schema with vectors and constraints."""
    return GraphSchema(
        name="data_catalog",
        node_types=[
            NodeType(
                name="table",
                properties=[
                    PropertyDef("database", "string"),
                    PropertyDef("row_count", "int64", nullable=True),
                ],
                vectors=[
                    VectorDef("desc_emb", "vector_fp32", dimension=8),
                ],
            ),
            NodeType(
                name="column",
                properties=[
                    PropertyDef("data_type", "string"),
                    PropertyDef("nullable", "bool"),
                ],
                vectors=[
                    VectorDef("desc_emb", "vector_fp32", dimension=8),
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
                name="similarity",
                directed=False,
                properties=[PropertyDef("confidence", "double")],
            ),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", source="table", target="column"),
            EdgeConstraint("foreign_key", source="table", target="table"),
        ],
    )


@pytest.fixture()
def graph_path(tmp_path: Path) -> str:
    """Return a path string for the test graph."""
    return str(tmp_path / "catalog_e2e")


class TestE2EGraphRAGWorkflow:
    """Full end-to-end integration test for the GraphRAG pipeline."""

    def _populate(self, g: Graph) -> None:
        """Add a realistic set of nodes and edges."""
        emb = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]

        # Tables
        g.add_node(
            "orders",
            "table",
            properties={"database": "analytics", "row_count": "1000000"},
            vectors={"desc_emb": emb},
        )
        g.add_node(
            "customers",
            "table",
            properties={"database": "analytics", "row_count": "50000"},
            vectors={"desc_emb": [x + 0.1 for x in emb]},
        )
        g.add_node(
            "products",
            "table",
            properties={"database": "analytics"},
        )

        # Columns for orders
        g.add_node(
            "orders.id",
            "column",
            properties={"data_type": "INT64", "nullable": "false"},
        )
        g.add_node(
            "orders.customer_id",
            "column",
            properties={"data_type": "INT64", "nullable": "false"},
        )
        g.add_node(
            "orders.amount",
            "column",
            properties={"data_type": "DOUBLE", "nullable": "false"},
        )

        # Columns for customers
        g.add_node(
            "customers.id",
            "column",
            properties={"data_type": "INT64", "nullable": "false"},
        )
        g.add_node(
            "customers.name",
            "column",
            properties={"data_type": "STRING", "nullable": "false"},
        )

        # Columns for products
        g.add_node(
            "products.id",
            "column",
            properties={"data_type": "INT64", "nullable": "false"},
        )

        # has_column edges
        g.add_edge("orders", "orders.id", "has_column")
        g.add_edge("orders", "orders.customer_id", "has_column")
        g.add_edge("orders", "orders.amount", "has_column")
        g.add_edge("customers", "customers.id", "has_column")
        g.add_edge("customers", "customers.name", "has_column")
        g.add_edge("products", "products.id", "has_column")

        # Foreign key
        g.add_edge(
            "orders",
            "customers",
            "foreign_key",
            properties={"on_column": "customer_id"},
        )

        # Undirected similarity (no constraint — any pair allowed)
        g.add_edge(
            "orders",
            "customers",
            "similarity",
            properties={"confidence": "0.92"},
        )

    def test_full_workflow(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        """Run the complete GraphRAG pipeline end-to-end."""
        # ---- Phase 1: Create and populate ----
        graph = Graph.create(graph_path, catalog_schema)
        self._populate(graph)

        # ---- Phase 2: Verify node/edge counts ----
        # 3 tables + 6 columns = 9 nodes
        for nid in [
            "orders",
            "customers",
            "products",
            "orders.id",
            "orders.customer_id",
            "orders.amount",
            "customers.id",
            "customers.name",
            "products.id",
        ]:
            node = graph.fetch_node(nid)
            assert node is not None, f"Node {nid} should exist"

        # Verify edge existence
        assert graph.fetch_edge("orders--has_column--orders.id") is not None
        assert (
            graph.fetch_edge("orders--foreign_key--customers") is not None
        )
        assert (
            graph.fetch_edge("orders--similarity--customers") is not None
        )

        # ---- Phase 3: Verify adjacency ----
        orders = graph.fetch_node("orders")
        assert orders is not None
        assert set(orders.neighbor_ids) == {
            "orders.id",
            "orders.customer_id",
            "orders.amount",
            "customers",
        }

        # Undirected edge: customers should also see orders as neighbor
        customers = graph.fetch_node("customers")
        assert customers is not None
        assert "orders" in customers.neighbor_ids

        # ---- Phase 4: Traversal — 1-hop from orders ----
        sg1 = graph.traverse("orders", depth=1)
        sg1_ids = {n.id for n in sg1.nodes}
        assert "orders" in sg1_ids
        assert "orders.id" in sg1_ids
        assert "orders.customer_id" in sg1_ids
        assert "orders.amount" in sg1_ids
        assert "customers" in sg1_ids  # via foreign_key and similarity
        assert sg1.truncated is False

        # ---- Phase 5: Traversal — 2-hop reaches customer columns ----
        sg2 = graph.traverse("orders", depth=2)
        sg2_ids = {n.id for n in sg2.nodes}
        assert "customers.id" in sg2_ids
        assert "customers.name" in sg2_ids

        # ---- Phase 6: Edge filter — only has_column ----
        sg_col = graph.traverse(
            "orders", depth=2, edge_filter="edge_type = 'has_column'"
        )
        sg_col_ids = {n.id for n in sg_col.nodes}
        # Should NOT reach customers (only follows has_column)
        assert "customers" not in sg_col_ids
        assert "orders.id" in sg_col_ids
        # Edges should all be has_column
        for e in sg_col.edges:
            assert e.edge_type == "has_column"

        # ---- Phase 7: Node filter — only columns ----
        sg_nodes = graph.traverse(
            "orders", depth=1, node_filter="node_type = 'column'"
        )
        sg_node_types = {n.node_type for n in sg_nodes.nodes}
        # Seed (orders) is always included regardless of node_filter
        assert any(n.id == "orders" for n in sg_nodes.nodes)
        # Non-seed nodes should be columns only
        for n in sg_nodes.nodes:
            if n.id != "orders":
                assert n.node_type == "column"

        # ---- Phase 8: Budget truncation ----
        sg_budget = graph.traverse("orders", depth=3, max_nodes=3)
        assert len(sg_budget.nodes) <= 3
        assert sg_budget.truncated is True

        # ---- Phase 9: Multi-seed traversal ----
        sg_multi = graph.traverse(
            start=["orders", "products"], depth=1
        )
        sg_multi_ids = {n.id for n in sg_multi.nodes}
        assert "orders" in sg_multi_ids
        assert "products" in sg_multi_ids
        assert "products.id" in sg_multi_ids

        # ---- Phase 10: Subgraph convenience methods ----
        sg_full = graph.traverse("orders", depth=2)
        tables = sg_full.nodes_of_type("table")
        columns = sg_full.nodes_of_type("column")
        assert len(tables) >= 2  # orders + customers
        assert len(columns) >= 3  # orders columns

        hc_edges = sg_full.edges_of_type("has_column")
        assert len(hc_edges) >= 3

        edges_from_orders = sg_full.edges_from("orders")
        assert len(edges_from_orders) >= 3

        neighbors = sg_full.neighbors("orders")
        neighbor_ids = {n.id for n in neighbors}
        assert "orders.id" in neighbor_ids

        # ---- Phase 11: Serialization — JSON ----
        json_str = sg_full.to_json()
        data = json.loads(json_str)
        assert "nodes" in data
        assert "edges" in data
        assert len(data["nodes"]) > 0
        assert len(data["edges"]) > 0
        assert "truncated" in data

        # Verify node structure in JSON
        node_map = {n["id"]: n for n in data["nodes"]}
        assert "orders" in node_map
        assert node_map["orders"]["node_type"] == "table"
        assert "properties" in node_map["orders"]

        # ---- Phase 12: Serialization — Text ----
        text = sg_full.to_text()
        assert "Subgraph:" in text
        assert "orders" in text
        assert "has_column" in text

        # ---- Phase 13: Update a node ----
        pre_update = graph.fetch_node("orders")
        assert pre_update is not None
        prev_version = pre_update.version
        graph.update_node("orders", {"row_count": "2000000"})
        updated = graph.fetch_node("orders")
        assert updated is not None
        assert updated.properties["row_count"] == "2000000"
        assert updated.version == prev_version + 1

        # ---- Phase 14: Remove an edge and verify cleanup ----
        graph.remove_edge("orders--has_column--orders.amount")
        assert graph.fetch_edge("orders--has_column--orders.amount") is None

        orders_after = graph.fetch_node("orders")
        assert orders_after is not None
        assert "orders.amount" not in orders_after.neighbor_ids

        amount_node = graph.fetch_node("orders.amount")
        assert amount_node is not None
        assert "orders" not in amount_node.neighbor_ids

        # ---- Phase 15: Remove a node with cascade ----
        graph.remove_node("products")
        assert graph.fetch_node("products") is None
        assert (
            graph.fetch_edge("products--has_column--products.id") is None
        )
        products_id = graph.fetch_node("products.id")
        assert products_id is not None
        assert "products" not in products_id.neighbor_ids

        # ---- Phase 16: Edge idempotency ----
        graph.add_edge("orders", "orders.id", "has_column")  # already exists
        orders_idem = graph.fetch_node("orders")
        assert orders_idem is not None
        count = sum(
            1
            for eid in orders_idem.neighbor_edge_ids
            if eid == "orders--has_column--orders.id"
        )
        assert count == 1

        # ---- Phase 17: Constraint enforcement ----
        with pytest.raises((ValueError, RuntimeError)):
            graph.add_edge("orders", "customers", "has_column")
            # has_column requires table -> column, not table -> table

        # ---- Phase 18: Reopen and verify persistence ----
        # Release the first handle (RocksDB single-writer lock)
        del graph
        # Reopen the same graph from disk
        graph2 = Graph.open(graph_path)

        # Verify nodes survived reopen
        orders_reopened = graph2.fetch_node("orders")
        assert orders_reopened is not None
        assert orders_reopened.properties["database"] == "analytics"
        assert orders_reopened.properties["row_count"] == "2000000"
        # Version reflects all mutations (add, edge adds, update, edge remove)
        assert orders_reopened.version > 1

        # Verify edges survived
        fk = graph2.fetch_edge("orders--foreign_key--customers")
        assert fk is not None
        assert fk.properties["on_column"] == "customer_id"

        # Verify removed items are still gone
        assert graph2.fetch_node("products") is None
        assert (
            graph2.fetch_edge("orders--has_column--orders.amount") is None
        )

        # Traversal on reopened graph
        sg_reopen = graph2.traverse("orders", depth=1)
        reopen_ids = {n.id for n in sg_reopen.nodes}
        assert "orders" in reopen_ids
        assert "customers" in reopen_ids

        # ---- Phase 19: Destroy ----
        graph2.destroy()
        assert not Path(graph_path).exists()


class TestE2ESchemaValidation:
    """E2E tests for schema validation across the full stack."""

    def test_invalid_node_type_rejected(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        graph = Graph.create(graph_path, catalog_schema)
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            graph.add_node("x", "nonexistent_type", {})
        graph.destroy()

    def test_invalid_edge_type_rejected(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        graph = Graph.create(graph_path, catalog_schema)
        graph.add_node("t1", "table", {"database": "db"})
        graph.add_node("t2", "table", {"database": "db"})
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            graph.add_edge("t1", "t2", "nonexistent_edge")
        graph.destroy()

    def test_edge_to_missing_node_rejected(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        graph = Graph.create(graph_path, catalog_schema)
        graph.add_node("t1", "table", {"database": "db"})
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            graph.add_edge("t1", "missing_node", "foreign_key")
        graph.destroy()


class TestE2EWithVectors:
    """E2E tests exercising vector field storage and retrieval."""

    def test_vectors_stored_and_retrieved(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        graph = Graph.create(graph_path, catalog_schema)
        emb = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        graph.add_node(
            "orders",
            "table",
            properties={"database": "test"},
            vectors={"desc_emb": emb},
        )
        node = graph.fetch_node("orders")
        assert node is not None
        assert "desc_emb" in node.vectors
        retrieved = node.vectors["desc_emb"]
        assert len(retrieved) == 8
        for orig, got in zip(emb, retrieved):
            assert abs(orig - got) < 1e-5
        graph.destroy()

    def test_vectors_persist_across_reopen(
        self, catalog_schema: GraphSchema, graph_path: str
    ) -> None:
        emb = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]
        graph = Graph.create(graph_path, catalog_schema)
        graph.add_node(
            "t1",
            "table",
            properties={"database": "db"},
            vectors={"desc_emb": emb},
        )

        # Release first handle before reopening (RocksDB lock)
        del graph
        graph2 = Graph.open(graph_path)
        node = graph2.fetch_node("t1")
        assert node is not None
        assert "desc_emb" in node.vectors
        retrieved = node.vectors["desc_emb"]
        for orig, got in zip(emb, retrieved):
            assert abs(orig - got) < 1e-5
        graph2.destroy()
