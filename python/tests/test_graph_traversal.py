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
)


@pytest.fixture()
def schema() -> GraphSchema:
    """Schema for a small database graph with tables, columns, and joins."""
    return GraphSchema(
        name="db_graph",
        node_types=[
            NodeType(
                name="table",
                properties=[PropertyDef("db_name", "string")],
            ),
            NodeType(
                name="column",
                properties=[PropertyDef("data_type", "string")],
            ),
        ],
        edge_types=[
            EdgeType(name="has_column"),
            EdgeType(name="fk_ref"),
        ],
    )


@pytest.fixture()
def populated_graph(tmp_path: Path, schema: GraphSchema) -> Graph:
    """Build a small graph:

    orders (table)
      --has_column--> orders.id (column)
      --has_column--> orders.user_id (column)
    users (table)
      --has_column--> users.id (column)
    orders.user_id --fk_ref--> users.id
    """
    g = Graph.create(str(tmp_path / "traversal_graph"), schema)

    g.add_node("orders", "table", {"db_name": "main"})
    g.add_node("users", "table", {"db_name": "main"})
    g.add_node("orders.id", "column", {"data_type": "int64"})
    g.add_node("orders.user_id", "column", {"data_type": "int64"})
    g.add_node("users.id", "column", {"data_type": "int64"})

    g.add_edge("orders", "orders.id", "has_column")
    g.add_edge("orders", "orders.user_id", "has_column")
    g.add_edge("users", "users.id", "has_column")
    g.add_edge("orders.user_id", "users.id", "fk_ref")

    yield g
    g.destroy()


class TestTraversal:
    """Tests for graph traversal operations."""

    def test_single_hop(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=1)
        node_ids = {n.id for n in sg.nodes}
        # Should include orders and its direct neighbors
        assert "orders" in node_ids
        assert "orders.id" in node_ids
        assert "orders.user_id" in node_ids

    def test_two_hops(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=2)
        node_ids = {n.id for n in sg.nodes}
        # Two hops from orders should reach users.id via orders.user_id
        assert "orders" in node_ids
        assert "orders.user_id" in node_ids
        assert "users.id" in node_ids

    def test_edge_filter(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse(
            "orders", depth=2, edge_filter="edge_type = 'has_column'"
        )
        edge_types = {e.edge_type for e in sg.edges}
        # Only has_column edges should be traversed
        assert "fk_ref" not in edge_types

    def test_max_nodes_truncates(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=3, max_nodes=2)
        # Should have at most 2 nodes
        assert len(sg.nodes) <= 2
        assert sg.truncated is True

    def test_depth_zero_returns_seeds(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=0)
        node_ids = {n.id for n in sg.nodes}
        assert node_ids == {"orders"}
        assert len(sg.edges) == 0

    def test_to_json(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=1)
        json_str = sg.to_json()
        data = json.loads(json_str)
        assert "nodes" in data
        assert "edges" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["edges"], list)

    def test_to_text(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=1)
        text = sg.to_text()
        assert isinstance(text, str)
        assert len(text) > 0

    def test_nodes_of_type(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=1)
        tables = sg.nodes_of_type("table")
        columns = sg.nodes_of_type("column")
        table_ids = {n.id for n in tables}
        column_ids = {n.id for n in columns}
        assert "orders" in table_ids
        assert "orders.id" in column_ids

    def test_edges_of_type(self, populated_graph: Graph) -> None:
        sg = populated_graph.traverse("orders", depth=2)
        has_col = sg.edges_of_type("has_column")
        assert len(has_col) >= 2  # orders has 2 columns
        for e in has_col:
            assert e.edge_type == "has_column"
