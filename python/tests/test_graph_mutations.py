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

import shutil
import tempfile
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
def graph_dir(tmp_path: Path) -> Path:
    """Provide a temporary directory for a graph."""
    return tmp_path / "test_graph"


@pytest.fixture()
def schema() -> GraphSchema:
    """A simple schema with tables, columns, and a constrained edge."""
    return GraphSchema(
        name="test_db",
        node_types=[
            NodeType(
                name="table",
                properties=[PropertyDef("db_name", "string")],
            ),
            NodeType(
                name="column",
                properties=[
                    PropertyDef("data_type", "string"),
                    PropertyDef("nullable", "string", nullable=True),
                ],
            ),
        ],
        edge_types=[
            EdgeType(name="has_column"),
            EdgeType(name="references"),
        ],
        edge_constraints=[
            EdgeConstraint("has_column", "table", "column"),
        ],
    )


@pytest.fixture()
def graph(graph_dir: Path, schema: GraphSchema) -> Graph:
    """Create a fresh graph for each test."""
    g = Graph.create(str(graph_dir), schema)
    yield g
    g.destroy()


class TestNodeMutations:
    """Tests for node CRUD operations."""

    def test_add_node(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        node = graph.fetch_node("orders")
        assert node is not None
        assert node.id == "orders"
        assert node.node_type == "table"
        assert node.properties["db_name"] == "main"
        assert node.version == 1

    def test_add_node_invalid_type_raises(self, graph: Graph) -> None:
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            graph.add_node("x", "nonexistent_type", {})

    def test_update_node(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.update_node("orders", {"db_name": "prod"})
        node = graph.fetch_node("orders")
        assert node is not None
        assert node.properties["db_name"] == "prod"
        assert node.version == 2

    def test_remove_node(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.remove_node("orders")
        node = graph.fetch_node("orders")
        assert node is None


class TestEdgeMutations:
    """Tests for edge CRUD operations."""

    def test_add_edge(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("orders.id", "column", {"data_type": "int64"})
        graph.add_edge("orders", "orders.id", "has_column")
        edge = graph.fetch_edge("orders--has_column--orders.id")
        assert edge is not None
        assert edge.source_id == "orders"
        assert edge.target_id == "orders.id"
        assert edge.edge_type == "has_column"

    def test_add_edge_updates_adjacency(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("orders.id", "column", {"data_type": "int64"})
        graph.add_edge("orders", "orders.id", "has_column")

        src = graph.fetch_node("orders")
        assert src is not None
        assert "orders.id" in src.neighbor_ids
        assert "orders--has_column--orders.id" in src.neighbor_edge_ids

        tgt = graph.fetch_node("orders.id")
        assert tgt is not None
        assert "orders" in tgt.neighbor_ids

    def test_add_edge_constraint_violation_raises(
        self, graph: Graph
    ) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("users", "table", {"db_name": "main"})
        # has_column is constrained: table -> column
        # table -> table should fail
        with pytest.raises((ValueError, RuntimeError)):
            graph.add_edge("orders", "users", "has_column")

    def test_remove_edge(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("orders.id", "column", {"data_type": "int64"})
        graph.add_edge("orders", "orders.id", "has_column")
        graph.remove_edge("orders--has_column--orders.id")
        edge = graph.fetch_edge("orders--has_column--orders.id")
        assert edge is None

    def test_remove_node_cascades_edges(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("orders.id", "column", {"data_type": "int64"})
        graph.add_edge("orders", "orders.id", "has_column")

        graph.remove_node("orders")

        # Edge should also be gone
        edge = graph.fetch_edge("orders--has_column--orders.id")
        assert edge is None

        # Target node should have clean adjacency
        col = graph.fetch_node("orders.id")
        assert col is not None
        assert "orders" not in col.neighbor_ids

    def test_add_edge_idempotent(self, graph: Graph) -> None:
        graph.add_node("orders", "table", {"db_name": "main"})
        graph.add_node("orders.id", "column", {"data_type": "int64"})
        graph.add_edge("orders", "orders.id", "has_column")
        # Adding the same edge again should be a no-op
        graph.add_edge("orders", "orders.id", "has_column")
        src = graph.fetch_node("orders")
        assert src is not None
        # Should still have exactly one edge
        edge_ids = [
            eid
            for eid in src.neighbor_edge_ids
            if eid == "orders--has_column--orders.id"
        ]
        assert len(edge_ids) == 1
