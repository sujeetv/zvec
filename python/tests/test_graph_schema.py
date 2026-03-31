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

import pytest

from zvec.graph import (
    EdgeConstraint,
    EdgeType,
    GraphSchema,
    NodeType,
    PropertyDef,
    VectorDef,
)


class TestGraphSchema:
    """Tests for graph schema construction in Python."""

    def test_create_empty_schema(self) -> None:
        schema = GraphSchema(name="empty")
        assert schema.name == "empty"
        assert schema.node_type_count == 0
        assert schema.edge_type_count == 0
        assert schema.node_types == []
        assert schema.edge_types == []
        assert schema.edge_constraints == []

    def test_create_schema_with_node_types(self) -> None:
        schema = GraphSchema(
            name="test",
            node_types=[
                NodeType(
                    name="table",
                    properties=[
                        PropertyDef("db_name", "string"),
                        PropertyDef("row_count", "int64", nullable=True),
                    ],
                    vectors=[
                        VectorDef("embedding", "vector_fp32", 128),
                    ],
                ),
                NodeType(name="column"),
            ],
        )
        assert schema.node_type_count == 2
        assert schema.edge_type_count == 0
        assert len(schema.node_types) == 2
        assert schema.node_types[0].name == "table"
        assert len(schema.node_types[0].properties) == 2
        assert len(schema.node_types[0].vectors) == 1

    def test_create_schema_with_edge_types(self) -> None:
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
                    properties=[
                        PropertyDef("ordinal", "int32"),
                    ],
                ),
                EdgeType(name="similar_to", directed=False),
            ],
        )
        assert schema.node_type_count == 2
        assert schema.edge_type_count == 2
        assert len(schema.edge_types) == 2
        assert schema.edge_types[0].name == "has_column"
        assert schema.edge_types[0].directed is True
        assert schema.edge_types[1].directed is False

    def test_edge_constraints(self) -> None:
        schema = GraphSchema(
            name="test",
            node_types=[
                NodeType(name="table"),
                NodeType(name="column"),
            ],
            edge_types=[
                EdgeType(name="has_column"),
            ],
            edge_constraints=[
                EdgeConstraint("has_column", "table", "column"),
            ],
        )
        assert len(schema.edge_constraints) == 1
        assert schema.edge_constraints[0].edge_type == "has_column"
        assert schema.edge_constraints[0].source == "table"
        assert schema.edge_constraints[0].target == "column"

    def test_duplicate_node_type_rejected(self) -> None:
        with pytest.raises((ValueError, RuntimeError)):
            GraphSchema(
                name="dup",
                node_types=[
                    NodeType(name="table"),
                    NodeType(name="table"),
                ],
            )

    def test_constraint_invalid_node_type_rejected(self) -> None:
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            GraphSchema(
                name="bad_constraint",
                node_types=[
                    NodeType(name="table"),
                ],
                edge_types=[
                    EdgeType(name="has_column"),
                ],
                edge_constraints=[
                    EdgeConstraint("has_column", "table", "nonexistent"),
                ],
            )
