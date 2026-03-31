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

from dataclasses import dataclass, field
from typing import Optional

from _zvec import (
    _EdgeTypeBuilder,
    _GraphSchema,
    _NodeTypeBuilder,
)

__all__ = [
    "PropertyDef",
    "VectorDef",
    "NodeType",
    "EdgeType",
    "EdgeConstraint",
    "GraphSchema",
]

# Proto DataType enum values (from zvec.proto)
_PROTO_DT_MAP = {
    "string": 2,
    "bool": 3,
    "int32": 4,
    "int64": 5,
    "uint32": 6,
    "uint64": 7,
    "float": 8,
    "double": 9,
    "vector_fp32": 23,
    "vector_fp64": 24,
    "vector_fp16": 22,
    "vector_int8": 26,
}


def _resolve_data_type(data_type: str | int) -> int:
    """Resolve a data type name or integer to a proto DataType enum value."""
    if isinstance(data_type, int):
        return data_type
    key = data_type.lower()
    if key in _PROTO_DT_MAP:
        return _PROTO_DT_MAP[key]
    raise ValueError(f"Unknown data type: {data_type!r}")


@dataclass
class PropertyDef:
    """Definition of a scalar property on a node or edge type."""

    name: str
    data_type: str | int
    nullable: bool = False


@dataclass
class VectorDef:
    """Definition of a vector field on a node or edge type."""

    name: str
    data_type: str | int
    dimension: int


@dataclass
class NodeType:
    """Definition of a node type in the graph schema."""

    name: str
    properties: list[PropertyDef] = field(default_factory=list)
    vectors: list[VectorDef] = field(default_factory=list)


@dataclass
class EdgeType:
    """Definition of an edge type in the graph schema."""

    name: str
    directed: bool = True
    properties: list[PropertyDef] = field(default_factory=list)
    vectors: list[VectorDef] = field(default_factory=list)


@dataclass
class EdgeConstraint:
    """Constraint specifying which node types an edge type can connect."""

    edge_type: str
    source: str
    target: str


class GraphSchema:
    """Python wrapper around the C++ GraphSchema.

    Builds the C++ schema object on construction and validates all
    node types, edge types, and constraints.

    Args:
        name: Schema name.
        node_types: List of node type definitions.
        edge_types: List of edge type definitions.
        edge_constraints: List of edge constraint definitions.

    Raises:
        ValueError: If the schema is invalid (e.g., duplicate types,
            invalid constraint references).
    """

    __slots__ = ("_cpp_schema", "_name", "_node_types", "_edge_types",
                 "_edge_constraints")

    def __init__(
        self,
        name: str,
        node_types: Optional[list[NodeType]] = None,
        edge_types: Optional[list[EdgeType]] = None,
        edge_constraints: Optional[list[EdgeConstraint]] = None,
    ) -> None:
        self._name = name
        self._node_types = node_types or []
        self._edge_types = edge_types or []
        self._edge_constraints = edge_constraints or []

        # Build the C++ schema object
        self._cpp_schema = _GraphSchema(name)

        for nt in self._node_types:
            builder = _NodeTypeBuilder(nt.name)
            for p in nt.properties:
                builder.add_property(p.name, _resolve_data_type(p.data_type),
                                     p.nullable)
            for v in nt.vectors:
                builder.add_vector(v.name, _resolve_data_type(v.data_type),
                                   v.dimension)
            self._cpp_schema.add_node_type(builder.build())

        for et in self._edge_types:
            builder = _EdgeTypeBuilder(et.name, et.directed)
            for p in et.properties:
                builder.add_property(p.name, _resolve_data_type(p.data_type),
                                     p.nullable)
            for v in et.vectors:
                builder.add_vector(v.name, _resolve_data_type(v.data_type),
                                   v.dimension)
            self._cpp_schema.add_edge_type(builder.build())

        for ec in self._edge_constraints:
            self._cpp_schema.add_edge_constraint(
                ec.edge_type, ec.source, ec.target
            )

    @property
    def name(self) -> str:
        """str: The schema name."""
        return self._name

    @property
    def node_types(self) -> list[NodeType]:
        """list[NodeType]: Registered node types."""
        return list(self._node_types)

    @property
    def edge_types(self) -> list[EdgeType]:
        """list[EdgeType]: Registered edge types."""
        return list(self._edge_types)

    @property
    def edge_constraints(self) -> list[EdgeConstraint]:
        """list[EdgeConstraint]: Registered edge constraints."""
        return list(self._edge_constraints)

    @property
    def node_type_count(self) -> int:
        """int: Number of registered node types."""
        return self._cpp_schema.node_type_count()

    @property
    def edge_type_count(self) -> int:
        """int: Number of registered edge types."""
        return self._cpp_schema.edge_type_count()

    def _get_cpp_schema(self) -> _GraphSchema:
        """Return the underlying C++ GraphSchema object."""
        return self._cpp_schema
