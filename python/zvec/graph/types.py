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

from typing import Optional

from _zvec import _GraphEdge, _GraphNode, _Subgraph

__all__ = [
    "GraphNode",
    "GraphEdge",
    "Subgraph",
]


class GraphNode:
    """Python wrapper around a C++ GraphNode."""

    __slots__ = ("_cpp_obj",)

    def __init__(self, cpp_obj: _GraphNode) -> None:
        self._cpp_obj = cpp_obj

    @property
    def id(self) -> str:
        """str: Node identifier."""
        return self._cpp_obj.id

    @property
    def node_type(self) -> str:
        """str: The type of this node."""
        return self._cpp_obj.node_type

    @property
    def properties(self) -> dict[str, str]:
        """dict[str, str]: Scalar properties."""
        return dict(self._cpp_obj.properties)

    @property
    def vectors(self) -> dict[str, list[float]]:
        """dict[str, list[float]]: Named vector fields."""
        return dict(self._cpp_obj.vectors)

    @property
    def neighbor_ids(self) -> list[str]:
        """list[str]: IDs of adjacent nodes."""
        return list(self._cpp_obj.neighbor_ids)

    @property
    def neighbor_edge_ids(self) -> list[str]:
        """list[str]: IDs of edges connecting to this node."""
        return list(self._cpp_obj.neighbor_edge_ids)

    @property
    def version(self) -> int:
        """int: Version counter."""
        return self._cpp_obj.version

    @property
    def updated_at(self) -> int:
        """int: Last-update timestamp (epoch ms)."""
        return self._cpp_obj.updated_at

    def __repr__(self) -> str:
        return f"GraphNode(id={self.id!r}, type={self.node_type!r})"


class GraphEdge:
    """Python wrapper around a C++ GraphEdge."""

    __slots__ = ("_cpp_obj",)

    def __init__(self, cpp_obj: _GraphEdge) -> None:
        self._cpp_obj = cpp_obj

    @property
    def id(self) -> str:
        """str: Edge identifier (source--type--target)."""
        return self._cpp_obj.id

    @property
    def source_id(self) -> str:
        """str: Source node ID."""
        return self._cpp_obj.source_id

    @property
    def target_id(self) -> str:
        """str: Target node ID."""
        return self._cpp_obj.target_id

    @property
    def edge_type(self) -> str:
        """str: The type of this edge."""
        return self._cpp_obj.edge_type

    @property
    def directed(self) -> bool:
        """bool: Whether this edge is directed."""
        return self._cpp_obj.directed

    @property
    def properties(self) -> dict[str, str]:
        """dict[str, str]: Scalar properties."""
        return dict(self._cpp_obj.properties)

    @property
    def vectors(self) -> dict[str, list[float]]:
        """dict[str, list[float]]: Named vector fields."""
        return dict(self._cpp_obj.vectors)

    @property
    def version(self) -> int:
        """int: Version counter."""
        return self._cpp_obj.version

    @property
    def updated_at(self) -> int:
        """int: Last-update timestamp (epoch ms)."""
        return self._cpp_obj.updated_at

    def __repr__(self) -> str:
        return (
            f"GraphEdge(id={self.id!r}, "
            f"{self.source_id!r}->{self.target_id!r})"
        )


class Subgraph:
    """Python wrapper around a C++ Subgraph traversal result."""

    __slots__ = ("_cpp_obj",)

    def __init__(self, cpp_obj: _Subgraph) -> None:
        self._cpp_obj = cpp_obj

    @property
    def nodes(self) -> list[GraphNode]:
        """list[GraphNode]: All nodes in the subgraph."""
        return [GraphNode(n) for n in self._cpp_obj.nodes]

    @property
    def edges(self) -> list[GraphEdge]:
        """list[GraphEdge]: All edges in the subgraph."""
        return [GraphEdge(e) for e in self._cpp_obj.edges]

    @property
    def truncated(self) -> bool:
        """bool: Whether the result was truncated by max_nodes."""
        return self._cpp_obj.truncated

    def to_json(self) -> str:
        """Serialize subgraph to JSON string."""
        return self._cpp_obj.to_json()

    def to_text(self) -> str:
        """Serialize subgraph to agent-readable text."""
        return self._cpp_obj.to_text()

    def nodes_of_type(self, node_type: str) -> list[GraphNode]:
        """Return nodes matching the given type."""
        return [GraphNode(n) for n in self._cpp_obj.nodes_of_type(node_type)]

    def edges_of_type(self, edge_type: str) -> list[GraphEdge]:
        """Return edges matching the given type."""
        return [GraphEdge(e) for e in self._cpp_obj.edges_of_type(edge_type)]

    def edges_from(self, node_id: str) -> list[GraphEdge]:
        """Return edges originating from the given node."""
        return [GraphEdge(e) for e in self._cpp_obj.edges_from(node_id)]

    def edges_to(self, node_id: str) -> list[GraphEdge]:
        """Return edges targeting the given node."""
        return [GraphEdge(e) for e in self._cpp_obj.edges_to(node_id)]

    def neighbors(self, node_id: str) -> list[GraphNode]:
        """Return neighbor nodes of the given node."""
        return [GraphNode(n) for n in self._cpp_obj.neighbors(node_id)]

    def __repr__(self) -> str:
        return (
            f"Subgraph(nodes={len(self._cpp_obj.nodes)}, "
            f"edges={len(self._cpp_obj.edges)}, "
            f"truncated={self.truncated})"
        )
