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

from _zvec import _GraphEngine, _GraphNode, _TraversalParams

from .schema import GraphSchema
from .types import GraphEdge, GraphNode, Subgraph

__all__ = ["Graph"]


class Graph:
    """Python wrapper around the C++ GraphEngine.

    Use the :meth:`create` or :meth:`open` class methods to obtain an
    instance.  Do not instantiate directly.
    """

    __slots__ = ("_engine",)

    def __init__(self, engine: _GraphEngine) -> None:
        self._engine = engine

    @classmethod
    def create(cls, path: str, schema: GraphSchema) -> Graph:
        """Create a new graph at the given path.

        Args:
            path: Filesystem path for graph storage.
            schema: Graph schema defining node/edge types.

        Returns:
            A new Graph instance.

        Raises:
            RuntimeError: If creation fails (e.g., path already exists).
        """
        engine = _GraphEngine.Create(path, schema._get_cpp_schema())
        return cls(engine)

    @classmethod
    def open(cls, path: str) -> Graph:
        """Open an existing graph from disk.

        Args:
            path: Filesystem path of the graph.

        Returns:
            An opened Graph instance.

        Raises:
            RuntimeError: If the graph cannot be opened.
        """
        engine = _GraphEngine.Open(path)
        return cls(engine)

    # --- Node operations ---

    def add_node(
        self,
        id: str,
        node_type: str,
        properties: Optional[dict[str, str]] = None,
        vectors: Optional[dict[str, list[float]]] = None,
    ) -> None:
        """Add a node to the graph.

        Args:
            id: Unique node identifier.
            node_type: Must match a registered node type in the schema.
            properties: Scalar property key-value pairs.
            vectors: Named vector fields.

        Raises:
            ValueError: If schema validation fails.
        """
        node = _GraphNode()
        node.id = id
        node.node_type = node_type
        node.properties = properties or {}
        node.vectors = vectors or {}
        self._engine.AddNode(node)

    def remove_node(self, id: str) -> None:
        """Remove a node and cascade-delete all connected edges.

        Args:
            id: Node identifier to remove.
        """
        self._engine.RemoveNode(id)

    def update_node(
        self,
        id: str,
        properties: Optional[dict[str, str]] = None,
    ) -> None:
        """Update properties on an existing node.

        Args:
            id: Node identifier.
            properties: Properties to merge into the node.

        Raises:
            KeyError: If the node does not exist.
        """
        self._engine.UpdateNode(id, properties or {})

    def fetch_node(self, id: str) -> Optional[GraphNode]:
        """Fetch a single node by ID.

        Args:
            id: Node identifier.

        Returns:
            The node, or None if not found.
        """
        results = self._engine.FetchNodes([id])
        if results:
            return GraphNode(results[0])
        return None

    # --- Edge operations ---

    def add_edge(
        self,
        source: str,
        target: str,
        edge_type: str,
        properties: Optional[dict[str, str]] = None,
    ) -> None:
        """Add an edge between two existing nodes.

        Args:
            source: Source node ID.
            target: Target node ID.
            edge_type: Must match a registered edge type in the schema.
            properties: Scalar property key-value pairs.

        Raises:
            ValueError: If schema validation or constraint check fails.
            KeyError: If source or target node does not exist.
        """
        self._engine.AddEdge(source, target, edge_type, properties or {})

    def remove_edge(self, edge_id: str) -> None:
        """Remove an edge and clean up adjacency lists.

        Args:
            edge_id: Edge identifier (format: source--type--target).

        Raises:
            KeyError: If the edge does not exist.
        """
        self._engine.RemoveEdge(edge_id)

    def fetch_edge(self, edge_id: str) -> Optional[GraphEdge]:
        """Fetch a single edge by ID.

        Args:
            edge_id: Edge identifier.

        Returns:
            The edge, or None if not found.
        """
        results = self._engine.FetchEdges([edge_id])
        if results:
            return GraphEdge(results[0])
        return None

    # --- Traversal ---

    def traverse(
        self,
        start: str | list[str],
        depth: int = 3,
        max_nodes: int = 0,
        edge_filter: str = "",
        node_filter: str = "",
    ) -> Subgraph:
        """Perform a multi-hop BFS traversal.

        Args:
            start: Seed node ID or list of seed IDs.
            depth: Maximum traversal depth (hops).
            max_nodes: Maximum nodes to collect (0 = unlimited).
            edge_filter: Simple filter like "edge_type = 'has_column'".
            node_filter: Simple filter like "node_type = 'column'".

        Returns:
            A Subgraph containing discovered nodes and edges.
        """
        params = _TraversalParams()
        params.start_ids = [start] if isinstance(start, str) else list(start)
        params.max_depth = depth
        params.max_nodes = max_nodes
        params.edge_filter = edge_filter
        params.node_filter = node_filter
        result = self._engine.Traverse(params)
        return Subgraph(result)

    # --- Lifecycle ---

    def destroy(self) -> None:
        """Destroy the graph (delete all storage and metadata).

        Warning:
            This operation is irreversible.
        """
        self._engine.Destroy()

    def repair(self) -> None:
        """Repair orphaned adjacency references."""
        self._engine.Repair()
