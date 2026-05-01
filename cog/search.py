from collections import deque
import logging

from cog.database import hash_predicate

logger = logging.getLogger(__name__)


class TraversalMixin:
    """Mixin providing BFS/DFS traversal methods for Graph."""

    def __get_adjacent(self, vertex, predicates, direction):
        """Get adjacent vertices based on direction: 'out', 'inc', or 'both'."""
        adjacent = []
        if direction in ("out", "both"):
            adjacent.extend(self._Graph__adjacent_vertices(vertex, predicates, 'out'))
        if direction in ("inc", "both"):
            adjacent.extend(self._Graph__adjacent_vertices(vertex, predicates, 'in'))
        return adjacent

    def bfs(self, predicates=None, max_depth=None, min_depth=0,
            direction="out", until=None, unique=True):
        """
        Traverse the graph breadth-first from current vertices.

        BFS explores level-by-level, visiting all neighbors at the current depth
        before moving deeper. Guarantees shortest path in unweighted graphs.

        :param predicates: Edge type(s) to follow: str, list, or None (all edges)
        :param max_depth: Maximum traversal depth (None = unlimited)
        :param min_depth: Minimum depth to include in results (default 0)
        :param direction: Traversal direction: "out", "inc", or "both"
        :param until: Stop condition lambda: func(vertex_id) -> bool
        :param unique: If True, visit each vertex only once (prevents cycles)
        :return: self for method chaining

        Example:
            g.v("alice").bfs(predicates="follows", max_depth=2).all()
            g.v("alice").bfs(max_depth=3, min_depth=2).all()  # depths 2-3 only
            g.v("alice").bfs(until=lambda v: v == "target").all()
        """
        if self._cloud:
            if until is not None:
                raise RuntimeError("bfs() with an 'until' lambda is not supported in cloud mode.")
            p = predicates
            if p is not None and not isinstance(p, list):
                p = [p]
            return self._cloud_append("bfs", predicates=p, max_depth=max_depth,
                                      min_depth=min_depth, direction=direction, unique=unique)

        # Normalize predicates
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        from cog.torque import Vertex
        result_vertices = []
        visited = set()
        queue = deque()  # (vertex, depth)
        track = self._track_paths

        # Initialize with current vertices at depth 0
        for v in self.last_visited_vertices:
            queue.append((v, 0))
            if unique:
                visited.add(v.id)

        while queue:
            current, depth = queue.popleft()

            if until and until(current.id):
                if depth >= min_depth:
                    if track:
                        result_vertex = Vertex(current.id)
                        result_vertex.tags = current.tags.copy()
                        result_vertex.edges = current.edges.copy()
                        result_vertex._path = current._path
                        result_vertices.append(result_vertex)
                    else:
                        result_vertices.append(Vertex(current.id))
                continue

            if depth > 0 and depth >= min_depth:
                if max_depth is None or depth <= max_depth:
                    if track:
                        result_vertex = Vertex(current.id)
                        result_vertex.tags = current.tags.copy()
                        result_vertex.edges = current.edges.copy()
                        result_vertex._path = current._path
                        result_vertices.append(result_vertex)
                    else:
                        result_vertices.append(Vertex(current.id))

            # Stop exploring if at max depth
            if max_depth is not None and depth >= max_depth:
                continue

            adjacent = self.__get_adjacent(current, predicates, direction)
            for adj in adjacent:
                if unique:
                    if adj.id in visited:
                        continue
                    visited.add(adj.id)
                if track:
                    adj.tags = current.tags.copy()
                    parent_path = current._path or [{'vertex': current.id}]
                    edge_hash = next(iter(adj.edges)) if adj.edges else None
                    edge_name = self._predicate_reverse_lookup_cache.get(edge_hash, edge_hash) if edge_hash else None
                    adj._path = list(parent_path) + ([{'edge': edge_name}] if edge_name else []) + [{'vertex': adj.id}]
                queue.append((adj, depth + 1))

        self.last_visited_vertices = result_vertices
        return self

    def dfs(self, predicates=None, max_depth=None, min_depth=0,
            direction="out", until=None, unique=True):
        """
        Traverse the graph depth-first from current vertices.

        DFS explores as deep as possible along each branch before backtracking.
        More memory-efficient than BFS for deep graphs.

        :param predicates: Edge type(s) to follow: str, list, or None (all edges)
        :param max_depth: Maximum traversal depth (None = unlimited)
        :param min_depth: Minimum depth to include in results (default 0)
        :param direction: Traversal direction: "out", "inc", or "both"
        :param until: Stop condition lambda: func(vertex_id) -> bool
        :param unique: If True, visit each vertex only once (prevents cycles)
        :return: self for method chaining

        Example:
            g.v("alice").dfs(predicates="follows", max_depth=3).all()
            g.v("alice").dfs(direction="both", max_depth=2).all()
        """
        if self._cloud:
            if until is not None:
                raise RuntimeError("dfs() with an 'until' lambda is not supported in cloud mode.")
            p = predicates
            if p is not None and not isinstance(p, list):
                p = [p]
            return self._cloud_append("dfs", predicates=p, max_depth=max_depth,
                                      min_depth=min_depth, direction=direction, unique=unique)
        # Normalize predicates
        if predicates is not None:
            if not isinstance(predicates, list):
                predicates = [predicates]
            predicates = list(map(hash_predicate, predicates))
        else:
            predicates = self.all_predicates

        from cog.torque import Vertex
        result_vertices = []
        visited = set()
        stack = []  # (vertex, depth)
        track = self._track_paths

        # Initialize with current vertices at depth 0
        for v in self.last_visited_vertices:
            stack.append((v, 0))
            if unique:
                visited.add(v.id)

        while stack:
            current, depth = stack.pop()  # LIFO for DFS

            if until and until(current.id):
                if depth >= min_depth:
                    if track:
                        result_vertex = Vertex(current.id)
                        result_vertex.tags = current.tags.copy()
                        result_vertex.edges = current.edges.copy()
                        result_vertex._path = current._path
                        result_vertices.append(result_vertex)
                    else:
                        result_vertices.append(Vertex(current.id))
                continue

            if depth > 0 and depth >= min_depth:
                if max_depth is None or depth <= max_depth:
                    if track:
                        result_vertex = Vertex(current.id)
                        result_vertex.tags = current.tags.copy()
                        result_vertex.edges = current.edges.copy()
                        result_vertex._path = current._path
                        result_vertices.append(result_vertex)
                    else:
                        result_vertices.append(Vertex(current.id))

            # Stop exploring if at max depth
            if max_depth is not None and depth >= max_depth:
                continue

            adjacent = self.__get_adjacent(current, predicates, direction)
            for adj in adjacent:
                if unique:
                    if adj.id in visited:
                        continue
                    visited.add(adj.id)
                if track:
                    adj.tags = current.tags.copy()
                    parent_path = current._path or [{'vertex': current.id}]
                    edge_hash = next(iter(adj.edges)) if adj.edges else None
                    edge_name = self._predicate_reverse_lookup_cache.get(edge_hash, edge_hash) if edge_hash else None
                    adj._path = list(parent_path) + ([{'edge': edge_name}] if edge_name else []) + [{'vertex': adj.id}]
                stack.append((adj, depth + 1))

        self.last_visited_vertices = result_vertices
        return self
