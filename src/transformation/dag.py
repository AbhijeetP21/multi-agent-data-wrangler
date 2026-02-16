"""Transformation DAG builder."""

from typing import Any

import pandas as pd

from src.common.types.transformation import Transformation, TransformationType


class TransformationDAG:
    """Directed Acyclic Graph for managing transformation dependencies."""

    def __init__(self):
        """Initialize the DAG."""
        self._nodes: dict[str, Transformation] = {}
        self._dependencies: dict[str, set[str]] = {}
        self._execution_order: list[str] = []

    def add_transformation(self, transformation: Transformation) -> None:
        """Add a transformation to the DAG.

        Args:
            transformation: The transformation to add.
        """
        self._nodes[transformation.id] = transformation
        self._dependencies[transformation.id] = set()

        # Clear execution order when new transformation is added
        self._execution_order = []

    def add_dependency(
        self, transformation_id: str, depends_on_id: str
    ) -> None:
        """Add a dependency between transformations.

        Args:
            transformation_id: The dependent transformation.
            depends_on_id: The transformation it depends on.

        Raises:
            ValueError: If either transformation doesn't exist.
        """
        if transformation_id not in self._nodes:
            raise ValueError(f"Transformation {transformation_id} not found in DAG")
        if depends_on_id not in self._nodes:
            raise ValueError(f"Transformation {depends_on_id} not found in DAG")

        self._dependencies[transformation_id].add(depends_on_id)

    def topological_sort(self) -> list[Transformation]:
        """Compute topological order for execution.

        Returns:
            List of transformations in execution order.

        Raises:
            ValueError: If the graph contains cycles.
        """
        if self._execution_order:
            return [self._nodes[id_] for id_ in self._execution_order]

        # Kahn's algorithm for topological sorting
        in_degree = {node: 0 for node in self._nodes}

        for node_id, deps in self._dependencies.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[node_id] += 1

        # Start with nodes that have no dependencies
        queue = [
            node_id for node_id, degree in in_degree.items() if degree == 0
        ]
        self._execution_order = []

        while queue:
            current = queue.pop(0)
            self._execution_order.append(current)

            # Reduce in-degree for dependent nodes
            for node_id, deps in self._dependencies.items():
                if current in deps:
                    in_degree[node_id] -= 1
                    if in_degree[node_id] == 0:
                        queue.append(node_id)

        # Check for cycles
        if len(self._execution_order) != len(self._nodes):
            raise ValueError("Circular dependency detected in transformation DAG")

        return [self._nodes[id_] for id_ in self._execution_order]

    def get_dependencies(self, transformation_id: str) -> set[str]:
        """Get dependencies for a transformation.

        Args:
            transformation_id: The transformation ID.

        Returns:
            Set of transformation IDs it depends on.
        """
        return self._dependencies.get(transformation_id, set()).copy()

    def get_dependents(self, transformation_id: str) -> set[str]:
        """Get transformations that depend on this one.

        Args:
            transformation_id: The transformation ID.

        Returns:
            Set of transformation IDs that depend on this one.
        """
        dependents = set()
        for node_id, deps in self._dependencies.items():
            if transformation_id in deps:
                dependents.add(node_id)
        return dependents

    def validate(self) -> bool:
        """Validate the DAG for cycles and missing dependencies.

        Returns:
            True if DAG is valid.
        """
        try:
            self.topological_sort()
            return True
        except ValueError:
            return False

    def __len__(self) -> int:
        """Get the number of transformations in the DAG."""
        return len(self._nodes)

    def __contains__(self, transformation_id: str) -> bool:
        """Check if a transformation is in the DAG."""
        return transformation_id in self._nodes


class TransformationDAGBuilder:
    """Builder for creating transformation DAGs."""

    def __init__(self):
        """Initialize the builder."""
        self._dag = TransformationDAG()

    def add_transformations(
        self, transformations: list[Transformation]
    ) -> "TransformationDAGBuilder":
        """Add multiple transformations to the DAG.

        Args:
            transformations: List of transformations to add.

        Returns:
            Self for chaining.
        """
        for transformation in transformations:
            self._dag.add_transformation(transformation)
        return self

    def add_transformation(
        self, transformation: Transformation
    ) -> "TransformationDAGBuilder":
        """Add a single transformation to the DAG.

        Args:
            transformation: The transformation to add.

        Returns:
            Self for chaining.
        """
        self._dag.add_transformation(transformation)
        return self

    def with_dependencies(
        self, dependencies: dict[str, list[str]]
    ) -> "TransformationDAGBuilder":
        """Add dependencies between transformations.

        Args:
            dependencies: Dict mapping transformation IDs to their dependencies.

        Returns:
            Self for chaining.
        """
        for transformation_id, depends_on in dependencies.items():
            for dep_id in depends_on:
                self._dag.add_dependency(transformation_id, dep_id)
        return self

    def auto_build_dependencies(
        self, transformations: list[Transformation]
    ) -> "TransformationDAGBuilder":
        """Automatically build dependencies based on column usage.

        Args:
            transformations: List of transformations.

        Returns:
            Self for chaining.
        """
        # Track which columns each transformation produces/modifies
        column_producers: dict[str, str] = {}  # column -> transformation_id

        for transformation in transformations:
            for col in transformation.target_columns:
                # If a column is modified by a transformation,
                # other transformations using that column depend on it
                if col in column_producers:
                    self._dag.add_dependency(
                        transformation.id, column_producers[col]
                    )
                column_producers[col] = transformation.id

        return self

    def build(self) -> TransformationDAG:
        """Build and return the DAG.

        Returns:
            The constructed TransformationDAG.
        """
        return self._dag
