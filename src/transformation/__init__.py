"""Transformation module for data wrangling.

This module provides transformation capabilities for data wrangling tasks,
including candidate generation, execution, and reversibility checking.
"""

from src.transformation.interfaces import TransformationEngine
from src.transformation.candidate_generator import CandidateGenerator
from src.transformation.executor import TransformationExecutor
from src.transformation.dag import TransformationDAG, TransformationDAGBuilder
from src.transformation.reversibility import ReversibilityChecker, ReversibilityTracker
from src.transformation.transformations import (
    BaseTransformation,
    FillMissingTransformation,
    NormalizeTransformation,
    EncodeCategoricalTransformation,
    RemoveOutliersTransformation,
    DropDuplicatesTransformation,
    CastTypeTransformation,
)

__all__ = [
    # Interfaces
    "TransformationEngine",
    # Core classes
    "CandidateGenerator",
    "TransformationExecutor",
    "TransformationDAG",
    "TransformationDAGBuilder",
    # Reversibility
    "ReversibilityChecker",
    "ReversibilityTracker",
    # Transformations
    "BaseTransformation",
    "FillMissingTransformation",
    "NormalizeTransformation",
    "EncodeCategoricalTransformation",
    "RemoveOutliersTransformation",
    "DropDuplicatesTransformation",
    "CastTypeTransformation",
]
