"""Transformation types."""

from pydantic import BaseModel
from typing import Optional, Any, Literal
from enum import Enum


class TransformationType(str, Enum):
    """Enumeration of available transformation types."""

    FILL_MISSING = "fill_missing"
    NORMALIZE = "normalize"
    ENCODE_CATEGORICAL = "encode_categorical"
    REMOVE_OUTLIERS = "remove_outliers"
    DROP_DUPLICATES = "drop_duplicates"
    CAST_TYPE = "cast_type"


class Transformation(BaseModel):
    """Represents a data transformation operation."""

    id: str
    type: TransformationType
    target_columns: list[str]
    params: dict[str, Any]  # Type-safe based on type
    reversible: bool
    description: str


class TransformationResult(BaseModel):
    """Result of applying a transformation."""

    transformation: Transformation
    success: bool
    output_data: Any  # pd.DataFrame - using Any to avoid circular imports
    error_message: Optional[str] = None
    execution_time_ms: float
