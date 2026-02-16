"""Data profiling types."""

from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class ColumnProfile(BaseModel):
    """Profile information for a single column."""

    name: str
    dtype: str
    null_count: int
    null_percentage: float
    unique_count: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    inferred_type: str  # 'numeric', 'categorical', 'datetime', 'text', 'boolean'


class DataProfile(BaseModel):
    """Complete data profile for a dataset."""

    timestamp: datetime
    row_count: int
    column_count: int
    columns: dict[str, ColumnProfile]
    overall_missing_percentage: float
    duplicate_rows: int
