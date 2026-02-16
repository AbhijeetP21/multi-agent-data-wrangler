"""Quality scoring types."""

from pydantic import BaseModel


class QualityMetrics(BaseModel):
    """Quality metrics for a dataset."""

    completeness: float  # 0-1: ratio of non-null values
    consistency: float  # 0-1: data type consistency
    validity: float  # 0-1: value range validity
    uniqueness: float  # 0-1: ratio of unique values
    overall: float  # weighted composite


class QualityDelta(BaseModel):
    """Change in quality metrics after a transformation."""

    before: QualityMetrics
    after: QualityMetrics
    improvement: QualityMetrics
    composite_delta: float
