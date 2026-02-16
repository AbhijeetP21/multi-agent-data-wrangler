"""Ranking types."""

from pydantic import BaseModel

from .transformation import Transformation
from .validation import ValidationResult
from .quality import QualityMetrics, QualityDelta


class TransformationCandidate(BaseModel):
    """A transformation candidate with validation and quality metrics."""

    transformation: Transformation
    validation_result: ValidationResult
    quality_before: QualityMetrics
    quality_after: QualityMetrics
    quality_delta: QualityDelta


class RankedTransformation(BaseModel):
    """A ranked transformation with score and reasoning."""

    rank: int
    candidate: TransformationCandidate
    composite_score: float
    reasoning: str
