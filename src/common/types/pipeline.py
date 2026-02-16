"""Pipeline types."""

from pydantic import BaseModel
from typing import Optional, Any
from enum import Enum

from .data_profile import DataProfile
from .ranking import TransformationCandidate, RankedTransformation


class PipelineStep(str, Enum):
    """Enumeration of pipeline steps."""

    PROFILING = "profiling"
    GENERATION = "generation"
    VALIDATION = "validation"
    EXECUTION = "execution"
    SCORING = "scoring"
    RANKING = "ranking"


class PipelineState(BaseModel):
    """State of the data wrangling pipeline."""

    current_step: PipelineStep
    completed_steps: list[PipelineStep]
    data_profile: Optional[DataProfile] = None
    candidates: list[TransformationCandidate] = []
    ranked_transformations: list[RankedTransformation] = []
    error: Optional[str] = None


class PipelineConfig(BaseModel):
    """Configuration for the pipeline."""

    max_iterations: int = 10
    timeout_seconds: int = 300
    enable_ranking: bool = True
    quality_threshold: float = 0.8
