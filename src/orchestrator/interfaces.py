"""Orchestrator module interfaces."""

from typing import Protocol, Optional
import pandas as pd

from src.common.types.pipeline import PipelineConfig, PipelineState
from src.common.types.data_profile import DataProfile
from src.common.types.ranking import RankedTransformation


class PipelineResult:
    """Result of pipeline execution."""

    def __init__(
        self,
        success: bool,
        data: Optional[pd.DataFrame] = None,
        profile: Optional[DataProfile] = None,
        ranked_transformations: Optional[list[RankedTransformation]] = None,
        error: Optional[str] = None,
        execution_time_seconds: float = 0.0,
    ):
        self.success = success
        self.data = data
        self.profile = profile
        self.ranked_transformations = ranked_transformations or []
        self.error = error
        self.execution_time_seconds = execution_time_seconds


class Orchestrator(Protocol):
    """Protocol defining the orchestrator interface."""

    def run(self, config: PipelineConfig) -> PipelineResult:
        """Run the full pipeline with the given configuration."""
        ...

    def recover(self, state: PipelineState) -> PipelineResult:
        """Recover and continue from a saved pipeline state."""
        ...

    def get_state(self) -> Optional[PipelineState]:
        """Get the current pipeline state."""
        ...


class Agent(Protocol):
    """Protocol defining an agent interface."""

    def execute(self, input_data: any) -> any:
        """Execute the agent's task."""
        ...

    def get_name(self) -> str:
        """Get the agent's name."""
        ...
