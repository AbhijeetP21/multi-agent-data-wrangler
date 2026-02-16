"""Pipeline execution orchestration."""

import time
import logging
from typing import Optional, List
import pandas as pd

from src.common.types.pipeline import PipelineConfig, PipelineState, PipelineStep
from src.common.types.data_profile import DataProfile
from src.common.types.transformation import Transformation, TransformationResult
from src.common.types.ranking import TransformationCandidate, RankedTransformation

from .agent_coordinator import AgentCoordinator
from .state_manager import StateManager
from .interfaces import PipelineResult


logger = logging.getLogger(__name__)


class PipelineManager:
    """Manages the execution of the data wrangling pipeline."""

    def __init__(
        self,
        coordinator: AgentCoordinator,
        state_manager: Optional[StateManager] = None,
    ):
        """Initialize the pipeline manager.

        Args:
            coordinator: Agent coordinator instance
            state_manager: Optional state manager for persistence
        """
        self.coordinator = coordinator
        self.state_manager = state_manager or StateManager()
        self._current_state: Optional[PipelineState] = None

    def run(
        self,
        data: pd.DataFrame,
        config: PipelineConfig,
        state_name: str = "default",
    ) -> PipelineResult:
        """Run the full pipeline.

        Args:
            data: Input DataFrame
            config: Pipeline configuration
            state_name: Name for state persistence

        Returns:
            PipelineResult with execution results
        """
        start_time = time.time()
        
        # Initialize pipeline state
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        try:
            # Step 1: Data Profiling
            logger.info("Step 1: Starting data profiling")
            state = self._execute_profiling(data, state)
            self._save_checkpoint(state, state_name)
            
            # Step 2: Generate Candidates
            logger.info("Step 2: Generating transformation candidates")
            state = self._execute_candidate_generation(state)
            self._save_checkpoint(state, state_name)
            
            # Step 3: Validate and Score Candidates
            logger.info("Step 3: Validating and scoring candidates")
            state = self._execute_validation_and_scoring(data, state)
            self._save_checkpoint(state, state_name)
            
            # Step 4: Ranking
            if config.enable_ranking:
                logger.info("Step 4: Ranking transformations")
                state = self._execute_ranking(state)
                self._save_checkpoint(state, state_name)
            
            # Get final transformed data (best transformation)
            final_data = data
            if state.ranked_transformations:
                best = state.ranked_transformations[0]
                if best.candidate.validation_result.passed:
                    # Re-execute the best transformation
                    result = self.coordinator.execution.execute(
                        data, best.candidate.transformation
                    )
                    if result.success:
                        final_data = result.output_data
            
            execution_time = time.time() - start_time
            
            return PipelineResult(
                success=True,
                data=final_data,
                profile=state.data_profile,
                ranked_transformations=state.ranked_transformations,
                execution_time_seconds=execution_time,
            )
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            state.error = str(e)
            self._save_checkpoint(state, state_name)
            
            return PipelineResult(
                success=False,
                error=str(e),
                execution_time_seconds=time.time() - start_time,
            )

    def recover(
        self,
        state_name: str = "default",
    ) -> PipelineResult:
        """Recover and continue from a saved state.

        Args:
            state_name: Name of the state to recover

        Returns:
            PipelineResult with recovered state
        """
        state = self.state_manager.load_state(state_name)
        
        if state is None:
            return PipelineResult(
                success=False,
                error="No saved state found",
            )
        
        self._current_state = state
        
        # Resume from current step
        # This is a simplified recovery - in production, 
        # you'd need to reload the data and continue
        return PipelineResult(
            success=True,
            profile=state.data_profile,
            ranked_transformations=state.ranked_transformations,
        )

    def get_state(self) -> Optional[PipelineState]:
        """Get the current pipeline state.

        Returns:
            Current pipeline state
        """
        return self._current_state

    def _execute_profiling(
        self, data: pd.DataFrame, state: PipelineState
    ) -> PipelineState:
        """Execute data profiling step."""
        profile = self.coordinator.profiler.execute(data)
        
        state.data_profile = profile
        state.current_step = PipelineStep.GENERATION
        state.completed_steps.append(PipelineStep.PROFILING)
        
        return state

    def _execute_candidate_generation(self, state: PipelineState) -> PipelineState:
        """Execute transformation candidate generation."""
        if state.data_profile is None:
            raise ValueError("Data profile is required for candidate generation")
        
        transformations = self.coordinator.transformation.execute(state.data_profile)
        
        # Store as candidates (empty for now, will be populated in validation)
        state.current_step = PipelineStep.VALIDATION
        state.completed_steps.append(PipelineStep.GENERATION)
        
        return state

    def _execute_validation_and_scoring(
        self, data: pd.DataFrame, state: PipelineState
    ) -> PipelineState:
        """Execute validation and quality scoring for each candidate."""
        if state.data_profile is None:
            raise ValueError("Data profile is required for validation")
        
        # Get candidates from coordinator
        transformations = self.coordinator.transformation.execute(state.data_profile)
        
        candidates: List[TransformationCandidate] = []
        
        for transformation in transformations:
            try:
                # Execute transformation
                result = self.coordinator.execution.execute(data, transformation)
                
                if not result.success:
                    continue
                
                # Validate transformed data
                validation_result = self.coordinator.validation.execute(
                    data, result.output_data, state.data_profile
                )
                
                if not validation_result.passed:
                    continue
                
                # Score before and after
                quality_before = self.coordinator.quality_scoring.execute(data, state.data_profile)
                quality_after = self.coordinator.quality_scoring.execute(
                    result.output_data, state.data_profile
                )
                
                # Compare quality
                quality_delta = self.coordinator.quality_scoring.scorer.compare(
                    quality_before, quality_after
                )
                
                # Create candidate
                candidate = TransformationCandidate(
                    transformation=transformation,
                    validation_result=validation_result,
                    quality_before=quality_before,
                    quality_after=quality_after,
                    quality_delta=quality_delta,
                )
                candidates.append(candidate)
                
            except Exception as e:
                logger.warning(f"Failed to process transformation {transformation.id}: {e}")
                continue
        
        state.candidates = candidates
        state.current_step = PipelineStep.RANKING
        state.completed_steps.append(PipelineStep.VALIDATION)
        
        return state

    def _execute_ranking(self, state: PipelineState) -> PipelineState:
        """Execute ranking of transformation candidates."""
        if not state.candidates:
            logger.warning("No candidates to rank")
            state.ranked_transformations = []
            return state
        
        ranked = self.coordinator.ranking.execute(state.candidates)
        
        state.ranked_transformations = ranked
        state.completed_steps.append(PipelineStep.RANKING)
        
        return state

    def _save_checkpoint(self, state: PipelineState, name: str) -> None:
        """Save a checkpoint of the current state."""
        try:
            self.state_manager.save_state(state, name)
            self._current_state = state
            logger.debug(f"Checkpoint saved: {state.current_step}")
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def run_profile_only(self, data: pd.DataFrame) -> DataProfile:
        """Run only the profiling step.

        Args:
            data: Input DataFrame

        Returns:
            DataProfile
        """
        return self.coordinator.profiler.execute(data)

    def run_generate_only(
        self, profile: DataProfile
    ) -> List[Transformation]:
        """Run only candidate generation.

        Args:
            profile: Data profile

        Returns:
            List of transformation candidates
        """
        return self.coordinator.transformation.execute(profile)

    def run_validate_only(
        self,
        original: pd.DataFrame,
        transformed: pd.DataFrame,
        profile: DataProfile,
    ) -> "ValidationResult":
        """Run only validation.

        Args:
            original: Original DataFrame
            transformed: Transformed DataFrame
            profile: Data profile

        Returns:
            ValidationResult
        """
        return self.coordinator.validation.execute(original, transformed, profile)

    def run_rank_only(
        self, candidates: List[TransformationCandidate]
    ) -> List[RankedTransformation]:
        """Run only ranking.

        Args:
            candidates: Transformation candidates

        Returns:
            Ranked transformations
        """
        return self.coordinator.ranking.execute(candidates)
