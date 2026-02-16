"""Tests for the orchestrator pipeline."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

import pandas as pd
import pytest

from src.orchestrator.interfaces import PipelineResult
from src.orchestrator.state_manager import StateManager
from src.orchestrator.agent_coordinator import (
    AgentCoordinator,
    ProfilerAgent,
    TransformationAgent,
    ExecutionAgent,
    ValidationAgent,
    QualityScoringAgent,
    RankingAgent,
)
from src.orchestrator.pipeline_manager import PipelineManager
from src.orchestrator.failure_recovery import (
    FailureRecovery,
    FailureStrategy,
    RetryConfig,
    CircuitBreaker,
    with_retry,
)
from src.common.types.pipeline import PipelineState, PipelineStep, PipelineConfig
from src.common.types.data_profile import DataProfile, ColumnProfile
from src.common.types.transformation import Transformation, TransformationType
from src.common.types.validation import ValidationResult, ValidationIssue
from src.common.types.quality import QualityMetrics, QualityDelta
from src.common.types.ranking import TransformationCandidate, RankedTransformation


class TestStateManager:
    """Tests for StateManager."""

    def test_save_and_load_state(self, tmp_path):
        """Test saving and loading pipeline state."""
        state_manager = StateManager(state_dir=str(tmp_path))
        
        # Create a test state
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        # Save state
        state_file = state_manager.save_state(state, "test")
        assert state_file.exists()
        
        # Load state
        loaded_state = state_manager.load_state("test")
        assert loaded_state is not None
        assert loaded_state.current_step == PipelineStep.PROFILING

    def test_has_state(self, tmp_path):
        """Test checking if state exists."""
        state_manager = StateManager(state_dir=str(tmp_path))
        
        assert not state_manager.has_state("test")
        
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        state_manager.save_state(state, "test")
        
        assert state_manager.has_state("test")

    def test_clear_state(self, tmp_path):
        """Test clearing state."""
        state_manager = StateManager(state_dir=str(tmp_path))
        
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        state_manager.save_state(state, "test")
        
        state_manager.clear_state("test")
        
        assert not state_manager.has_state("test")

    def test_list_states(self, tmp_path):
        """Test listing states."""
        state_manager = StateManager(state_dir=str(tmp_path))
        
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        state_manager.save_state(state, "state1")
        state_manager.save_state(state, "state2")
        
        states = state_manager.list_states()
        assert "state1" in states
        assert "state2" in states


class TestAgentCoordinator:
    """Tests for AgentCoordinator."""

    def test_get_agent(self):
        """Test getting an agent by name."""
        mock_profiler = Mock()
        mock_engine = Mock()
        
        coordinator = AgentCoordinator(
            profiler=mock_profiler,
            transformation_engine=mock_engine,
            validation_engine=mock_engine,
            quality_scorer=mock_engine,
            ranking_engine=mock_engine,
        )
        
        profiler_agent = coordinator.get_agent("profiling")
        assert profiler_agent is not None
        assert isinstance(profiler_agent, ProfilerAgent)

    def test_list_agents(self):
        """Test listing all agents."""
        mock_profiler = Mock()
        mock_engine = Mock()
        
        coordinator = AgentCoordinator(
            profiler=mock_profiler,
            transformation_engine=mock_engine,
            validation_engine=mock_engine,
            quality_scorer=mock_engine,
            ranking_engine=mock_engine,
        )
        
        agents = coordinator.list_agents()
        assert "profiling" in agents
        assert "transformation" in agents
        assert "validation" in agents
        assert "ranking" in agents

    def test_execute_agent(self):
        """Test executing an agent."""
        mock_profiler = Mock()
        mock_profiler.profile.return_value = Mock()
        
        coordinator = AgentCoordinator(
            profiler=mock_profiler,
            transformation_engine=Mock(),
            validation_engine=Mock(),
            quality_scorer=Mock(),
            ranking_engine=Mock(),
        )
        
        data = pd.DataFrame({"a": [1, 2, 3]})
        result = coordinator.execute_agent("profiling", data)
        
        mock_profiler.profile.assert_called_once_with(data)


class TestPipelineManager:
    """Tests for PipelineManager."""

    @pytest.fixture
    def mock_coordinator(self):
        """Create a mock coordinator."""
        coordinator = Mock(spec=AgentCoordinator)
        
        # Mock profiler
        mock_profiler = Mock()
        mock_profile = DataProfile(
            timestamp=datetime.now(),
            row_count=5,
            column_count=3,
            columns={
                "id": ColumnProfile(
                    name="id", dtype="int64", null_count=0,
                    null_percentage=0.0, inferred_type="numeric"
                ),
                "name": ColumnProfile(
                    name="name", dtype="object", null_count=0,
                    null_percentage=0.0, inferred_type="categorical"
                ),
                "value": ColumnProfile(
                    name="value", dtype="float64", null_count=1,
                    null_percentage=20.0, inferred_type="numeric"
                ),
            },
            overall_missing_percentage=5.0,
            duplicate_rows=0,
        )
        mock_profiler.profile.return_value = mock_profile
        coordinator.profiler = ProfilerAgent(mock_profiler)
        
        # Mock transformation
        mock_transformation = Mock()
        mock_transformation.generate.return_value = []
        coordinator.transformation = TransformationAgent(mock_transformation)
        
        # Mock execution
        coordinator.execution = ExecutionAgent(mock_transformation)
        
        # Mock validation
        mock_validation = Mock()
        mock_validation.validate.return_value = ValidationResult(
            passed=True,
            issues=[],
            original_row_count=5,
            transformed_row_count=5,
            schema_compatible=True,
        )
        coordinator.validation = ValidationAgent(mock_validation)
        
        # Mock quality scoring
        mock_scorer = Mock()
        mock_scorer.score.return_value = QualityMetrics(
            completeness=0.9,
            consistency=0.9,
            validity=0.9,
            uniqueness=0.9,
            overall=0.9,
        )
        mock_scorer.compare.return_value = QualityDelta(
            before=QualityMetrics(
                completeness=0.8, consistency=0.8,
                validity=0.8, uniqueness=0.8, overall=0.8
            ),
            after=QualityMetrics(
                completeness=0.9, consistency=0.9,
                validity=0.9, uniqueness=0.9, overall=0.9
            ),
            improvement=QualityMetrics(
                completeness=0.1, consistency=0.1,
                validity=0.1, uniqueness=0.1, overall=0.1
            ),
            composite_delta=0.1,
        )
        coordinator.quality_scoring = QualityScoringAgent(mock_scorer)
        
        # Mock ranking
        mock_ranker = Mock()
        mock_ranker.rank.return_value = []
        coordinator.ranking = RankingAgent(mock_ranker)
        
        return coordinator

    def test_run_pipeline(self, mock_coordinator, tmp_path):
        """Test running the full pipeline."""
        state_manager = StateManager(state_dir=str(tmp_path))
        pipeline = PipelineManager(mock_coordinator, state_manager)
        
        data = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["a", "b", "c", "d", "e"],
            "value": [1.0, 2.0, 3.0, 4.0, 5.0],
        })
        
        config = PipelineConfig(
            max_iterations=10,
            timeout_seconds=300,
            enable_ranking=True,
            quality_threshold=0.8,
        )
        
        result = pipeline.run(data, config, "test")
        
        # The result depends on mock setup
        assert isinstance(result, PipelineResult)

    def test_get_state(self, mock_coordinator, tmp_path):
        """Test getting current pipeline state."""
        state_manager = StateManager(state_dir=str(tmp_path))
        pipeline = PipelineManager(mock_coordinator, state_manager)
        
        assert pipeline.get_state() is None

    def test_recover_no_state(self, mock_coordinator, tmp_path):
        """Test recovering when no state exists."""
        state_manager = StateManager(state_dir=str(tmp_path))
        pipeline = PipelineManager(mock_coordinator, state_manager)
        
        result = pipeline.recover("nonexistent")
        
        assert not result.success
        assert "No saved state found" in result.error


class TestFailureRecovery:
    """Tests for FailureRecovery."""

    def test_handle_failure_abort(self):
        """Test handling failure with abort strategy."""
        recovery = FailureRecovery(strategy=FailureStrategy.ABORT)
        
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        error = ValueError("Test error")
        strategy, result_state = recovery.handle_failure(
            PipelineStep.PROFILING, error, state
        )
        
        assert strategy == FailureStrategy.ABORT
        assert result_state.error is not None

    def test_handle_failure_skip(self):
        """Test handling failure with skip strategy."""
        recovery = FailureRecovery(strategy=FailureStrategy.SKIP)
        
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        error = ValueError("Test error")
        strategy, result_state = recovery.handle_failure(
            PipelineStep.PROFILING, error, state
        )
        
        assert strategy == FailureStrategy.SKIP

    def test_retry_decorator(self):
        """Test retry decorator."""
        call_count = 0
        
        @with_retry(max_retries=2, delay=0.01)
        def failing_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Fail")
            return "success"
        
        result = failing_function()
        assert result == "success"
        assert call_count == 3

    def test_circuit_breaker(self):
        """Test circuit breaker."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)
        
        def fail_once():
            fail_once.call_count += 1
            if fail_once.call_count < 3:
                raise ValueError("Fail")
            return "success"
        fail_once.call_count = 0
        
        # First two calls fail
        with pytest.raises(ValueError):
            cb.call(fail_once)
        
        with pytest.raises(ValueError):
            cb.call(fail_once)
        
        # Circuit should be open now
        assert cb.is_open


class TestPipelineResult:
    """Tests for PipelineResult."""

    def test_pipeline_result_success(self):
        """Test successful pipeline result."""
        result = PipelineResult(
            success=True,
            data=pd.DataFrame({"a": [1, 2, 3]}),
            execution_time_seconds=10.5,
        )
        
        assert result.success is True
        assert result.error is None
        assert result.execution_time_seconds == 10.5

    def test_pipeline_result_failure(self):
        """Test failed pipeline result."""
        result = PipelineResult(
            success=False,
            error="Test error",
        )
        
        assert result.success is False
        assert result.error == "Test error"


class TestPipelineIntegration:
    """Integration tests for the pipeline."""

    def test_pipeline_end_to_end(self, sample_dataframe, tmp_path):
        """Test complete pipeline execution."""
        # This is a simplified integration test
        # In a real scenario, we'd use actual module implementations
        
        state_manager = StateManager(state_dir=str(tmp_path))
        
        # Verify state manager works
        state = PipelineState(
            current_step=PipelineStep.PROFILING,
            completed_steps=[],
        )
        
        state_manager.save_state(state, "integration_test")
        loaded = state_manager.load_state("integration_test")
        
        assert loaded.current_step == PipelineStep.PROFILING


# Run tests if executed directly
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
