"""Agent coordination module."""

from typing import Dict, List, Optional, Any, Protocol
import pandas as pd
import logging

from src.common.types.data_profile import DataProfile
from src.common.types.transformation import Transformation, TransformationResult
from src.common.types.validation import ValidationResult
from src.common.types.quality import QualityMetrics, QualityDelta
from src.common.types.ranking import TransformationCandidate, RankedTransformation


logger = logging.getLogger(__name__)


class ProfilerAgent:
    """Data profiling agent."""

    def __init__(self, profiler):
        self.profiler = profiler

    def execute(self, data: pd.DataFrame) -> DataProfile:
        """Profile the input data."""
        logger.info("Executing profiling agent")
        return self.profiler.profile(data)

    def get_name(self) -> str:
        return "profiling"


class TransformationAgent:
    """Transformation candidate generation agent."""

    def __init__(self, engine):
        self.engine = engine

    def execute(self, profile: DataProfile) -> List[Transformation]:
        """Generate transformation candidates."""
        logger.info("Executing transformation agent - generating candidates")
        return self.engine.generate_candidates(profile)

    def get_name(self) -> str:
        return "transformation"


class ExecutionAgent:
    """Transformation execution agent."""

    def __init__(self, engine):
        self.engine = engine

    def execute(self, data: pd.DataFrame, transformation: Transformation) -> TransformationResult:
        """Execute a transformation."""
        logger.info(f"Executing transformation: {transformation.id}")
        return self.engine.execute(data, transformation)

    def get_name(self) -> str:
        return "execution"


class ValidationAgent:
    """Validation agent."""

    def __init__(self, engine):
        self.engine = engine

    def execute(
        self, original: pd.DataFrame, transformed: pd.DataFrame, profile: DataProfile
    ) -> ValidationResult:
        """Validate transformed data."""
        logger.info("Executing validation agent")
        return self.engine.validate(original, transformed, profile)

    def get_name(self) -> str:
        return "validation"


class QualityScoringAgent:
    """Quality scoring agent."""

    def __init__(self, scorer):
        self.scorer = scorer

    def execute(self, data: pd.DataFrame, profile: Optional[DataProfile] = None) -> QualityMetrics:
        """Score data quality."""
        logger.info("Executing quality scoring agent")
        return self.scorer.score(data, profile)

    def get_name(self) -> str:
        return "quality_scoring"


class RankingAgent:
    """Ranking agent."""

    def __init__(self, engine):
        self.engine = engine

    def execute(self, candidates: List[TransformationCandidate]) -> List[RankedTransformation]:
        """Rank transformation candidates."""
        logger.info("Executing ranking agent")
        return self.engine.rank(candidates)

    def get_name(self) -> str:
        return "ranking"


class AgentCoordinator:
    """Coordinates all agents in the pipeline."""

    def __init__(
        self,
        profiler,
        transformation_engine,
        validation_engine,
        quality_scorer,
        ranking_engine,
    ):
        """Initialize the agent coordinator.

        Args:
            profiler: Data profiler instance
            transformation_engine: Transformation engine instance
            validation_engine: Validation engine instance
            quality_scorer: Quality scorer instance
            ranking_engine: Ranking engine instance
        """
        self.profiler = ProfilerAgent(profiler)
        self.transformation = TransformationAgent(transformation_engine)
        self.execution = ExecutionAgent(transformation_engine)
        self.validation = ValidationAgent(validation_engine)
        self.quality_scoring = QualityScoringAgent(quality_scorer)
        self.ranking = RankingAgent(ranking_engine)

        # Agent registry for lookup
        self._agents: Dict[str, Any] = {
            "profiling": self.profiler,
            "transformation": self.transformation,
            "execution": self.execution,
            "validation": self.validation,
            "quality_scoring": self.quality_scoring,
            "ranking": self.ranking,
        }

    def get_agent(self, name: str) -> Optional[Any]:
        """Get an agent by name.

        Args:
            name: Agent name

        Returns:
            The agent instance, or None if not found
        """
        return self._agents.get(name)

    def list_agents(self) -> List[str]:
        """List all available agents.

        Returns:
            List of agent names
        """
        return list(self._agents.keys())

    def execute_agent(self, name: str, *args, **kwargs) -> Any:
        """Execute an agent by name.

        Args:
            name: Agent name
            *args: Positional arguments for the agent
            **kwargs: Keyword arguments for the agent

        Returns:
            Agent execution result

        Raises:
            ValueError: If agent not found
        """
        agent = self.get_agent(name)
        if agent is None:
            raise ValueError(f"Agent not found: {name}")

        logger.info(f"Executing agent: {name}")
        return agent.execute(*args, **kwargs)
