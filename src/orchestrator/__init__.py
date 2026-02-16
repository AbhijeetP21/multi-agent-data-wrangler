"""Orchestrator module for coordinating the data wrangling pipeline.

This module provides the core orchestration functionality for the
Multi-Agent Data Wrangler system.
"""

from .interfaces import Orchestrator, PipelineResult, Agent
from .state_manager import StateManager
from .agent_coordinator import (
    AgentCoordinator,
    ProfilerAgent,
    TransformationAgent,
    ExecutionAgent,
    ValidationAgent,
    QualityScoringAgent,
    RankingAgent,
)
from .pipeline_manager import PipelineManager
from .failure_recovery import (
    FailureRecovery,
    FailureStrategy,
    RetryConfig,
    RecoveryAction,
    CircuitBreaker,
    with_retry,
    with_fallback,
)
from .cli import main, run_pipeline, profile_data


__all__ = [
    # Interfaces
    "Orchestrator",
    "PipelineResult",
    "Agent",
    # State Management
    "StateManager",
    # Agent Coordination
    "AgentCoordinator",
    "ProfilerAgent",
    "TransformationAgent",
    "ExecutionAgent",
    "ValidationAgent",
    "QualityScoringAgent",
    "RankingAgent",
    # Pipeline Management
    "PipelineManager",
    # Failure Recovery
    "FailureRecovery",
    "FailureStrategy",
    "RetryConfig",
    "RecoveryAction",
    "CircuitBreaker",
    "with_retry",
    "with_fallback",
    # CLI
    "main",
    "run_pipeline",
    "profile_data",
]


__version__ = "1.0.0"
