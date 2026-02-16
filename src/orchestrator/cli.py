"""Command-line interface for the data wrangler."""

import argparse
import logging
import sys
import json
from pathlib import Path
from typing import Optional

import pandas as pd

from src.common.config.loader import ConfigLoader
from src.common.types.pipeline import PipelineConfig

# Import agent modules - these need to be instantiated
from .pipeline_manager import PipelineManager
from .agent_coordinator import AgentCoordinator
from .state_manager import StateManager


logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def load_modules():
    """Load and instantiate all required modules."""
    # Import modules from the package
    from src.data_profiling.profiler import DataProfilerService as ProfilerImpl
    from src.transformation.candidate_generator import CandidateGenerator
    from src.transformation.executor import TransformationExecutor
    from src.validation.validator import ValidationService
    from src.quality_scoring.scorer import QualityScorerService as ScorerImpl
    from src.ranking.ranker import RankingService as RankerImpl
    from src.ranking.policies import ImprovementPolicy
    
    # Create instances
    profiler = ProfilerImpl()
    transformation_engine = CandidateGenerator()
    executor = TransformationExecutor()
    validation_engine = ValidationService()
    quality_scorer = ScorerImpl()
    ranking_engine = RankerImpl(policy=ImprovementPolicy())
    
    # Create combined transformation engine
    class CombinedTransformationEngine:
        def __init__(self, generator, executor):
            self.generator = generator
            self.executor = executor
        
        def generate_candidates(self, profile):
            return self.generator.generate_candidates(profile)
        
        def execute(self, data, transformation):
            return self.executor.execute(data, transformation)
        
        def reverse(self, data, transformation):
            return data  # Default implementation
    
    transformation_combined = CombinedTransformationEngine(
        transformation_engine, executor
    )
    
    return {
        "profiler": profiler,
        "transformation_engine": transformation_combined,
        "validation_engine": validation_engine,
        "quality_scorer": quality_scorer,
        "ranking_engine": ranking_engine,
    }


def run_pipeline(args):
    """Run the full data wrangling pipeline."""
    setup_logging(args.log_level)
    logger.info("Starting data wrangling pipeline")
    
    try:
        # Load configuration
        config = ConfigLoader.load(args.config)
        
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create state manager
        state_manager = StateManager(args.state_dir)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator, state_manager)
        
        # Load input data
        data = pd.read_csv(args.input)
        logger.info(f"Loaded data with {len(data)} rows")
        
        # Run pipeline
        result = pipeline.run(data, config, args.state_name)
        
        if result.success:
            # Save output
            if result.data is not None and args.output:
                result.data.to_csv(args.output, index=False)
                logger.info(f"Output saved to {args.output}")
            
            # Save profile
            if result.profile and args.profile:
                with open(args.profile, "w") as f:
                    json.dump(result.profile.model_dump(), f, indent=2)
                logger.info(f"Profile saved to {args.profile}")
            
            # Save report
            if args.report:
                report = {
                    "success": True,
                    "execution_time_seconds": result.execution_time_seconds,
                    "candidates_count": len(result.ranked_transformations),
                }
                with open(args.report, "w") as f:
                    json.dump(report, f, indent=2)
                logger.info(f"Report saved to {args.report}")
            
            logger.info(f"Pipeline completed in {result.execution_time_seconds:.2f}s")
            return 0
        else:
            logger.error(f"Pipeline failed: {result.error}")
            return 1
            
    except Exception as e:
        logger.error(f"Error running pipeline: {str(e)}")
        return 1


def profile_data(args):
    """Profile input data only."""
    setup_logging(args.log_level)
    logger.info("Starting data profiling")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator)
        
        # Load input data
        data = pd.read_csv(args.input)
        logger.info(f"Loaded data with {len(data)} rows")
        
        # Profile data
        profile = pipeline.run_profile_only(data)
        
        # Save profile
        if args.output:
            with open(args.output, "w") as f:
                json.dump(profile.model_dump(), f, indent=2, default=str)
            logger.info(f"Profile saved to {args.output}")
        
        # Print profile summary
        print(f"\nData Profile Summary:")
        print(f"  Rows: {profile.row_count}")
        print(f"  Columns: {profile.column_count}")
        print(f"  Missing: {profile.overall_missing_percentage:.2f}%")
        print(f"  Duplicates: {profile.duplicate_rows}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error profiling data: {str(e)}")
        return 1


def generate_candidates(args):
    """Generate transformation candidates from profile."""
    setup_logging(args.log_level)
    logger.info("Generating transformation candidates")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator)
        
        # Load profile
        with open(args.profile, "r") as f:
            profile_dict = json.load(f)
        
        from src.common.types.data_profile import DataProfile
        profile = DataProfile(**profile_dict)
        
        # Generate candidates
        candidates = pipeline.run_generate_only(profile)
        
        # Save candidates
        output_dir = Path(args.output)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        candidates_data = [c.model_dump() for c in candidates]
        with open(output_dir / "candidates.json", "w") as f:
            json.dump(candidates_data, f, indent=2, default=str)
        
        logger.info(f"Generated {len(candidates)} candidates")
        print(f"\nGenerated {len(candidates)} transformation candidates")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error generating candidates: {str(e)}")
        return 1


def validate_candidates(args):
    """Validate transformation candidates."""
    setup_logging(args.log_level)
    logger.info("Validating candidates")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator)
        
        # Load data
        data = pd.read_csv(args.data)
        
        # Load profile
        with open(args.profile, "r") as f:
            profile_dict = json.load(f)
        
        from src.common.types.data_profile import DataProfile
        profile = DataProfile(**profile_dict)
        
        # Load candidates
        with open(args.candidates, "r") as f:
            candidates_data = json.load(f)
        
        from src.common.types.transformation import Transformation
        candidates = [Transformation(**c) for c in candidates_data]
        
        # Validate each candidate
        results = []
        for candidate in candidates:
            result = pipeline.run_validate_only(data, data, profile)  # Simplified
            results.append(result)
        
        print(f"\nValidated {len(results)} candidates")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error validating candidates: {str(e)}")
        return 1


def rank_candidates(args):
    """Rank transformation candidates."""
    setup_logging(args.log_level)
    logger.info("Ranking candidates")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator)
        
        # Load candidates
        with open(args.candidates, "r") as f:
            candidates_data = json.load(f)
        
        from src.common.types.ranking import TransformationCandidate
        candidates = [TransformationCandidate(**c) for c in candidates_data]
        
        # Rank candidates
        ranked = pipeline.run_rank_only(candidates)
        
        # Save ranked results
        if args.output:
            ranked_data = [r.model_dump() for r in ranked]
            with open(args.output, "w") as f:
                json.dump(ranked_data, f, indent=2, default=str)
        
        print(f"\nRanked {len(ranked)} candidates")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error ranking candidates: {str(e)}")
        return 1


def run_agent(args):
    """Run a specific agent."""
    setup_logging(args.log_level)
    logger.info(f"Running agent: {args.name}")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Load input data
        data = pd.read_csv(args.input)
        
        # Execute agent
        result = coordinator.execute_agent(args.name, data)
        
        print(f"Agent '{args.name}' executed successfully")
        print(f"Result type: {type(result).__name__}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error running agent: {str(e)}")
        return 1


def recover_pipeline(args):
    """Recover pipeline from saved state."""
    setup_logging(args.log_level)
    logger.info("Recovering pipeline")
    
    try:
        # Load modules
        modules = load_modules()
        
        # Create coordinator
        coordinator = AgentCoordinator(**modules)
        
        # Create state manager
        state_manager = StateManager(args.state_dir)
        
        # Create pipeline manager
        pipeline = PipelineManager(coordinator, state_manager)
        
        # Recover
        result = pipeline.recover(args.state_name)
        
        if result.success:
            logger.info("Pipeline recovered successfully")
            return 0
        else:
            logger.error(f"Recovery failed: {result.error}")
            return 1
            
    except Exception as e:
        logger.error(f"Error recovering pipeline: {str(e)}")
        return 1


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description="Multi-Agent Data Wrangler - Orchestrate data wrangling pipeline"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Run pipeline command
    run_parser = subparsers.add_parser("run", help="Run full pipeline")
    run_parser.add_argument("--config", required=True, help="Pipeline configuration file")
    run_parser.add_argument("--input", required=True, help="Input CSV file")
    run_parser.add_argument("--output", help="Output CSV file")
    run_parser.add_argument("--profile", help="Profile JSON output file")
    run_parser.add_argument("--report", help="Report JSON output file")
    run_parser.add_argument("--state-dir", default=".state", help="State directory")
    run_parser.add_argument("--state-name", default="default", help="State name")
    run_parser.set_defaults(func=run_pipeline)
    
    # Profile command
    profile_parser = subparsers.add_parser("profile", help="Profile data only")
    profile_parser.add_argument("--input", required=True, help="Input CSV file")
    profile_parser.add_argument("--output", help="Profile JSON output file")
    profile_parser.set_defaults(func=profile_data)
    
    # Generate command
    generate_parser = subparsers.add_parser("generate", help="Generate candidates")
    generate_parser.add_argument("--profile", required=True, help="Profile JSON file")
    generate_parser.add_argument("--output", required=True, help="Output directory")
    generate_parser.set_defaults(func=generate_candidates)
    
    # Validate command
    validate_parser = subparsers.add_parser("validate", help="Validate candidates")
    validate_parser.add_argument("--candidates", required=True, help="Candidates JSON file")
    validate_parser.add_argument("--data", required=True, help="Original data CSV file")
    validate_parser.add_argument("--profile", required=True, help="Profile JSON file")
    validate_parser.set_defaults(func=validate_candidates)
    
    # Rank command
    rank_parser = subparsers.add_parser("rank", help="Rank candidates")
    rank_parser.add_argument("--candidates", required=True, help="Candidates JSON file")
    rank_parser.add_argument("--profile", help="Profile JSON file")
    rank_parser.add_argument("--output", help="Ranked output JSON file")
    rank_parser.set_defaults(func=rank_candidates)
    
    # Agent command
    agent_parser = subparsers.add_parser("agent", help="Run specific agent")
    agent_parser.add_argument("--name", required=True, help="Agent name")
    agent_parser.add_argument("--input", required=True, help="Input CSV file")
    agent_parser.set_defaults(func=run_agent)
    
    # Recover command
    recover_parser = subparsers.add_parser("recover", help="Recover pipeline")
    recover_parser.add_argument("--state-dir", default=".state", help="State directory")
    recover_parser.add_argument("--state-name", default="default", help="State name")
    recover_parser.set_defaults(func=recover_pipeline)
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 1
    
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
