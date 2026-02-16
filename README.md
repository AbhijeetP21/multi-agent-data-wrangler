# Multi-Agent Data Wrangler

A production-grade, evaluation-driven data wrangling system employing multi-agent architecture with an integrated Streamlit web interface.

## Overview

This system provides a modular, extensible framework for automated data transformation and quality improvement. It leverages a multi-agent architecture to intelligently profile, transform, validate, and rank data improvements based on configurable quality metrics.

The pipeline operates through five primary stages:

1. **Data Profiling** - Analyzes input data to understand schema, infer data types, detect missing values, and generate statistical summaries
2. **Candidate Generation** - Produces transformation candidates based on the data profile and detected quality issues
3. **Validation** - Applies data integrity checks and leakage detection to ensure transformations are safe
4. **Quality Scoring** - Evaluates transformations using composite metrics (completeness, consistency, validity, uniqueness)
5. **Ranking** - Orders transformations by quality improvement using configurable scoring policies

## Features

- **Data Profiling**: Schema inference, missing value detection, type detection (numeric, categorical, boolean, datetime, mixed), statistical summaries, duplicate detection
- **Transformation Engine**: Deterministic transformations, candidate generation, transformation DAG, reversibility support
- **Validation Layer**: Data integrity checks, schema validation, leakage detection
- **Quality Scoring**: Completeness, consistency, validity, uniqueness metrics with composite scoring
- **Ranking System**: Pluggable ranking policies, composite scoring, improvement-based ordering
- **Orchestration**: Agent coordination, state management, failure recovery strategies (SKIP, RETRY, ABORT, FALLBACK)
- **Web Interface**: Streamlit-based UI for file upload, pipeline configuration, and result visualization

## Architecture

```
+-----------------------------------------------------------------+
|                        CLI / Web UI                              |
+-----------------------------------------------------------------+
                                |
                                v
+-----------------------------------------------------------------+
|                      Pipeline Manager                            |
+-----------------------------------------------------------------+
                                |
        +-------------+-------------+-------------+-------------+
        v             v             v             v             v
+-----------+ +-----------+ +-----------+ +-----------+ +-----------+
|   Data    | | Transfor- | | Validat-  | |  Quality  | |  Ranking  |
| Profiling | |  mation   | |   ion     | |  Scoring  | |  System   |
+-----------+ +-----------+ +-----------+ +-----------+ +-----------+
        |             |             |             |             |
        +-------------+-------------+-------------+-------------+
                                |
                                v
+-----------------------------------------------------------------+
|                      Result Aggregator                           |
+-----------------------------------------------------------------+
```

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .

# For web interface only
pip install -r web_app/requirements.txt
```

## Usage

### Web Interface

```bash
# Start the Streamlit web interface
cd web_app
streamlit run app.py
```

The web interface provides:
- CSV file upload with preview
- Pipeline configuration (iterations, ranking toggle, quality threshold)
- Real-time progress indication
- Results visualization with quality metrics
- Download options for transformed data and JSON reports

### Command Line Interface

```bash
# Run full pipeline with configuration
python -m src.orchestrator.cli run --config config/pipeline.yaml

# Profile data only
python -m src.orchestrator.cli profile --input data/input.csv --output profile.json
```

### Programmatic Usage

```python
from src.orchestrator.pipeline_manager import PipelineManager
from src.orchestrator.agent_coordinator import AgentCoordinator
from src.data_profiling.profiler import DataProfilerService
from src.transformation.candidate_generator import CandidateGenerator
from src.transformation.executor import TransformationExecutor
from src.validation.validator import ValidationService
from src.quality_scoring.scorer import QualityScorerService
from src.ranking.ranker import RankingService
from src.ranking.policies import ImprovementPolicy

# Initialize components
profiler = DataProfilerService()
transformation_engine = CandidateGenerator()
executor = TransformationExecutor()
validation_engine = ValidationService()
quality_scorer = QualityScorerService()
ranking_engine = RankingService(policy=ImprovementPolicy())

# Create coordinator
coordinator = AgentCoordinator(
    profiler=profiler,
    transformation_engine=transformation_engine,
    validation_engine=validation_engine,
    quality_scorer=quality_scorer,
    ranking_engine=ranking_engine,
)

# Run pipeline
pipeline = PipelineManager(coordinator=coordinator)
result = pipeline.run(data=df, config=pipeline_config)
```

## Configuration

Configuration is managed through YAML files in the `config/` directory:

- `app_config.yaml` - Application settings
- `pipeline.yaml` - Pipeline execution parameters
- `logging.yaml` - Logging configuration

### Pipeline Configuration Options

- `max_iterations`: Maximum transformation iterations (1-50)
- `enable_ranking`: Toggle ranking system
- `quality_threshold`: Minimum quality score to accept
- `failure_strategy`: How to handle transformation failures (SKIP, RETRY, ABORT, FALLBACK)

## Supported Transformations

The system supports the following transformation types:

- **Cast Type**: Convert columns to different data types
- **Drop Duplicates**: Remove duplicate rows
- **Encode Categorical**: Label encoding and one-hot encoding with cardinality limits
- **Fill Missing**: Fill missing values with constants, mean, median, forward/backward fill
- **Normalize**: Standard (z-score), min-max, and robust (IQR-based) normalization
- **Remove Outliers**: IQR-based or z-score-based outlier handling with mask or remove options

## Quality Metrics

The scoring system evaluates data quality across four dimensions:

- **Completeness**: Ratio of non-null values
- **Validity**: Conformance to expected types and formats
- **Uniqueness**: Ratio of distinct values
- **Consistency**: Absence of contradictory values

These metrics are combined using configurable weights to produce a composite score.

## Testing

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=src --cov-report=html

# Run specific test modules
pytest tests/data_profiling/
pytest tests/transformation/
pytest tests/quality_scoring/
```

## Project Structure

```
src/
├── common/                    # Shared utilities
│   ├── config/               # Configuration loading and schemas
│   ├── exceptions/           # Custom exception hierarchy
│   ├── logging/              # Logging setup
│   ├── types/                 # Pydantic type definitions
│   └── utils/                # Utility functions
├── data_profiling/           # Data profiling module
│   ├── profiler.py           # Main profiler service
│   ├── schema_detector.py    # Type inference
│   ├── statistical_summarizer.py
│   └── missing_value_analyzer.py
├── transformation/           # Transformation engine
│   ├── candidate_generator.py # Transformation suggestions
│   ├── executor.py           # Transformation execution
│   ├── dag.py                # Transformation dependencies
│   └── transformations/      # Individual transformation types
├── validation/               # Validation layer
│   ├── validator.py          # Main validation service
│   ├── schema_validator.py
│   ├── integrity_validator.py
│   └── leakage_detector.py
├── quality_scoring/         # Quality scoring
│   ├── scorer.py             # Main scoring service
│   ├── composite_calculator.py
│   └── metrics/              # Individual quality metrics
├── ranking/                 # Ranking system
│   ├── ranker.py            # Main ranking service
│   └── policies/            # Ranking policies
└── orchestrator/            # Pipeline orchestration
    ├── pipeline_manager.py   # Main pipeline executor
    ├── agent_coordinator.py # Agent coordination
    ├── state_manager.py     # State persistence
    └── failure_recovery.py  # Failure handling

web_app/
├── app.py                  # Streamlit application
└── requirements.txt         # Web app dependencies

tests/
├── conftest.py             # Pytest fixtures
├── data_profiling/         # Profiler tests
├── transformation/         # Transformation tests
├── quality_scoring/        # Scoring tests
├── ranking/               # Ranking tests
├── validation/            # Validation tests
└── fixtures/              # Test data
```

## Development Standards

This project adheres to professional software engineering practices:

- **Type Hints**: Comprehensive Python type annotations throughout
- **Schema Validation**: Pydantic models for configuration and data validation
- **Structured Logging**: Configurable logging with appropriate levels
- **Unit Testing**: pytest with fixtures and parametrized tests
- **Configuration-Driven**: Behavior controlled via YAML configuration
- **Separation of Concerns**: Clear boundaries between modules
- **Idempotent Operations**: Transformations are reproducible
- **Error Handling**: Comprehensive exception hierarchy with recovery strategies

## Performance Considerations

The system includes optimizations for medium to large datasets:

- Adaptive sampling for statistical computations
- Efficient duplicate detection algorithms
- Caching of intermediate results
- Configurable quality metric sampling thresholds

Tested successfully with datasets up to 200,000 rows and 42 columns.

## License

MIT License
