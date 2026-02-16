# Task: Common Infrastructure Module

**Worker**: common_module_worker  
**Directory**: `src/common/`  
**Priority**: HIGH

## Objective

Implement the common infrastructure module that provides shared types, configuration, logging, and exceptions for all other modules.

## Deliverables

### 1. Types (src/common/types/)

Create Pydantic schemas for all data types:

- `types/__init__.py` - Re-export all types
- `types/data_profile.py` - DataProfile, ColumnProfile
- `types/transformation.py` - Transformation, TransformationType, TransformationResult
- `types/validation.py` - ValidationResult, ValidationIssue
- `types/quality.py` - QualityMetrics, QualityDelta
- `types/ranking.py` - TransformationCandidate, RankedTransformation
- `types/pipeline.py` - PipelineState, PipelineStep, PipelineConfig

### 2. Configuration (src/common/config/)

- `config/__init__.py`
- `config/loader.py` - YAML config loading with validation
- `config/schemas.py` - Config Pydantic models

### 3. Logging (src/common/logging/)

- `logging/__init__.py`
- `logging/setup.py` - Structured logging setup using structlog

### 4. Exceptions (src/common/exceptions/)

- `exceptions/__init__.py`
- `exceptions/base.py` - DataWranglerError base class
- `exceptions/profiling.py` - ProfilingError
- `exceptions/transformation.py` - TransformationError
- `exceptions/validation.py` - ValidationError
- `exceptions/scoring.py` - ScoringError
- `exceptions/ranking.py` - RankingError
- `exceptions/orchestration.py` - OrchestrationError

### 5. Utils (src/common/utils/)

- `utils/__init__.py`
- `utils/deterministic.py` - Seed management, reproducibility helpers

### 6. Tests

- `tests/common/test_types.py`
- `tests/common/test_config.py`
- `tests/common/test_logging.py`

## Interface Requirements

All types must follow the interfaces defined in `.cline/interfaces.md`.

## Dependencies

- pydantic>=2.0
- pyyaml>=6.0
- structlog>=24.0

## Acceptance Criteria

1. All Pydantic models serialize/deserialize correctly
2. Configuration loads from YAML with validation
3. Logging produces structured JSON output
4. All custom exceptions inherit from DataWranglerError
5. Deterministic utilities ensure reproducibility
6. Tests pass for all components
