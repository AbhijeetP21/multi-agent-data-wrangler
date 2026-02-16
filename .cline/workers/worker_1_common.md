# Worker 1: Common Infrastructure Module

**Directory**: `src/common/`  
**Task File**: `.cline/tasks/task_001_common.md`

## Context
You are implementing the Common Infrastructure Module - the foundation that ALL other modules depend on. This module MUST complete first.

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 001 section) - Full specification
- `.cline/tasks/task_001_common.md` - Detailed task requirements

## What to Implement

### 1. Types (src/common/types/)
Create Pydantic schemas in separate files:
- `types/data_profile.py` - DataProfile, ColumnProfile
- `types/transformation.py` - Transformation, TransformationType, TransformationResult
- `types/validation.py` - ValidationResult, ValidationIssue
- `types/quality.py` - QualityMetrics, QualityDelta
- `types/ranking.py` - TransformationCandidate, RankedTransformation
- `types/pipeline.py` - PipelineState, PipelineStep, PipelineConfig
- `types/__init__.py` - Re-export all types

### 2. Configuration (src/common/config/)
- `config/loader.py` - YAML config loading
- `config/schemas.py` - Config Pydantic models

### 3. Logging (src/common/logging/)
- `logging/setup.py` - Structured logging with structlog

### 4. Exceptions (src/common/exceptions/)
- `exceptions/base.py` - DataWranglerError base class
- `exceptions/profiling.py` - ProfilingError
- `exceptions/transformation.py` - TransformationError
- `exceptions/validation.py` - ValidationError
- `exceptions/scoring.py` - ScoringError
- `exceptions/ranking.py` - RankingError
- `exceptions/orchestration.py` - OrchestrationError

### 5. Utils (src/common/utils/)
- `utils/deterministic.py` - Seed management, reproducibility

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/common/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **FOLLOW INTERFACES**: All types must match `.cline/interfaces.md` exactly
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## Dependencies
- pydantic>=2.0
- pyyaml>=6.0
- structlog>=24.0

## Acceptance Criteria
- [ ] All Pydantic models serialize/deserialize correctly
- [ ] Configuration loads from YAML with validation
- [ ] Logging produces structured JSON output
- [ ] All custom exceptions inherit from DataWranglerError
- [ ] Tests can be run after implementation
