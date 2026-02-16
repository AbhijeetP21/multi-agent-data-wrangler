# Worker 7: Orchestrator Module

**Directory**: `src/orchestrator/`  
**Task File**: `.cline/tasks/task_007_orchestrator.md`

## Context
You are implementing the Orchestrator Module. This module coordinates all agents, manages pipeline state, handles failures, and provides CLI interface.

## Prerequisites
This module depends on ALL other modules:
- **Common Module** (`src/common/`) - for types and exceptions
- **Data Profiling Module** (`src/data_profiling/`)
- **Transformation Module** (`src/transformation/`)
- **Validation Module** (`src/validation/`)
- **Quality Scoring Module** (`src/quality_scoring/`)
- **Ranking Module** (`src/ranking/`)

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 007 section) - Full specification
- `.cline/tasks/task_007_orchestrator.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/orchestrator/`:

1. **interfaces.py** - Orchestrator Protocol
2. **state_manager.py** - Pipeline state management (persist/load)
3. **agent_coordinator.py** - Coordinate agent activities
4. **pipeline_manager.py** - Pipeline execution orchestration
5. **failure_recovery.py** - Error handling and recovery
6. **cli.py** - Command-line interface
7. **__init__.py** - Module exports

### Tests
Create in `tests/orchestrator/`:
- `test_pipeline.py` - Test pipeline execution

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/orchestrator/` and `tests/orchestrator/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## CLI Interface Requirements
```bash
# Run full pipeline
data-wrangler run --config config/pipeline.yaml

# Profile only
data-wrangler profile --input data/input.csv

# Generate candidates
data-wrangler generate --profile profile.json --output candidates/

# Validate
data-wrangler validate --candidates candidates/ --data data/input.csv

# Score and rank
data-wrangler rank --candidates candidates/ --profile profile.json
```

## Acceptance Criteria
- [ ] Executes full pipeline in correct order
- [ ] Manages pipeline state across executions
- [ ] Handles failures gracefully with recovery
- [ ] Provides CLI interface
- [ ] Tests pass
