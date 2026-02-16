# Worker 2: Data Profiling Module

**Directory**: `src/data_profiling/`  
**Task File**: `.cline/tasks/task_002_data_profiling.md`

## Context
You are implementing the Data Profiling Module. This module analyzes input data to produce structured profiles with schema, types, and statistics.

## Prerequisites
This module depends on the **Common Module** (`src/common/`). Ensure Common types are available before importing.

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 002 section) - Full specification
- `.cline/tasks/task_002_data_profiling.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/data_profiling/`:
- `interfaces.py` - DataProfiler Protocol
- `schema_detector.py` - Infer column types (numeric, categorical, datetime, text, boolean)
- `missing_value_analyzer.py` - Detect and quantify missing values
- `statistical_summarizer.py` - Compute descriptive statistics
- `profiler.py` - Main DataProfilerService class
- `__init__.py` - Module exports

### Tests
Create in `tests/data_profiling/`:
- `test_profiler.py` - Test the profiler service
- `test_schema_detection.py` - Test type inference

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/data_profiling/` and `tests/data_profiling/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## Dependencies
- pandas>=2.0
- numpy>=1.24

## Input/Output
- **Input**: `pd.DataFrame` from input data file
- **Output**: `DataProfile` with column analysis (types, missing values, statistics)

## Acceptance Criteria
- [ ] Correctly infers data types for numeric, categorical, datetime, text, boolean
- [ ] Accurately detects missing values (null, NaN, empty strings)
- [ ] Computes correct statistical summaries
- [ ] Returns valid DataProfile object matching interface
- [ ] Tests pass
