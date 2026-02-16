# Worker 4: Transformation Module

**Directory**: `src/transformation/`  
**Task File**: `.cline/tasks/task_003_transformation.md`

## Context
You are implementing the Transformation Module. This module generates and executes deterministic data transformations based on data profiles.

## Prerequisites
This module depends on:
- **Common Module** (`src/common/`) - for types and exceptions
- **Data Profiling Module** (`src/data_profiling/`) - for DataProfile input

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 003 section) - Full specification
- `.cline/tasks/task_003_transformation.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/transformation/`:

1. **interfaces.py** - TransformationEngine Protocol
2. **candidate_generator.py** - Generate transformation candidates from profile
3. **transformations/** - Individual transformation implementations:
   - `transformations/base.py` - BaseTransformation abstract class
   - `transformations/fill_missing.py` - FillMissingTransformation
   - `transformations/normalize.py` - NormalizeTransformation
   - `transformations/encode_categorical.py` - EncodeCategoricalTransformation
   - `transformations/remove_outliers.py` - RemoveOutliersTransformation
   - `transformations/drop_duplicates.py` - DropDuplicatesTransformation
   - `transformations/cast_type.py` - CastTypeTransformation
4. **executor.py** - Execute transformations deterministically
5. **dag.py** - Transformation DAG builder
6. **reversibility.py** - Reversibility checking
7. **__init__.py** - Module exports

### Tests
Create in `tests/transformation/`:
- `test_candidate_generator.py` - Test candidate generation
- `test_executor.py` - Test transformation execution

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/transformation/` and `tests/transformation/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable - same input = same output

## Dependencies
- pandas>=2.0
- numpy>=1.24
- scikit-learn>=1.3 (for encoding, scaling)

## Input/Output
- **Input**: `DataProfile` from profiling module
- **Output**: List of `Transformation` candidates, executed transformations with `TransformationResult`

## Acceptance Criteria
- [ ] Generates relevant candidates based on profile
- [ ] Executes transformations deterministically (same input = same output)
- [ ] Supports reversibility where applicable
- [ ] All transformations follow Transformation protocol
- [ ] Tests pass
