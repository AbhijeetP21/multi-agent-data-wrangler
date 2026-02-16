# Worker 5: Validation Module

**Directory**: `src/validation/`  
**Task File**: `.cline/tasks/task_004_validation.md`

## Context
You are implementing the Validation Module. This module ensures transformations preserve data integrity and detects information leakage.

## Prerequisites
This module depends on:
- **Common Module** (`src/common/`) - for types and exceptions
- **Data Profiling Module** (`src/data_profiling/`) - for DataProfile
- **Transformation Module** (`src/transformation/`) - for Transformation types

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 004 section) - Full specification
- `.cline/tasks/task_004_validation.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/validation/`:

1. **interfaces.py** - ValidationEngine Protocol
2. **integrity_validator.py** - Data integrity checks (row count tolerance, null preservation)
3. **leakage_detector.py** - Information leakage detection
4. **schema_validator.py** - Schema compatibility checking
5. **validator.py** - Main ValidationService class
6. **__init__.py** - Module exports

### Tests
Create in `tests/validation/`:
- `test_integrity.py` - Test integrity validation
- `test_leakage.py` - Test leakage detection

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/validation/` and `tests/validation/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## Dependencies
- pandas>=2.0

## Input/Output
- **Input**: Original `pd.DataFrame`, Transformed `pd.DataFrame`, `DataProfile`
- **Output**: `ValidationResult` with issues and schema compatibility

## Acceptance Criteria
- [ ] Detects data integrity issues (excessive row loss, type changes)
- [ ] Detects information leakage
- [ ] Validates schema compatibility
- [ ] Returns valid ValidationResult matching interface
- [ ] Tests pass
