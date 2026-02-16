# Worker 3: Quality Scoring Module

**Directory**: `src/quality_scoring/`  
**Task File**: `.cline/tasks/task_005_quality_scoring.md`

## Context
You are implementing the Quality Scoring Module. This module computes quality metrics (completeness, consistency, validity, uniqueness) and composite scores for data transformations.

## Prerequisites
This module depends on the **Common Module** (`src/common/`). Ensure Common types are available.

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 005 section) - Full specification
- `.cline/tasks/task_005_quality_scoring.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/quality_scoring/`:

1. **interfaces.py** - QualityScorer Protocol
2. **metrics/** - Individual metric calculators:
   - `metrics/base.py` - BaseMetric abstract class
   - `metrics/completeness.py` - CompletenessMetric
   - `metrics/consistency.py` - ConsistencyMetric
   - `metrics/validity.py` - ValidityMetric
   - `metrics/uniqueness.py` - UniquenessMetric
3. **composite_calculator.py** - Weighted score calculation
4. **comparator.py** - Pre/post comparison
5. **scorer.py** - Main QualityScorerService class
6. **__init__.py** - Module exports

### Tests
Create in `tests/quality_scoring/`:
- `test_metrics.py` - Test individual metrics
- `test_composite.py` - Test composite scoring

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/quality_scoring/` and `tests/quality_scoring/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## Dependencies
- pandas>=2.0
- numpy>=1.24

## Input/Output
- **Input**: `pd.DataFrame`, optionally `DataProfile`
- **Output**: `QualityMetrics` (0-1 scores for each dimension), `QualityDelta` for comparisons

## Acceptance Criteria
- [ ] Correctly calculates completeness (ratio of non-null values)
- [ ] Correctly calculates consistency (data type consistency)
- [ ] Correctly calculates validity (value range validity)
- [ ] Correctly calculates uniqueness (ratio of unique values)
- [ ] Weighted composite score is accurate
- [ ] Comparison correctly computes deltas
- [ ] Returns valid QualityMetrics and QualityDelta objects
- [ ] Tests pass
