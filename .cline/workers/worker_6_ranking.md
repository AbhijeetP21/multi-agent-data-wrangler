# Worker 6: Ranking Module

**Directory**: `src/ranking/`  
**Task File**: `.cline/tasks/task_006_ranking.md`

## Context
You are implementing the Ranking Module. This module scores and ranks transformation candidates based on quality improvement.

## Prerequisites
This module depends on:
- **Common Module** (`src/common/`) - for types and exceptions
- **Validation Module** (`src/validation/`) - for ValidationResult
- **Quality Scoring Module** (`src/quality_scoring/`) - for QualityMetrics

## Your Task
Read the following files before implementing:
- `.cline/interfaces.md` - Interface contracts
- `.cline/delegation_plan.md` (Task 006 section) - Full specification
- `.cline/tasks/task_006_ranking.md` - Detailed task requirements

## What to Implement

### Core Components
Create these files in `src/ranking/`:

1. **interfaces.py** - RankingEngine Protocol
2. **policies/** - Ranking policies:
   - `policies/base.py` - BaseRankingPolicy abstract class
   - `policies/composite_score.py` - CompositeScorePolicy
   - `policies/improvement.py` - ImprovementPolicy
3. **scorer.py** - Score transformation candidates
4. **ranker.py** - Main RankingService class
5. **__init__.py** - Module exports

### Tests
Create in `tests/ranking/`:
- `test_ranker.py` - Test ranking service
- `test_policies.py` - Test ranking policies

## CRITICAL RULES

1. **STAY IN YOUR DIRECTORY**: Only create/modify files inside `src/ranking/` and `tests/ranking/`
2. **NO CROSS-MODULE CHANGES**: Do NOT touch files in other directories
3. **NO HARMFUL COMMANDS**: Do not run commands like `rm -rf /`, `format`, or anything destructive
4. **IMPORT FROM COMMON**: Use types from `src.common.types` - don't redefine types
5. **USE TYPE HINTS**: All code must have proper Python type hints
6. **IDEMPOTENT**: Code must be safely re-runnable

## Input/Output
- **Input**: List of `TransformationCandidate` (transformation + validation result + quality metrics)
- **Output**: List of `RankedTransformation` with rank, score, and reasoning

## Acceptance Criteria
- [ ] Ranks transformations by quality improvement
- [ ] Supports pluggable ranking policies
- [ ] Provides reasoning for rankings
- [ ] Returns valid RankedTransformation list
- [ ] Tests pass
