# Task: Ranking Module

**Worker**: ranking_worker  
**Directory**: `src/ranking/`  
**Priority**: MEDIUM

## Objective

Implement the ranking module that scores and ranks transformation candidates.

## Deliverables

### Core Components

- `interfaces.py` - RankingEngine Protocol
- `policies/`
  - `__init__.py`
  - `base.py` - BaseRankingPolicy abstract class
  - `composite_score.py` - CompositeScorePolicy
  - `improvement.py` - ImprovementPolicy
- `scorer.py` - Score transformation candidates
- `ranker.py` - Main RankingService class
- `__init__.py` - Module exports

### Tests

- `tests/ranking/test_ranker.py`
- `tests/ranking/test_policies.py`

## Interface Requirements

```python
class RankingEngine(Protocol):
    def rank(self, candidates: list[TransformationCandidate]) -> list[RankedTransformation]: ...
    def set_policy(self, policy: RankingPolicy) -> None: ...
```

## Input

- List of `TransformationCandidate` (transformation + validation result + quality metrics)

## Output

- List of `RankedTransformation`:
  - rank: integer position
  - candidate: original candidate
  - composite_score: float
  - reasoning: str explanation

## Acceptance Criteria

1. Ranks transformations by quality improvement
2. Supports pluggable ranking policies
3. Provides reasoning for rankings
4. Returns valid RankedTransformation list
5. Tests pass
