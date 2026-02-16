# Task: Quality Scoring Module

**Worker**: quality_scoring_worker  
**Directory**: `src/quality_scoring/`  
**Priority**: MEDIUM

## Objective

Implement the quality scoring module that computes quality metrics and composite scores.

## Deliverables

### Core Components

- `interfaces.py` - QualityScorer Protocol
- `metrics/`
  - `__init__.py`
  - `base.py` - BaseMetric abstract class
  - `completeness.py` - CompletenessMetric
  - `consistency.py` - ConsistencyMetric
  - `validity.py` - ValidityMetric
  - `uniqueness.py` - UniquenessMetric
- `composite_calculator.py` - Weighted score calculation
- `comparator.py` - Pre/post comparison
- `scorer.py` - Main QualityScorerService class
- `__init__.py` - Module exports

### Tests

- `tests/quality_scoring/test_metrics.py`
- `tests/quality_scoring/test_composite.py`

## Interface Requirements

```python
class QualityScorer(Protocol):
    def score(self, data: pd.DataFrame, profile: DataProfile) -> QualityMetrics: ...
    def compare(self, before: QualityMetrics, after: QualityMetrics) -> QualityDelta: ...
```

## Input

- `pd.DataFrame`
- `DataProfile` (optional, for additional context)

## Output

- `QualityMetrics` with scores (0-1):
  - completeness: ratio of non-null values
  - consistency: data type consistency
  - validity: value range validity
  - uniqueness: ratio of unique values
  - overall: weighted composite

- `QualityDelta` for comparisons:
  - before/after metrics
  - improvement values
  - composite_delta

## Acceptance Criteria

1. Correctly calculates all four quality metrics
2. Weighted composite score is accurate
3. Comparison correctly computes deltas
4. Returns valid QualityMetrics and QualityDelta
5. Tests pass
