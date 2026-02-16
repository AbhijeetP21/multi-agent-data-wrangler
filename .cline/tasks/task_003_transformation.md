# Task: Transformation Module

**Worker**: transformation_worker  
**Directory**: `src/transformation/`  
**Priority**: HIGH

## Objective

Implement the transformation module that generates and executes deterministic data transformations.

## Deliverables

### Core Components

- `interfaces.py` - TransformationEngine Protocol
- `candidate_generator.py` - Generate transformation candidates from profile
- `transformations/`
  - `__init__.py`
  - `base.py` - BaseTransformation abstract class
  - `fill_missing.py` - FillMissingTransformation
  - `normalize.py` - NormalizeTransformation
  - `encode_categorical.py` - EncodeCategoricalTransformation
  - `remove_outliers.py` - RemoveOutliersTransformation
  - `drop_duplicates.py` - DropDuplicatesTransformation
  - `cast_type.py` - CastTypeTransformation
- `executor.py` - Execute transformations deterministically
- `dag.py` - Transformation DAG builder
- `reversibility.py` - Reversibility checking
- `__init__.py` - Module exports

### Tests

- `tests/transformation/test_candidate_generator.py`
- `tests/transformation/test_executor.py`

## Interface Requirements

```python
class TransformationEngine(Protocol):
    def generate_candidates(self, profile: DataProfile) -> list[Transformation]: ...
    def execute(self, data: pd.DataFrame, transformation: Transformation) -> pd.DataFrame: ...
    def reverse(self, data: pd.DataFrame, transformation: Transformation) -> pd.DataFrame: ...
```

## Input

- `DataProfile` from profiling module

## Output

- List of `Transformation` candidates
- Executed transformations with results (TransformationResult)

## Dependencies

- pandas>=2.0
- numpy>=1.24
- scikit-learn>=1.3 (for encoding, scaling)

## Acceptance Criteria

1. Generates relevant candidates based on profile
2. Executes transformations deterministically (same input = same output)
3. Supports reversibility where applicable
4. All transformations follow Transformation protocol
5. Tests pass
