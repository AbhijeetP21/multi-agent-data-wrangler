# Task: Validation Module

**Worker**: validation_worker  
**Directory**: `src/validation/`  
**Priority**: MEDIUM

## Objective

Implement the validation module that ensures transformations preserve data integrity and detects leakage.

## Deliverables

### Core Components

- `interfaces.py` - ValidationEngine Protocol
- `integrity_validator.py` - Data integrity checks (row count, null preservation)
- `leakage_detector.py` - Information leakage detection
- `schema_validator.py` - Schema compatibility checking
- `validator.py` - Main ValidationService class
- `__init__.py` - Module exports

### Tests

- `tests/validation/test_integrity.py`
- `tests/validation/test_leakage.py`

## Interface Requirements

```python
class ValidationEngine(Protocol):
    def validate(self, original: pd.DataFrame, transformed: pd.DataFrame, 
                 profile: DataProfile) -> ValidationResult: ...
    def check_leakage(self, original: pd.DataFrame, transformed: pd.DataFrame) -> bool: ...
```

## Input

- Original `pd.DataFrame`
- Transformed `pd.DataFrame`
- `DataProfile` (original profile)

## Output

- `ValidationResult` with:
  - Pass/fail status
  - List of issues (severity, code, message, column)
  - Schema compatibility flag

## Acceptance Criteria

1. Detects data integrity issues (excessive row loss, type changes)
2. Detects information leakage
3. Validates schema compatibility
4. Returns valid ValidationResult
5. Tests pass
