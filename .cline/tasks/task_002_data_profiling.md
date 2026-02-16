# Task: Data Profiling Module

**Worker**: data_profiling_worker  
**Directory**: `src/data_profiling/`  
**Priority**: HIGH

## Objective

Implement the data profiling module that analyzes input data and produces structured profiles.

## Deliverables

### Core Components

- `interfaces.py` - DataProfiler Protocol defining the public interface
- `schema_detector.py` - Infer column types (numeric, categorical, datetime, text, boolean)
- `missing_value_analyzer.py` - Detect and quantify missing values
- `statistical_summarizer.py` - Compute descriptive statistics
- `profiler.py` - Main DataProfilerService class
- `__init__.py` - Module exports

### Tests

- `tests/data_profiling/test_profiler.py`
- `tests/data_profiling/test_schema_detection.py`

## Interface Requirements

```python
class DataProfiler(Protocol):
    def profile(self, data: pd.DataFrame) -> DataProfile: ...
```

## Input

- `pd.DataFrame` from input data file

## Output

- `DataProfile` with:
  - Schema information per column
  - Missing value counts and percentages
  - Type inference results
  - Statistical summaries (min, max, mean, std for numeric)

## Dependencies

- pandas>=2.0
- numpy>=1.24

## Acceptance Criteria

1. Correctly infers data types for numeric, categorical, datetime, text, boolean columns
2. Accurately detects missing values (null, NaN, empty strings)
3. Computes correct statistical summaries
4. Returns valid DataProfile object
5. Tests pass
