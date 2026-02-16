# Delegation Plan - Multi-Agent Data Wrangler

## Phase 1: Common Infrastructure (Priority: HIGH)

**Worker**: common_module_worker  
**Directory**: `src/common/`

### Deliverables:
1. **types/** - Pydantic schemas
   - `__init__.py`
   - `data_profile.py` - DataProfile, ColumnProfile
   - `transformation.py` - Transformation, TransformationType, TransformationResult
   - `validation.py` - ValidationResult, ValidationIssue
   - `quality.py` - QualityMetrics, QualityDelta
   - `ranking.py` - TransformationCandidate, RankedTransformation
   - `pipeline.py` - PipelineState, PipelineStep, PipelineConfig

2. **config/** - Configuration management
   - `__init__.py`
   - `loader.py` - YAML config loading
   - `schemas.py` - Config Pydantic models

3. **logging/** - Structured logging
   - `__init__.py`
   - `setup.py` - Log configuration

4. **exceptions/** - Custom exceptions
   - `__init__.py`
   - `base.py` - DataWranglerError base class
   - `profiling.py` - ProfilingError
   - `transformation.py` - TransformationError
   - `validation.py` - ValidationError
   - `scoring.py` - ScoringError
   - `ranking.py` - RankingError
   - `orchestration.py` - OrchestrationError

5. **utils/** - Shared utilities
   - `__init__.py`
   - `deterministic.py` - Seed management, reproducibility helpers

### Tests:
- `tests/common/test_types.py`
- `tests/common/test_config.py`
- `tests/common/test_logging.py`

---

## Phase 2: Data Profiling Module (Priority: HIGH)

**Worker**: data_profiling_worker  
**Directory**: `src/data_profiling/`

### Deliverables:
1. **interfaces.py** - DataProfiler Protocol
2. **schema_detector.py** - Type inference
3. **missing_value_analyzer.py** - Missing value detection
4. **statistical_summarizer.py** - Statistics computation
5. **profiler.py** - Main service class
6. **__init__.py** - Module exports

### Input:
- `pd.DataFrame` from input data

### Output:
- `DataProfile` with column analysis
- Per-column `ColumnProfile`

### Tests:
- `tests/data_profiling/test_profiler.py`
- `tests/data_profiling/test_schema_detection.py`

---

## Phase 3: Transformation Module (Priority: HIGH)

**Worker**: transformation_worker  
**Directory**: `src/transformation/`

### Deliverables:
1. **interfaces.py** - TransformationEngine Protocol
2. **candidate_generator.py** - Generate transformation candidates
3. **transformations/** - Individual transformation implementations
   - `__init__.py`
   - `base.py` - BaseTransformation abstract class
   - `fill_missing.py` - FillMissingTransformation
   - `normalize.py` - NormalizeTransformation
   - `encode_categorical.py` - EncodeCategoricalTransformation
   - `remove_outliers.py` - RemoveOutliersTransformation
   - `drop_duplicates.py` - DropDuplicatesTransformation
   - `cast_type.py` - CastTypeTransformation
4. **executor.py** - Execute transformations
5. **dag.py** - Transformation DAG builder
6. **reversibility.py** - Reversibility checking
7. **__init__.py** - Module exports

### Input:
- `DataProfile` from profiling

### Output:
- List of `Transformation` candidates
- Executed transformations with results

### Tests:
- `tests/transformation/test_candidate_generator.py`
- `tests/transformation/test_executor.py`

---

## Phase 4: Validation Module (Priority: MEDIUM)

**Worker**: validation_worker  
**Directory**: `src/validation/`

### Deliverables:
1. **interfaces.py** - ValidationEngine Protocol
2. **integrity_validator.py** - Data integrity checks
3. **leakage_detector.py** - Information leakage detection
4. **schema_validator.py** - Schema compatibility
5. **validator.py** - Main validation service
6. **__init__.py** - Module exports

### Input:
- Original `pd.DataFrame`
- Transformed `pd.DataFrame`
- `DataProfile`

### Output:
- `ValidationResult` with issues

### Tests:
- `tests/validation/test_integrity.py`
- `tests/validation/test_leakage.py`

---

## Phase 5: Quality Scoring Module (Priority: MEDIUM)

**Worker**: quality_scoring_worker  
**Directory**: `src/quality_scoring/`

### Deliverables:
1. **interfaces.py** - QualityScorer Protocol
2. **metrics/** - Individual metric calculators
   - `__init__.py`
   - `base.py` - BaseMetric
   - `completeness.py` - CompletenessMetric
   - `consistency.py` - ConsistencyMetric
   - `validity.py` - ValidityMetric
   - `uniqueness.py` - UniquenessMetric
3. **composite_calculator.py** - Weighted score calculation
4. **comparator.py** - Pre/post comparison
5. **scorer.py** - Main scoring service
6. **__init__.py** - Module exports

### Input:
- `pd.DataFrame`
- `DataProfile`

### Output:
- `QualityMetrics`
- `QualityDelta` (for comparisons)

### Tests:
- `tests/quality_scoring/test_metrics.py`
- `tests/quality_scoring/test_composite.py`

---

## Phase 6: Ranking Module (Priority: MEDIUM)

**Worker**: ranking_worker  
**Directory**: `src/ranking/`

### Deliverables:
1. **interfaces.py** - RankingEngine Protocol
2. **policies/** - Ranking policies
   - `__init__.py`
   - `base.py` - BaseRankingPolicy
   - `composite_score.py` - CompositeScorePolicy
   - `improvement.py` - ImprovementPolicy
3. **scorer.py** - Score transformation candidates
4. **ranker.py** - Main ranking service
5. **__init__.py** - Module exports

### Input:
- List of `TransformationCandidate`
- Ranking policy

### Output:
- `RankedTransformation` list

### Tests:
- `tests/ranking/test_ranker.py`
- `tests/ranking/test_policies.py`

---

## Phase 7: Orchestrator Module (Priority: HIGH)

**Worker**: orchestrator_worker  
**Directory**: `src/orchestrator/`

### Deliverables:
1. **interfaces.py** - Orchestrator Protocol
2. **state_manager.py** - Pipeline state management
3. **agent_coordinator.py** - Coordinate agents
4. **pipeline_manager.py** - Pipeline execution
5. **failure_recovery.py** - Error handling
6. **cli.py** - Command-line interface
7. **__init__.py` - Module exports

### Input:
- Pipeline configuration (YAML)

### Output:
- `PipelineResult` with transformed data and report

### Tests:
- `tests/orchestrator/test_pipeline.py`
- `tests/integration/test_end_to_end.py`

---

## Phase 8: Configuration & Testing (Priority: MEDIUM)

**Worker**: setup_worker  
**Directories**: `config/`, `tests/`

### Deliverables:
1. **config/pipeline.yaml** - Default pipeline configuration
2. **config/logging.yaml** - Logging configuration
3. **tests/conftest.py** - Pytest fixtures
4. **tests/fixtures/** - Test data
   - `sample_data.csv`
   - `expected_output.csv`
5. **pyproject.toml** - Project configuration
6. **requirements.txt** - Dependencies

---

## Execution Order

```
Phase 1 (Common) ────────────────────────────────────────┐
    │                                                      │
    ▼                                                      │
Phase 2 (Profiling)                                        │
    │                                                      │
    ▼                                                      │
Phase 3 (Transformation) ────┐                             │
    │                         │                            │
    ▼                         │                            │
Phase 4 (Validation) ◄──────┘                             │
    │                                                      │
    ▼                                                      │
Phase 5 (Quality Scoring)                                  │
    │                                                      │
    ▼                                                      │
Phase 6 (Ranking) ─────────────────────────────────────────┘
    │
    ▼
Phase 7 (Orchestrator)
    │
    ▼
Phase 8 (Config & Tests)
```

---

## Key Principles

1. **No implementation in this plan file** - Only architecture decisions
2. **Worker isolation** - Each worker operates in their assigned directory only
3. **Interface-first** - Define protocols before implementations
4. **Test-driven** - Tests should exist alongside implementations
5. **Idempotent** - All operations must be safely re-runnable
