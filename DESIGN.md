# Multi-Agent Data Wrangler - System Architecture

## 1. Overview

**Project**: Multi-Agent Data Wrangler  
**Purpose**: Production-grade, evaluation-driven data wrangling system using multi-agent architecture  
**Target Users**: ML Engineers, Data Scientists, Data Engineers  
**Python Version**: 3.11+

## 2. Architecture Principles

1. **Modular Scope Discipline**: Each module has exactly one responsibility
2. **Interface Contracts**: All inter-module communication via typed interfaces
3. **Deterministic Transformations**: Same input → same output, always
4. **Evaluation-Driven**: Quality metrics drive transformation ranking
5. **Config-Driven**: YAML-based experimentation
6. **Idempotent Components**: Safe to re-run without side effects

## 3. System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI Entry Point                          │
│                      (src/orchestrator/cli.py)                  │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Orchestrator Agent                          │
│                  (src/orchestrator/core.py)                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐              │
│  │State Manager │ │ Agent Coord. │ │   Logger     │              │
│  └──────────────┘ └──────────────┘ └──────────────┘              │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────┬───────────┼───────────┬───────────┐
        ▼           ▼           ▼           ▼           ▼
┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
│   Data    │ │Transfor-  │ │ Validat-  │ │  Quality  │ │  Ranking  │
│ Profiling │ │  mation   │ │   ion     │ │  Scoring  │ │  System   │
│  Agent    │ │  Agent    │ │  Agent    │ │   Agent   │ │   Agent   │
└───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘
```

## 4. Module Boundaries

### 4.1 Data Profiling Module (`src/data_profiling/`)

**Responsibility**: Analyze input data and produce structured profile

**Public Interface**:
```python
class DataProfiler(Protocol):
    def profile(self, data: pd.DataFrame) -> DataProfile: ...
```

**Artifacts**:
- `DataProfile`: Schema, missing values, type inference, statistics
- `ColumnProfile`: Per-column analysis
- `DataQualityReport`: Initial quality assessment

**Key Classes**:
- `SchemaDetector`: Infer column types
- `MissingValueAnalyzer`: Detect and quantify missing values
- `StatisticalSummarizer`: Compute descriptive statistics
- `DataProfilerService`: Main orchestrator for profiling

### 4.2 Transformation Module (`src/transformation/`)

**Responsibility**: Generate and execute deterministic data transformations

**Public Interface**:
```python
class TransformationEngine(Protocol):
    def generate_candidates(self, profile: DataProfile) -> list[Transformation]: ...
    def execute(self, data: pd.DataFrame, transformation: Transformation) -> pd.DataFrame: ...
    def reverse(self, data: pd.DataFrame, transformation: Transformation) -> pd.DataFrame: ...
```

**Artifacts**:
- `Transformation`: Type-safe transformation definition
- `TransformationDAG`: Directed acyclic graph of transformations
- `TransformationResult`: Execution result with metadata

**Key Classes**:
- `CandidateGenerator`: Generate transformation candidates
- `TransformationExecutor`: Execute transformations deterministically
- `DAGBuilder`: Build and validate transformation DAG
- `ReversibilityChecker`: Verify transformations can be reversed

### 4.3 Validation Module (`src/validation/`)

**Responsibility**: Ensure transformations preserve data integrity

**Public Interface**:
```python
class ValidationEngine(Protocol):
    def validate(self, original: pd.DataFrame, transformed: pd.DataFrame, 
                  profile: DataProfile) -> ValidationResult: ...
    def check_leakage(self, original: pd.DataFrame, transformed: pd.DataFrame) -> bool: ...
```

**Artifacts**:
- `ValidationResult`: Pass/fail with detailed issues
- `IntegrityCheck`: Schema, range, uniqueness validation
- `LeakageReport`: Information leakage detection

**Key Classes**:
- `IntegrityValidator`: Check data integrity
- `LeakageDetector`: Detect information leakage
- `SchemaValidator`: Verify schema compatibility

### 4.4 Quality Scoring Module (`src/quality_scoring/`)

**Responsibility**: Compute quality metrics and composite scores

**Public Interface**:
```python
class QualityScorer(Protocol):
    def score(self, data: pd.DataFrame, profile: DataProfile) -> QualityMetrics: ...
    def compare(self, before: QualityMetrics, after: QualityMetrics) -> QualityDelta: ...
```

**Artifacts**:
- `QualityMetrics`: Individual quality metric values
- `QualityDelta`: Before/after comparison
- `CompositeScore`: Weighted quality score

**Key Classes**:
- `MetricCalculator`: Calculate individual metrics
- `CompositeScoreCalculator`: Compute weighted scores
- `QualityComparator`: Compare pre/post transformation quality

### 4.5 Ranking Module (`src/ranking/`)

**Responsibility**: Rank transformations by quality improvement

**Public Interface**:
```python
class RankingEngine(Protocol):
    def rank(self, candidates: list[TransformationCandidate]) -> list[RankedTransformation]: ...
    def set_policy(self, policy: RankingPolicy) -> None: ...
```

**Artifacts**:
- `RankedTransformation`: Transformation with score and rank
- `RankingReport`: Full ranking analysis

**Key Classes**:
- `ScoreRanker`: Rank by composite score
- `PolicyManager`: Manage ranking policies
- `RankingReporter`: Generate ranking reports

### 4.6 Orchestrator Module (`src/orchestrator/`)

**Responsibility**: Coordinate agents, manage state, handle failures

**Public Interface**:
```python
class Orchestrator(Protocol):
    def run(self, config: PipelineConfig) -> PipelineResult: ...
    def recover(self, state: PipelineState) -> None: ...
```

**Artifacts**:
- `PipelineState`: Current pipeline execution state
- `PipelineResult`: Final pipeline output
- `ExecutionLog`: Detailed execution trace

**Key Classes**:
- `PipelineManager`: Manage pipeline execution
- `AgentCoordinator`: Coordinate agent activities
- `StateManager`: Manage pipeline state
- `FailureRecovery`: Handle and recover from failures

### 4.7 Common Module (`src/common/`)

**Shared utilities**:
- `types/`: Pydantic schemas for all data types
- `config/`: Configuration management
- `logging/`: Structured logging setup
- `exceptions/`: Custom exceptions

## 5. Data Flow

```
Input Data → Profiling → Profile + Metadata
                            │
                            ▼
                     Generate Candidates
                            │
                            ▼
                     Validate Candidates
                            │
                            ▼
                     Execute Transformations
                            │
                            ▼
                     Score Results
                            │
                            ▼
                     Rank & Select Best
                            │
                            ▼
                     Output: Transformed Data + Report
```

## 6. Configuration Schema (YAML)

```yaml
pipeline:
  name: "data_wrangler"
  version: "1.0.0"

data:
  input_path: "data/input.csv"
  output_path: "data/output.csv"

profiling:
  detect_types: true
  compute_statistics: true
  missing_threshold: 0.5

transformation:
  max_candidates: 100
  allowed_types:
    - fill_missing
    - normalize
    - encode_categorical
    - remove_outliers

validation:
  check_leakage: true
  preserve_schema: true
  row_count_tolerance: 0.1

quality:
  metrics:
    - completeness
    - consistency
    - validity
  weights:
    completeness: 0.3
    consistency: 0.3
    validity: 0.4

ranking:
  policy: "composite_score"
  top_k: 10
```

## 7. Logging & Observability

- Structured JSON logging via `structlog`
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Context: correlation_id, agent_name, step, duration_ms
- Output: console + file rotation

## 8. Testing Strategy

- **Unit Tests**: Each module has dedicated unit tests
- **Integration Tests**: Cross-module interaction tests
- **Fixtures**: Shared test data and fixtures
- **Coverage Target**: 80%+ for core modules

## 9. CLI Interface

```bash
# Run full pipeline
data-wrangler run --config config/pipeline.yaml

# Profile only
data-wrangler profile --input data/input.csv

# Generate candidates
data-wrangler generate --profile profile.json --output candidates/

# Validate
data-wrangler validate --candidates candidates/ --data data/input.csv

# Score and rank
data-wrangler rank --candidates candidates/ --profile profile.json

# Run specific agent
data-wrangler agent --name profiling --input data/input.csv
```

## 10. Extension Points

1. **Custom Transformations**: Implement `Transformation` protocol
2. **Custom Metrics**: Implement `QualityMetric` protocol
3. **Custom Ranking Policies**: Implement `RankingPolicy` protocol
4. **Custom Validators**: Implement `Validator` protocol

## 11. Error Handling

- All errors inherit from `DataWranglerError`
- Pipeline failures are logged with full context
- State is persisted for recovery
- Graceful degradation where possible
