# Interface Contracts

## Shared Types (src/common/types/)

### DataProfile
```python
from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ColumnProfile(BaseModel):
    name: str
    dtype: str
    null_count: int
    null_percentage: float
    unique_count: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    inferred_type: str  # 'numeric', 'categorical', 'datetime', 'text', 'boolean'

class DataProfile(BaseModel):
    timestamp: datetime
    row_count: int
    column_count: int
    columns: dict[str, ColumnProfile]
    overall_missing_percentage: float
    duplicate_rows: int
```

### Transformation Types
```python
from pydantic import BaseModel
from typing import Optional, Literal
from enum import Enum

class TransformationType(str, Enum):
    FILL_MISSING = "fill_missing"
    NORMALIZE = "normalize"
    ENCODE_CATEGORICAL = "encode_categorical"
    REMOVE_OUTLIERS = "remove_outliers"
    DROP_DUPLICATES = "drop_duplicates"
    CAST_TYPE = "cast_type"

class Transformation(BaseModel):
    id: str
    type: TransformationType
    target_columns: list[str]
    params: dict  # Type-safe based on type
    reversible: bool
    description: str

class TransformationResult(BaseModel):
    transformation: Transformation
    success: bool
    output_data: pd.DataFrame
    error_message: Optional[str] = None
    execution_time_ms: float
```

### Validation Types
```python
from pydantic import BaseModel
from typing import Optional

class ValidationIssue(BaseModel):
    severity: Literal["error", "warning", "info"]
    code: str
    message: str
    column: Optional[str] = None

class ValidationResult(BaseModel):
    passed: bool
    issues: list[ValidationIssue]
    original_row_count: int
    transformed_row_count: int
    schema_compatible: bool
```

### Quality Scoring Types
```python
from pydantic import BaseModel

class QualityMetrics(BaseModel):
    completeness: float  # 0-1: ratio of non-null values
    consistency: float  # 0-1: data type consistency
    validity: float     # 0-1: value range validity
    uniqueness: float   # 0-1: ratio of unique values
    overall: float      # weighted composite

class QualityDelta(BaseModel):
    before: QualityMetrics
    after: QualityMetrics
    improvement: QualityMetrics
    composite_delta: float
```

### Ranking Types
```python
from pydantic import BaseModel

class TransformationCandidate(BaseModel):
    transformation: Transformation
    validation_result: ValidationResult
    quality_before: QualityMetrics
    quality_after: QualityMetrics
    quality_delta: QualityDelta

class RankedTransformation(BaseModel):
    rank: int
    candidate: TransformationCandidate
    composite_score: float
    reasoning: str
```

### Pipeline Types
```python
from pydantic import BaseModel
from typing import Optional, Literal

class PipelineStep(str, Enum):
    PROFILING = "profiling"
    GENERATION = "generation"
    VALIDATION = "validation"
    EXECUTION = "execution"
    SCORING = "scoring"
    RANKING = "ranking"

class PipelineState(BaseModel):
    current_step: PipelineStep
    completed_steps: list[PipelineStep]
    data_profile: Optional[DataProfile] = None
    candidates: list[TransformationCandidate] = []
    ranked_transformations: list[RankedTransformation] = []
    error: Optional[str] = None
