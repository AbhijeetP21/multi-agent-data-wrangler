"""Configuration schemas using Pydantic."""

from pydantic import BaseModel, Field
from typing import Optional


class DataSourceConfig(BaseModel):
    """Configuration for a data source."""

    type: str = Field(description="Type of data source (e.g., 'csv', 'parquet', 'database')")
    path: Optional[str] = Field(default=None, description="Path to the data file")
    connection_string: Optional[str] = Field(default=None, description="Database connection string")
    options: dict = Field(default_factory=dict, description="Additional options")


class ProfilingConfig(BaseModel):
    """Configuration for data profiling."""

    enabled: bool = True
    sample_size: Optional[int] = Field(default=None, description="Sample size for profiling (None for full data)")
    compute_stats: bool = True
    compute_correlations: bool = False


class TransformationConfig(BaseModel):
    """Configuration for transformations."""

    max_candidates: int = Field(default=100, description="Maximum number of transformation candidates")
    allowed_types: list[str] = Field(
        default_factory=lambda: [
            "fill_missing",
            "normalize",
            "encode_categorical",
            "remove_outliers",
            "drop_duplicates",
            "cast_type",
        ]
    )


class ValidationConfig(BaseModel):
    """Configuration for validation."""

    strict_mode: bool = False
    check_schema: bool = True
    check_constraints: bool = True


class ScoringConfig(BaseModel):
    """Configuration for quality scoring."""

    weights: dict[str, float] = Field(
        default_factory=lambda: {
            "completeness": 0.3,
            "consistency": 0.2,
            "validity": 0.3,
            "uniqueness": 0.2,
        }
    )


class RankingConfig(BaseModel):
    """Configuration for ranking transformations."""

    enabled: bool = True
    top_k: int = Field(default=10, description="Number of top transformations to return")


class LoggingConfig(BaseModel):
    """Configuration for logging."""

    level: str = Field(default="INFO", description="Log level")
    format: str = Field(default="json", description="Log format (json or console)")
    output: str = Field(default="stdout", description="Output destination")


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""

    name: str = "data_wrangler"
    version: str = "1.0.0"
    max_iterations: int = Field(default=10, description="Maximum pipeline iterations")
    timeout_seconds: int = Field(default=300, description="Pipeline timeout in seconds")
    data_source: DataSourceConfig
    profiling: ProfilingConfig = Field(default_factory=ProfilingConfig)
    transformation: TransformationConfig = Field(default_factory=TransformationConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    ranking: RankingConfig = Field(default_factory=RankingConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
