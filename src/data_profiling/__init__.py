"""Data profiling module for analyzing and profiling DataFrames."""

from .interfaces import DataProfiler
from .schema_detector import (
    SchemaDetector,
    infer_column_type,
    INFERRED_NUMERIC,
    INFERRED_CATEGORICAL,
    INFERRED_DATETIME,
    INFERRED_TEXT,
    INFERRED_BOOLEAN,
)
from .missing_value_analyzer import MissingValueAnalyzer
from .statistical_summarizer import StatisticalSummarizer
from .profiler import DataProfilerService

__all__ = [
    # Interfaces
    "DataProfiler",
    # Schema detection
    "SchemaDetector",
    "infer_column_type",
    "INFERRED_NUMERIC",
    "INFERRED_CATEGORICAL",
    "INFERRED_DATETIME",
    "INFERRED_TEXT",
    "INFERRED_BOOLEAN",
    # Missing value analysis
    "MissingValueAnalyzer",
    # Statistical summarization
    "StatisticalSummarizer",
    # Main profiler service
    "DataProfilerService",
]
