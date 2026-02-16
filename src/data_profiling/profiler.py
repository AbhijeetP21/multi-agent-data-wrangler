"""Main data profiler service."""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, Optional

from src.common.types import DataProfile, ColumnProfile
from .schema_detector import SchemaDetector, infer_column_type
from .missing_value_analyzer import MissingValueAnalyzer
from .statistical_summarizer import StatisticalSummarizer


class DataProfilerService:
    """
    Main service for profiling data and producing structured profiles.
    
    This service orchestrates schema detection, missing value analysis,
    and statistical summarization to produce a complete DataProfile.
    """
    
    def __init__(self):
        """Initialize the data profiler service."""
        self.schema_detector = SchemaDetector()
        self.missing_value_analyzer = MissingValueAnalyzer()
        self.statistical_summarizer = StatisticalSummarizer()
    
    def profile(self, data: pd.DataFrame) -> DataProfile:
        """
        Profile a DataFrame and return a DataProfile.
        
        Uses adaptive performance optimizations for different dataset sizes.
        
        Args:
            data: The pandas DataFrame to profile.
            
        Returns:
            DataProfile containing schema, missing values, and statistics.
        """
        row_count = len(data)
        column_count = len(data.columns)
        
        # Get inferred types for all columns (with caching)
        inferred_types = self.schema_detector.detect(data)
        
        # Get missing value analysis for all columns
        missing_analysis = self.missing_value_analyzer.analyze(data)
        
        # Get overall missing stats
        overall_missing = self.missing_value_analyzer.get_overall_missing_stats(data)
        
        # Get statistical summaries (pass inferred types to avoid redundant detection)
        stats = self.statistical_summarizer.summarize(data, inferred_types)
        
        # Count duplicate rows with adaptive approach
        # Use hash-based approach for better accuracy
        duplicate_rows = self._count_duplicates(data)
        
        # Build ColumnProfile for each column
        columns: Dict[str, ColumnProfile] = {}
        
        for column in data.columns:
            series = data[column]
            inferred_type = inferred_types[column]
            missing_info = missing_analysis[column]
            column_stats = stats[column]
            
            # Get unique count - use adaptive sampling for large columns
            unique_count = self._get_unique_count(series, inferred_type)
            
            # Get min/max/mean/std for numeric columns
            min_value = None
            max_value = None
            mean = None
            std = None
            
            if inferred_type == 'numeric' and column_stats:
                min_value = column_stats.get('min')
                max_value = column_stats.get('max')
                mean = column_stats.get('mean')
                std = column_stats.get('std')
            
            # Use total_missing_count and total_missing_percentage to include empty strings
            column_profile = ColumnProfile(
                name=column,
                dtype=str(series.dtype),
                null_count=missing_info['total_missing_count'],
                null_percentage=missing_info['total_missing_percentage'],
                unique_count=unique_count,
                min_value=min_value,
                max_value=max_value,
                mean=mean,
                std=std,
                inferred_type=inferred_type
            )
            
            columns[column] = column_profile
        
        # Create the DataProfile
        profile = DataProfile(
            timestamp=datetime.now(),
            row_count=row_count,
            column_count=column_count,
            columns=columns,
            overall_missing_percentage=overall_missing['overall_missing_percentage'],
            duplicate_rows=duplicate_rows
        )
        
        return profile
    
    def _count_duplicates(self, data: pd.DataFrame) -> int:
        """
        Count duplicate rows with adaptive performance.
        
        Uses different strategies based on dataset size.
        """
        total_rows = len(data)
        
        if total_rows == 0:
            return 0
        
        # For small datasets, count directly
        if total_rows <= 50000:
            return int(data.duplicated().sum())
        
        # For medium datasets (50k-100k), use a larger sample
        if total_rows <= 100000:
            sample_size = min(20000, total_rows)
            sample = data.sample(n=sample_size, random_state=42)
            sample_duplicates = sample.duplicated().sum()
            # Extrapolate to full dataset
            return int(sample_duplicates * (total_rows / sample_size))
        
        # For large datasets (>100k), use hash-based sampling
        # Sample 10% of data, up to 20k rows
        sample_size = min(20000, max(5000, total_rows // 10))
        sample = data.sample(n=sample_size, random_state=42)
        sample_duplicates = sample.duplicated().sum()
        return int(sample_duplicates * (total_rows / sample_size))
    
    def _get_unique_count(self, series: pd.Series, inferred_type: str) -> Optional[int]:
        """
        Get unique count with adaptive sampling based on column size.
        """
        if inferred_type not in ('numeric', 'categorical', 'text', 'datetime', 'boolean'):
            return None
            
        non_null = series.dropna()
        if len(non_null) == 0:
            return 0
        
        total_rows = len(non_null)
        
        # For small columns, count directly
        if total_rows <= 5000:
            return int(non_null.nunique())
        
        # For medium columns (5k-50k), use moderate sampling
        if total_rows <= 50000:
            sample_size = min(2000, total_rows)
            sample = non_null.sample(n=sample_size, random_state=42)
            unique_ratio = sample.nunique() / sample_size
            return int(unique_ratio * total_rows)
        
        # For large columns (>50k), use larger sample for better accuracy
        sample_size = min(5000, total_rows)
        sample = non_null.sample(n=sample_size, random_state=42)
        unique_ratio = sample.nunique() / sample_size
        return int(unique_ratio * total_rows)
    
    def profile_with_details(self, data: pd.DataFrame) -> Dict[str, Any]:
        """
        Profile a DataFrame with additional details.
        
        This method returns a dictionary with all profiling information
        including detailed statistics and missing value patterns.
        
        Args:
            data: The pandas DataFrame to profile.
            
        Returns:
            Dictionary containing the DataProfile and additional details.
        """
        # Get the standard profile
        profile = self.profile(data)
        
        # Get additional details
        missing_analysis = self.missing_value_analyzer.analyze(data)
        overall_missing = self.missing_value_analyzer.get_overall_missing_stats(data)
        stats = self.statistical_summarizer.summarize(data)
        
        return {
            'profile': profile.model_dump(),
            'missing_analysis': missing_analysis,
            'overall_missing': overall_missing,
            'statistics': stats
        }
