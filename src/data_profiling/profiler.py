"""Main data profiler service."""

import pandas as pd
from datetime import datetime
from typing import Dict, Any

from common.types import DataProfile, ColumnProfile
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
        
        Args:
            data: The pandas DataFrame to profile.
            
        Returns:
            DataProfile containing schema, missing values, and statistics.
        """
        row_count = len(data)
        column_count = len(data.columns)
        
        # Get inferred types for all columns
        inferred_types = self.schema_detector.detect(data)
        
        # Get missing value analysis for all columns
        missing_analysis = self.missing_value_analyzer.analyze(data)
        
        # Get overall missing stats
        overall_missing = self.missing_value_analyzer.get_overall_missing_stats(data)
        
        # Get statistical summaries
        stats = self.statistical_summarizer.summarize(data, inferred_types)
        
        # Count duplicate rows
        duplicate_rows = data.duplicated().sum()
        
        # Build ColumnProfile for each column
        columns: Dict[str, ColumnProfile] = {}
        
        for column in data.columns:
            series = data[column]
            inferred_type = inferred_types[column]
            missing_info = missing_analysis[column]
            column_stats = stats[column]
            
            # Get unique count
            unique_count = None
            if inferred_type in ('numeric', 'categorical', 'text', 'datetime', 'boolean'):
                non_null = series.dropna()
                if len(non_null) > 0:
                    unique_count = int(non_null.nunique())
            
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
