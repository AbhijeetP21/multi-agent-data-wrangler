"""Missing value analysis module."""

import pandas as pd
import numpy as np
from typing import Dict, Tuple


class MissingValueAnalyzer:
    """Analyzes missing values in a DataFrame."""
    
    def analyze(self, data: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """
        Analyze missing values for all columns in a DataFrame.
        
        Args:
            data: The pandas DataFrame to analyze.
            
        Returns:
            Dictionary mapping column names to {
                'null_count': int,
                'null_percentage': float,
                'empty_string_count': int,
                'empty_string_percentage': float,
                'total_missing_count': int,
                'total_missing_percentage': float
            }
        """
        results = {}
        total_rows = len(data)
        
        for column in data.columns:
            # Get the series
            series = data[column]
            
            # Count null/NaN values
            null_count = series.isna().sum()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0.0
            
            # Count empty strings (only for object/string columns)
            if series.dtype == 'object' or pd.api.types.is_string_dtype(series):
                empty_string_count = (series == '').sum()
            else:
                empty_string_count = 0
            empty_string_percentage = (empty_string_count / total_rows * 100) if total_rows > 0 else 0.0
            
            # Total missing = null + empty strings
            total_missing_count = null_count + empty_string_count
            total_missing_percentage = (total_missing_count / total_rows * 100) if total_rows > 0 else 0.0
            
            results[column] = {
                'null_count': int(null_count),
                'null_percentage': float(null_percentage),
                'empty_string_count': int(empty_string_count),
                'empty_string_percentage': float(empty_string_percentage),
                'total_missing_count': int(total_missing_count),
                'total_missing_percentage': float(total_missing_percentage)
            }
        
        return results
    
    def get_overall_missing_stats(self, data: pd.DataFrame) -> Dict[str, float]:
        """
        Get overall missing value statistics for the entire DataFrame.
        
        Args:
            data: The pandas DataFrame to analyze.
            
        Returns:
            Dictionary with:
                - 'total_cells': int
                - 'total_missing': int
                - 'overall_missing_percentage': float
                - 'columns_with_missing': int
                - 'columns_complete': int
        """
        total_rows = len(data)
        total_columns = len(data.columns)
        total_cells = total_rows * total_columns
        
        # Calculate total missing across all columns
        total_null = data.isna().sum().sum()
        
        # Count empty strings for object columns
        total_empty = 0
        for column in data.columns:
            if data[column].dtype == 'object' or pd.api.types.is_string_dtype(data[column]):
                total_empty += (data[column] == '').sum()
        
        total_missing = total_null + total_empty
        overall_missing_percentage = (total_missing / total_cells * 100) if total_cells > 0 else 0.0
        
        # Count columns with any missing values
        columns_with_missing = 0
        for column in data.columns:
            series = data[column]
            null_count = series.isna().sum()
            empty_count = 0
            if series.dtype == 'object' or pd.api.types.is_string_dtype(series):
                empty_count = (series == '').sum()
            if null_count + empty_count > 0:
                columns_with_missing += 1
        
        columns_complete = total_columns - columns_with_missing
        
        return {
            'total_cells': int(total_cells),
            'total_missing': int(total_missing),
            'overall_missing_percentage': float(overall_missing_percentage),
            'columns_with_missing': int(columns_with_missing),
            'columns_complete': int(columns_complete)
        }
    
    def get_missing_patterns(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Analyze patterns in missing values across rows.
        
        Args:
            data: The pandas DataFrame to analyze.
            
        Returns:
            DataFrame with missing value pattern analysis.
        """
        # Create a boolean mask for missing values
        missing_mask = data.isna()
        
        # Add empty string detection columns
        for column in data.columns:
            if data[column].dtype == 'object' or pd.api.types.is_string_dtype(data[column]):
                missing_mask[column] = missing_mask[column] | (data[column] == '')
        
        # Count missing values per row
        missing_per_row = missing_mask.sum(axis=1)
        
        # Get unique patterns and their counts
        pattern_counts = missing_mask.groupby(by=missing_per_row).size()
        
        result = pd.DataFrame({
            'missing_count': pattern_counts.index,
            'num_rows': pattern_counts.values,
            'percentage': (pattern_counts.values / len(data) * 100) if len(data) > 0 else 0
        })
        
        return result
