"""Statistical summarization module."""

import pandas as pd
import numpy as np
from typing import Dict, Optional, Any
from .schema_detector import (
    INFERRED_NUMERIC,
    INFERRED_CATEGORICAL,
    INFERRED_DATETIME,
    INFERRED_TEXT,
    INFERRED_BOOLEAN,
    infer_column_type
)


# Adaptive sampling thresholds
SMALL_DATASET = 10000
MEDIUM_DATASET = 50000
LARGE_DATASET = 100000

SAMPLE_SIZE_SMALL = 2000
SAMPLE_SIZE_MEDIUM = 5000
SAMPLE_SIZE_LARGE = 10000


def _get_sample_size(total_rows: int) -> int:
    """Get appropriate sample size based on dataset size."""
    if total_rows > LARGE_DATASET:
        return SAMPLE_SIZE_LARGE
    elif total_rows > MEDIUM_DATASET:
        return SAMPLE_SIZE_MEDIUM
    elif total_rows > SMALL_DATASET:
        return SAMPLE_SIZE_SMALL
    return total_rows  # No sampling for small datasets


class StatisticalSummarizer:
    """Computes descriptive statistics for DataFrame columns."""
    
    def summarize_numeric(self, series: pd.Series) -> Dict[str, Optional[float]]:
        """
        Compute statistical summaries for numeric columns.
        
        Uses adaptive sampling based on dataset size for better accuracy/performance trade-off.
        
        Args:
            series: The numeric pandas Series to summarize.
            
        Returns:
            Dictionary with min, max, mean, std, median, q25, q75, etc.
        """
        # Get non-null values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'min': None,
                'max': None,
                'mean': None,
                'std': None,
                'median': None,
                'q25': None,
                'q75': None,
                'variance': None,
                'skewness': None,
                'kurtosis': None,
                'count': int(len(series)),
                'valid_count': 0,
                'sampled': False
            }
        
        # Try to convert to numeric - this handles string columns that should be numeric
        numeric_series = pd.to_numeric(non_null, errors='coerce')
        
        # Check if we have valid numeric data after conversion
        valid_numeric = numeric_series.dropna()
        
        if len(valid_numeric) == 0:
            # No valid numeric data - return empty stats
            return {
                'min': None,
                'max': None,
                'mean': None,
                'std': None,
                'median': None,
                'q25': None,
                'q75': None,
                'variance': None,
                'skewness': None,
                'kurtosis': None,
                'count': int(len(series)),
                'valid_count': 0,
                'sampled': False
            }
        
        # Use adaptive sampling based on dataset size
        sample_size = _get_sample_size(len(valid_numeric))
        
        if len(valid_numeric) > sample_size:
            # Use sampled data for stats calculation
            sampled = valid_numeric.sample(n=sample_size, random_state=42)
            stats = {
                'min': float(sampled.min()),
                'max': float(sampled.max()),
                'mean': float(sampled.mean()),
                'std': float(sampled.std(ddof=0)) if len(sampled) > 1 else 0.0,
                'median': float(sampled.median()),
                'q25': float(sampled.quantile(0.25)),
                'q75': float(sampled.quantile(0.75)),
                'variance': float(sampled.var(ddof=0)) if len(sampled) > 1 else 0.0,
                'skewness': None,
                'kurtosis': None,
                'count': int(len(series)),
                'valid_count': int(len(valid_numeric)),
                'sampled': True,
                'sample_size': sample_size
            }
        else:
            # Calculate full statistics for smaller datasets
            stats = {
                'min': float(valid_numeric.min()),
                'max': float(valid_numeric.max()),
                'mean': float(valid_numeric.mean()),
                'std': float(valid_numeric.std(ddof=0)) if len(valid_numeric) > 1 else 0.0,
                'median': float(valid_numeric.median()),
                'q25': float(valid_numeric.quantile(0.25)),
                'q75': float(valid_numeric.quantile(0.75)),
                'variance': float(valid_numeric.var(ddof=0)) if len(valid_numeric) > 1 else 0.0,
                'skewness': None,
                'kurtosis': None,
                'count': int(len(series)),
                'valid_count': int(len(valid_numeric)),
                'sampled': False
            }
        
        return stats
    
    def summarize_categorical(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute statistical summaries for categorical columns.
        
        Uses sampling for large columns to improve performance.
        
        Args:
            series: The categorical pandas Series to summarize.
            
        Returns:
            Dictionary with unique count, mode, frequencies, etc.
        """
        # Get non-null values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'unique_count': 0,
                'mode': None,
                'mode_frequency': 0,
                'frequency_distribution': {},
                'count': 0,
                'valid_count': 0
            }
        
        # Calculate unique count - use sampling for very large columns
        sample_size = 5000
        if len(non_null) > sample_size:
            # Sample for unique count estimation
            sample = non_null.sample(n=sample_size, random_state=42)
            unique_count = int(sample.nunique() * (len(non_null) / sample_size))
            
            # Get frequency distribution from sample (top 10)
            value_counts = sample.value_counts()
            frequency_distribution = {
                str(k): int(v) for k, v in value_counts.head(10).items()
            }
            
            # Estimate mode from sample
            mode_value = value_counts.index[0] if len(value_counts) > 0 else None
            mode_frequency = int(value_counts.iloc[0]) if len(value_counts) > 0 else 0
        else:
            # Full calculation for smaller columns
            unique_count = int(non_null.nunique())
            
            # Get frequency distribution (top 10)
            value_counts = non_null.value_counts()
            frequency_distribution = {
                str(k): int(v) for k, v in value_counts.head(10).items()
            }
            
            # Get mode
            mode_value = value_counts.index[0] if len(value_counts) > 0 else None
            mode_frequency = int(value_counts.iloc[0]) if len(value_counts) > 0 else 0
        
        return {
            'unique_count': unique_count,
            'mode': str(mode_value) if mode_value is not None else None,
            'mode_frequency': mode_frequency,
            'frequency_distribution': frequency_distribution,
            'count': int(len(series)),
            'valid_count': int(len(non_null))
        }
    
    def summarize_datetime(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute statistical summaries for datetime columns.
        
        Args:
            series: The datetime pandas Series to summarize.
            
        Returns:
            Dictionary with min, max, range, etc.
        """
        # Get non-null values and convert to datetime
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'min': None,
                'max': None,
                'range_days': None,
                'count': 0,
                'valid_count': 0
            }
        
        # Try to parse as datetime
        try:
            datetime_series = pd.to_datetime(non_null, errors='coerce')
            valid_datetimes = datetime_series.dropna()
            
            if len(valid_datetimes) == 0:
                return {
                    'min': None,
                    'max': None,
                    'range_days': None,
                    'count': int(len(series)),
                    'valid_count': 0
                }
            
            min_date = valid_datetimes.min()
            max_date = valid_datetimes.max()
            
            # Calculate range in days
            range_days = (max_date - min_date).days if min_date is not None and max_date is not None else None
            
            return {
                'min': min_date.isoformat() if min_date is not None else None,
                'max': max_date.isoformat() if max_date is not None else None,
                'range_days': range_days,
                'count': int(len(series)),
                'valid_count': int(len(valid_datetimes))
            }
        except Exception:
            return {
                'min': None,
                'max': None,
                'range_days': None,
                'count': int(len(series)),
                'valid_count': 0
            }
    
    def summarize_boolean(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute statistical summaries for boolean columns.
        
        Args:
            series: The boolean pandas Series to summarize.
            
        Returns:
            Dictionary with true_count, false_count, true_percentage, etc.
        """
        # Get non-null values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'true_count': 0,
                'false_count': 0,
                'true_percentage': 0.0,
                'false_percentage': 0.0,
                'count': 0,
                'valid_count': 0
            }
        
        # Convert to string and count true/false
        string_values = non_null.astype(str).str.lower().str.strip()
        
        true_values = {'true', '1', 'yes', 't', 'y'}
        false_values = {'false', '0', 'no', 'f', 'n'}
        
        true_count = int(string_values.isin(true_values).sum())
        false_count = int(string_values.isin(false_values).sum())
        
        valid_count = len(non_null)
        true_percentage = (true_count / valid_count * 100) if valid_count > 0 else 0.0
        false_percentage = (false_count / valid_count * 100) if valid_count > 0 else 0.0
        
        return {
            'true_count': true_count,
            'false_count': false_count,
            'true_percentage': true_percentage,
            'false_percentage': false_percentage,
            'count': int(len(series)),
            'valid_count': valid_count
        }
    
    def summarize_text(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute statistical summaries for text columns.
        
        Args:
            series: The text pandas Series to summarize.
            
        Returns:
            Dictionary with min_length, max_length, avg_length, etc.
        """
        # Get non-null values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'unique_count': 0,
                'min_length': 0,
                'max_length': 0,
                'avg_length': 0.0,
                'count': 0,
                'valid_count': 0
            }
        
        # Calculate string lengths
        string_lengths = non_null.astype(str).str.len()
        
        unique_count = int(non_null.nunique())
        
        return {
            'unique_count': unique_count,
            'min_length': int(string_lengths.min()),
            'max_length': int(string_lengths.max()),
            'avg_length': float(string_lengths.mean()),
            'count': int(len(series)),
            'valid_count': int(len(non_null))
        }
    
    def summarize(self, data: pd.DataFrame, inferred_types: Dict[str, str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Compute statistical summaries for all columns in a DataFrame.
        
        Args:
            data: The pandas DataFrame to summarize.
            inferred_types: Optional dictionary of column types. If not provided,
                           will be inferred.
            
        Returns:
            Dictionary mapping column names to their statistical summaries.
        """
        results = {}
        
        # Use provided inferred types or infer them
        if inferred_types is None:
            inferred_types = {}
            for column in data.columns:
                inferred_types[column] = infer_column_type(data[column])
        
        for column in data.columns:
            series = data[column]
            
            # First check if it's a datetime dtype directly
            if pd.api.types.is_datetime64_any_dtype(series):
                inferred_type = INFERRED_DATETIME
            else:
                inferred_type = inferred_types.get(column, infer_column_type(series))
            
            if inferred_type == INFERRED_NUMERIC:
                results[column] = self.summarize_numeric(series)
            elif inferred_type == INFERRED_CATEGORICAL:
                results[column] = self.summarize_categorical(series)
            elif inferred_type == INFERRED_DATETIME:
                results[column] = self.summarize_datetime(series)
            elif inferred_type == INFERRED_BOOLEAN:
                results[column] = self.summarize_boolean(series)
            else:  # text
                results[column] = self.summarize_text(series)
        
        return results
