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


class StatisticalSummarizer:
    """Computes descriptive statistics for DataFrame columns."""
    
    def summarize_numeric(self, series: pd.Series) -> Dict[str, Optional[float]]:
        """
        Compute statistical summaries for numeric columns.
        
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
                'count': 0,
                'valid_count': 0
            }
        
        # Calculate statistics
        stats = {
            'min': float(non_null.min()),
            'max': float(non_null.max()),
            'mean': float(non_null.mean()),
            'std': float(non_null.std()) if len(non_null) > 1 else 0.0,
            'median': float(non_null.median()),
            'q25': float(non_null.quantile(0.25)),
            'q75': float(non_null.quantile(0.75)),
            'variance': float(non_null.var()) if len(non_null) > 1 else 0.0,
            'skewness': float(non_null.skew()) if len(non_null) > 2 else 0.0,
            'kurtosis': float(non_null.kurtosis()) if len(non_null) > 3 else 0.0,
            'count': int(len(series)),
            'valid_count': int(len(non_null))
        }
        
        return stats
    
    def summarize_categorical(self, series: pd.Series) -> Dict[str, Any]:
        """
        Compute statistical summaries for categorical columns.
        
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
        
        # Calculate unique count
        unique_count = int(non_null.nunique())
        
        # Get mode
        mode = non_null.mode()
        mode_value = mode.iloc[0] if len(mode) > 0 else None
        mode_frequency = int(non_null[non_null == mode_value].count()) if mode_value is not None else 0
        
        # Get frequency distribution (top 10)
        value_counts = non_null.value_counts()
        frequency_distribution = {
            str(k): int(v) for k, v in value_counts.head(10).items()
        }
        
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
