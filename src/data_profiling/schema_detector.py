"""Schema detection module for inferring column types."""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional
import re


# Type inference constants
INFERRED_NUMERIC = "numeric"
INFERRED_CATEGORICAL = "categorical"
INFERRED_DATETIME = "datetime"
INFERRED_TEXT = "text"
INFERRED_BOOLEAN = "boolean"

# Thresholds for type inference
UNIQUE_RATIO_CATEGORICAL = 0.6  # If unique values < 60% of total, consider categorical
MIN_UNIQUE_FOR_TEXT = 10  # Minimum unique values to consider text
BOOLEAN_TRUE_VALUES = {'true', 'false', '1', '0', 'yes', 'no', 't', 'f', 'y', 'n'}
DATETIME_FORMATS = [
    '%Y-%m-%d',
    '%Y/%m/%d',
    '%d-%m-%Y',
    '%d/%m/%Y',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S',
    '%Y-%m-%dT%H:%M:%S',
    '%Y-%m-%d %H:%M:%S.%f',
]


def is_boolean(series: pd.Series) -> bool:
    """
    Check if a series represents boolean values.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        True if the series contains boolean values, False otherwise.
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    # Convert to string and lowercase for comparison
    string_values = non_null.astype(str).str.lower().str.strip()
    
    # Check if all values are in boolean true/false values
    unique_values = set(string_values.unique())
    
    # Must be a subset of boolean values
    if not unique_values.issubset(BOOLEAN_TRUE_VALUES):
        return False
    
    # Must have at least some variety
    return len(unique_values) >= 1


def is_datetime(series: pd.Series) -> bool:
    """
    Check if a series represents datetime values.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        True if the series contains datetime values, False otherwise.
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    # Check if already datetime dtype
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    
    # First check if it looks numeric - if so, skip datetime check
    # pd.to_datetime can parse numeric strings as dates, which we don't want
    try:
        numeric_converted = pd.to_numeric(non_null, errors='coerce')
        if numeric_converted.notna().sum() / len(non_null) > 0.8:
            return False  # It's numeric, not datetime
    except Exception:
        pass
    
    # Try to parse as datetime
    try:
        parsed = pd.to_datetime(non_null, errors='coerce')
        # If more than 80% of non-null values parse successfully, it's datetime
        success_rate = parsed.notna().sum() / len(non_null)
        return success_rate > 0.8
    except Exception:
        return False


def is_numeric(series: pd.Series) -> bool:
    """
    Check if a series represents numeric values.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        True if the series contains numeric values, False otherwise.
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    # Check if already numeric dtype
    if pd.api.types.is_numeric_dtype(series):
        return True
    
    # Try to convert to numeric
    try:
        converted = pd.to_numeric(non_null, errors='coerce')
        success_rate = converted.notna().sum() / len(non_null)
        return success_rate > 0.8
    except Exception:
        return False


def is_categorical(series: pd.Series) -> bool:
    """
    Check if a series represents categorical values.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        True if the series contains categorical values, False otherwise.
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    # Calculate unique ratio
    total_values = len(non_null)
    unique_count = non_null.nunique()
    unique_ratio = unique_count / total_values
    
    # If unique ratio is below threshold, it's likely categorical
    return unique_ratio <= UNIQUE_RATIO_CATEGORICAL


def is_text(series: pd.Series) -> bool:
    """
    Check if a series represents text values.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        True if the series contains text values, False otherwise.
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False
    
    # Check if already string/object dtype
    if pd.api.types.is_string_dtype(series) or series.dtype == 'object':
        # Check if it has enough unique values to be considered text
        unique_count = non_null.nunique()
        return unique_count >= MIN_UNIQUE_FOR_TEXT
    
    return False


def infer_column_type(series: pd.Series) -> str:
    """
    Infer the type of a column.
    
    The inference order is important:
    1. Boolean (before numeric since 0/1 could be confused)
    2. Datetime (before numeric since datetime can be confused)
    3. Numeric (integers and floats)
    4. Categorical (low cardinality)
    5. Text (high cardinality strings)
    
    Args:
        series: The pandas Series to infer type for.
        
    Returns:
        One of: 'numeric', 'categorical', 'datetime', 'text', 'boolean'
    """
    # Check for boolean first
    if is_boolean(series):
        return INFERRED_BOOLEAN
    
    # Check for datetime (before numeric since datetime can be confused with numeric)
    if is_datetime(series):
        return INFERRED_DATETIME
    
    # Check for numeric
    if is_numeric(series):
        return INFERRED_NUMERIC
    
    # Check for categorical (low cardinality)
    if is_categorical(series):
        return INFERRED_CATEGORICAL
    
    # Check for text (high cardinality strings)
    if is_text(series):
        return INFERRED_TEXT
    
    # Default to text if nothing else fits
    return INFERRED_TEXT


class SchemaDetector:
    """Detects and infers column types for a DataFrame."""
    
    def detect(self, data: pd.DataFrame) -> dict[str, str]:
        """
        Detect column types for all columns in a DataFrame.
        
        Args:
            data: The pandas DataFrame to detect types for.
            
        Returns:
            Dictionary mapping column names to inferred types.
        """
        inferred_types = {}
        
        for column in data.columns:
            inferred_types[column] = infer_column_type(data[column])
        
        return inferred_types
    
    def detect_with_confidence(self, data: pd.DataFrame) -> dict[str, tuple[str, float]]:
        """
        Detect column types with confidence scores.
        
        Args:
            data: The pandas DataFrame to detect types for.
            
        Returns:
            Dictionary mapping column names to (inferred_type, confidence) tuples.
        """
        results = {}
        
        for column in data.columns:
            series = data[column]
            inferred_type = infer_column_type(series)
            
            # Calculate confidence based on how well the type fits
            non_null = series.dropna()
            if len(non_null) == 0:
                confidence = 0.0
            else:
                # Basic confidence calculation
                if inferred_type == INFERRED_BOOLEAN:
                    unique_count = non_null.nunique()
                    confidence = min(1.0, unique_count / 2)
                elif inferred_type == INFERRED_NUMERIC:
                    converted = pd.to_numeric(non_null, errors='coerce')
                    confidence = converted.notna().sum() / len(non_null)
                elif inferred_type == INFERRED_DATETIME:
                    parsed = pd.to_datetime(non_null, errors='coerce')
                    confidence = parsed.notna().sum() / len(non_null)
                elif inferred_type == INFERRED_CATEGORICAL:
                    unique_ratio = non_null.nunique() / len(non_null)
                    confidence = 1.0 - unique_ratio  # Lower ratio = higher confidence
                else:  # text
                    confidence = 0.5  # Default confidence for text
            
            results[column] = (inferred_type, confidence)
        
        return results
