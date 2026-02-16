"""Schema detection module for inferring column types."""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, Tuple
import re
from functools import lru_cache


# Type inference constants
INFERRED_NUMERIC = "numeric"
INFERRED_CATEGORICAL = "categorical"
INFERRED_DATETIME = "datetime"
INFERRED_TEXT = "text"
INFERRED_BOOLEAN = "boolean"
INFERRED_MIXED = "mixed"  # New: for columns with mixed types

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

# Performance thresholds
LARGE_DATASET_THRESHOLD = 50000  # Rows threshold for using sampling
SAMPLE_SIZE_SMALL = 1000  # Sample size for small-medium datasets
SAMPLE_SIZE_LARGE = 5000  # Sample size for large datasets
MIXED_TYPE_THRESHOLD = 0.15  # If >15% values don't match main type, consider mixed


def _get_sample_size(total_rows: int) -> int:
    """Get appropriate sample size based on dataset size."""
    if total_rows > 100000:
        return SAMPLE_SIZE_LARGE
    return SAMPLE_SIZE_SMALL


def is_mixed_type(series: pd.Series, sample_size: int = 1000) -> Tuple[bool, str]:
    """
    Check if a series contains mixed types (e.g., numbers and strings).
    
    Returns:
        Tuple of (is_mixed, primary_type)
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, INFERRED_TEXT
    
    # Sample for efficiency
    if len(non_null) > sample_size:
        sample = non_null.sample(n=sample_size, random_state=42)
    else:
        sample = non_null
    
    # Analyze type distribution
    type_counts = {
        'numeric': 0,
        'string': 0,
        'datetime': 0,
        'boolean': 0,
        'other': 0
    }
    
    for val in sample:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            continue
        elif isinstance(val, bool):
            type_counts['boolean'] += 1
        elif isinstance(val, (int, float)) and not isinstance(val, bool):
            type_counts['numeric'] += 1
        elif isinstance(val, str):
            # Check if string looks like numeric
            try:
                float(val)
                type_counts['numeric'] += 1
            except (ValueError, TypeError):
                type_counts['string'] += 1
        else:
            type_counts['other'] += 1
    
    total = sum(type_counts.values())
    if total == 0:
        return False, INFERRED_TEXT
    
    # Find primary type
    primary_type = max(type_counts, key=type_counts.get)
    primary_ratio = type_counts[primary_type] / total
    
    # If primary type is less than 85%, it's mixed
    is_mixed = primary_ratio < (1.0 - MIXED_TYPE_THRESHOLD)
    
    type_mapping = {
        'numeric': INFERRED_NUMERIC,
        'string': INFERRED_TEXT,
        'datetime': INFERRED_DATETIME,
        'boolean': INFERRED_BOOLEAN,
        'other': INFERRED_TEXT
    }
    
    return is_mixed, type_mapping.get(primary_type, INFERRED_TEXT)


def is_boolean(series: pd.Series) -> Tuple[bool, float]:
    """
    Check if a series represents boolean values with confidence score.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        Tuple of (is_boolean, confidence_score)
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0
    
    # Convert to string and lowercase for comparison
    string_values = non_null.astype(str).str.lower().str.strip()
    
    # Check if all values are in boolean true/false values
    unique_values = set(string_values.unique())
    
    # Must be a subset of boolean values
    if not unique_values.issubset(BOOLEAN_TRUE_VALUES):
        return False, 0.0
    
    # Calculate confidence based on unique value count
    # Boolean columns typically have only 2 unique values
    confidence = 1.0 if len(unique_values) <= 2 else 0.5
    
    return True, confidence


def is_datetime(series: pd.Series, sample_size: int = 1000) -> Tuple[bool, float]:
    """
    Check if a series represents datetime values with confidence.
    
    Args:
        series: The pandas Series to check.
        sample_size: Number of values to sample.
        
    Returns:
        Tuple of (is_datetime, confidence_score)
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0
    
    # Check if already datetime dtype
    if pd.api.types.is_datetime64_any_dtype(series):
        return True, 1.0
    
    # Use adaptive sample size
    actual_sample_size = min(sample_size, len(non_null))
    sample = non_null.sample(n=actual_sample_size, random_state=42) if len(non_null) > sample_size else non_null
    
    # First check if it looks numeric - if so, skip datetime check
    try:
        numeric_converted = pd.to_numeric(sample, errors='coerce')
        if numeric_converted.notna().sum() / len(sample) > 0.8:
            return False, 0.0
    except Exception:
        pass
    
    # Try to parse as datetime
    try:
        parsed = pd.to_datetime(sample, errors='coerce')
        success_rate = parsed.notna().sum() / len(sample)
        
        if success_rate > 0.8:
            # Do full check for large datasets
            if len(non_null) > sample_size * 2:
                full_parsed = pd.to_datetime(non_null, errors='coerce')
                full_success_rate = full_parsed.notna().sum() / len(non_null)
                return full_success_rate > 0.8, full_success_rate
            
            return True, success_rate
        return False, success_rate
    except Exception:
        return False, 0.0


def is_numeric(series: pd.Series, sample_size: int = 1000) -> Tuple[bool, float]:
    """
    Check if a series represents numeric values with confidence.
    
    Args:
        series: The pandas Series to check.
        sample_size: Number of values to sample for quick check.
        
    Returns:
        Tuple of (is_numeric, confidence_score)
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0
    
    # Check if already numeric dtype
    if pd.api.types.is_numeric_dtype(series):
        return True, 1.0
    
    # Use adaptive sample size based on dataset size
    actual_sample_size = _get_sample_size(len(non_null))
    if len(non_null) > actual_sample_size:
        sample = non_null.sample(n=actual_sample_size, random_state=42)
    else:
        sample = non_null
    
    # Try to convert sample to numeric
    try:
        converted = pd.to_numeric(sample, errors='coerce')
        success_rate = converted.notna().sum() / len(sample)
        
        if success_rate < 0.8:
            return False, success_rate
        
        # For large columns, use sample result with higher threshold
        if len(non_null) > sample_size:
            return success_rate > 0.9, success_rate
        
        # For smaller columns, do full check
        full_converted = pd.to_numeric(non_null, errors='coerce')
        full_success_rate = full_converted.notna().sum() / len(non_null)
        return full_success_rate > 0.8, full_success_rate
    except Exception:
        return False, 0.0


def is_categorical(series: pd.Series) -> Tuple[bool, float]:
    """
    Check if a series represents categorical values with confidence.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        Tuple of (is_categorical, confidence_score)
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0
    
    # Calculate unique ratio
    total_values = len(non_null)
    unique_count = non_null.nunique()
    unique_ratio = unique_count / total_values
    
    # Higher confidence when ratio is lower
    confidence = 1.0 - unique_ratio if unique_ratio < 1.0 else 0.0
    
    # If unique ratio is below threshold, it's likely categorical
    is_categorical = unique_ratio <= UNIQUE_RATIO_CATEGORICAL
    
    return is_categorical, confidence


def is_text(series: pd.Series) -> Tuple[bool, float]:
    """
    Check if a series represents text values with confidence.
    
    Args:
        series: The pandas Series to check.
        
    Returns:
        Tuple of (is_text, confidence_score)
    """
    # Get non-null values
    non_null = series.dropna()
    if len(non_null) == 0:
        return False, 0.0
    
    # Check if already string/object dtype
    if pd.api.types.is_string_dtype(series) or series.dtype == 'object':
        # Check if it has enough unique values to be considered text
        unique_count = non_null.nunique()
        is_text = unique_count >= MIN_UNIQUE_FOR_TEXT
        # Confidence based on unique count
        confidence = min(1.0, unique_count / 100)
        return is_text, confidence
    
    return False, 0.0


def infer_column_type(series: pd.Series, detect_mixed: bool = True) -> str:
    """
    Infer the type of a column with improved handling for mixed data.
    
    The inference order is important:
    1. Boolean (before numeric since 0/1 could be confused)
    2. Datetime (before numeric since datetime can be confused)
    3. Numeric (integers and floats)
    4. Categorical (low cardinality)
    5. Text (high cardinality strings)
    6. Mixed (columns with multiple types)
    
    Args:
        series: The pandas Series to infer type for.
        detect_mixed: Whether to detect mixed type columns.
        
    Returns:
        One of: 'numeric', 'categorical', 'datetime', 'text', 'boolean', 'mixed'
    """
    # Check for mixed types first (if enabled)
    if detect_mixed:
        is_mixed, primary_type = is_mixed_type(series)
        if is_mixed:
            return INFERRED_MIXED
    
    # Check for boolean first
    is_bool, _ = is_boolean(series)
    if is_bool:
        return INFERRED_BOOLEAN
    
    # Check for datetime (before numeric since datetime can be confused with numeric)
    is_datetime_val, _ = is_datetime(series)
    if is_datetime_val:
        return INFERRED_DATETIME
    
    # Check for numeric
    is_numeric_val, _ = is_numeric(series)
    if is_numeric_val:
        return INFERRED_NUMERIC
    
    # Check for categorical (low cardinality)
    is_cat, _ = is_categorical(series)
    if is_cat:
        return INFERRED_CATEGORICAL
    
    # Check for text (high cardinality strings)
    is_text_val, _ = is_text(series)
    if is_text_val:
        return INFERRED_TEXT
    
    # Default to text if nothing else fits
    return INFERRED_TEXT


class SchemaDetector:
    """Detects and infers column types for a DataFrame with caching."""
    
    def __init__(self):
        """Initialize the schema detector with caching."""
        self._cache: Dict[str, str] = {}
    
    def detect(self, data: pd.DataFrame) -> dict[str, str]:
        """
        Detect column types for all columns in a DataFrame.
        
        Uses caching to avoid redundant computation.
        
        Args:
            data: The pandas DataFrame to detect types for.
            
        Returns:
            Dictionary mapping column names to inferred types.
        """
        inferred_types = {}
        cache_key_base = f"{id(data)}"
        
        for column in data.columns:
            cache_key = f"{cache_key_base}_{column}"
            
            # Check cache
            if cache_key in self._cache:
                inferred_types[column] = self._cache[cache_key]
                continue
            
            # Detect type
            col_type = infer_column_type(data[column])
            inferred_types[column] = col_type
            
            # Store in cache
            self._cache[cache_key] = col_type
        
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
            
            # Check each type and get confidence
            is_bool, bool_conf = is_boolean(series)
            if is_bool:
                results[column] = (INFERRED_BOOLEAN, bool_conf)
                continue
            
            is_dt, dt_conf = is_datetime(series)
            if is_dt:
                results[column] = (INFERRED_DATETIME, dt_conf)
                continue
            
            is_num, num_conf = is_numeric(series)
            if is_num:
                results[column] = (INFERRED_NUMERIC, num_conf)
                continue
            
            is_cat, cat_conf = is_categorical(series)
            if is_cat:
                results[column] = (INFERRED_CATEGORICAL, cat_conf)
                continue
            
            is_txt, txt_conf = is_text(series)
            if is_txt:
                results[column] = (INFERRED_TEXT, txt_conf)
                continue
            
            # Default
            results[column] = (INFERRED_TEXT, 0.5)
        
        return results
    
    def clear_cache(self) -> None:
        """Clear the type detection cache."""
        self._cache.clear()
