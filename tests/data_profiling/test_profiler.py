"""Tests for the data profiler service."""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime

from data_profiling import DataProfilerService
from common.types import DataProfile


class TestDataProfilerService:
    """Tests for DataProfilerService."""
    
    def test_profile_returns_dataprofile(self):
        """Test that profile returns a valid DataProfile object."""
        # Create sample data
        data = pd.DataFrame({
            'numeric_col': [1, 2, 3, 4, 5],
            'text_col': ['a', 'b', 'c', 'd', 'e']
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        assert isinstance(profile, DataProfile)
    
    def test_profile_basic_stats(self):
        """Test that profile returns correct basic statistics."""
        data = pd.DataFrame({
            'col1': [1, 2, 3, 4, 5],
            'col2': ['a', 'b', 'c', 'd', 'e']
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        assert profile.row_count == 5
        assert profile.column_count == 2
        assert len(profile.columns) == 2
    
    def test_profile_numeric_column(self):
        """Test profiling a numeric column."""
        data = pd.DataFrame({
            'numbers': [1.0, 2.0, 3.0, 4.0, 5.0]
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['numbers']
        assert col_profile.inferred_type == 'numeric'
        assert col_profile.min_value == 1.0
        assert col_profile.max_value == 5.0
        assert col_profile.mean == 3.0
    
    def test_profile_with_missing_values(self):
        """Test profiling with missing values."""
        data = pd.DataFrame({
            'col': [1, 2, None, 4, None]
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['col']
        assert col_profile.null_count == 2
        assert col_profile.null_percentage == 40.0
    
    def test_profile_with_empty_strings(self):
        """Test profiling with empty strings."""
        data = pd.DataFrame({
            'text': ['a', '', 'b', '', 'c']
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['text']
        # Empty strings should be counted as missing
        assert col_profile.null_count == 2
    
    def test_profile_categorical_column(self):
        """Test profiling a categorical column."""
        data = pd.DataFrame({
            'category': ['A', 'A', 'B', 'B', 'B']
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['category']
        assert col_profile.inferred_type == 'categorical'
        assert col_profile.unique_count == 2
    
    def test_profile_boolean_column(self):
        """Test profiling a boolean column."""
        data = pd.DataFrame({
            'flag': [True, False, True, True, False]
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['flag']
        assert col_profile.inferred_type == 'boolean'
    
    def test_profile_datetime_column(self):
        """Test profiling a datetime column."""
        data = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        col_profile = profile.columns['date']
        assert col_profile.inferred_type == 'datetime'
    
    def test_profile_duplicate_rows(self):
        """Test detection of duplicate rows."""
        data = pd.DataFrame({
            'col1': [1, 2, 1, 2],
            'col2': ['a', 'b', 'a', 'b']
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        assert profile.duplicate_rows == 2
    
    def test_profile_overall_missing_percentage(self):
        """Test overall missing percentage calculation."""
        data = pd.DataFrame({
            'col1': [1, None, 3, None, 5],
            'col2': ['a', 'b', 'c', None, None]
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        # 2 missing in col1 + 2 missing in col2 = 4 missing
        # Total cells = 2 columns * 5 rows = 10
        # 4/10 = 40%
        assert profile.overall_missing_percentage > 0
    
    def test_profile_with_details(self):
        """Test profile_with_details method."""
        data = pd.DataFrame({
            'col': [1, 2, 3]
        })
        
        profiler = DataProfilerService()
        details = profiler.profile_with_details(data)
        
        assert 'profile' in details
        assert 'missing_analysis' in details
        assert 'overall_missing' in details
        assert 'statistics' in details
    
    def test_profile_mixed_types(self):
        """Test profiling a DataFrame with mixed column types."""
        data = pd.DataFrame({
            'numeric': [1, 2, 3],
            'text': ['a', 'b', 'c'],
            'bool': [True, False, True],
            'dates': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        assert profile.columns['numeric'].inferred_type == 'numeric'
        assert profile.columns['text'].inferred_type == 'text'
        assert profile.columns['bool'].inferred_type == 'boolean'
        assert profile.columns['dates'].inferred_type == 'datetime'
    
    def test_profile_empty_dataframe(self):
        """Test profiling an empty DataFrame."""
        data = pd.DataFrame()
        
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        
        assert profile.row_count == 0
        assert profile.column_count == 0
        assert len(profile.columns) == 0
    
    def test_profile_timestamp_set(self):
        """Test that timestamp is set correctly."""
        data = pd.DataFrame({'col': [1, 2, 3]})
        
        before = datetime.now()
        profiler = DataProfilerService()
        profile = profiler.profile(data)
        after = datetime.now()
        
        assert before <= profile.timestamp <= after
