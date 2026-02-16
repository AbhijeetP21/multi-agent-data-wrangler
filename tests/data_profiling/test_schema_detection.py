"""Tests for schema detection."""

import pytest
import pandas as pd
import numpy as np

from src.data_profiling.schema_detector import (
    SchemaDetector,
    infer_column_type,
    is_numeric,
    is_boolean,
    is_datetime,
    is_categorical,
    is_text,
    INFERRED_NUMERIC,
    INFERRED_BOOLEAN,
    INFERRED_DATETIME,
    INFERRED_CATEGORICAL,
    INFERRED_TEXT,
)


class TestSchemaDetector:
    """Tests for SchemaDetector class."""
    
    def test_detect_numeric_column(self):
        """Test detecting a numeric column."""
        data = pd.DataFrame({'col': [1, 2, 3, 4, 5]})
        detector = SchemaDetector()
        result = detector.detect(data)
        assert result['col'] == INFERRED_NUMERIC
    
    def test_detect_boolean_column(self):
        """Test detecting a boolean column."""
        data = pd.DataFrame({'col': [True, False, True, False]})
        detector = SchemaDetector()
        result = detector.detect(data)
        assert result['col'] == INFERRED_BOOLEAN
    
    def test_detect_datetime_column(self):
        """Test detecting a datetime column."""
        data = pd.DataFrame({
            'col': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03'])
        })
        detector = SchemaDetector()
        result = detector.detect(data)
        assert result['col'] == INFERRED_DATETIME
    
    def test_detect_categorical_column(self):
        """Test detecting a categorical column."""
        data = pd.DataFrame({'col': ['A', 'A', 'B', 'B', 'C']})
        detector = SchemaDetector()
        result = detector.detect(data)
        assert result['col'] == INFERRED_CATEGORICAL
    
    def test_detect_text_column(self):
        """Test detecting a text column."""
        data = pd.DataFrame({'col': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']})
        detector = SchemaDetector()
        result = detector.detect(data)
        assert result['col'] == INFERRED_TEXT
    
    def test_detect_multiple_columns(self):
        """Test detecting multiple columns with different types."""
        data = pd.DataFrame({
            'numeric': [1, 2, 3],
            'text': ['a', 'b', 'c'],
            'bool': [True, False, True]
        })
        detector = SchemaDetector()
        result = detector.detect(data)
        
        assert result['numeric'] == INFERRED_NUMERIC
        assert result['text'] == INFERRED_TEXT
        assert result['bool'] == INFERRED_BOOLEAN


class TestIsNumeric:
    """Tests for is_numeric function."""
    
    def test_is_numeric_with_integers(self):
        """Test is_numeric with integer values."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert is_numeric(series)[0] == True
    
    def test_is_numeric_with_floats(self):
        """Test is_numeric with float values."""
        series = pd.Series([1.1, 2.2, 3.3, 4.4, 5.5])
        assert is_numeric(series)[0] == True
    
    def test_is_numeric_with_mixed(self):
        """Test is_numeric with mixed numeric types."""
        series = pd.Series([1, 2.5, 3, 4.5, 5])
        assert is_numeric(series)[0] == True
    
    def test_is_numeric_with_strings(self):
        """Test is_numeric with string values."""
        series = pd.Series(['a', 'b', 'c'])
        assert is_numeric(series)[0] == False
    
    def test_is_numeric_with_empty(self):
        """Test is_numeric with empty series."""
        series = pd.Series([], dtype=float)
        assert is_numeric(series)[0] == False


class TestIsBoolean:
    """Tests for is_boolean function."""
    
    def test_is_boolean_with_bools(self):
        """Test is_boolean with boolean values."""
        series = pd.Series([True, False, True, False])
        assert is_boolean(series)[0] is True
    
    def test_is_boolean_with_01(self):
        """Test is_boolean with 0/1 values."""
        series = pd.Series([0, 1, 0, 1])
        assert is_boolean(series)[0] is True
    
    def test_is_boolean_with_yes_no(self):
        """Test is_boolean with yes/no values."""
        series = pd.Series(['yes', 'no', 'yes', 'no'])
        assert is_boolean(series)[0] is True
    
    def test_is_boolean_with_text(self):
        """Test is_boolean with text values."""
        series = pd.Series(['a', 'b', 'c'])
        assert is_boolean(series)[0] is False


class TestIsDatetime:
    """Tests for is_datetime function."""
    
    def test_is_datetime_with_datetime(self):
        """Test is_datetime with datetime values."""
        series = pd.Series(pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']))
        assert is_datetime(series)[0] == True
    
    def test_is_datetime_with_datetime_strings(self):
        """Test is_datetime with datetime string values."""
        series = pd.Series(['2023-01-01', '2023-01-02', '2023-01-03'])
        assert is_datetime(series)[0] == True
    
    def test_is_datetime_with_numeric(self):
        """Test is_datetime with numeric values."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert is_datetime(series)[0] == False


class TestIsCategorical:
    """Tests for is_categorical function."""
    
    def test_is_categorical_with_low_cardinality(self):
        """Test is_categorical with low cardinality."""
        series = pd.Series(['A', 'A', 'B', 'B', 'C'])
        assert is_categorical(series)[0] is True
    
    def test_is_categorical_with_high_cardinality(self):
        """Test is_categorical with high cardinality."""
        series = pd.Series(['a', 'b', 'c', 'd', 'e'])
        assert is_categorical(series)[0] is False


class TestIsText:
    """Tests for is_text function."""
    
    def test_is_text_with_unique_strings(self):
        """Test is_text with unique string values."""
        series = pd.Series(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
        assert is_text(series)[0] is True
    
    def test_is_text_with_numeric(self):
        """Test is_text with numeric values."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert is_text(series)[0] is False


class TestInferColumnType:
    """Tests for infer_column_type function."""
    
    def test_infer_column_type_numeric(self):
        """Test infer_column_type for numeric."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert infer_column_type(series) == INFERRED_NUMERIC
    
    def test_infer_column_type_boolean(self):
        """Test infer_column_type for boolean."""
        series = pd.Series([True, False, True])
        assert infer_column_type(series) == INFERRED_BOOLEAN
    
    def test_infer_column_type_datetime(self):
        """Test infer_column_type for datetime."""
        series = pd.Series(pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']))
        assert infer_column_type(series) == INFERRED_DATETIME
    
    def test_infer_column_type_categorical(self):
        """Test infer_column_type for categorical."""
        series = pd.Series(['A', 'A', 'B', 'B', 'B'])
        assert infer_column_type(series) == INFERRED_CATEGORICAL
    
    def test_infer_column_type_text(self):
        """Test infer_column_type for text."""
        series = pd.Series(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
        assert infer_column_type(series) == INFERRED_TEXT


class TestSchemaDetectorWithConfidence:
    """Tests for SchemaDetector.detect_with_confidence method."""
    
    def test_detect_with_confidence(self):
        """Test detect_with_confidence returns correct structure."""
        data = pd.DataFrame({'col': [1, 2, 3, 4, 5]})
        detector = SchemaDetector()
        result = detector.detect_with_confidence(data)
        
        assert 'col' in result
        assert isinstance(result['col'], tuple)
        assert len(result['col']) == 2
        assert isinstance(result['col'][0], str)  # type
        assert isinstance(result['col'][1], float)  # confidence
