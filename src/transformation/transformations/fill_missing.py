"""Fill missing value transformation."""

from typing import Any, Union

import pandas as pd
import numpy as np

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class FillMissingTransformation(BaseTransformation):
    """Transformation for filling missing values."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Fill missing values in the target column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with missing values filled.
        """
        strategy = self.params.get("strategy", "mean")
        column = self.target_columns[0]

        result = data.copy()
        series = result[column]
        
        # Handle both NaN and empty strings for object/string columns
        is_string_column = series.dtype == 'object' or pd.api.types.is_string_dtype(series)
        
        if is_string_column:
            # For string columns, treat empty strings as missing
            # Replace empty strings with NaN first
            series = series.replace('', np.nan)
            
            if strategy == "mode":
                # For strings, mode is the most frequent non-null value
                non_null = series.dropna()
                if len(non_null) > 0:
                    fill_value = non_null.mode()[0] if not non_null.mode().empty else ""
                else:
                    fill_value = ""
            elif strategy == "constant":
                fill_value = self.params.get("fill_value", "")
            elif strategy == "forward_fill":
                # Forward fill - propagate last valid value forward
                result[column] = series.fillna(method='ffill')
                self.set_metadata("fill_value", None)
                self.set_metadata("strategy", strategy)
                self.set_metadata("column", column)
                return result
            elif strategy == "backward_fill":
                # Backward fill - propagate next valid value backward
                result[column] = series.fillna(method='bfill')
                self.set_metadata("fill_value", None)
                self.set_metadata("strategy", strategy)
                self.set_metadata("column", column)
                return result
            else:
                # For numeric strategies on string columns, use empty string as default
                fill_value = ""
        else:
            # For numeric columns
            if strategy == "mean":
                fill_value = data[column].mean()
            elif strategy == "median":
                fill_value = data[column].median()
            elif strategy == "mode":
                mode_result = data[column].mode()
                fill_value = mode_result.iloc[0] if not mode_result.empty else 0
            elif strategy == "constant":
                fill_value = self.params.get("fill_value", 0)
            elif strategy == "forward_fill":
                result[column] = series.fillna(method='ffill')
                self.set_metadata("fill_value", None)
                self.set_metadata("strategy", strategy)
                self.set_metadata("column", column)
                return result
            elif strategy == "backward_fill":
                result[column] = series.fillna(method='bfill')
                self.set_metadata("fill_value", None)
                self.set_metadata("strategy", strategy)
                self.set_metadata("column", column)
                return result
            else:
                fill_value = 0

        # Handle NaN fill value
        if pd.isna(fill_value):
            fill_value = 0 if not is_string_column else ""
        
        # Apply fill
        result[column] = series.fillna(fill_value)

        # Store metadata for reversal
        self.set_metadata("fill_value", fill_value)
        self.set_metadata("strategy", strategy)
        self.set_metadata("column", column)
        self.set_metadata("is_string_column", is_string_column)

        return result

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse the fill missing operation.

        Note: This is a best-effort reversal and may not recover
        the exact original values if mean/median was used.

        Args:
            data: Transformed DataFrame.

        Returns:
            Original DataFrame with NaN restored.
        """
        # Cannot truly reverse fill_missing as we lost
        # information about which values were originally missing
        raise NotImplementedError(
            "Cannot reverse fill_missing transformation as "
            "original missing value positions are not preserved."
        )
