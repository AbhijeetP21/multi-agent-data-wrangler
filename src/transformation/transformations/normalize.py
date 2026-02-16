"""Normalize transformation."""

import pandas as pd
import numpy as np

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class NormalizeTransformation(BaseTransformation):
    """Transformation for normalizing numeric columns."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Normalize the target column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with normalized values.
        """
        method = self.params.get("method", "standard")
        column = self.target_columns[0]

        result = data.copy()

        # Convert to numeric first, coercing errors to NaN
        numeric_col = pd.to_numeric(data[column], errors='coerce')
        
        # Replace infinity values with NaN
        numeric_col = numeric_col.replace([np.inf, -np.inf], np.nan)
        
        # Check if we have valid numeric data
        valid_count = numeric_col.notna().sum()
        if valid_count == 0:
            # No valid numeric data, skip transformation
            return result

        # Get valid (non-NaN) values for calculation
        valid_values = numeric_col.dropna()
        
        # Handle edge case: single value or constant column
        if len(valid_values) == 1:
            # Single value - for standard, set to 0; for minmax, set to 0.5
            if method == "standard":
                result[column] = 0
                self.set_metadata("mean", float(valid_values.iloc[0]))
                self.set_metadata("std", 0)
                self.set_metadata("method", "standard")
            else:
                result[column] = 0.5
                self.set_metadata("min", float(valid_values.iloc[0]))
                self.set_metadata("max", float(valid_values.iloc[0]))
                self.set_metadata("method", "minmax")
            return result

        if method == "standard":
            # Z-score normalization (standardization)
            mean = valid_values.mean()
            std = valid_values.std(ddof=1)  # Use sample std (ddof=1) for proper normalization

            # Handle edge case where std is 0 or NaN (constant column)
            if pd.isna(std) or std == 0:
                result[column] = 0
            else:
                result[column] = (numeric_col - mean) / std

            # Store metadata for reversal
            self.set_metadata("mean", float(mean) if not pd.isna(mean) else 0.0)
            self.set_metadata("std", float(std) if not pd.isna(std) else 1.0)
            self.set_metadata("method", "standard")

        elif method == "minmax":
            # Min-max normalization to [0, 1]
            min_val = valid_values.min()
            max_val = valid_values.max()

            # Handle edge cases
            if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
                result[column] = 0.5 if valid_count > 0 else 0
            else:
                result[column] = (numeric_col - min_val) / (max_val - min_val)

            # Store metadata for reversal
            self.set_metadata("min", float(min_val) if not pd.isna(min_val) else 0.0)
            self.set_metadata("max", float(max_val) if not pd.isna(max_val) else 1.0)
            self.set_metadata("method", "minmax")
        
        elif method == "robust":
            # Robust normalization using median and IQR
            median = valid_values.median()
            q75 = valid_values.quantile(0.75)
            q25 = valid_values.quantile(0.25)
            iqr = q75 - q25
            
            if pd.isna(iqr) or iqr == 0:
                result[column] = 0 if valid_count > 0 else np.nan
            else:
                result[column] = (numeric_col - median) / iqr
            
            self.set_metadata("median", float(median) if not pd.isna(median) else 0.0)
            self.set_metadata("iqr", float(iqr) if not pd.isna(iqr) else 1.0)
            self.set_metadata("method", "robust")

        return result

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse the normalization.

        Args:
            data: Transformed DataFrame.

        Returns:
            Original scale DataFrame.
        """
        method = self.params.get("method")

        result = data.copy()
        column = self.target_columns[0]

        if method == "standard":
            mean = self.get_metadata().get("mean", 0)
            std = self.get_metadata().get("std", 1)

            if std > 0:
                result[column] = data[column] * std + mean

        elif method == "minmax":
            min_val = self.get_metadata().get("min", 0)
            max_val = self.get_metadata().get("max", 1)

            if max_val > min_val:
                result[column] = data[column] * (max_val - min_val) + min_val

        return result
