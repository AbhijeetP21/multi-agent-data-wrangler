"""Encode categorical transformation."""

from typing import Any, List, Tuple

import pandas as pd
import numpy as np

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class EncodeCategoricalTransformation(BaseTransformation):
    """Transformation for encoding categorical variables."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Encode categorical values in the target column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with encoded values.
        """
        method = self.params.get("method", "label")
        column = self.target_columns[0]

        result = data.copy()

        if method == "label":
            # Label encoding - convert categories to integers
            # Get unique values and handle mixed types safely
            unique_values = self._get_sorted_unique_values(data[column])
            mapping = {val: idx for idx, val in enumerate(unique_values)}

            # Map values, handling unknown values as -1
            result[column] = data[column].map(lambda x: mapping.get(x, -1) if pd.notna(x) else np.nan)

            # Store mapping for reversal
            self.set_metadata("mapping", mapping)
            self.set_metadata("method", "label")
            self.set_metadata("original_column", column)

        elif method == "onehot":
            # One-hot encoding - create binary columns for each category
            # Handle NaN values by filling them first
            fill_value = self.params.get("fill_na", "Unknown")
            series_filled = data[column].fillna(fill_value)
            
            dummies = pd.get_dummies(series_filled, prefix=column, drop_first=False)

            # Add one-hot columns and drop original
            result = pd.concat([data, dummies], axis=1)
            result = result.drop(columns=[column])

            # Store categories for reversal
            self.set_metadata("categories", list(dummies.columns))
            self.set_metadata("method", "onehot")
            self.set_metadata("original_column", column)
            self.set_metadata("fill_na", fill_value)

        return result
    
    def _get_sorted_unique_values(self, series: pd.Series) -> List[Any]:
        """Get sorted unique values from a series, handling mixed types safely."""
        # Drop NaN values
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return []
        
        # Get unique values
        unique_vals = non_null.unique()
        
        # Try to sort numerically first, fall back to string sorting
        try:
            # Try numeric sort
            numeric_vals = []
            for v in unique_vals:
                try:
                    numeric_vals.append((float(v), v))
                except (ValueError, TypeError):
                    # Not numeric, use string
                    numeric_vals.append((float('inf'), v))
            
            numeric_vals.sort(key=lambda x: x[0])
            return [v[1] for v in numeric_vals]
        except Exception:
            # Fall back to string sorting with safe string conversion
            try:
                return sorted(unique_vals, key=lambda x: str(x))
            except Exception:
                # Last resort - return as is
                return list(unique_vals)

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse the encoding.

        Args:
            data: Transformed DataFrame.

        Returns:
            Original categorical DataFrame.
        """
        method = self.params.get("method")
        result = data.copy()

        if method == "label":
            mapping = self.get_metadata().get("mapping", {})
            reverse_mapping = {v: k for k, v in mapping.items()}

            column = self.target_columns[0]
            result[column] = data[column].map(reverse_mapping)

        elif method == "onehot":
            # Reverse one-hot encoding
            column = self.target_columns[0]
            categories = self.get_metadata().get("categories", [])

            # Find which column has value 1 for each row
            if categories:
                # Create a Series to hold the decoded values
                decoded = pd.Series(index=data.index, dtype=object)

                for cat in categories:
                    mask = data[cat] == 1
                    decoded[mask] = cat.replace(f"{column}_", "")

                # Drop one-hot columns and add original
                result = data.drop(columns=categories)
                result[column] = decoded

        return result
