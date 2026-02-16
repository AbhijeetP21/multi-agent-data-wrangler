"""Encode categorical transformation."""

from typing import Any

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
            unique_values = sorted(data[column].dropna().unique())
            mapping = {val: idx for idx, val in enumerate(unique_values)}

            result[column] = data[column].map(mapping)

            # Store mapping for reversal
            self.set_metadata("mapping", mapping)
            self.set_metadata("method", "label")
            self.set_metadata("original_column", column)

        elif method == "onehot":
            # One-hot encoding - create binary columns for each category
            dummies = pd.get_dummies(data[column], prefix=column, drop_first=False)

            # Add one-hot columns and drop original
            result = pd.concat([data, dummies], axis=1)
            result = result.drop(columns=[column])

            # Store categories for reversal
            self.set_metadata("categories", list(dummies.columns))
            self.set_metadata("method", "onehot")
            self.set_metadata("original_column", column)

        return result

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
