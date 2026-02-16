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

        if method == "standard":
            # Z-score normalization (standardization)
            mean = data[column].mean()
            std = data[column].std()

            if std > 0:
                result[column] = (data[column] - mean) / std
            else:
                result[column] = 0

            # Store metadata for reversal
            self.set_metadata("mean", mean)
            self.set_metadata("std", std)
            self.set_metadata("method", "standard")

        elif method == "minmax":
            # Min-max normalization to [0, 1]
            min_val = data[column].min()
            max_val = data[column].max()

            if max_val > min_val:
                result[column] = (data[column] - min_val) / (max_val - min_val)
            else:
                result[column] = 0

            # Store metadata for reversal
            self.set_metadata("min", min_val)
            self.set_metadata("max", max_val)
            self.set_metadata("method", "minmax")

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
