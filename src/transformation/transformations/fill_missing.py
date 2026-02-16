"""Fill missing value transformation."""

from typing import Any

import pandas as pd

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

        if strategy == "mean":
            fill_value = data[column].mean()
        elif strategy == "median":
            fill_value = data[column].median()
        elif strategy == "mode":
            fill_value = data[column].mode()[0] if not data[column].mode().empty else None
        elif strategy == "constant":
            fill_value = self.params.get("fill_value", 0)
        else:
            fill_value = 0

        result[column] = result[column].fillna(fill_value)

        # Store metadata for reversal
        self.set_metadata("fill_value", fill_value)
        self.set_metadata("strategy", strategy)
        self.set_metadata("column", column)

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
