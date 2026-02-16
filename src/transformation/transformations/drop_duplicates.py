"""Drop duplicates transformation."""

import pandas as pd

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class DropDuplicatesTransformation(BaseTransformation):
    """Transformation for removing duplicate rows."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows from the data.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with duplicates removed.
        """
        result = data.copy()

        # Get columns to check for duplicates (or use all if none specified)
        subset = self.target_columns if self.target_columns else None

        # Track which rows were kept
        duplicate_mask = data.duplicated(subset=subset, keep="first")
        self.set_metadata("duplicate_count", duplicate_mask.sum())

        # Drop duplicates
        result = result.drop_duplicates(subset=subset, keep="first")

        return result

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse is not possible for duplicate removal.

        Args:
            data: Transformed DataFrame.

        Raises:
            NotImplementedError: Duplicate removal cannot be reversed.
        """
        raise NotImplementedError(
            "Cannot reverse drop_duplicates transformation as "
            "duplicate rows have been permanently removed."
        )
