"""Remove outliers transformation."""

import pandas as pd
import numpy as np

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class RemoveOutliersTransformation(BaseTransformation):
    """Transformation for removing outliers from numeric columns."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Remove outliers from the target column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with outliers removed.
        """
        method = self.params.get("method", "iqr")
        threshold = self.params.get("threshold", 1.5)
        column = self.target_columns[0]

        result = data.copy()
        
        # Convert to numeric first
        numeric_col = pd.to_numeric(data[column], errors='coerce')
        
        # Check if we have valid numeric data
        if numeric_col.isna().all():
            # No valid numeric data, skip transformation
            return result

        if method == "iqr":
            # IQR-based outlier removal
            Q1 = numeric_col.quantile(0.25)
            Q3 = numeric_col.quantile(0.75)
            IQR = Q3 - Q1

            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            mask = (numeric_col >= lower_bound) & (numeric_col <= upper_bound)
            result = result[mask]

            # Store bounds for reference
            self.set_metadata("lower_bound", lower_bound)
            self.set_metadata("upper_bound", upper_bound)
            self.set_metadata("method", "iqr")
            self.set_metadata("threshold", threshold)

        elif method == "zscore":
            # Z-score based outlier removal
            mean = numeric_col.mean()
            std = numeric_col.std()

            if std > 0:
                z_scores = np.abs((numeric_col - mean) / std)
                result = result[z_scores <= threshold]

            # Store stats for reference
            self.set_metadata("mean", mean)
            self.set_metadata("std", std)
            self.set_metadata("method", "zscore")
            self.set_metadata("threshold", threshold)

        # Store original indices for reference
        self.set_metadata("original_indices", result.index.tolist())

        return result

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse is not possible for outlier removal.

        Args:
            data: Transformed DataFrame.

        Raises:
            NotImplementedError: Outlier removal cannot be reversed.
        """
        raise NotImplementedError(
            "Cannot reverse remove_outliers transformation as "
            "rows have been permanently removed."
        )
