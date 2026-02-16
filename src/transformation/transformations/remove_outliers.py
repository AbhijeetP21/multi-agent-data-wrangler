"""Remove outliers transformation."""

import pandas as pd
import numpy as np

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class RemoveOutliersTransformation(BaseTransformation):
    """Transformation for handling outliers in numeric columns."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Handle outliers in the target column.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with outliers handled (either removed or masked).
        """
        method = self.params.get("method", "iqr")
        threshold = self.params.get("threshold", 1.5)
        action = self.params.get("action", "mask")  # 'mask' or 'remove'
        fill_value = self.params.get("fill_value", np.nan)
        column = self.target_columns[0]

        result = data.copy()
        
        # Convert to numeric first
        numeric_col = pd.to_numeric(data[column], errors='coerce')
        
        # Check if we have valid numeric data
        if numeric_col.isna().all():
            # No valid numeric data, skip transformation
            return result

        if method == "iqr":
            # IQR-based outlier detection
            Q1 = numeric_col.quantile(0.25)
            Q3 = numeric_col.quantile(0.75)
            IQR = Q3 - Q1

            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR

            outlier_mask = (numeric_col < lower_bound) | (numeric_col > upper_bound)
            
            if action == "remove":
                # Remove rows with outliers
                result = result[~outlier_mask]
            else:
                # Mask outliers with fill_value (default: NaN)
                result[column] = numeric_col.where(~outlier_mask, fill_value)

            # Store bounds for reference
            self.set_metadata("lower_bound", float(lower_bound) if not pd.isna(lower_bound) else None)
            self.set_metadata("upper_bound", float(upper_bound) if not pd.isna(upper_bound) else None)
            self.set_metadata("method", "iqr")
            self.set_metadata("threshold", threshold)
            self.set_metadata("action", action)
            self.set_metadata("outlier_count", int(outlier_mask.sum()))

        elif method == "zscore":
            # Z-score based outlier detection
            mean = numeric_col.mean()
            std = numeric_col.std()

            if std > 0:
                z_scores = np.abs((numeric_col - mean) / std)
                outlier_mask = z_scores > threshold
                
                if action == "remove":
                    result = result[~outlier_mask]
                else:
                    # Mask outliers with fill_value
                    result[column] = numeric_col.where(~outlier_mask, fill_value)
                
                self.set_metadata("outlier_count", int(outlier_mask.sum()))
            else:
                # All values are the same (std=0), no outliers
                self.set_metadata("outlier_count", 0)

            # Store stats for reference
            self.set_metadata("mean", float(mean) if not pd.isna(mean) else None)
            self.set_metadata("std", float(std) if not pd.isna(std) else None)
            self.set_metadata("method", "zscore")
            self.set_metadata("threshold", threshold)
            self.set_metadata("action", action)

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
