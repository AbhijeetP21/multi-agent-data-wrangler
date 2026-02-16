"""Cast type transformation."""

import pandas as pd

from src.common.types.transformation import Transformation
from src.transformation.transformations.base import BaseTransformation


class CastTypeTransformation(BaseTransformation):
    """Transformation for casting column types."""

    def apply(self, data: pd.DataFrame) -> pd.DataFrame:
        """Cast the target column to a different type.

        Args:
            data: Input DataFrame.

        Returns:
            DataFrame with cast type.
        """
        target_type = self.params.get("target_type", "numeric")
        column = self.target_columns[0]

        result = data.copy()

        # Store original dtype for reversal
        self.set_metadata("original_dtype", str(data[column].dtype))
        self.set_metadata("column", column)

        if target_type == "numeric":
            # Convert to numeric, coercing errors to NaN
            result[column] = pd.to_numeric(data[column], errors="coerce")
            self.set_metadata("target_type", "numeric")

        elif target_type == "datetime":
            # Convert to datetime
            result[column] = pd.to_datetime(data[column], errors="coerce")
            self.set_metadata("target_type", "datetime")

        elif target_type == "string":
            # Convert to string
            result[column] = data[column].astype(str)
            self.set_metadata("target_type", "string")

        elif target_type == "boolean":
            # Convert to boolean
            result[column] = data[column].astype(bool)
            self.set_metadata("target_type", "boolean")

        elif target_type == "category":
            # Convert to categorical
            result[column] = data[column].astype("category")
            self.set_metadata("target_type", "category")

        return result

    def reverse(self, data: pd.DataFrame) -> pd.DataFrame:
        """Reverse the type cast.

        Args:
            data: Transformed DataFrame.

        Returns:
            Original type DataFrame.
        """
        original_dtype = self.get_metadata().get("original_dtype", "object")
        column = self.target_columns[0]

        result = data.copy()

        # Try to restore original dtype
        try:
            result[column] = result[column].astype(original_dtype)
        except (ValueError, TypeError):
            # If direct conversion fails, convert through string
            result[column] = result[column].astype(str).astype(original_dtype)

        return result
