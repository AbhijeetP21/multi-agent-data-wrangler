"""Transformation candidate generator."""

import uuid
from typing import Optional

from src.common.types.data_profile import DataProfile, ColumnProfile
from src.common.types.transformation import Transformation, TransformationType


class CandidateGenerator:
    """Generates transformation candidates based on data profile."""

    def generate_candidates(self, profile: DataProfile) -> list[Transformation]:
        """Generate transformation candidates based on the data profile.

        Args:
            profile: The data profile to generate candidates from.

        Returns:
            List of transformation candidates.
        """
        candidates = []

        # Check for missing values and generate fill_missing candidates
        candidates.extend(self._generate_fill_missing_candidates(profile))

        # Generate normalize candidates for numeric columns
        candidates.extend(self._generate_normalize_candidates(profile))

        # Generate encode_categorical candidates
        candidates.extend(self._generate_encode_categorical_candidates(profile))

        # Generate remove_outliers candidates
        candidates.extend(self._generate_remove_outliers_candidates(profile))

        # Generate drop_duplicates candidate
        if profile.duplicate_rows > 0:
            candidates.append(self._create_drop_duplicates_candidate())

        # Generate cast_type candidates for columns with type issues
        candidates.extend(self._generate_cast_type_candidates(profile))

        return candidates

    def _generate_fill_missing_candidates(
        self, profile: DataProfile
    ) -> list[Transformation]:
        """Generate fill missing value transformation candidates."""
        candidates = []

        for col_name, col_profile in profile.columns.items():
            if col_profile.null_count > 0:
                # Determine fill strategy based on column type
                if col_profile.inferred_type == "numeric":
                    if col_profile.mean is not None:
                        # Use mean for numeric columns
                        candidates.append(
                            Transformation(
                                id=str(uuid.uuid4()),
                                type=TransformationType.FILL_MISSING,
                                target_columns=[col_name],
                                params={"strategy": "mean"},
                                reversible=True,
                                description=f"Fill missing values in {col_name} with mean",
                            )
                        )
                    candidates.append(
                        Transformation(
                            id=str(uuid.uuid4()),
                            type=TransformationType.FILL_MISSING,
                            target_columns=[col_name],
                            params={"strategy": "median"},
                            reversible=True,
                            description=f"Fill missing values in {col_name} with median",
                        )
                    )
                elif col_profile.inferred_type == "categorical":
                    candidates.append(
                        Transformation(
                            id=str(uuid.uuid4()),
                            type=TransformationType.FILL_MISSING,
                            target_columns=[col_name],
                            params={"strategy": "mode"},
                            reversible=True,
                            description=f"Fill missing values in {col_name} with mode",
                        )
                    )

                # Always add constant fill option
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.FILL_MISSING,
                        target_columns=[col_name],
                        params={"strategy": "constant", "fill_value": 0},
                        reversible=True,
                        description=f"Fill missing values in {col_name} with constant",
                    )
                )

        return candidates

    def _generate_normalize_candidates(
        self, profile: DataProfile
    ) -> list[Transformation]:
        """Generate normalization transformation candidates."""
        candidates = []

        for col_name, col_profile in profile.columns.items():
            if col_profile.inferred_type == "numeric":
                # Standard normalization (z-score)
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.NORMALIZE,
                        target_columns=[col_name],
                        params={"method": "standard"},
                        reversible=True,
                        description=f"Standard normalize {col_name} (z-score)",
                    )
                )

                # Min-max normalization
                if col_profile.min_value is not None and col_profile.max_value is not None:
                    candidates.append(
                        Transformation(
                            id=str(uuid.uuid4()),
                            type=TransformationType.NORMALIZE,
                            target_columns=[col_name],
                            params={"method": "minmax"},
                            reversible=True,
                            description=f"Min-max normalize {col_name}",
                        )
                    )

        return candidates

    def _generate_encode_categorical_candidates(
        self, profile: DataProfile
    ) -> list[Transformation]:
        """Generate categorical encoding transformation candidates."""
        candidates = []

        for col_name, col_profile in profile.columns.items():
            if col_profile.inferred_type == "categorical":
                # One-hot encoding
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.ENCODE_CATEGORICAL,
                        target_columns=[col_name],
                        params={"method": "onehot"},
                        reversible=False,
                        description=f"One-hot encode {col_name}",
                    )
                )

                # Label encoding
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.ENCODE_CATEGORICAL,
                        target_columns=[col_name],
                        params={"method": "label"},
                        reversible=True,
                        description=f"Label encode {col_name}",
                    )
                )

        return candidates

    def _generate_remove_outliers_candidates(
        self, profile: DataProfile
    ) -> list[Transformation]:
        """Generate outlier removal transformation candidates."""
        candidates = []

        for col_name, col_profile in profile.columns.items():
            if col_profile.inferred_type == "numeric" and col_profile.std is not None:
                # IQR-based outlier removal
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.REMOVE_OUTLIERS,
                        target_columns=[col_name],
                        params={"method": "iqr", "threshold": 1.5},
                        reversible=False,
                        description=f"Remove outliers from {col_name} using IQR",
                    )
                )

                # Z-score based outlier removal
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.REMOVE_OUTLIERS,
                        target_columns=[col_name],
                        params={"method": "zscore", "threshold": 3.0},
                        reversible=False,
                        description=f"Remove outliers from {col_name} using z-score",
                    )
                )

        return candidates

    def _create_drop_duplicates_candidate(self) -> Transformation:
        """Create a drop duplicates transformation candidate."""
        return Transformation(
            id=str(uuid.uuid4()),
            type=TransformationType.DROP_DUPLICATES,
            target_columns=[],
            params={},
            reversible=False,
            description="Remove duplicate rows",
        )

    def _generate_cast_type_candidates(
        self, profile: DataProfile
    ) -> list[Transformation]:
        """Generate type casting transformation candidates."""
        candidates = []

        for col_name, col_profile in profile.columns.items():
            # Check for datetime conversion candidates
            if col_profile.inferred_type == "text":
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.CAST_TYPE,
                        target_columns=[col_name],
                        params={"target_type": "datetime"},
                        reversible=True,
                        description=f"Cast {col_name} to datetime",
                    )
                )

            # Check for numeric conversion candidates
            if col_profile.inferred_type == "text" and col_profile.null_count == 0:
                candidates.append(
                    Transformation(
                        id=str(uuid.uuid4()),
                        type=TransformationType.CAST_TYPE,
                        target_columns=[col_name],
                        params={"target_type": "numeric"},
                        reversible=True,
                        description=f"Cast {col_name} to numeric",
                    )
                )

        return candidates
