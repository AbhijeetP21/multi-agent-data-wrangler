"""Quality metrics comparator."""

from src.common.types import QualityMetrics, QualityDelta


class Comparator:
    """Comparator for quality metrics to compute deltas."""

    def compare(self, before: QualityMetrics, after: QualityMetrics) -> QualityDelta:
        """
        Compare quality metrics before and after transformation.

        Args:
            before: Quality metrics before transformation.
            after: Quality metrics after transformation.

        Returns:
            QualityDelta showing the difference between before and after.
        """
        # Calculate improvement (after - before)
        improvement = QualityMetrics(
            completeness=after.completeness - before.completeness,
            consistency=after.consistency - before.consistency,
            validity=after.validity - before.validity,
            uniqueness=after.uniqueness - before.uniqueness,
            overall=after.overall - before.overall,
        )

        # Calculate composite delta (overall after - overall before)
        composite_delta = after.overall - before.overall

        return QualityDelta(
            before=before,
            after=after,
            improvement=improvement,
            composite_delta=composite_delta,
        )
