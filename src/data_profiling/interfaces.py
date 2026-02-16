"""Data profiling interfaces."""

from typing import Protocol
import pandas as pd
from common.types import DataProfile


class DataProfiler(Protocol):
    """Protocol defining the data profiler interface."""
    
    def profile(self, data: pd.DataFrame) -> DataProfile:
        """
        Profile a DataFrame and return a DataProfile.
        
        Args:
            data: The pandas DataFrame to profile.
            
        Returns:
            DataProfile containing schema, missing values, and statistics.
        """
        ...
