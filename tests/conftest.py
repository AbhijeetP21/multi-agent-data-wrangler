"""
Pytest configuration and fixtures for Multi-Agent Data Wrangler tests.
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Generator

import pandas as pd
import pytest
import yaml


# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture
def project_root() -> Path:
    """Return the project root directory."""
    return PROJECT_ROOT


@pytest.fixture
def config_dir() -> Path:
    """Return the config directory."""
    return PROJECT_ROOT / "config"


@pytest.fixture
def data_dir() -> Path:
    """Return the data directory."""
    return PROJECT_ROOT / "data"


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """Return a sample DataFrame for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "score": [85.5, 92.0, 78.5, 88.0, 95.5],
        "category": ["A", "B", "A", "C", "B"],
    })


@pytest.fixture
def sample_dataframe_with_nulls() -> pd.DataFrame:
    """Return a sample DataFrame with null values for testing."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", None, "Charlie", "Diana", "Eve"],
        "age": [25, 30, None, 28, 32],
        "score": [85.5, 92.0, 78.5, None, 95.5],
        "category": ["A", "B", "A", "C", None],
    })


@pytest.fixture
def sample_csv_path(tmp_path: Path) -> Path:
    """Create a temporary CSV file and return its path."""
    csv_file = tmp_path / "sample.csv"
    df = pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 28, 32],
        "score": [85.5, 92.0, 78.5, 88.0, 95.5],
    })
    df.to_csv(csv_file, index=False)
    return csv_file


@pytest.fixture
def pipeline_config(config_dir: Path) -> dict[str, Any]:
    """Load and return the pipeline configuration."""
    config_path = config_dir / "pipeline.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture
def logging_config(config_dir: Path) -> dict[str, Any]:
    """Load and return the logging configuration."""
    config_path = config_dir / "logging.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture
def temp_output_dir(tmp_path: Path) -> Generator[Path, None, None]:
    """Create a temporary output directory."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    yield output_dir


@pytest.fixture
def sample_transformed_dataframe() -> pd.DataFrame:
    """Return an expected transformed DataFrame for comparison."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25.0, 30.0, 35.0, 28.0, 32.0],
        "score": [0.4, 0.8, 0.0, 0.6, 1.0],  # Normalized to [0, 1]
        "category_encoded": [0, 1, 0, 2, 1],  # Label encoded
    })


@pytest.fixture
def sample_quality_scores() -> dict[str, float]:
    """Return sample quality scores for testing."""
    return {
        "completeness": 0.85,
        "consistency": 0.92,
        "validity": 0.88,
        "uniqueness": 0.75,
    }


@pytest.fixture
def mock_data_profile() -> dict[str, Any]:
    """Return a mock data profile for testing."""
    return {
        "total_rows": 1000,
        "total_columns": 5,
        "columns": {
            "id": {"dtype": "int64", "null_count": 0, "null_pct": 0.0},
            "name": {"dtype": "object", "null_count": 5, "null_pct": 0.5},
            "age": {"dtype": "float64", "null_count": 10, "null_pct": 1.0},
            "score": {"dtype": "float64", "null_count": 3, "null_pct": 0.3},
            "category": {"dtype": "object", "null_count": 0, "null_pct": 0.0},
        },
    }
