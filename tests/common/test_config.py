"""Tests for common configuration."""

import pytest
import tempfile
import os

from src.common.config import (
    PipelineConfig,
    DataSourceConfig,
    ProfilingConfig,
    TransformationConfig,
    ValidationConfig,
    ScoringConfig,
    RankingConfig,
    LoggingConfig,
    ConfigLoader,
)


class TestConfigSchemas:
    """Tests for configuration schemas."""

    def test_data_source_config(self):
        """Test DataSourceConfig creation."""
        config = DataSourceConfig(
            type="csv",
            path="/path/to/data.csv",
            options={"delimiter": ","},
        )
        assert config.type == "csv"
        assert config.path == "/path/to/data.csv"

    def test_profiling_config_defaults(self):
        """Test ProfilingConfig default values."""
        config = ProfilingConfig()
        assert config.enabled is True
        assert config.sample_size is None
        assert config.compute_stats is True

    def test_transformation_config_defaults(self):
        """Test TransformationConfig default values."""
        config = TransformationConfig()
        assert config.max_candidates == 100
        assert "fill_missing" in config.allowed_types

    def test_validation_config_defaults(self):
        """Test ValidationConfig default values."""
        config = ValidationConfig()
        assert config.strict_mode is False
        assert config.check_schema is True

    def test_scoring_config_defaults(self):
        """Test ScoringConfig default values."""
        config = ScoringConfig()
        assert config.weights["completeness"] == 0.3
        assert config.weights["consistency"] == 0.2

    def test_ranking_config_defaults(self):
        """Test RankingConfig default values."""
        config = RankingConfig()
        assert config.enabled is True
        assert config.top_k == 10

    def test_logging_config_defaults(self):
        """Test LoggingConfig default values."""
        config = LoggingConfig()
        assert config.level == "INFO"
        assert config.format == "json"
        assert config.output == "stdout"

    def test_pipeline_config_creation(self):
        """Test PipelineConfig creation."""
        config = PipelineConfig(
            name="test_pipeline",
            version="1.0.0",
            data_source=DataSourceConfig(type="csv", path="/data.csv"),
        )
        assert config.name == "test_pipeline"
        assert config.version == "1.0.0"
        assert config.max_iterations == 10


class TestConfigLoader:
    """Tests for configuration loader."""

    def test_load_default_config(self):
        """Test loading default configuration."""
        config = ConfigLoader.load_or_default()
        assert config.name == "data_wrangler"
        assert config.version == "1.0.0"

    def test_load_from_file(self):
        """Test loading configuration from YAML file."""
        yaml_content = """
name: test_pipeline
version: 2.0.0
max_iterations: 5
data_source:
  type: parquet
  path: /data/file.parquet
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(yaml_content)
            temp_path = f.name

        try:
            config = ConfigLoader.load(temp_path)
            assert config.name == "test_pipeline"
            assert config.version == "2.0.0"
            assert config.max_iterations == 5
            assert config.data_source.type == "parquet"
        finally:
            os.unlink(temp_path)

    def test_load_nonexistent_file(self):
        """Test loading a non-existent configuration file."""
        with pytest.raises(FileNotFoundError):
            ConfigLoader.load("/nonexistent/config.yaml")

    def test_load_empty_file(self):
        """Test loading an empty configuration file."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("")
            temp_path = f.name

        try:
            with pytest.raises(ValueError):
                ConfigLoader.load(temp_path)
        finally:
            os.unlink(temp_path)

    def test_save_config(self):
        """Test saving configuration to a YAML file."""
        config = PipelineConfig(
            name="save_test",
            version="1.0.0",
            data_source=DataSourceConfig(type="csv", path="/data.csv"),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "config.yaml")
            ConfigLoader.save(config, config_path)

            # Load it back
            loaded = ConfigLoader.load(config_path)
            assert loaded.name == "save_test"
            assert loaded.data_source.type == "csv"
