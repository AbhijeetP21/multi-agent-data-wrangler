"""Configuration loader for YAML files."""

import yaml
from pathlib import Path
from typing import Optional, Union

from .schemas import PipelineConfig


class ConfigLoader:
    """Loads and validates configuration from YAML files."""

    @staticmethod
    def load(config_path: Union[str, Path]) -> PipelineConfig:
        """Load configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file

        Returns:
            PipelineConfig: Validated pipeline configuration

        Raises:
            FileNotFoundError: If the config file doesn't exist
            ValueError: If the config file is invalid
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, "r") as f:
            config_dict = yaml.safe_load(f)

        if config_dict is None:
            raise ValueError(f"Empty configuration file: {config_path}")

        return PipelineConfig(**config_dict)

    @staticmethod
    def load_or_default(config_path: Optional[Union[str, Path]] = None) -> PipelineConfig:
        """Load configuration from a YAML file or return defaults.

        Args:
            config_path: Optional path to the YAML configuration file

        Returns:
            PipelineConfig: Validated pipeline configuration
        """
        if config_path is None:
            # Return default configuration
            return PipelineConfig(
                name="data_wrangler",
                version="1.0.0",
                data_source={"type": "csv", "path": None},
            )

        return ConfigLoader.load(config_path)

    @staticmethod
    def save(config: PipelineConfig, config_path: Union[str, Path]) -> None:
        """Save configuration to a YAML file.

        Args:
            config: Pipeline configuration to save
            config_path: Path to save the YAML configuration file
        """
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, "w") as f:
            yaml.dump(config.model_dump(), f, default_flow_style=False, sort_keys=False)
