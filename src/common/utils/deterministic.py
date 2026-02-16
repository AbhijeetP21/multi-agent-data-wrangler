"""Deterministic utilities for seed management and reproducibility."""

import random
from typing import Any, Optional, Sequence

import numpy as np


class DeterministicManager:
    """Manager for ensuring deterministic execution."""

    _seed: Optional[int] = None
    _original_random_state: Optional[random.Random] = None
    _original_numpy_state: Optional[str] = None

    @classmethod
    def set_seed(cls, seed: int) -> None:
        """Set the random seed for reproducibility.

        Args:
            seed: The seed value to use
        """
        cls._seed = seed

        # Store original states
        cls._original_random_state = random.getstate()
        cls._original_numpy_state = np.random.get_state()

        # Set new seeds
        random.seed(seed)
        np.random.seed(seed)

    @classmethod
    def get_seed(cls) -> Optional[int]:
        """Get the current seed value.

        Returns:
            The current seed value, or None if not set
        """
        return cls._seed

    @classmethod
    def reset(cls) -> None:
        """Reset to the original random state."""
        if cls._original_random_state is not None:
            random.setstate(cls._original_random_state)

        if cls._original_numpy_state is not None:
            np.random.set_state(cls._original_numpy_state)

        cls._seed = None
        cls._original_random_state = None
        cls._original_numpy_state = None


def set_random_seed(seed: int) -> None:
    """Set the random seed for reproducibility.

    This is a convenience function that wraps DeterministicManager.

    Args:
        seed: The seed value to use
    """
    DeterministicManager.set_seed(seed)


def get_random_seed() -> Optional[int]:
    """Get the current seed value.

    Returns:
        The current seed value, or None if not set
    """
    return DeterministicManager.get_seed()


def reset_random_state() -> None:
    """Reset to the original random state."""
    DeterministicManager.reset()


def sample_without_replacement(
    population: Sequence[Any],
    k: int,
    seed: Optional[int] = None,
) -> list[Any]:
    """Sample k items from population without replacement.

    Args:
        population: The population to sample from
        k: Number of items to sample
        seed: Optional seed for reproducibility

    Returns:
        A list of k sampled items
    """
    if seed is not None:
        DeterministicManager.set_seed(seed)

    try:
        return random.sample(population, k)
    finally:
        if seed is not None:
            DeterministicManager.reset()
