"""Pipeline state management."""

import json
import pickle
from pathlib import Path
from typing import Optional
from datetime import datetime

from src.common.types.pipeline import PipelineState, PipelineStep


class StateManager:
    """Manages pipeline state persistence and recovery."""

    def __init__(self, state_dir: str = ".state"):
        """Initialize the state manager.

        Args:
            state_dir: Directory to store state files
        """
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._current_state: Optional[PipelineState] = None

    def save_state(self, state: PipelineState, name: str = "default") -> Path:
        """Save pipeline state to disk.

        Args:
            state: The pipeline state to save
            name: Name identifier for the state

        Returns:
            Path to the saved state file
        """
        state_file = self.state_dir / f"{name}_state.json"
        
        # Convert state to dict for JSON serialization
        state_dict = {
            "current_step": state.current_step.value,
            "completed_steps": [s.value for s in state.completed_steps],
            "data_profile": state.data_profile.model_dump_json() if state.data_profile else None,
            "candidates": [c.model_dump_json() for c in state.candidates],
            "ranked_transformations": [r.model_dump_json() for r in state.ranked_transformations],
            "error": state.error,
            "saved_at": datetime.now().isoformat(),
        }
        
        with open(state_file, "w") as f:
            json.dump(state_dict, f, indent=2)
        
        return state_file

    def load_state(self, name: str = "default") -> Optional[PipelineState]:
        """Load pipeline state from disk.

        Args:
            name: Name identifier for the state

        Returns:
            The loaded pipeline state, or None if not found
        """
        state_file = self.state_dir / f"{name}_state.json"
        
        if not state_file.exists():
            return None
        
        with open(state_file, "r") as f:
            state_dict = json.load(f)
        
        # Import here to avoid circular imports
        from src.common.types.data_profile import DataProfile
        from src.common.types.ranking import TransformationCandidate, RankedTransformation
        
        # Reconstruct the state
        state = PipelineState(
            current_step=PipelineStep(state_dict["current_step"]),
            completed_steps=[PipelineStep(s) for s in state_dict["completed_steps"]],
            data_profile=DataProfile.model_validate_json(state_dict["data_profile"]) if state_dict.get("data_profile") else None,
            candidates=[TransformationCandidate.model_validate_json(c) for c in state_dict.get("candidates", [])],
            ranked_transformations=[RankedTransformation.model_validate_json(r) for r in state_dict.get("ranked_transformations", [])],
            error=state_dict.get("error"),
        )
        
        self._current_state = state
        return state

    def get_current_state(self) -> Optional[PipelineState]:
        """Get the current in-memory state.

        Returns:
            The current pipeline state
        """
        return self._current_state

    def set_current_state(self, state: PipelineState) -> None:
        """Set the current in-memory state.

        Args:
            state: The pipeline state to set
        """
        self._current_state = state

    def clear_state(self, name: str = "default") -> None:
        """Clear saved state from disk.

        Args:
            name: Name identifier for the state to clear
        """
        state_file = self.state_dir / f"{name}_state.json"
        if state_file.exists():
            state_file.unlink()
        
        if self._current_state is not None:
            self._current_state = None

    def list_states(self) -> list[str]:
        """List all saved state files.

        Returns:
            List of state names
        """
        return [f.stem.replace("_state", "") for f in self.state_dir.glob("*_state.json")]

    def has_state(self, name: str = "default") -> bool:
        """Check if a state exists.

        Args:
            name: Name identifier for the state

        Returns:
            True if state exists, False otherwise
        """
        state_file = self.state_dir / f"{name}_state.json"
        return state_file.exists()
