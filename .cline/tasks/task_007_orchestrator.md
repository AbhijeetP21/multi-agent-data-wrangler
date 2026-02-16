# Task: Orchestrator Module

**Worker**: orchestrator_worker  
**Directory**: `src/orchestrator/`  
**Priority**: HIGH

## Objective

Implement the orchestrator module that coordinates agents, manages pipeline state, and handles failures.

## Deliverables

### Core Components

- `interfaces.py` - Orchestrator Protocol
- `state_manager.py` - Pipeline state management (persist/load)
- `agent_coordinator.py` - Coordinate agent activities
- `pipeline_manager.py` - Pipeline execution orchestration
- `failure_recovery.py` - Error handling and recovery
- `cli.py` - Command-line interface
- `__init__.py` - Module exports

### Tests

- `tests/orchestrator/test_pipeline.py`
- `tests/integration/test_end_to_end.py`

## Interface Requirements

```python
class Orchestrator(Protocol):
    def run(self, config: PipelineConfig) -> PipelineResult: ...
    def recover(self, state: PipelineState) -> None: ...
```

## CLI Interface

```bash
# Run full pipeline
data-wrangler run --config config/pipeline.yaml

# Profile only
data-wrangler profile --input data/input.csv

# Generate candidates
data-wrangler generate --profile profile.json --output candidates/

# Validate
data-wrangler validate --candidates candidates/ --data data/input.csv

# Score and rank
data-wrangler rank --candidates candidates/ --profile profile.json

# Run specific agent
data-wrangler agent --name profiling --input data/input.csv
```

## Acceptance Criteria

1. Executes full pipeline in correct order
2. Manages pipeline state across executions
3. Handles failures gracefully with recovery
4. Provides CLI interface
5. Tests pass
