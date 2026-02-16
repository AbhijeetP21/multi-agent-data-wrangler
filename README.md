# Multi-Agent Data Wrangler

A production-grade, evaluation-driven data wrangling system using multi-agent architecture.

## Overview

This system provides a modular, extensible framework for automated data transformation and quality improvement. It uses a multi-agent architecture to:

1. **Profile** input data to understand schema, types, and quality issues
2. **Generate** candidate transformations based on the data profile
3. **Validate** transformations to ensure data integrity
4. **Score** transformations using quality metrics
5. **Rank** transformations by quality improvement

## Features

- **Data Profiling**: Schema inference, missing value detection, type detection, statistical summaries
- **Transformation Engine**: Deterministic transformations, candidate generation, transformation DAG
- **Validation Layer**: Data integrity checks, leakage detection
- **Quality Scoring**: Completeness, consistency, validity, uniqueness metrics
- **Ranking System**: Pluggable ranking policies, composite scoring
- **Orchestration**: Agent coordination, state management, failure recovery

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLI Entry Point                          │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Orchestrator Agent                          │
└─────────────────────────────────────────────────────────────────┘
                                │
        ┌───────────┬───────────┼───────────┬───────────┐
        ▼           ▼           ▼           ▼           ▼
┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐ ┌───────────┐
│   Data    │ │Transfor-  │ │ Validat-  │ │  Quality  │ │  Ranking  │
│ Profiling │ │  mation   │ │   ion     │ │  Scoring  │ │  System   │
└───────────┘ └───────────┘ └───────────┘ └───────────┘ └───────────┘
```

## Installation

```bash
# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

## Usage

### Run Full Pipeline

```bash
data-wrangler run --config config/pipeline.yaml
```

### Profile Data

```bash
data-wrangler profile --input data/input.csv
```

### Generate Candidates

```bash
data-wrangler generate --profile profile.json --output candidates/
```

### Validate

```bash
data-wrangler validate --candidates candidates/ --data data/input.csv
```

### Score and Rank

```bash
data-wrangler rank --candidates candidates/ --profile profile.json
```

## Configuration

See `config/pipeline.yaml` for configuration options.

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src
```

## Project Structure

```
src/
├── common/           # Shared types, config, logging, exceptions
├── data_profiling/   # Data profiling module
├── transformation/   # Transformation engine
├── validation/       # Validation layer
├── quality_scoring/ # Quality scoring
├── ranking/         # Ranking system
└── orchestrator/    # Orchestration and CLI
```

## Development

This project follows strict engineering standards:

- Python 3.11+ with type hints
- Pydantic for schema validation
- Structured logging
- Unit tests with pytest
- Config-driven experimentation
- Clear separation of concerns
- Idempotent components

## License

MIT
