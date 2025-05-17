# Zowatari

A DAG-based ETL library inspired by Dagster.

## Overview

Zowatari is a lightweight ETL (Extract, Transform, Load) framework designed to simplify data processing workflows. It provides a clear structure for defining, composing, and executing data pipelines.

Core concepts:

- **Pebble**: Individual functions decorated with `@pebble()` that perform a specific ETL task
- **Cement**: Groups pebble functions together with parameter passing between them
- **Construct**: Arranges cement functions in a specific sequence to form a complete workflow
- **Pass**: Executes a construct (either as a one-time `first_pass` or as a recurring `scheduled_pass`)

## Features

- Declarative pipeline definition
- Dependency tracking between steps
- Data validation using Pydantic models
- Persistence in PostgreSQL
- Comprehensive logging

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/zowatari.git
cd zowatari

# Install the package in development mode
pip install -e .
```

## Prerequisites

Zowatari requires a PostgreSQL database. You can set one up locally using Docker:

```bash
docker run -d --name zowatari-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=zowatari \
  -p 5432:5432 \
  postgres:14
```

## Usage Example

Here's a simple example of using Zowatari to create an ETL pipeline:

```python
from zowatari import pebble, cement, construct, first_pass
from zowatari.models.base import CementInstruction
from pydantic import BaseModel


# Define data models with Pydantic
class InputData(BaseModel):
    value: int


class OutputData(BaseModel):
    result: int


# Define pebble functions
@pebble(name="extract_data")
def extract_data() -> InputData:
    """Extract data from a source."""
    return InputData(value=42)


@pebble(name="transform_data")
def transform_data(data: InputData) -> OutputData:
    """Transform the input data."""
    return OutputData(result=data.value * 2)


@pebble(name="load_data")
def load_data(data: OutputData) -> None:
    """Load the data to a destination."""
    print(f"Loaded data: {data.result}")


# Define cement function to connect the pebbles
extract_transform_load = cement(
    name="extract_transform_load",
    pebble_instructions=[
        CementInstruction(
            pebble_name="extract_data",
            parameters={},
            order=1,
            depends_on=[],
        ),
        CementInstruction(
            pebble_name="transform_data",
            parameters={"data": "$extract_data"},
            order=2,
            depends_on=["extract_data"],
        ),
        CementInstruction(
            pebble_name="load_data",
            parameters={"data": "$transform_data"},
            order=3,
            depends_on=["transform_data"],
        ),
    ],
)

# Define construct function with the cement function
etl_pipeline = construct(
    name="etl_pipeline",
    cement_order=[("extract_transform_load", 1)],
)

# Execute the pipeline
first_pass("etl_pipeline")
```

## Database Integration

Zowatari uses a PostgreSQL database to store:

- Pipeline definitions (pebbles, cements, constructs)
- Execution logs
- Scheduling information

Initialize the database before using Zowatari:

```python
from zowatari.db import init_db

# Initialize the database
engine = init_db("postgresql://postgres:postgres@localhost:5432/zowatari")
```

## License

MIT