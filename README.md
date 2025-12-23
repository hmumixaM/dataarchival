# Award Archive

Data ingestion pipeline for award availability data with Delta Lake storage on S3.

## Features

- **Deduplication**: Uses Delta Lake MERGE to only store changed records
- **Content Hashing**: Detects actual data changes to avoid unnecessary updates
- **Partitioned Storage**: Partitions data by mileage program source
- **Automatic Optimization**: Compacts small files and vacuums old versions
- **Multi-Source**: Supports seats.aero flight availability and iPrefer hotel availability

## Project Structure

```
award-archive/
├── src/
│   └── award_archive/
│       ├── __init__.py
│       ├── api/                    # External API clients
│       │   ├── __init__.py
│       │   ├── seats_aero.py       # Seats.aero API client
│       │   └── iprefer.py          # iPrefer API client
│       ├── models/                 # Pydantic data models
│       │   ├── __init__.py
│       │   ├── seats_aero.py       # Flight availability models
│       │   ├── iprefer.py          # Hotel availability models
│       │   └── source.py           # Mileage program sources
│       ├── pipeline/               # Data ingestion pipelines
│       │   ├── __init__.py
│       │   ├── seats_aero.py       # Flight availability ingestion
│       │   └── iprefer.py          # Hotel availability ingestion
│       ├── storage/                # Delta Lake operations
│       │   ├── __init__.py
│       │   ├── credentials.py
│       │   ├── delta.py
│       │   └── hashing.py
│       ├── iprefer/                # Backwards compatibility exports
│       │   └── __init__.py
│       ├── app/                    # Streamlit explorer
│       │   ├── __init__.py
│       │   └── explorer.py
│       └── cli.py                  # Command-line interface
├── tests/
│   ├── unit/                       # Unit tests (no AWS needed)
│   └── integration/                # Integration tests (real S3)
├── pyproject.toml                  # uv dependencies
├── uv.lock                         # Locked versions
├── Dockerfile                      # Multi-stage builds
└── README.md
```

## Quick Start

### Using uv (recommended)

```bash
# Install dependencies
uv sync

# Run CLI
uv run award-archive seats --source aeroplan
uv run award-archive iprefer --max-hotels 10

# Run unit tests
uv run pytest tests/unit/

# Run all tests (requires AWS credentials)
uv run pytest
```

### Using Docker

```bash
# Build the production image
docker build -t award-archive .

# Run seats.aero ingestion
docker run -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx \
    -e SEATS_AERO_API_KEY=xxx \
    award-archive award-archive seats --source aeroplan

# Run iPrefer ingestion
docker run -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx \
    award-archive award-archive iprefer

# Build and run Streamlit app
docker build --target streamlit -t award-archive-app .
docker run -p 8501:8501 \
    -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx \
    award-archive-app
```

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AWS_ACCESS_KEY_ID` | AWS access key | Yes |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key | Yes |
| `AWS_REGION` | AWS region (default: us-east-1) | No |
| `SEATS_AERO_API_KEY` | Seats.aero API key | Yes (for seats command) |

## CLI Usage

The CLI uses subcommands to organize different data sources:

### Seats.aero (Flight Availability)

```bash
# Ingest from a specific source
uv run award-archive seats --source aeroplan

# Multiple sources
uv run award-archive seats --source aeroplan united

# With date filters
uv run award-archive seats --source united --start-date 2025-01-01 --end-date 2025-03-31

# With optimization after ingestion
uv run award-archive seats --source lifemiles --optimize
```

### iPrefer (Hotel Availability)

```bash
# Full pipeline (hotels + availability)
uv run award-archive iprefer

# Hotels only (no availability)
uv run award-archive iprefer --hotels-only

# Availability only
uv run award-archive iprefer --availability-only

# Points rates only (skip cash)
uv run award-archive iprefer --points-only

# Cash rates only (skip points)
uv run award-archive iprefer --cash-only

# Limit for testing
uv run award-archive iprefer --max-hotels 10

# Custom S3 paths
uv run award-archive iprefer \
    --hotels-path s3://my-bucket/hotels \
    --availability-path s3://my-bucket/availability
```

### Table Info

```bash
# Show Delta table metadata
uv run award-archive info s3://award-archive/availability
uv run award-archive info s3://award-archive/iprefer_hotels
```

## Python API

### Seats.aero

```python
from award_archive.api import SeatsAeroClient
from award_archive.pipeline import ingest_availability

# Direct API usage
client = SeatsAeroClient(api_key="your-key")
response = client.get_bulk_availability(source="aeroplan")

# Full pipeline
stats = ingest_availability(
    api_key="your-key",
    source="aeroplan",
    max_pages=5
)
```

### iPrefer

```python
from award_archive.pipeline import (
    ingest_iprefer,
    ingest_iprefer_hotels,
    ingest_iprefer_availability,
)

# Full pipeline (hotels + availability)
stats = ingest_iprefer(max_hotels=10)

# Hotels only
hotel_stats = ingest_iprefer_hotels(max_hotels=10)

# Availability only (with specific NIDs)
avail_stats = ingest_iprefer_availability(
    nids=[12345, 67890],
    include_points=True,
    include_cash=False,
)
```

### Delta Lake Operations

```python
from award_archive.storage import save_to_delta, get_table_info, optimize_table
import pandas as pd

# Save DataFrame with deduplication
df = pd.DataFrame({"ID": ["a1"], "value": [100]})
stats = save_to_delta(
    df,
    "s3://award-archive/my-table",
    mode="merge",
    merge_keys=["ID"]
)

# Get table metadata
info = get_table_info("s3://award-archive/my-table")

# Optimize table (compact files, vacuum)
optimize_table("s3://award-archive/my-table")
```

## Module Organization

The codebase follows a consistent structure for both data sources:

| Layer | Seats.aero | iPrefer |
|-------|------------|---------|
| **API Client** | `api/seats_aero.py` | `api/iprefer.py` |
| **Models** | `models/seats_aero.py` | `models/iprefer.py` |
| **Pipeline** | `pipeline/seats_aero.py` | `pipeline/iprefer.py` |

This separation ensures:
- **API clients** handle external communication with retry logic
- **Models** define Pydantic schemas for data validation
- **Pipelines** orchestrate fetching, transformation, and storage

## How Deduplication Works

1. **Content Hashing**: Each record gets a `content_hash` computed from its data fields
2. **MERGE Operation**: When ingesting, records are matched by merge keys
3. **Change Detection**: Only records with different `content_hash` are updated
4. **Storage Savings**: Duplicate ingestions don't increase storage

## Development

### Standards

This project follows specific development standards:

1. **Dependency Management**: All dependencies are managed via **uv**.
2. **HTTP Client**: Uses **httpx** for robust, timeout-aware requests.
3. **Fail Fast**: No broad `try-except` blocks; let exceptions propagate to identify issues immediately.
4. **Data Validation**: Uses **Pydantic** for all data models.

### Commands

```bash
# Install with dev dependencies
uv sync --all-extras

# Run linter
uv run ruff check src/

# Run tests with coverage
uv run pytest --cov=award_archive

# Format code
uv run ruff format src/
```

## License

MIT
