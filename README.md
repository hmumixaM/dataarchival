# Award Archive

Data ingestion pipeline for award availability data with Delta Lake storage on S3.

## Features

- **Deduplication**: Uses Delta Lake MERGE to only store changed records
- **Content Hashing**: Detects actual data changes to avoid unnecessary updates
- **Partitioned Storage**: Partitions data by mileage program source
- **Automatic Optimization**: Compacts small files and vacuums old versions

## Project Structure

```
award-archive/
├── src/
│   └── award_archive/
│       ├── __init__.py
│       ├── api/                    # Seats.aero API client
│       │   ├── __init__.py
│       │   └── client.py
│       ├── models/                 # Pydantic data models
│       │   ├── __init__.py
│       │   └── availability.py
│       ├── storage/                # Delta Lake storage operations
│       │   ├── __init__.py
│       │   ├── credentials.py
│       │   ├── delta.py
│       │   └── hashing.py
│       ├── pipeline/               # Data ingestion pipeline
│       │   ├── __init__.py
│       │   └── ingest.py
│       ├── app/                    # Streamlit explorer
│       │   ├── __init__.py
│       │   └── explorer.py
│       └── cli.py                  # Command-line interface
├── tests/
│   ├── unit/                       # Unit tests (no AWS needed)
│   │   ├── test_hashing.py
│   │   └── test_models.py
│   └── integration/                # Integration tests (real S3)
│       └── test_s3_delta.py
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
uv run award-archive --source aeroplan

# Run unit tests
uv run pytest tests/unit/

# Run all tests (requires AWS credentials)
uv run pytest
```

### Using Docker

```bash
# Build the production image
docker build -t award-archive .

# Run ingestion
docker run -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx \
    -e SEATS_AERO_API_KEY=xxx \
    award-archive award-archive --source aeroplan

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
| `SEATS_AERO_API_KEY` | Seats.aero API key | Yes (for ingestion) |

## CLI Usage

```bash
# Ingest from a specific source
uv run award-archive --source aeroplan

# With date filters
uv run award-archive --source united --start-date 2025-01-01 --end-date 2025-03-31

# With optimization after ingestion
uv run award-archive --source lifemiles --optimize

# Show table info
uv run award-archive --source aeroplan --info
```

## Python API

```python
from award_archive import SeatsAeroClient, save_to_delta
from award_archive.pipeline import ingest_availability

# Direct API usage
client = SeatsAeroClient(api_key="your-key")
response = client.get_bulk_availability(source="aeroplan")

# Save DataFrame to Delta Lake with deduplication
import pandas as pd
df = pd.DataFrame({"ID": ["a1"], "value": [100]})
stats = save_to_delta(
    df, 
    "s3://award-archive/my-table",
    mode="merge",
    merge_keys=["ID"]
)

# Full pipeline
stats = ingest_availability(
    api_key="your-key",
    source="aeroplan",
    max_pages=5
)
```

## How Deduplication Works

1. **Content Hashing**: Each record gets a `content_hash` computed from its data fields
2. **MERGE Operation**: When ingesting, records are matched by ID
3. **Change Detection**: Only records with different `content_hash` are updated
4. **Storage Savings**: Duplicate ingestions don't increase storage

## Development

### Standards

This project follows specific development standards:

1.  **Dependency Management**: All dependencies are managed via **uv**.
2.  **HTTP Client**: Uses **httpx** for robust, timeout-aware requests.
3.  **Fail Fast**: No broad `try-except` blocks; let exceptions propagate to identify issues immediately.

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
