"""
Award Archive - Data ingestion pipeline for award availability data.

This package provides tools for:
- Fetching award availability from Seats.aero API
- Storing data in Delta Lake format on S3 with deduplication
- Querying stored data via DuckDB
"""

__version__ = "0.1.0"

from award_archive.api.client import SeatsAeroClient
from award_archive.storage.delta import get_table_info, optimize_table, save_to_delta

__all__ = [
    "save_to_delta",
    "get_table_info",
    "optimize_table",
    "SeatsAeroClient",
]

