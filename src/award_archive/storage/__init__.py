"""Storage module for Delta Lake operations."""

from award_archive.storage.credentials import get_storage_options
from award_archive.storage.delta import (
    get_table_info,
    optimize_table,
    save_to_delta,
)
from award_archive.storage.hashing import (
    add_metadata_columns,
    compute_row_hash,
)

__all__ = [
    "save_to_delta",
    "get_table_info",
    "optimize_table",
    "compute_row_hash",
    "add_metadata_columns",
    "get_storage_options",
]

