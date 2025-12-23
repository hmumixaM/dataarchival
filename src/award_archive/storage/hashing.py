"""Content hashing for deduplication."""

import hashlib
import json
from datetime import UTC, datetime

import pandas as pd


def compute_row_hash(row: pd.Series, exclude_cols: list[str] | None = None) -> str:
    """
    Compute a deterministic hash of a row's content for change detection.

    Args:
        row: A pandas Series representing a row.
        exclude_cols: Columns to exclude from hashing (e.g., timestamps).

    Returns:
        A 16-character hex string hash of the row content.
    """
    exclude_cols = exclude_cols or []
    # Create a dict of relevant columns, sorted for determinism
    data = {k: v for k, v in sorted(row.items()) if k not in exclude_cols}
    # Convert to JSON string for hashing (handles None, etc.)
    content = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def add_metadata_columns(
    df: pd.DataFrame,
    hash_exclude_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Add ingestion metadata columns to a DataFrame.

    Adds:
    - ingestion_timestamp: UTC timestamp when this record was ingested
    - content_hash: Hash of record content for deduplication

    Args:
        df: Input DataFrame.
        hash_exclude_cols: Columns to exclude from content hash.

    Returns:
        DataFrame with metadata columns added.
    """
    df = df.copy()

    # Add ingestion timestamp
    df["ingestion_timestamp"] = datetime.now(UTC).isoformat()

    # Compute content hash for each row (excluding metadata columns)
    exclude = (hash_exclude_cols or []) + ["ingestion_timestamp", "content_hash"]
    df["content_hash"] = df.apply(
        lambda row: compute_row_hash(row, exclude_cols=exclude),
        axis=1,
    )

    return df

