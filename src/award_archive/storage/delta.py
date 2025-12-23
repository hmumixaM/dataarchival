"""Delta Lake storage operations."""

import logging
import time
from datetime import UTC, datetime
from typing import Literal

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import TableNotFoundError

from award_archive.storage.credentials import get_storage_options
from award_archive.storage.hashing import add_metadata_columns

log = logging.getLogger(__name__)

# Retry settings for concurrent write conflicts
MAX_MERGE_RETRIES = 5
RETRY_DELAY_SECONDS = 2


def _write(
    df: pd.DataFrame,
    s3_path: str,
    mode: str,
    storage_options: dict,
    partition_by: list[str] | None,
) -> None:
    """Write DataFrame to Delta Lake."""
    # Convert object columns with all None to string to avoid Null type error
    for col in df.columns:
        if df[col].isna().all():
            df[col] = df[col].astype(str)

    write_deltalake(
        s3_path,
        df,
        mode=mode,
        storage_options=storage_options,
        partition_by=partition_by,
    )


def save_to_delta(
    df: pd.DataFrame,
    s3_path: str,
    mode: Literal["append", "overwrite", "merge"] = "append",
    merge_keys: list[str] | None = None,
    storage_options: dict | None = None,
    add_metadata: bool = True,
    partition_by: list[str] | None = None,
) -> dict:
    """Save DataFrame to Delta Lake on S3."""
    storage_options = storage_options or get_storage_options()

    if add_metadata:
        df = add_metadata_columns(df)

    stats = {
        "input_rows": len(df),
        "mode": mode,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    log.info(f"Saving {len(df)} rows to {s3_path} (mode={mode})")

    if mode == "merge":
        stats.update(_merge_to_delta(df, s3_path, merge_keys, storage_options, partition_by))
    else:
        _write(df, s3_path, mode, storage_options, partition_by)
        stats["rows_written"] = len(df)

    log.info(f"Save complete: {stats}")
    return stats


def _merge_to_delta(
    df: pd.DataFrame,
    s3_path: str,
    merge_keys: list[str],
    storage_options: dict,
    partition_by: list[str] | None,
) -> dict:
    """MERGE/upsert records using content_hash for change detection.

    Includes retry logic for concurrent write conflicts.
    """
    for attempt in range(1, MAX_MERGE_RETRIES + 1):
        try:
            return _execute_merge(df, s3_path, merge_keys, storage_options, partition_by)
        except Exception as e:
            error_msg = str(e).lower()
            is_conflict = "concurrent" in error_msg or "conflict" in error_msg

            if is_conflict and attempt < MAX_MERGE_RETRIES:
                delay = RETRY_DELAY_SECONDS * attempt
                log.warning(
                    f"Concurrent write conflict (attempt {attempt}/{MAX_MERGE_RETRIES}), "
                    f"retrying in {delay}s..."
                )
                time.sleep(delay)
            else:
                raise

    # Should not reach here, but just in case
    raise RuntimeError("Max retries exceeded for merge operation")


def _execute_merge(
    df: pd.DataFrame,
    s3_path: str,
    merge_keys: list[str],
    storage_options: dict,
    partition_by: list[str] | None,
) -> dict:
    """Execute a single merge attempt."""
    try:
        dt = DeltaTable(s3_path, storage_options=storage_options)
    except TableNotFoundError:
        log.info(f"Table doesn't exist, creating: {s3_path}")
        _write(df, s3_path, "overwrite", storage_options, partition_by)
        return {"rows_inserted": len(df), "table_created": True}

    predicate = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)

    (
        dt.merge(
            source=pa.Table.from_pandas(df),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update(
            predicate="target.content_hash != source.content_hash",
            updates={col: f"source.{col}" for col in df.columns},
        )
        .when_not_matched_insert_all()
        .execute()
    )

    return {"merge_completed": True, "table_created": False}


def optimize_table(
    s3_path: str,
    storage_options: dict | None = None,
    vacuum_hours: int = 168,
) -> dict:
    """Compact files and vacuum old versions."""
    storage_options = storage_options or get_storage_options()
    dt = DeltaTable(s3_path, storage_options=storage_options)

    log.info(f"Optimizing table: {s3_path}")

    optimize_result = dt.optimize.compact()
    vacuum_result = dt.vacuum(retention_hours=vacuum_hours, enforce_retention_duration=False)

    stats = {
        "s3_path": s3_path,
        "files_removed": optimize_result.get("numFilesRemoved", 0),
        "files_added": optimize_result.get("numFilesAdded", 0),
        "files_deleted": len(vacuum_result),
    }
    log.info(f"Optimization complete: {stats}")
    return stats


def get_table_info(s3_path: str, storage_options: dict | None = None) -> dict:
    """Get Delta table metadata."""
    storage_options = storage_options or get_storage_options()
    dt = DeltaTable(s3_path, storage_options=storage_options)

    return {
        "version": dt.version(),
        "files": len(dt.file_uris()),
        "schema": str(dt.schema()),
        "metadata": dt.metadata(),
    }
