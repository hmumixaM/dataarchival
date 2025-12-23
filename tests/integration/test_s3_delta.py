"""Integration tests for Delta Lake on S3."""

import os

import pytest
from deltalake import DeltaTable

from award_archive.storage import get_storage_options, get_table_info, save_to_delta
from tests.conftest import make_availability_df

requires_aws = pytest.mark.skipif(
    not (os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")),
    reason="AWS credentials not configured",
)


@requires_aws
class TestSaveToDelta:
    """Integration tests for save_to_delta."""

    def test_save_and_read(self, s3_test_path, sample_df):
        """Save and read back DataFrame."""
        stats = save_to_delta(sample_df, s3_test_path)

        assert stats["input_rows"] == 3
        assert stats["mode"] == "append"

        dt = DeltaTable(s3_test_path, storage_options=get_storage_options())
        result = dt.to_pandas()

        assert len(result) == 3
        assert "ingestion_timestamp" in result.columns
        assert "content_hash" in result.columns

    def test_overwrite(self, s3_test_path, sample_df):
        """Overwrite replaces all data."""
        save_to_delta(sample_df, s3_test_path, mode="overwrite")
        save_to_delta(make_availability_df(1), s3_test_path, mode="overwrite")

        result = DeltaTable(s3_test_path, storage_options=get_storage_options()).to_pandas()
        assert len(result) == 1


@requires_aws
class TestMergeDeduplication:
    """Integration tests for MERGE mode."""

    def test_initial_insert(self, s3_test_path, sample_df):
        """First merge creates table."""
        stats = save_to_delta(sample_df, s3_test_path, mode="merge", merge_keys=["ID"])

        assert stats["table_created"]
        assert len(DeltaTable(s3_test_path, storage_options=get_storage_options()).to_pandas()) == 3

    def test_skip_unchanged(self, s3_test_path, sample_df):
        """Unchanged records not duplicated."""
        save_to_delta(sample_df, s3_test_path, mode="merge", merge_keys=["ID"])
        save_to_delta(sample_df, s3_test_path, mode="merge", merge_keys=["ID"])

        result = DeltaTable(s3_test_path, storage_options=get_storage_options()).to_pandas()
        assert len(result) == 3

    def test_update_changed(self, s3_test_path, sample_df):
        """Changed records updated."""
        save_to_delta(sample_df, s3_test_path, mode="merge", merge_keys=["ID"])

        modified = sample_df.copy()
        modified.loc[modified["ID"] == "avail-000", "JMileageCost"] = "99999"
        save_to_delta(modified, s3_test_path, mode="merge", merge_keys=["ID"])

        result = DeltaTable(s3_test_path, storage_options=get_storage_options()).to_pandas()
        assert result[result["ID"] == "avail-000"]["JMileageCost"].iloc[0] == "99999"

    def test_mixed_insert_update(self, s3_test_path, sample_df):
        """Handle insert + update together."""
        save_to_delta(sample_df.iloc[:2], s3_test_path, mode="merge", merge_keys=["ID"])

        batch2 = sample_df.iloc[1:3].copy()
        batch2.loc[batch2["ID"] == "avail-001", "JMileageCost"] = "88888"
        save_to_delta(batch2, s3_test_path, mode="merge", merge_keys=["ID"])

        result = DeltaTable(s3_test_path, storage_options=get_storage_options()).to_pandas()
        assert len(result) == 3
        assert result[result["ID"] == "avail-001"]["JMileageCost"].iloc[0] == "88888"


@requires_aws
class TestTableInfo:
    """Tests for table info."""

    def test_get_info(self, s3_test_path, sample_df):
        """Get table metadata."""
        save_to_delta(sample_df, s3_test_path)

        info = get_table_info(s3_test_path)
        assert info["version"] >= 0
        assert info["files"] >= 1
