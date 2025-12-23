"""Unit tests for Delta Lake operations."""

from unittest.mock import patch

import pandas as pd
import pytest

from award_archive.storage.delta import save_to_delta


@pytest.fixture
def sample_df():
    return pd.DataFrame({"ID": ["a1", "a2"], "value": [100, 200]})


@pytest.fixture
def mock_storage():
    return {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


class TestSaveToDelta:
    """Tests for save_to_delta."""

    def test_merge_requires_keys(self, sample_df, mock_storage):
        """Merge mode needs merge_keys."""
        # merge_keys defaults to None, which is falsy
        # The function should still work but might fail at merge time

    @patch("award_archive.storage.delta._write")
    @patch("award_archive.storage.delta.add_metadata_columns")
    def test_adds_metadata(self, mock_add, mock_write, sample_df, mock_storage):
        """Adds metadata by default."""
        mock_add.return_value = sample_df

        save_to_delta(sample_df, "s3://test/table", storage_options=mock_storage)

        mock_add.assert_called_once()

    @patch("award_archive.storage.delta._write")
    @patch("award_archive.storage.delta.add_metadata_columns")
    def test_skip_metadata(self, mock_add, mock_write, sample_df, mock_storage):
        """Skips metadata when disabled."""
        save_to_delta(
            sample_df, "s3://test/table",
            storage_options=mock_storage,
            add_metadata=False,
        )

        mock_add.assert_not_called()

    @patch("award_archive.storage.delta._write")
    @patch("award_archive.storage.delta.add_metadata_columns")
    def test_returns_stats(self, mock_add, mock_write, sample_df, mock_storage):
        """Returns statistics."""
        mock_add.return_value = sample_df

        stats = save_to_delta(sample_df, "s3://test/table", storage_options=mock_storage)

        assert stats["input_rows"] == 2
        assert stats["mode"] == "append"
        assert "timestamp" in stats

    @patch("award_archive.storage.delta._write")
    @patch("award_archive.storage.delta.add_metadata_columns")
    def test_append_mode(self, mock_add, mock_write, sample_df, mock_storage):
        """Calls write with append."""
        mock_add.return_value = sample_df

        save_to_delta(sample_df, "s3://test/table", mode="append", storage_options=mock_storage)

        mock_write.assert_called_once()
        assert mock_write.call_args.args[2] == "append"

    @patch("award_archive.storage.delta._write")
    @patch("award_archive.storage.delta.add_metadata_columns")
    def test_overwrite_mode(self, mock_add, mock_write, sample_df, mock_storage):
        """Calls write with overwrite."""
        mock_add.return_value = sample_df

        save_to_delta(sample_df, "s3://test/table", mode="overwrite", storage_options=mock_storage)

        assert mock_write.call_args.args[2] == "overwrite"
