"""Unit tests for content hashing functionality."""

from datetime import datetime

import pandas as pd
import pytest

from award_archive.storage.hashing import add_metadata_columns, compute_row_hash


class TestComputeRowHash:
    """Tests for compute_row_hash function."""

    def test_deterministic(self):
        """Hash should be deterministic for same data."""
        row = pd.Series({"a": 1, "b": "test", "c": None})
        hash1 = compute_row_hash(row)
        hash2 = compute_row_hash(row)
        assert hash1 == hash2

    def test_different_for_different_data(self):
        """Hash should differ for different data."""
        row1 = pd.Series({"a": 1, "b": "test"})
        row2 = pd.Series({"a": 2, "b": "test"})
        assert compute_row_hash(row1) != compute_row_hash(row2)

    def test_excludes_columns(self):
        """Hash should ignore excluded columns."""
        row1 = pd.Series({"a": 1, "timestamp": "2024-01-01"})
        row2 = pd.Series({"a": 1, "timestamp": "2024-12-31"})
        hash1 = compute_row_hash(row1, exclude_cols=["timestamp"])
        hash2 = compute_row_hash(row2, exclude_cols=["timestamp"])
        assert hash1 == hash2

    def test_order_independent(self):
        """Hash should be independent of column order."""
        row1 = pd.Series({"a": 1, "b": 2})
        row2 = pd.Series({"b": 2, "a": 1})
        assert compute_row_hash(row1) == compute_row_hash(row2)

    def test_handles_none_values(self):
        """Hash should handle None values consistently."""
        row1 = pd.Series({"a": None, "b": "test"})
        row2 = pd.Series({"a": None, "b": "test"})
        assert compute_row_hash(row1) == compute_row_hash(row2)

    def test_returns_16_char_string(self):
        """Hash should return a 16-character hex string."""
        row = pd.Series({"a": 1})
        hash_value = compute_row_hash(row)
        assert len(hash_value) == 16
        assert all(c in "0123456789abcdef" for c in hash_value)


class TestAddMetadataColumns:
    """Tests for add_metadata_columns function."""

    @pytest.fixture
    def sample_df(self):
        """Sample DataFrame for testing."""
        return pd.DataFrame(
            {
                "ID": ["a1", "a2", "a3"],
                "value": [100, 200, 300],
                "name": ["foo", "bar", "baz"],
            }
        )

    def test_adds_ingestion_timestamp(self, sample_df):
        """Should add ingestion_timestamp column."""
        result = add_metadata_columns(sample_df)
        assert "ingestion_timestamp" in result.columns
        assert all(pd.notna(result["ingestion_timestamp"]))

    def test_adds_content_hash(self, sample_df):
        """Should add content_hash column."""
        result = add_metadata_columns(sample_df)
        assert "content_hash" in result.columns
        assert all(pd.notna(result["content_hash"]))

    def test_unique_hashes_for_different_rows(self, sample_df):
        """Hashes should be unique for rows with different data."""
        result = add_metadata_columns(sample_df)
        assert len(result["content_hash"].unique()) == len(result)

    def test_does_not_modify_original(self, sample_df):
        """Should not modify the original DataFrame."""
        original_cols = set(sample_df.columns)
        add_metadata_columns(sample_df)
        assert set(sample_df.columns) == original_cols

    def test_preserves_original_columns(self, sample_df):
        """Should preserve all original columns."""
        result = add_metadata_columns(sample_df)
        for col in sample_df.columns:
            assert col in result.columns
            assert list(result[col]) == list(sample_df[col])

    def test_timestamp_is_valid_iso_format(self, sample_df):
        """Timestamp should be valid ISO format."""
        result = add_metadata_columns(sample_df)
        for ts in result["ingestion_timestamp"]:
            # Should not raise
            datetime.fromisoformat(ts.replace("Z", "+00:00"))

