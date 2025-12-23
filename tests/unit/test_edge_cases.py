"""Edge case tests to ensure robustness."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from award_archive.api.client import SeatsAeroClient
from award_archive.storage.hashing import add_metadata_columns, compute_row_hash


class TestHashingEdgeCases:
    """Edge case tests for hashing functionality."""

    def test_hash_with_special_characters(self):
        """Should handle special characters in data."""
        row = pd.Series({"name": "Test™ © ® 日本語", "value": "emoji 🎉"})
        hash_value = compute_row_hash(row)
        assert len(hash_value) == 16

    def test_hash_with_very_long_strings(self):
        """Should handle very long strings."""
        row = pd.Series({"long_string": "x" * 100000})
        hash_value = compute_row_hash(row)
        assert len(hash_value) == 16

    def test_hash_with_numeric_types(self):
        """Should handle various numeric types."""
        row = pd.Series({
            "int_val": 42,
            "float_val": 3.14159,
            "neg_val": -100,
            "zero": 0,
        })
        hash_value = compute_row_hash(row)
        assert len(hash_value) == 16

    def test_hash_with_all_none_values(self):
        """Should handle row with all None values."""
        row = pd.Series({"a": None, "b": None, "c": None})
        hash_value = compute_row_hash(row)
        assert len(hash_value) == 16

    def test_metadata_with_empty_dataframe(self):
        """Should handle empty DataFrame."""
        df = pd.DataFrame(columns=["a", "b", "c"])
        result = add_metadata_columns(df)
        assert "ingestion_timestamp" in result.columns
        assert "content_hash" in result.columns
        assert len(result) == 0

    def test_metadata_with_single_row(self):
        """Should handle single-row DataFrame."""
        df = pd.DataFrame({"id": ["single"]})
        result = add_metadata_columns(df)
        assert len(result) == 1
        assert pd.notna(result["content_hash"].iloc[0])

    def test_metadata_columns_not_in_hash(self):
        """Metadata columns should be excluded from hash calculation."""
        df = pd.DataFrame({"id": ["test"]})
        result1 = add_metadata_columns(df)
        
        # Run again - different timestamp but same data
        import time
        time.sleep(0.01)  # Small delay to ensure different timestamp
        result2 = add_metadata_columns(df)
        
        # Hashes should be the same (timestamp excluded)
        assert result1["content_hash"].iloc[0] == result2["content_hash"].iloc[0]


class TestAPIClientEdgeCases:
    """Edge case tests for API client."""

    def test_client_with_whitespace_api_key(self):
        """Should work with API key that has whitespace (trimmed by API)."""
        client = SeatsAeroClient(api_key="  test_key  ")
        assert client.api_key == "  test_key  "  # Client doesn't trim

    @patch("award_archive.api.client.httpx.request")
    @patch("award_archive.api.client.time.sleep")
    def test_search_with_date_objects(self, mock_sleep, mock_request):
        """Should properly serialize date objects."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [],
            "count": 0,
            "hasMore": False,
            "cursor": 0,
        }
        mock_request.return_value = mock_response

        client = SeatsAeroClient(api_key="test_key")
        client.search(
            origin_airport="SFO",
            destination_airport="NRT",
            start_date=date(2025, 3, 15),
            end_date=date(2025, 3, 20),
        )

        call_kwargs = mock_request.call_args.kwargs
        params = call_kwargs["params"]
        assert params["start_date"] == "2025-03-15"
        assert params["end_date"] == "2025-03-20"

    @patch("award_archive.api.client.httpx.request")
    @patch("award_archive.api.client.time.sleep")
    def test_bulk_availability_with_all_optional_params(self, mock_sleep, mock_request):
        """Should handle all optional parameters."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [],
            "count": 0,
            "hasMore": False,
            "cursor": 0,
        }
        mock_request.return_value = mock_response

        client = SeatsAeroClient(api_key="test_key")
        client.get_bulk_availability(
            source="aeroplan",
            cabin="business",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 12, 31),
            origin_region="North America",
            destination_region="Asia",
            take=100,
            cursor=50,
            skip=10,
            include_filtered=True,
        )

        call_kwargs = mock_request.call_args.kwargs
        params = call_kwargs["params"]
        
        assert params["source"] == "aeroplan"
        assert params["cabin"] == "business"
        assert params["take"] == 100
        assert params["include_filtered"] is True


class TestDataFrameEdgeCases:
    """Edge case tests for DataFrame handling."""

    def test_dataframe_with_mixed_types(self):
        """Should handle DataFrame with mixed types in columns."""
        df = pd.DataFrame({
            "id": ["a", "b", "c"],
            "value": [1, "two", 3.0],  # Mixed types
            "flag": [True, False, None],
        })
        result = add_metadata_columns(df)
        assert len(result) == 3
        assert all(pd.notna(result["content_hash"]))

    def test_dataframe_with_duplicate_rows(self):
        """Duplicate rows should get same hash."""
        df = pd.DataFrame({
            "id": ["a", "a"],
            "value": [1, 1],
        })
        result = add_metadata_columns(df)
        # Same data = same hash
        assert result["content_hash"].iloc[0] == result["content_hash"].iloc[1]

    def test_dataframe_preserves_index(self):
        """Should preserve DataFrame index."""
        df = pd.DataFrame(
            {"value": [1, 2, 3]},
            index=["x", "y", "z"]
        )
        result = add_metadata_columns(df)
        assert list(result.index) == ["x", "y", "z"]


