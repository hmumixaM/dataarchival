"""Unit tests for iPrefer pipeline."""

import pandas as pd
import pytest

from award_archive.models.iprefer import (
    HotelAvailability,
    HotelDetails,
    HotelItem,
    RateCalendarResponse,
    RateCalendarResult,
)
from award_archive.pipeline.iprefer import flatten_availability, flatten_hotel_details


class TestHotelItem:
    """Tests for HotelItem model."""

    def test_url_property(self):
        """Should construct full URL from site and path."""
        item = HotelItem(
            site="https://preferredhotels.com",
            path="/hotels/japan/hotel-chinzanso-tokyo",
            title="Hotel Chinzanso Tokyo",
        )
        assert item.url == "https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo"

    def test_url_handles_trailing_slash(self):
        """Should handle trailing slash in site."""
        item = HotelItem(
            site="https://preferredhotels.com/",
            path="/hotels/japan/hotel-chinzanso-tokyo",
            title="Hotel Chinzanso Tokyo",
        )
        assert item.url == "https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo"

    def test_url_handles_missing_leading_slash(self):
        """Should handle missing leading slash in path."""
        item = HotelItem(
            site="https://preferredhotels.com",
            path="hotels/japan/hotel-chinzanso-tokyo",
            title="Hotel Chinzanso Tokyo",
        )
        assert item.url == "https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo"


class TestHotelDetails:
    """Tests for HotelDetails model."""

    def test_minimal_hotel_details(self):
        """Should accept minimal required fields."""
        details = HotelDetails(
            nid=12345,
            url="https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo",
        )
        assert details.nid == 12345
        assert details.name is None
        assert details.country is None

    def test_full_hotel_details(self):
        """Should accept all fields."""
        details = HotelDetails(
            nid=12345,
            name="Hotel Chinzanso Tokyo",
            url="https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo",
            canonical_url="https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo",
            code="CHIN",
            num_rooms=267,
            country="Japan",
            title="Hotel Chinzanso Tokyo - Luxury Hotel in Tokyo",
            description="A luxury hotel in Tokyo",
            choice_points="15000",
            average_rate="$350",
            synxis_id="SYN123",
            book_with_points=True,
            book_with_choice=True,
        )
        assert details.nid == 12345
        assert details.name == "Hotel Chinzanso Tokyo"
        assert details.country == "Japan"
        assert details.book_with_points is True


class TestHotelAvailability:
    """Tests for HotelAvailability model."""

    def test_points_availability(self):
        """Should create points availability record."""
        avail = HotelAvailability(
            nid=12345,
            currency_code="USD",
            date="2025-03-15",
            is_points=True,
            lowest_points_rate=15000,
            available=True,
        )
        assert avail.nid == 12345
        assert avail.is_points is True
        assert avail.lowest_points_rate == 15000

    def test_cash_availability(self):
        """Should create cash availability record."""
        avail = HotelAvailability(
            nid=12345,
            currency_code="USD",
            date="2025-03-15",
            is_points=False,
            lowest_rate=350.00,
            available=True,
        )
        assert avail.nid == 12345
        assert avail.is_points is False
        assert avail.lowest_rate == 350.00


class TestRateCalendarResponse:
    """Tests for RateCalendarResponse model."""

    def test_parse_response(self):
        """Should parse API response."""
        data = {
            "count": 2,
            "currency_code": "USD",
            "results": {
                "2025-03-15": {
                    "lowestRate": 350.00,
                    "lowestPointsRate": 15000,
                    "available": True,
                },
                "2025-03-16": {
                    "lowestRate": 400.00,
                    "lowestPointsRate": 18000,
                    "available": True,
                },
            },
        }
        response = RateCalendarResponse.model_validate(data)
        assert response.count == 2
        assert response.currency_code == "USD"
        assert len(response.results) == 2
        assert response.results["2025-03-15"].lowestRate == 350.00

    def test_empty_results(self):
        """Should handle empty results."""
        data = {"count": 0, "results": {}}
        response = RateCalendarResponse.model_validate(data)
        assert response.count == 0
        assert len(response.results) == 0


class TestFlattenHotelDetails:
    """Tests for flatten_hotel_details function."""

    @pytest.fixture
    def sample_hotels(self):
        """Sample hotel details."""
        return [
            HotelDetails(
                nid=12345,
                name="Hotel Chinzanso Tokyo",
                url="https://preferredhotels.com/hotels/japan/hotel-chinzanso-tokyo",
                country="Japan",
                num_rooms=267,
                book_with_points=True,
            ),
            HotelDetails(
                nid=67890,
                name="The Carlyle",
                url="https://preferredhotels.com/hotels/usa/the-carlyle",
                country="USA",
                num_rooms=190,
                book_with_points=True,
            ),
        ]

    def test_returns_dataframe(self, sample_hotels):
        """Should return a pandas DataFrame."""
        result = flatten_hotel_details(sample_hotels)
        assert isinstance(result, pd.DataFrame)

    def test_correct_columns(self, sample_hotels):
        """Should have expected columns."""
        result = flatten_hotel_details(sample_hotels)
        expected_columns = [
            "nid",
            "name",
            "url",
            "canonical_url",
            "code",
            "num_rooms",
            "country",
            "title",
            "description",
            "choice_points",
            "average_rate",
            "synxis_id",
            "book_with_points",
            "book_with_choice",
        ]
        assert list(result.columns) == expected_columns

    def test_correct_row_count(self, sample_hotels):
        """Should have correct number of rows."""
        result = flatten_hotel_details(sample_hotels)
        assert len(result) == 2

    def test_preserves_values(self, sample_hotels):
        """Should preserve hotel values."""
        result = flatten_hotel_details(sample_hotels)
        assert result["nid"].iloc[0] == 12345
        assert result["name"].iloc[0] == "Hotel Chinzanso Tokyo"
        assert result["country"].iloc[0] == "Japan"

    def test_handles_empty_list(self):
        """Should return empty DataFrame for empty input."""
        result = flatten_hotel_details([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestFlattenAvailability:
    """Tests for flatten_availability function."""

    @pytest.fixture
    def sample_availability(self):
        """Sample availability data."""
        return [
            HotelAvailability(
                nid=12345,
                currency_code="USD",
                date="2025-03-15",
                is_points=True,
                lowest_points_rate=15000,
                available=True,
            ),
            HotelAvailability(
                nid=12345,
                currency_code="USD",
                date="2025-03-15",
                is_points=False,
                lowest_rate=350.00,
                available=True,
            ),
            HotelAvailability(
                nid=12345,
                currency_code="USD",
                date="2025-03-16",
                is_points=True,
                lowest_points_rate=18000,
                available=False,
            ),
        ]

    def test_returns_dataframe(self, sample_availability):
        """Should return a pandas DataFrame."""
        result = flatten_availability(sample_availability)
        assert isinstance(result, pd.DataFrame)

    def test_correct_columns(self, sample_availability):
        """Should have expected columns."""
        result = flatten_availability(sample_availability)
        expected_columns = [
            "nid",
            "currency_code",
            "date",
            "is_points",
            "lowest_rate",
            "lowest_points_rate",
            "available",
        ]
        assert list(result.columns) == expected_columns

    def test_correct_row_count(self, sample_availability):
        """Should have correct number of rows."""
        result = flatten_availability(sample_availability)
        assert len(result) == 3

    def test_preserves_values(self, sample_availability):
        """Should preserve availability values."""
        result = flatten_availability(sample_availability)
        assert result["nid"].iloc[0] == 12345
        assert result["date"].iloc[0] == "2025-03-15"
        assert result["is_points"].iloc[0] == True  # noqa: E712 (numpy bool comparison)
        assert result["lowest_points_rate"].iloc[0] == 15000

    def test_handles_empty_list(self):
        """Should return empty DataFrame for empty input."""
        result = flatten_availability([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_separates_points_and_cash(self, sample_availability):
        """Should correctly separate points and cash records."""
        result = flatten_availability(sample_availability)
        points_records = result[result["is_points"] == True]  # noqa: E712
        cash_records = result[result["is_points"] == False]  # noqa: E712
        assert len(points_records) == 2
        assert len(cash_records) == 1

