"""Unit tests for API client."""

from unittest.mock import MagicMock, patch

import pytest

from award_archive.api.client import RATE_LIMIT_SECONDS, SeatsAeroClient


@pytest.fixture
def client():
    return SeatsAeroClient(api_key="test_key")


@pytest.fixture
def mock_request():
    with patch("award_archive.api.client.httpx.request") as mock_req:
        with patch("award_archive.api.client.time.sleep"):
            mock_resp = MagicMock()
            mock_req.return_value = mock_resp
            yield mock_req, mock_resp


class TestRequest:
    """Tests for _request method."""

    def test_rate_limiting(self, client):
        """Rate limits before request."""
        with patch("award_archive.api.client.httpx.request") as mock_req:
            with patch("award_archive.api.client.time.sleep") as mock_sleep:
                mock_req.return_value.json.return_value = {}
                client._request("GET", "test")
                mock_sleep.assert_called_once_with(RATE_LIMIT_SECONDS)

    def test_headers(self, client, mock_request):
        """Includes auth header."""
        mock_req, mock_resp = mock_request
        mock_resp.json.return_value = {}

        client._request("GET", "endpoint")

        assert mock_req.call_args.kwargs["headers"] == {"Partner-Authorization": "test_key"}

    def test_url(self, client, mock_request):
        """Builds correct URL."""
        mock_req, mock_resp = mock_request
        mock_resp.json.return_value = {}

        client._request("GET", "availability")

        assert mock_req.call_args.args[1] == "https://seats.aero/partnerapi/availability"


class TestGetRoutes:
    """Tests for get_routes."""

    def test_returns_routes(self, client, mock_request):
        """Parses route response."""
        _, mock_resp = mock_request
        mock_resp.json.return_value = [{
            "ID": "route-1",
            "OriginAirport": "SFO",
            "OriginRegion": "NA",
            "DestinationAirport": "NRT",
            "DestinationRegion": "Asia",
            "NumDaysOut": 30,
            "Distance": 5000,
            "Source": "aeroplan",
        }]

        routes = client.get_routes(source="aeroplan")

        assert len(routes) == 1
        assert routes[0].OriginAirport == "SFO"


class TestGetBulkAvailability:
    """Tests for get_bulk_availability."""

    def test_returns_response(self, client, mock_request):
        """Parses availability response."""
        _, mock_resp = mock_request
        mock_resp.json.return_value = {
            "data": [],
            "count": 0,
            "hasMore": False,
            "cursor": 0,
        }

        result = client.get_bulk_availability(source="aeroplan")

        assert result.count == 0
        assert not result.hasMore

    def test_filters_none_params(self, client, mock_request):
        """Removes None params."""
        mock_req, mock_resp = mock_request
        mock_resp.json.return_value = {"data": [], "count": 0, "hasMore": False, "cursor": 0}

        client.get_bulk_availability(source="aeroplan", cabin=None, take=100)

        params = mock_req.call_args.kwargs["params"]
        assert "cabin" not in params
        assert params["take"] == 100


class TestGetTripById:
    """Tests for get_trip_by_id."""

    def test_direct_response(self, client, mock_request):
        """Handles direct response."""
        _, mock_resp = mock_request
        mock_resp.json.return_value = {"ID": "trip-123"}

        trip = client.get_trip_by_id("trip-123")
        assert trip.ID == "trip-123"

    def test_nested_response(self, client, mock_request):
        """Handles nested data response."""
        _, mock_resp = mock_request
        mock_resp.json.return_value = {"data": [{"ID": "trip-456"}]}

        trip = client.get_trip_by_id("trip-456")
        assert trip.ID == "trip-456"
