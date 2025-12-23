"""Seats.aero Partner API client."""

import logging
import time
from datetime import date

import httpx

from award_archive.models.seats_aero import (
    AvailabilityResponse,
    Route,
    SearchResponse,
    TripDetails,
)

log = logging.getLogger(__name__)

RATE_LIMIT_SECONDS = 1
BASE_URL = "https://seats.aero/partnerapi"


def _filter_none(params: dict) -> dict:
    """Remove None values from dict."""
    return {k: v for k, v in params.items() if v is not None}


def _serialize_date(d: date | None) -> str | None:
    """Convert date to ISO string."""
    return d.isoformat() if d else None


class SeatsAeroClient:
    """Client for the Seats.aero Partner API."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"Partner-Authorization": api_key}

    def _request(self, method: str, endpoint: str, params: dict | None = None) -> dict:
        """Make rate-limited API request."""
        time.sleep(RATE_LIMIT_SECONDS)
        url = f"{BASE_URL}/{endpoint}"

        log.debug(f"{method} {url}")
        response = httpx.request(method, url, headers=self.headers, params=params, timeout=30.0)
        response.raise_for_status()

        return response.json()

    def get_routes(self, source: str) -> list[Route]:
        """Get routes for a mileage program."""
        data = self._request("GET", "routes", {"source": source})
        return [Route(**item) for item in data]

    def get_trip_by_id(self, trip_id: str) -> TripDetails:
        """Get trip details."""
        data = self._request("GET", f"trips/{trip_id}")
        # API returns either direct object or nested in data list
        trip_data = data["data"][0] if "data" in data and data["data"] else data
        return TripDetails(**trip_data)

    def get_bulk_availability(
        self,
        source: str,
        cabin: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        origin_region: str | None = None,
        destination_region: str | None = None,
        take: int | None = None,
        cursor: int | None = None,
        skip: int | None = None,
        include_filtered: bool | None = None,
    ) -> AvailabilityResponse:
        """Get bulk availability for a mileage program."""
        params = _filter_none({
            "source": source,
            "cabin": cabin,
            "start_date": _serialize_date(start_date),
            "end_date": _serialize_date(end_date),
            "origin_region": origin_region,
            "destination_region": destination_region,
            "take": take,
            "cursor": cursor,
            "skip": skip,
            "include_filtered": include_filtered,
        })
        log.info(f"Fetching availability: source={source}")
        return AvailabilityResponse(**self._request("GET", "availability", params))

    def search(
        self,
        origin_airport: str,
        destination_airport: str,
        start_date: date | None = None,
        end_date: date | None = None,
        cursor: int | None = None,
        take: int | None = None,
        order_by: str | None = None,
        skip: int | None = None,
        include_trips: bool | None = None,
        only_direct_flights: bool | None = None,
        carriers: str | None = None,
        include_filtered: bool | None = None,
        sources: str | None = None,
        minify_trips: bool | None = None,
        cabins: str | None = None,
    ) -> SearchResponse:
        """Search award availability between airports."""
        params = _filter_none({
            "origin_airport": origin_airport,
            "destination_airport": destination_airport,
            "start_date": _serialize_date(start_date),
            "end_date": _serialize_date(end_date),
            "cursor": cursor,
            "take": take,
            "order_by": order_by,
            "skip": skip,
            "include_trips": include_trips,
            "only_direct_flights": only_direct_flights,
            "carriers": carriers,
            "include_filtered": include_filtered,
            "sources": sources,
            "minify_trips": minify_trips,
            "cabins": cabins,
        })
        log.info(f"Searching: {origin_airport} -> {destination_airport}")
        return SearchResponse(**self._request("GET", "search", params))

