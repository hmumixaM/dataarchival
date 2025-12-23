"""Data models for award availability."""

# Seats.aero models
# iPrefer models
from award_archive.models.iprefer import (
    HotelAvailability,
    HotelDetails,
    HotelItem,
    RateCalendarResponse,
    RateCalendarResult,
)
from award_archive.models.seats_aero import (
    AvailabilityResponse,
    AvailabilityResult,
    Route,
    SearchResponse,
    SearchResult,
    TripDetails,
)

# Sources
from award_archive.models.source import ALL_SOURCES, Source

__all__ = [
    # Seats.aero
    "Route",
    "AvailabilityResult",
    "AvailabilityResponse",
    "SearchResult",
    "SearchResponse",
    "TripDetails",
    # iPrefer
    "HotelAvailability",
    "HotelDetails",
    "HotelItem",
    "RateCalendarResponse",
    "RateCalendarResult",
    # Sources
    "Source",
    "ALL_SOURCES",
]
