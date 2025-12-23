"""iPrefer module - backwards compatibility re-exports.

This module re-exports from the canonical locations for backwards compatibility.
Prefer importing directly from:
- award_archive.api (for API functions)
- award_archive.models (for data models)
- award_archive.pipeline (for ingestion functions)
"""

# API functions
from award_archive.api import fetch_calendar, fetch_hotel_details, fetch_hotel_links

# Models
from award_archive.models import (
    HotelAvailability,
    HotelDetails,
    HotelItem,
    RateCalendarResponse,
    RateCalendarResult,
)

__all__ = [
    # API
    "fetch_calendar",
    "fetch_hotel_details",
    "fetch_hotel_links",
    # Models
    "HotelAvailability",
    "HotelDetails",
    "HotelItem",
    "RateCalendarResponse",
    "RateCalendarResult",
]
