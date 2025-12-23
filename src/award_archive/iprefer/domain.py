"""Domain models - backwards compatibility re-exports.

Prefer importing directly from award_archive.models.
"""

from award_archive.models import (
    HotelAvailability,
    HotelDetails,
    HotelItem,
    RateCalendarResponse,
    RateCalendarResult,
)

__all__ = [
    "HotelAvailability",
    "HotelDetails",
    "HotelItem",
    "RateCalendarResponse",
    "RateCalendarResult",
]
