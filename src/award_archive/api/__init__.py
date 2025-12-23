"""API clients for external data sources."""

from award_archive.api.iprefer import (
    fetch_calendar,
    fetch_hotel_details,
    fetch_hotel_links,
)
from award_archive.api.seats_aero import SeatsAeroClient

__all__ = [
    # Seats.aero
    "SeatsAeroClient",
    # iPrefer
    "fetch_hotel_links",
    "fetch_hotel_details",
    "fetch_calendar",
]
