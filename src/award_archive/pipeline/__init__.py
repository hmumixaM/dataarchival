"""Data ingestion pipelines."""

# Seats.aero pipeline
# iPrefer pipeline
from award_archive.pipeline.iprefer import (
    AVAILABILITY_TABLE as IPREFER_AVAILABILITY_TABLE,
)
from award_archive.pipeline.iprefer import (
    HOTELS_TABLE as IPREFER_HOTELS_TABLE,
)
from award_archive.pipeline.iprefer import (
    flatten_availability,
    flatten_hotel_details,
    ingest_iprefer,
    ingest_iprefer_availability,
    ingest_iprefer_hotels,
)
from award_archive.pipeline.seats_aero import (
    AVAILABILITY_TABLE,
    flatten_availability_data,
    ingest_availability,
)

__all__ = [
    # Seats.aero
    "ingest_availability",
    "flatten_availability_data",
    "AVAILABILITY_TABLE",
    # iPrefer
    "ingest_iprefer",
    "ingest_iprefer_hotels",
    "ingest_iprefer_availability",
    "flatten_hotel_details",
    "flatten_availability",
    "IPREFER_HOTELS_TABLE",
    "IPREFER_AVAILABILITY_TABLE",
]
