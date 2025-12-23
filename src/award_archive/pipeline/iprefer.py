"""Data ingestion pipeline for iPrefer hotel availability."""

import logging
from itertools import batched

import httpx
import pandas as pd
from deltalake import DeltaTable

from award_archive.api import fetch_calendar, fetch_hotel_details, fetch_hotel_links
from award_archive.models import HotelAvailability, HotelDetails
from award_archive.storage import save_to_delta
from award_archive.storage.credentials import get_storage_options

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "s3://award-archive"
HOTELS_TABLE = f"{DEFAULT_BUCKET}/iprefer_hotels"
AVAILABILITY_TABLE = f"{DEFAULT_BUCKET}/iprefer_availability"

BATCH_SIZE_HOTELS = 30
BATCH_SIZE_CALENDAR = 30
DEFAULT_TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def flatten_hotel_details(hotels: list[HotelDetails]) -> pd.DataFrame:
    """Convert hotel details to DataFrame."""
    return pd.DataFrame([hotel.model_dump() for hotel in hotels])


def flatten_availability(availability: list[HotelAvailability]) -> pd.DataFrame:
    """Convert availability data to DataFrame."""
    return pd.DataFrame([avail.model_dump() for avail in availability])


def _fetch_all_hotel_details(
    client: httpx.Client,
    links: list,
) -> list[HotelDetails]:
    """Fetch details for all hotel links."""
    return [fetch_hotel_details(client, link.url) for link in links]


def _fetch_availability_for_nid(
    client: httpx.Client,
    nid: int,
    include_points: bool,
    include_cash: bool,
) -> list[HotelAvailability]:
    """Fetch points and/or cash availability for a single hotel."""
    availability = []
    if include_points:
        availability.extend(fetch_calendar(client, nid, is_points=True))
    if include_cash:
        availability.extend(fetch_calendar(client, nid, is_points=False))
    return availability


def _get_nids_from_hotels_table(hotels_path: str = HOTELS_TABLE) -> list[int]:
    """Read hotel NIDs from the Delta Lake hotels table."""
    storage_options = get_storage_options()
    dt = DeltaTable(hotels_path, storage_options=storage_options)
    df = dt.to_pandas(columns=["nid"])
    return df["nid"].tolist()


def ingest_iprefer_hotels(
    s3_path: str = HOTELS_TABLE,
    max_hotels: int | None = None,
) -> dict:
    """Ingest hotel details from iPrefer to Delta Lake.

    Args:
        s3_path: S3 path for Delta table
        max_hotels: Maximum number of hotels to process (for testing)

    Returns:
        Statistics about the ingestion
    """
    log.info(f"Starting iPrefer hotel ingestion to {s3_path}")

    total_hotels = 0
    rows_written = 0

    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        links = fetch_hotel_links(client)
        log.info(f"Found {len(links)} hotels in directory")

        if max_hotels:
            links = links[:max_hotels]

        for batch in batched(links, BATCH_SIZE_HOTELS):
            batch_details = _fetch_all_hotel_details(client, batch)
            total_hotels += len(batch_details)

            df = flatten_hotel_details(batch_details)
            stats = save_to_delta(
                df=df,
                s3_path=s3_path,
                mode="merge",
                merge_keys=["nid"],
                partition_by=["country"],
            )
            rows_written += stats.get("rows_written", len(df))
            log.info(f"Processed batch: {len(batch_details)} hotels")

    return {
        "hotels_processed": total_hotels,
        "rows_written": rows_written,
        "s3_path": s3_path,
    }


def ingest_iprefer_availability(
    s3_path: str = AVAILABILITY_TABLE,
    hotels_path: str = HOTELS_TABLE,
    nids: list[int] | None = None,
    include_points: bool = True,
    include_cash: bool = True,
    max_hotels: int | None = None,
) -> dict:
    """Ingest hotel availability from iPrefer to Delta Lake.

    Args:
        s3_path: S3 path for availability Delta table
        hotels_path: S3 path for hotels Delta table (used to get NIDs)
        nids: Specific hotel NIDs to fetch (if None, reads from hotels table)
        include_points: Include points availability
        include_cash: Include cash availability
        max_hotels: Maximum number of hotels to process (for testing)

    Returns:
        Statistics about the ingestion
    """
    log.info(f"Starting iPrefer availability ingestion to {s3_path}")

    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        # Resolve NIDs if not provided - read from Delta table
        if nids is None:
            log.info(f"Reading NIDs from hotels table: {hotels_path}")
            nids = _get_nids_from_hotels_table(hotels_path)
            log.info(f"Found {len(nids)} hotels in table")

        if max_hotels and len(nids) > max_hotels:
            nids = nids[:max_hotels]

        log.info(f"Processing availability for {len(nids)} hotels")

        total_availability = 0
        rows_written = 0

        for batch_nids in batched(nids, BATCH_SIZE_CALENDAR):
            batch_availability = [
                avail
                for nid in batch_nids
                for avail in _fetch_availability_for_nid(
                    client, nid, include_points, include_cash
                )
            ]

            df = flatten_availability(batch_availability)
            stats = save_to_delta(
                df=df,
                s3_path=s3_path,
                mode="merge",
                merge_keys=["nid", "date", "is_points"],
                partition_by=["is_points", "nid", "date"],
            )
            total_availability += len(batch_availability)
            rows_written += stats.get("rows_written", len(df))
            log.info(
                f"Processed batch: {len(batch_nids)} hotels, "
                f"{len(batch_availability)} availability records"
            )

    return {
        "hotels_processed": len(nids),
        "availability_records": total_availability,
        "rows_written": rows_written,
        "s3_path": s3_path,
    }


def ingest_iprefer(
    hotels_path: str = HOTELS_TABLE,
    availability_path: str = AVAILABILITY_TABLE,
    max_hotels: int | None = None,
    include_points: bool = True,
    include_cash: bool = True,
) -> dict:
    """Run full iPrefer ingestion pipeline.

    Args:
        hotels_path: S3 path for hotels Delta table
        availability_path: S3 path for availability Delta table
        max_hotels: Maximum number of hotels to process (for testing)
        include_points: Include points availability
        include_cash: Include cash availability

    Returns:
        Combined statistics from both ingestions
    """
    log.info("Running full iPrefer ingestion pipeline")

    # Step 1: Ingest hotel details
    hotel_stats = ingest_iprefer_hotels(s3_path=hotels_path, max_hotels=max_hotels)
    log.info(f"Hotel ingestion complete: {hotel_stats}")

    # Step 2: Read NIDs from freshly updated hotels table and ingest availability
    avail_stats = ingest_iprefer_availability(
        s3_path=availability_path,
        hotels_path=hotels_path,
        max_hotels=max_hotels,
        include_points=include_points,
        include_cash=include_cash,
    )
    log.info(f"Availability ingestion complete: {avail_stats}")

    return {"hotels": hotel_stats, "availability": avail_stats}
