"""Data ingestion pipeline for iPrefer hotel availability."""

import logging
from itertools import batched

import httpx
import pandas as pd

from award_archive.api import fetch_calendar, fetch_hotel_details, fetch_hotel_links
from award_archive.models import HotelAvailability, HotelDetails
from award_archive.storage import save_to_delta

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "s3://award-archive"
HOTELS_TABLE = f"{DEFAULT_BUCKET}/iprefer_hotels"
AVAILABILITY_TABLE = f"{DEFAULT_BUCKET}/iprefer_availability"

BATCH_SIZE_HOTELS = 10
BATCH_SIZE_CALENDAR = 5
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
    nids: list[int] | None = None,
    include_points: bool = True,
    include_cash: bool = True,
    max_hotels: int | None = None,
) -> dict:
    """Ingest hotel availability from iPrefer to Delta Lake.

    Args:
        s3_path: S3 path for availability Delta table
        nids: Specific hotel NIDs to fetch (if None, fetches from directory)
        include_points: Include points availability
        include_cash: Include cash availability
        max_hotels: Maximum number of hotels to process (for testing)

    Returns:
        Statistics about the ingestion
    """
    log.info(f"Starting iPrefer availability ingestion to {s3_path}")

    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        # Resolve NIDs if not provided
        if nids is None:
            log.info("No NIDs provided, fetching from hotel directory")
            links = fetch_hotel_links(client)
            if max_hotels:
                links = links[:max_hotels]
            nids = [fetch_hotel_details(client, link.url).nid for link in links]

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
                partition_by=["is_points"],
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

    # Step 2: Collect NIDs and ingest availability
    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        links = fetch_hotel_links(client)
        if max_hotels:
            links = links[:max_hotels]
        nids = [fetch_hotel_details(client, link.url).nid for link in links]

    avail_stats = ingest_iprefer_availability(
        s3_path=availability_path,
        nids=nids,
        include_points=include_points,
        include_cash=include_cash,
    )
    log.info(f"Availability ingestion complete: {avail_stats}")

    return {"hotels": hotel_stats, "availability": avail_stats}
