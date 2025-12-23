"""Data ingestion pipeline for award availability."""

import logging
from datetime import date

import backoff
import httpx
import pandas as pd

from award_archive.api import SeatsAeroClient
from award_archive.storage import save_to_delta

log = logging.getLogger(__name__)


@backoff.on_exception(
    backoff.constant,
    (httpx.HTTPError, httpx.HTTPStatusError, httpx.TimeoutException),
    max_tries=3,
    interval=30,
    on_backoff=lambda details: log.warning(
        f"Request failed, retrying in 30s (attempt {details['tries']}/3)..."
    ),
)
def _fetch_availability(client: SeatsAeroClient, **kwargs):
    """Fetch availability with retry backoff."""
    return client.get_bulk_availability(**kwargs)

DEFAULT_BUCKET = "s3://award-archive"
AVAILABILITY_TABLE = f"{DEFAULT_BUCKET}/availability"


def flatten_availability_data(data: list[dict]) -> pd.DataFrame:
    """Flatten nested availability response into DataFrame."""
    records = [
        {
            # Core identifiers
            "ID": item.get("ID"),
            "RouteID": item.get("RouteID"),
            "Date": item.get("Date"),
            "ParsedDate": item.get("ParsedDate"),
            # Route info (flattened)
            "OriginAirport": item.get("Route", {}).get("OriginAirport"),
            "OriginRegion": item.get("Route", {}).get("OriginRegion"),
            "DestinationAirport": item.get("Route", {}).get("DestinationAirport"),
            "DestinationRegion": item.get("Route", {}).get("DestinationRegion"),
            "NumDaysOut": item.get("Route", {}).get("NumDaysOut"),
            "Distance": item.get("Route", {}).get("Distance"),
            # Availability flags
            "YAvailable": item.get("YAvailable"),
            "WAvailable": item.get("WAvailable"),
            "JAvailable": item.get("JAvailable"),
            "FAvailable": item.get("FAvailable"),
            "YAvailableRaw": item.get("YAvailableRaw"),
            "WAvailableRaw": item.get("WAvailableRaw"),
            "JAvailableRaw": item.get("JAvailableRaw"),
            "FAvailableRaw": item.get("FAvailableRaw"),
            # Mileage costs (string)
            "YMileageCost": item.get("YMileageCost"),
            "WMileageCost": item.get("WMileageCost"),
            "JMileageCost": item.get("JMileageCost"),
            "FMileageCost": item.get("FMileageCost"),
            # Mileage costs (raw integer)
            "YMileageCostRaw": item.get("YMileageCostRaw"),
            "WMileageCostRaw": item.get("WMileageCostRaw"),
            "JMileageCostRaw": item.get("JMileageCostRaw"),
            "FMileageCostRaw": item.get("FMileageCostRaw"),
            # Direct mileage costs
            "YDirectMileageCost": item.get("YDirectMileageCost"),
            "WDirectMileageCost": item.get("WDirectMileageCost"),
            "JDirectMileageCost": item.get("JDirectMileageCost"),
            "FDirectMileageCost": item.get("FDirectMileageCost"),
            "YDirectMileageCostRaw": item.get("YDirectMileageCostRaw"),
            "WDirectMileageCostRaw": item.get("WDirectMileageCostRaw"),
            "JDirectMileageCostRaw": item.get("JDirectMileageCostRaw"),
            "FDirectMileageCostRaw": item.get("FDirectMileageCostRaw"),
            # Taxes
            "TaxesCurrency": item.get("TaxesCurrency"),
            "YTotalTaxes": item.get("YTotalTaxes"),
            "WTotalTaxes": item.get("WTotalTaxes"),
            "JTotalTaxes": item.get("JTotalTaxes"),
            "FTotalTaxes": item.get("FTotalTaxes"),
            "YTotalTaxesRaw": item.get("YTotalTaxesRaw"),
            "WTotalTaxesRaw": item.get("WTotalTaxesRaw"),
            "JTotalTaxesRaw": item.get("JTotalTaxesRaw"),
            "FTotalTaxesRaw": item.get("FTotalTaxesRaw"),
            # Direct taxes
            "YDirectTotalTaxes": item.get("YDirectTotalTaxes"),
            "WDirectTotalTaxes": item.get("WDirectTotalTaxes"),
            "JDirectTotalTaxes": item.get("JDirectTotalTaxes"),
            "FDirectTotalTaxes": item.get("FDirectTotalTaxes"),
            "YDirectTotalTaxesRaw": item.get("YDirectTotalTaxesRaw"),
            "WDirectTotalTaxesRaw": item.get("WDirectTotalTaxesRaw"),
            "JDirectTotalTaxesRaw": item.get("JDirectTotalTaxesRaw"),
            "FDirectTotalTaxesRaw": item.get("FDirectTotalTaxesRaw"),
            # Remaining seats
            "YRemainingSeats": item.get("YRemainingSeats"),
            "WRemainingSeats": item.get("WRemainingSeats"),
            "JRemainingSeats": item.get("JRemainingSeats"),
            "FRemainingSeats": item.get("FRemainingSeats"),
            "YRemainingSeatsRaw": item.get("YRemainingSeatsRaw"),
            "WRemainingSeatsRaw": item.get("WRemainingSeatsRaw"),
            "JRemainingSeatsRaw": item.get("JRemainingSeatsRaw"),
            "FRemainingSeatsRaw": item.get("FRemainingSeatsRaw"),
            # Direct remaining seats
            "YDirectRemainingSeats": item.get("YDirectRemainingSeats"),
            "WDirectRemainingSeats": item.get("WDirectRemainingSeats"),
            "JDirectRemainingSeats": item.get("JDirectRemainingSeats"),
            "FDirectRemainingSeats": item.get("FDirectRemainingSeats"),
            "YDirectRemainingSeatsRaw": item.get("YDirectRemainingSeatsRaw"),
            "WDirectRemainingSeatsRaw": item.get("WDirectRemainingSeatsRaw"),
            "JDirectRemainingSeatsRaw": item.get("JDirectRemainingSeatsRaw"),
            "FDirectRemainingSeatsRaw": item.get("FDirectRemainingSeatsRaw"),
            # Airlines
            "YAirlines": item.get("YAirlines"),
            "WAirlines": item.get("WAirlines"),
            "JAirlines": item.get("JAirlines"),
            "FAirlines": item.get("FAirlines"),
            "YAirlinesRaw": item.get("YAirlinesRaw"),
            "WAirlinesRaw": item.get("WAirlinesRaw"),
            "JAirlinesRaw": item.get("JAirlinesRaw"),
            "FAirlinesRaw": item.get("FAirlinesRaw"),
            # Direct airlines
            "YDirectAirlines": item.get("YDirectAirlines"),
            "WDirectAirlines": item.get("WDirectAirlines"),
            "JDirectAirlines": item.get("JDirectAirlines"),
            "FDirectAirlines": item.get("FDirectAirlines"),
            "YDirectAirlinesRaw": item.get("YDirectAirlinesRaw"),
            "WDirectAirlinesRaw": item.get("WDirectAirlinesRaw"),
            "JDirectAirlinesRaw": item.get("JDirectAirlinesRaw"),
            "FDirectAirlinesRaw": item.get("FDirectAirlinesRaw"),
            # Direct flight flags
            "YDirect": item.get("YDirect"),
            "WDirect": item.get("WDirect"),
            "JDirect": item.get("JDirect"),
            "FDirect": item.get("FDirect"),
            "YDirectRaw": item.get("YDirectRaw"),
            "WDirectRaw": item.get("WDirectRaw"),
            "JDirectRaw": item.get("JDirectRaw"),
            "FDirectRaw": item.get("FDirectRaw"),
            # Metadata
            "Source": item.get("Source"),
            "CreatedAt": item.get("CreatedAt"),
            "UpdatedAt": item.get("UpdatedAt"),
        }
        for item in data
    ]
    return pd.DataFrame(records)


def ingest_availability(
    api_key: str,
    source: str,
    s3_path: str = AVAILABILITY_TABLE,
    start_date: date | None = None,
    end_date: date | None = None,
    cabin: str | None = None,
    max_pages: int | None = None,
    page_size: int = 5000,
    start_skip: int = 0,
) -> dict:
    """Ingest availability data from API to Delta Lake with deduplication."""
    client = SeatsAeroClient(api_key=api_key)

    log.info(f"Starting ingestion: source={source}, start_skip={start_skip}")

    page = 0
    total_records = 0
    total_input_rows = 0
    rows_written = 0
    rows_inserted = 0
    table_created = False
    last_timestamp = None

    while max_pages is None or page < max_pages:
        page += 1
        skip = start_skip + (page - 1) * page_size
        log.info(f"Fetching page {page} (skip={skip})")

        response = _fetch_availability(
            client,
            source=source,
            cabin=cabin,
            start_date=start_date,
            end_date=end_date,
            skip=skip,
            take=page_size,
        )

        page_data = [item.model_dump() for item in response.data]
        total_records += len(page_data)

        if not page_data:
            log.info(f"No records on page {page}")
            break

        df_page = flatten_availability_data(page_data)
        page_stats = save_to_delta(
            df=df_page,
            s3_path=s3_path,
            mode="merge",
            merge_keys=["RouteID", "UpdatedAt"],
            partition_by=["Source", "Date"],
        )

        total_input_rows += page_stats.get("input_rows", len(df_page))
        rows_written += page_stats.get("rows_written", len(df_page))
        rows_inserted += page_stats.get("rows_inserted", 0)
        table_created = table_created or page_stats.get("table_created", False)
        last_timestamp = page_stats.get("timestamp", last_timestamp)

        if not response.hasMore:
            break

    if total_records == 0:
        log.warning("No data fetched")
        return {"status": "no_data", "records_fetched": 0}

    stats = {
        "input_rows": total_input_rows,
        "rows_written": rows_written,
        "rows_inserted": rows_inserted,
        "table_created": table_created,
        "mode": "merge",
        "timestamp": last_timestamp,
        "source": source,
        "pages_fetched": page,
        "api_records": total_records,
    }

    log.info(f"Ingestion complete: {stats}")
    return stats
