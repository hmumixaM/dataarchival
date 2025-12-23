"""Shared test fixtures."""

import os
import uuid
from datetime import UTC, datetime

import boto3
import pandas as pd
import pytest

BUCKET_NAME = "award-archive"
TEST_PREFIX = "test_runs"


def make_availability_df(n: int = 3) -> pd.DataFrame:
    """Create sample availability DataFrame."""
    now = datetime.now(UTC).isoformat()
    return pd.DataFrame({
        # Core identifiers
        "ID": [f"avail-{i:03d}" for i in range(n)],
        "RouteID": [f"route-{i}" for i in range(n)],
        "Date": [f"2025-03-{15+i}" for i in range(n)],
        "ParsedDate": [f"2025-03-{15+i}T00:00:00Z" for i in range(n)],
        # Route info (flattened)
        "OriginAirport": ["SFO", "LAX", "JFK"][:n],
        "OriginRegion": ["North America", "North America", "North America"][:n],
        "DestinationAirport": ["NRT", "HND", "LHR"][:n],
        "DestinationRegion": ["Asia", "Asia", "Europe"][:n],
        "NumDaysOut": [60, 60, 60][:n],
        "Distance": [5000, 5500, 3500][:n],
        # Availability flags
        "YAvailable": [True, False, True][:n],
        "WAvailable": [False, True, False][:n],
        "JAvailable": [True, True, True][:n],
        "FAvailable": [False, False, True][:n],
        "YAvailableRaw": [True, False, True][:n],
        "WAvailableRaw": [False, True, False][:n],
        "JAvailableRaw": [True, True, True][:n],
        "FAvailableRaw": [False, False, True][:n],
        # Mileage costs (string)
        "YMileageCost": ["50000", "0", "45000"][:n],
        "WMileageCost": ["0", "75000", "0"][:n],
        "JMileageCost": ["85000", "90000", "100000"][:n],
        "FMileageCost": ["0", "0", "150000"][:n],
        # Mileage costs (raw)
        "YMileageCostRaw": [50000, 0, 45000][:n],
        "WMileageCostRaw": [0, 75000, 0][:n],
        "JMileageCostRaw": [85000, 90000, 100000][:n],
        "FMileageCostRaw": [0, 0, 150000][:n],
        # Direct mileage costs
        "YDirectMileageCost": [50000, 0, 45000][:n],
        "WDirectMileageCost": [0, 0, 0][:n],
        "JDirectMileageCost": [85000, 90000, 100000][:n],
        "FDirectMileageCost": [0, 0, 150000][:n],
        "YDirectMileageCostRaw": [50000, 0, 45000][:n],
        "WDirectMileageCostRaw": [0, 0, 0][:n],
        "JDirectMileageCostRaw": [85000, 90000, 100000][:n],
        "FDirectMileageCostRaw": [0, 0, 150000][:n],
        # Taxes
        "TaxesCurrency": ["USD", "EUR", "GBP"][:n],
        "YTotalTaxes": [5600, 0, 4500][:n],
        "WTotalTaxes": [0, 7500, 0][:n],
        "JTotalTaxes": [5600, 9000, 10000][:n],
        "FTotalTaxes": [0, 0, 15000][:n],
        "YTotalTaxesRaw": [5600, 0, 4500][:n],
        "WTotalTaxesRaw": [0, 7500, 0][:n],
        "JTotalTaxesRaw": [5600, 9000, 10000][:n],
        "FTotalTaxesRaw": [0, 0, 15000][:n],
        # Direct taxes
        "YDirectTotalTaxes": [5600, 0, 4500][:n],
        "WDirectTotalTaxes": [0, 0, 0][:n],
        "JDirectTotalTaxes": [5600, 9000, 10000][:n],
        "FDirectTotalTaxes": [0, 0, 15000][:n],
        "YDirectTotalTaxesRaw": [5600, 0, 4500][:n],
        "WDirectTotalTaxesRaw": [0, 0, 0][:n],
        "JDirectTotalTaxesRaw": [5600, 9000, 10000][:n],
        "FDirectTotalTaxesRaw": [0, 0, 15000][:n],
        # Remaining seats
        "YRemainingSeats": [2, 0, 4][:n],
        "WRemainingSeats": [0, 1, 0][:n],
        "JRemainingSeats": [1, 2, 1][:n],
        "FRemainingSeats": [0, 0, 1][:n],
        "YRemainingSeatsRaw": [2, 0, 4][:n],
        "WRemainingSeatsRaw": [0, 1, 0][:n],
        "JRemainingSeatsRaw": [1, 2, 1][:n],
        "FRemainingSeatsRaw": [0, 0, 1][:n],
        # Direct remaining seats
        "YDirectRemainingSeats": [2, 0, 4][:n],
        "WDirectRemainingSeats": [0, 0, 0][:n],
        "JDirectRemainingSeats": [1, 2, 1][:n],
        "FDirectRemainingSeats": [0, 0, 1][:n],
        "YDirectRemainingSeatsRaw": [2, 0, 4][:n],
        "WDirectRemainingSeatsRaw": [0, 0, 0][:n],
        "JDirectRemainingSeatsRaw": [1, 2, 1][:n],
        "FDirectRemainingSeatsRaw": [0, 0, 1][:n],
        # Airlines
        "YAirlines": ["AC", "", "BA"][:n],
        "WAirlines": ["", "UA", ""][:n],
        "JAirlines": ["AC", "UA", "BA"][:n],
        "FAirlines": ["", "", "BA"][:n],
        "YAirlinesRaw": ["AC", "", "BA"][:n],
        "WAirlinesRaw": ["", "UA", ""][:n],
        "JAirlinesRaw": ["AC", "UA", "BA"][:n],
        "FAirlinesRaw": ["", "", "BA"][:n],
        # Direct airlines
        "YDirectAirlines": ["AC", "", "BA"][:n],
        "WDirectAirlines": ["", "", ""][:n],
        "JDirectAirlines": ["AC", "UA", "BA"][:n],
        "FDirectAirlines": ["", "", "BA"][:n],
        "YDirectAirlinesRaw": ["AC", "", "BA"][:n],
        "WDirectAirlinesRaw": ["", "", ""][:n],
        "JDirectAirlinesRaw": ["AC", "UA", "BA"][:n],
        "FDirectAirlinesRaw": ["", "", "BA"][:n],
        # Direct flags
        "YDirect": [True, False, True][:n],
        "WDirect": [False, False, False][:n],
        "JDirect": [True, True, True][:n],
        "FDirect": [False, False, True][:n],
        "YDirectRaw": [True, False, True][:n],
        "WDirectRaw": [False, False, False][:n],
        "JDirectRaw": [True, True, True][:n],
        "FDirectRaw": [False, False, True][:n],
        # Metadata
        "Source": ["aeroplan", "united", "lifemiles"][:n],
        "CreatedAt": [now] * n,
        "UpdatedAt": [now] * n,
    })


@pytest.fixture
def sample_df():
    """Sample availability DataFrame."""
    return make_availability_df()


@pytest.fixture
def s3_test_path():
    """Unique S3 path with cleanup."""
    s3_path = f"s3://{BUCKET_NAME}/{TEST_PREFIX}/{uuid.uuid4()}"
    yield s3_path
    _cleanup_s3(s3_path)


def _cleanup_s3(s3_path: str) -> None:
    """Delete all objects under S3 path."""
    bucket, prefix = s3_path.replace("s3://", "").split("/", 1)

    client = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )

    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        if objects := page.get("Contents"):
            client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": obj["Key"]} for obj in objects]},
            )

