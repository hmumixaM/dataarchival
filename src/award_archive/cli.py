"""Command-line interface for award archive."""

import argparse
import logging
import os
import sys
from datetime import datetime

from award_archive.models import ALL_SOURCES
from award_archive.pipeline import ingest_availability
from award_archive.pipeline.ingest import AVAILABILITY_TABLE
from award_archive.storage import get_table_info, optimize_table

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(description="Ingest award availability data to Delta Lake")
    parser.add_argument(
        "--source",
        nargs="*",
        default=None,
        choices=ALL_SOURCES,
        metavar="SOURCE",
        help="Mileage program source(s) to ingest. Can specify multiple (e.g., --source aeroplan united). "
        f"If omitted, ingests all sources. Valid sources: {', '.join(ALL_SOURCES)}",
    )
    parser.add_argument(
        "--s3-path",
        default=AVAILABILITY_TABLE,
        help=f"S3 path for Delta table (default: {AVAILABILITY_TABLE})",
    )
    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="Start date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        help="End date filter (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--cabin",
        choices=["economy", "premium", "business", "first"],
        help="Cabin class filter",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum API pages to fetch (default: all pages)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Starting skip offset to resume ingestion from (default: 0)",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run table optimization after ingestion",
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show table info and exit",
    )

    args = parser.parse_args()

    # Get API key from environment
    api_key = os.environ.get("SEATS_AERO_API_KEY")
    if not api_key and not args.info:
        logger.error("SEATS_AERO_API_KEY environment variable not set")
        return 1

    if args.info:
        info = get_table_info(args.s3_path)
        print(f"Table Info for {args.s3_path}:")
        for key, value in info.items():
            print(f"  {key}: {value}")
        return 0

    # Determine sources to ingest (empty list or None means all sources)
    sources = args.source if args.source else ALL_SOURCES
    
    # Run ingestion for each source
    all_stats = []
    
    for source in sources:
        logger.info(f"Ingesting source: {source}")
        stats = ingest_availability(
            api_key=api_key,
            source=source,
            s3_path=args.s3_path,
            start_date=args.start_date,
            end_date=args.end_date,
            cabin=args.cabin,
            max_pages=args.max_pages,
            start_skip=args.skip,
        )
        all_stats.append(stats)
        print(f"Ingestion completed for {source}:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    print(f"\nIngestion summary: {len(all_stats)}/{len(sources)} sources completed successfully")

    # Optionally optimize
    if args.optimize:
        logger.info("Running table optimization...")
        opt_stats = optimize_table(args.s3_path)
        print("Optimization completed:")
        for key, value in opt_stats.items():
            print(f"  {key}: {value}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

