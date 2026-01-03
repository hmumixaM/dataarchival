"""Command-line interface for award archive using Click."""

import logging
import os
from datetime import datetime

import click

from award_archive.models import ALL_SOURCES
from award_archive.pipeline import (
    AVAILABILITY_TABLE,
    IPREFER_AVAILABILITY_TABLE,
    IPREFER_HOTELS_TABLE,
    ingest_availability,
    ingest_iprefer,
    ingest_iprefer_availability,
    ingest_iprefer_hotels,
)
from award_archive.storage import get_table_info, optimize_table

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def parse_date(ctx, param, value):
    """Parse date string to date object."""
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """Award Archive - Ingest award availability data to Delta Lake."""
    pass


def validate_sources(sources: tuple) -> list:
    """Validate source names against ALL_SOURCES."""
    validated = []
    for src in sources:
        src_lower = src.lower()
        if src_lower not in [s.lower() for s in ALL_SOURCES]:
            raise click.ClickException(
                f"Invalid source '{src}'. Valid sources: {', '.join(ALL_SOURCES)}"
            )
        # Find the canonical name
        for canonical in ALL_SOURCES:
            if canonical.lower() == src_lower:
                validated.append(canonical)
                break
    return validated


@cli.command()
@click.argument("sources", nargs=-1)
@click.option(
    "--source",
    "-s",
    multiple=True,
    help=f"Mileage program source(s). Can specify multiple times. "
    f"Valid: {', '.join(ALL_SOURCES)}. Omit for all sources.",
)
@click.option(
    "--s3-path",
    default=AVAILABILITY_TABLE,
    show_default=True,
    help="S3 path for Delta table.",
)
@click.option(
    "--start-date",
    callback=parse_date,
    help="Start date filter (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    callback=parse_date,
    help="End date filter (YYYY-MM-DD).",
)
@click.option(
    "--cabin",
    type=click.Choice(["economy", "premium", "business", "first"]),
    help="Cabin class filter.",
)
@click.option(
    "--max-pages",
    type=int,
    help="Maximum API pages to fetch.",
)
@click.option(
    "--skip",
    type=int,
    default=0,
    show_default=True,
    help="Starting skip offset to resume ingestion.",
)
@click.option(
    "--optimize",
    is_flag=True,
    help="Run table optimization after ingestion.",
)
def seats(sources, source, s3_path, start_date, end_date, cabin, max_pages, skip, optimize):
    """Ingest flight availability from seats.aero API.

    SOURCES can be provided as positional arguments (e.g., 'seats eurobonus saudia')
    or via --source/-s options. Omit to process all sources.
    """
    api_key = os.environ.get("SEATS_AERO_API_KEY")
    if not api_key:
        raise click.ClickException("SEATS_AERO_API_KEY environment variable not set")

    # Combine positional args and --source options
    all_sources_input = sources + source
    if all_sources_input:
        sources = validate_sources(all_sources_input)
    else:
        sources = list(ALL_SOURCES)
    all_stats = []

    for src in sources:
        logger.info(f"Ingesting source: {src}")
        stats = ingest_availability(
            api_key=api_key,
            source=src,
            s3_path=s3_path,
            start_date=start_date,
            end_date=end_date,
            cabin=cabin,
            max_pages=max_pages,
            start_skip=skip,
        )
        all_stats.append(stats)
        click.echo(f"Ingestion completed for {src}:")
        for key, value in stats.items():
            click.echo(f"  {key}: {value}")

    click.echo(f"\nIngestion summary: {len(all_stats)}/{len(sources)} sources completed")

    if optimize:
        logger.info("Running table optimization...")
        opt_stats = optimize_table(s3_path)
        click.echo("Optimization completed:")
        for key, value in opt_stats.items():
            click.echo(f"  {key}: {value}")


@cli.command()
@click.option(
    "--hotels-path",
    default=IPREFER_HOTELS_TABLE,
    show_default=True,
    help="S3 path for hotels Delta table.",
)
@click.option(
    "--availability-path",
    default=IPREFER_AVAILABILITY_TABLE,
    show_default=True,
    help="S3 path for availability Delta table.",
)
@click.option(
    "--hotels-only",
    is_flag=True,
    help="Only ingest hotel details (skip availability).",
)
@click.option(
    "--availability-only",
    is_flag=True,
    help="Only ingest availability (skip hotel details).",
)
@click.option(
    "--points-only",
    is_flag=True,
    help="Only fetch points availability (skip cash rates).",
)
@click.option(
    "--cash-only",
    is_flag=True,
    help="Only fetch cash availability (skip points rates).",
)
@click.option(
    "--max-hotels",
    type=int,
    help="Maximum number of hotels to process (for testing).",
)
@click.option(
    "--optimize",
    is_flag=True,
    help="Run table optimization after ingestion.",
)
def iprefer(
    hotels_path,
    availability_path,
    hotels_only,
    availability_only,
    points_only,
    cash_only,
    max_hotels,
    optimize,
):
    """Ingest hotel availability from iPrefer (Preferred Hotels)."""
    if hotels_only and availability_only:
        raise click.ClickException("Cannot use both --hotels-only and --availability-only")

    if points_only and cash_only:
        raise click.ClickException("Cannot use both --points-only and --cash-only")

    if hotels_only:
        logger.info("Running iPrefer hotels-only ingestion")
        stats = ingest_iprefer_hotels(s3_path=hotels_path, max_hotels=max_hotels)
        click.echo("Hotel ingestion completed:")
        for key, value in stats.items():
            click.echo(f"  {key}: {value}")

    elif availability_only:
        logger.info("Running iPrefer availability-only ingestion")
        stats = ingest_iprefer_availability(
            s3_path=availability_path,
            include_points=not cash_only,
            include_cash=not points_only,
            max_hotels=max_hotels,
        )
        click.echo("Availability ingestion completed:")
        for key, value in stats.items():
            click.echo(f"  {key}: {value}")

    else:
        logger.info("Running full iPrefer ingestion pipeline")
        stats = ingest_iprefer(
            hotels_path=hotels_path,
            availability_path=availability_path,
            max_hotels=max_hotels,
            include_points=not cash_only,
            include_cash=not points_only,
        )
        click.echo("iPrefer ingestion completed:")
        click.echo("  Hotels:")
        for key, value in stats.get("hotels", {}).items():
            click.echo(f"    {key}: {value}")
        click.echo("  Availability:")
        for key, value in stats.get("availability", {}).items():
            click.echo(f"    {key}: {value}")

    if optimize:
        logger.info("Running table optimization...")
        for path in [hotels_path, availability_path]:
            opt_stats = optimize_table(path)
            click.echo(f"Optimization completed for {path}:")
            for key, value in opt_stats.items():
                click.echo(f"  {key}: {value}")


@cli.command()
@click.argument("s3_path")
def info(s3_path):
    """Show Delta table info for S3_PATH."""
    table_info = get_table_info(s3_path)
    click.echo(f"Table Info for {s3_path}:")
    for key, value in table_info.items():
        click.echo(f"  {key}: {value}")


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
