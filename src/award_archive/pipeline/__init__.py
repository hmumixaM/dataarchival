"""Data ingestion pipeline."""

from award_archive.pipeline.ingest import flatten_availability_data, ingest_availability

__all__ = ["ingest_availability", "flatten_availability_data"]

