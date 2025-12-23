"""FastAPI server for award archive ingestion."""

import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from enum import Enum

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from award_archive.pipeline import ingest_availability

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = FastAPI(
    title="Award Archive API",
    description="API for ingesting award availability data to Delta Lake",
    version="0.1.0",
)


class Source(str, Enum):
    """Available mileage program sources from Seats.aero."""
    
    # All 24 mileage programs from seats.aero
    aeromexico = "aeromexico"              # Aeromexico Club Premier
    aeroplan = "aeroplan"                  # Air Canada Aeroplan
    flying_blue = "flying_blue"            # Air France/KLM Flying Blue
    alaska = "alaska"                      # Alaska Atmos Rewards
    american = "american"                  # American Airlines AAdvantage
    azul = "azul"                          # Azul Fidelidade
    connectmiles = "connectmiles"          # Copa ConnectMiles
    delta_skymiles = "delta_skymiles"      # Delta SkyMiles
    emirates = "emirates"                  # Emirates Skywards
    ethiopian = "ethiopian"                # Ethiopian ShebaMiles
    etihad = "etihad"                      # Etihad Guest
    finnair = "finnair"                    # Finnair Plus
    smiles = "smiles"                      # GOL Smiles
    jetblue = "jetblue"                    # JetBlue TrueBlue
    lufthansa = "lufthansa"                # Lufthansa Miles & More
    qantas = "qantas"                      # Qantas Frequent Flyer
    qatar = "qatar"                        # Qatar Airways Privilege Club
    eurobonus = "eurobonus"                # SAS EuroBonus
    saudia = "saudia"                      # Saudia AlFursan
    singapore = "singapore"                # Singapore Airlines KrisFlyer
    turkish = "turkish"                    # Turkish Miles&Smiles
    united = "united"                      # United MileagePlus
    virgin_atlantic = "virgin_atlantic"    # Virgin Atlantic Flying Club
    velocity = "velocity"                  # Virgin Australia Velocity


ALL_SOURCES = [s.value for s in Source]


class IngestRequest(BaseModel):
    """Request body for ingestion."""
    
    start_date: date | None = None
    end_date: date | None = None
    cabin: str | None = None
    max_pages: int = 100  # Default high to get all data


class IngestResponse(BaseModel):
    """Response from ingestion."""
    
    status: str
    source: str
    stats: dict | None = None
    error: str | None = None


class BulkIngestResponse(BaseModel):
    """Response from bulk ingestion."""
    
    status: str
    total_sources: int
    results: list[IngestResponse]
    summary: dict


def _get_api_key() -> str:
    """Get API key from environment."""
    api_key = os.environ.get("SEATS_AERO_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="SEATS_AERO_API_KEY not configured")
    return api_key


def _ingest_source(
    source: str,
    start_date: date | None,
    end_date: date | None,
    cabin: str | None,
    max_pages: int,
) -> IngestResponse:
    """Ingest a single source and return response."""
    try:
        api_key = _get_api_key()
        stats = ingest_availability(
            api_key=api_key,
            source=source,
            start_date=start_date,
            end_date=end_date,
            cabin=cabin,
            max_pages=max_pages,
        )
        return IngestResponse(status="success", source=source, stats=stats)
    except Exception as e:
        log.error(f"Failed to ingest {source}: {e}")
        return IngestResponse(status="error", source=source, error=str(e))


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"status": "ok", "service": "award-archive"}


@app.get("/sources", response_model=list[str])
async def list_sources():
    """List all available mileage program sources."""
    return ALL_SOURCES


@app.post("/ingest/{source}", response_model=IngestResponse)
async def ingest_source(
    source: Source,
    request: IngestRequest | None = None,
):
    """Ingest availability data from a specific source."""
    request = request or IngestRequest()
    
    log.info(f"Starting ingestion for source: {source.value}")
    
    return _ingest_source(
        source=source.value,
        start_date=request.start_date,
        end_date=request.end_date,
        cabin=request.cabin,
        max_pages=request.max_pages,
    )


@app.post("/ingest", response_model=BulkIngestResponse)
async def ingest_all_sources(
    request: IngestRequest | None = None,
    sources: list[Source] | None = None,
):
    """
    Ingest availability data from all sources (or specified subset).
    
    Runs ingestion for each source sequentially to respect API rate limits.
    """
    request = request or IngestRequest()
    source_list = [s.value for s in sources] if sources else ALL_SOURCES
    
    log.info(f"Starting bulk ingestion for {len(source_list)} sources")
    
    results = []
    for source in source_list:
        log.info(f"Ingesting source: {source}")
        result = _ingest_source(
            source=source,
            start_date=request.start_date,
            end_date=request.end_date,
            cabin=request.cabin,
            max_pages=request.max_pages,
        )
        results.append(result)
    
    # Calculate summary
    successful = [r for r in results if r.status == "success"]
    failed = [r for r in results if r.status == "error"]
    
    total_records = sum(
        r.stats.get("api_records", 0) for r in successful if r.stats
    )
    
    summary = {
        "successful_sources": len(successful),
        "failed_sources": len(failed),
        "total_records_ingested": total_records,
        "failed_source_names": [r.source for r in failed],
    }
    
    return BulkIngestResponse(
        status="completed",
        total_sources=len(source_list),
        results=results,
        summary=summary,
    )


@app.post("/ingest/background", response_model=dict)
async def ingest_all_background(background_tasks: BackgroundTasks):
    """
    Start bulk ingestion of ALL data in the background.
    
    Ingests all availability from all 24 sources with no filters.
    Returns immediately while ingestion runs asynchronously.
    """
    
    def run_bulk_ingest():
        for source in ALL_SOURCES:
            log.info(f"[Background] Ingesting all data from: {source}")
            _ingest_source(
                source=source,
                start_date=None,  # No date filter - get everything
                end_date=None,
                cabin=None,       # All cabins
                max_pages=100,    # High limit to get all pages
            )
        log.info("[Background] Bulk ingestion complete for all sources")
    
    background_tasks.add_task(run_bulk_ingest)
    
    return {
        "status": "started",
        "message": f"Bulk ingestion started for ALL data from {len(ALL_SOURCES)} sources",
        "sources": ALL_SOURCES,
    }


def run():
    """Run the server."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    run()

