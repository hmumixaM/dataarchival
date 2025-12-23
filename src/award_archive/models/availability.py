"""Pydantic models for Seats.aero API responses."""

from pydantic import BaseModel, ConfigDict


class Route(BaseModel):
    """Route information between two airports."""

    ID: str
    OriginAirport: str
    OriginRegion: str
    DestinationAirport: str
    DestinationRegion: str
    NumDaysOut: int
    Distance: int
    Source: str


class AvailabilityResult(BaseModel):
    """Single availability result from bulk availability endpoint."""

    ID: str
    RouteID: str
    Route: Route
    Date: str
    ParsedDate: str

    # Availability flags
    YAvailable: bool
    WAvailable: bool
    JAvailable: bool
    FAvailable: bool
    YAvailableRaw: bool | None = None
    WAvailableRaw: bool | None = None
    JAvailableRaw: bool | None = None
    FAvailableRaw: bool | None = None

    # Mileage costs (string format, filtered)
    YMileageCost: str | None = None
    WMileageCost: str | None = None
    JMileageCost: str | None = None
    FMileageCost: str | None = None

    # Mileage costs (raw integer)
    YMileageCostRaw: int | None = None
    WMileageCostRaw: int | None = None
    JMileageCostRaw: int | None = None
    FMileageCostRaw: int | None = None

    # Direct mileage costs
    YDirectMileageCost: int | None = None
    WDirectMileageCost: int | None = None
    JDirectMileageCost: int | None = None
    FDirectMileageCost: int | None = None
    YDirectMileageCostRaw: int | None = None
    WDirectMileageCostRaw: int | None = None
    JDirectMileageCostRaw: int | None = None
    FDirectMileageCostRaw: int | None = None

    # Taxes
    TaxesCurrency: str | None = None
    YTotalTaxes: int | None = None
    WTotalTaxes: int | None = None
    JTotalTaxes: int | None = None
    FTotalTaxes: int | None = None
    YTotalTaxesRaw: int | None = None
    WTotalTaxesRaw: int | None = None
    JTotalTaxesRaw: int | None = None
    FTotalTaxesRaw: int | None = None

    # Direct taxes
    YDirectTotalTaxes: int | None = None
    WDirectTotalTaxes: int | None = None
    JDirectTotalTaxes: int | None = None
    FDirectTotalTaxes: int | None = None
    YDirectTotalTaxesRaw: int | None = None
    WDirectTotalTaxesRaw: int | None = None
    JDirectTotalTaxesRaw: int | None = None
    FDirectTotalTaxesRaw: int | None = None

    # Remaining seats
    YRemainingSeats: int
    WRemainingSeats: int
    JRemainingSeats: int
    FRemainingSeats: int
    YRemainingSeatsRaw: int | None = None
    WRemainingSeatsRaw: int | None = None
    JRemainingSeatsRaw: int | None = None
    FRemainingSeatsRaw: int | None = None

    # Direct remaining seats
    YDirectRemainingSeats: int | None = None
    WDirectRemainingSeats: int | None = None
    JDirectRemainingSeats: int | None = None
    FDirectRemainingSeats: int | None = None
    YDirectRemainingSeatsRaw: int | None = None
    WDirectRemainingSeatsRaw: int | None = None
    JDirectRemainingSeatsRaw: int | None = None
    FDirectRemainingSeatsRaw: int | None = None

    # Airlines
    YAirlines: str | None = None
    WAirlines: str | None = None
    JAirlines: str | None = None
    FAirlines: str | None = None
    YAirlinesRaw: str | None = None
    WAirlinesRaw: str | None = None
    JAirlinesRaw: str | None = None
    FAirlinesRaw: str | None = None

    # Direct airlines
    YDirectAirlines: str | None = None
    WDirectAirlines: str | None = None
    JDirectAirlines: str | None = None
    FDirectAirlines: str | None = None
    YDirectAirlinesRaw: str | None = None
    WDirectAirlinesRaw: str | None = None
    JDirectAirlinesRaw: str | None = None
    FDirectAirlinesRaw: str | None = None

    # Direct flight flags
    YDirect: bool
    WDirect: bool
    JDirect: bool
    FDirect: bool
    YDirectRaw: bool | None = None
    WDirectRaw: bool | None = None
    JDirectRaw: bool | None = None
    FDirectRaw: bool | None = None

    # Metadata
    Source: str
    CreatedAt: str
    UpdatedAt: str
    AvailabilityTrips: list | None = None


class AvailabilityResponse(BaseModel):
    """Response from bulk availability endpoint."""

    data: list[AvailabilityResult]
    count: int
    hasMore: bool
    moreURL: str | None = None
    cursor: int


class SearchResult(BaseModel):
    """Single result from search endpoint."""

    ID: str
    RouteID: str
    Route: Route
    Date: str
    ParsedDate: str

    # Availability flags
    YAvailable: bool
    WAvailable: bool
    JAvailable: bool
    FAvailable: bool
    YAvailableRaw: bool | None = None
    WAvailableRaw: bool | None = None
    JAvailableRaw: bool | None = None
    FAvailableRaw: bool | None = None

    # Mileage costs (string format, filtered)
    YMileageCost: str | None = None
    WMileageCost: str | None = None
    JMileageCost: str | None = None
    FMileageCost: str | None = None

    # Mileage costs (raw integer)
    YMileageCostRaw: int | None = None
    WMileageCostRaw: int | None = None
    JMileageCostRaw: int | None = None
    FMileageCostRaw: int | None = None

    # Direct mileage costs
    YDirectMileageCost: int | None = None
    WDirectMileageCost: int | None = None
    JDirectMileageCost: int | None = None
    FDirectMileageCost: int | None = None
    YDirectMileageCostRaw: int | None = None
    WDirectMileageCostRaw: int | None = None
    JDirectMileageCostRaw: int | None = None
    FDirectMileageCostRaw: int | None = None

    # Taxes
    TaxesCurrency: str | None = None
    YTotalTaxes: int | None = None
    WTotalTaxes: int | None = None
    JTotalTaxes: int | None = None
    FTotalTaxes: int | None = None
    YTotalTaxesRaw: int | None = None
    WTotalTaxesRaw: int | None = None
    JTotalTaxesRaw: int | None = None
    FTotalTaxesRaw: int | None = None

    # Direct taxes
    YDirectTotalTaxes: int | None = None
    WDirectTotalTaxes: int | None = None
    JDirectTotalTaxes: int | None = None
    FDirectTotalTaxes: int | None = None
    YDirectTotalTaxesRaw: int | None = None
    WDirectTotalTaxesRaw: int | None = None
    JDirectTotalTaxesRaw: int | None = None
    FDirectTotalTaxesRaw: int | None = None

    # Remaining seats
    YRemainingSeats: int
    WRemainingSeats: int
    JRemainingSeats: int
    FRemainingSeats: int
    YRemainingSeatsRaw: int | None = None
    WRemainingSeatsRaw: int | None = None
    JRemainingSeatsRaw: int | None = None
    FRemainingSeatsRaw: int | None = None

    # Direct remaining seats
    YDirectRemainingSeats: int | None = None
    WDirectRemainingSeats: int | None = None
    JDirectRemainingSeats: int | None = None
    FDirectRemainingSeats: int | None = None
    YDirectRemainingSeatsRaw: int | None = None
    WDirectRemainingSeatsRaw: int | None = None
    JDirectRemainingSeatsRaw: int | None = None
    FDirectRemainingSeatsRaw: int | None = None

    # Airlines
    YAirlines: str | None = None
    WAirlines: str | None = None
    JAirlines: str | None = None
    FAirlines: str | None = None
    YAirlinesRaw: str | None = None
    WAirlinesRaw: str | None = None
    JAirlinesRaw: str | None = None
    FAirlinesRaw: str | None = None

    # Direct airlines
    YDirectAirlines: str | None = None
    WDirectAirlines: str | None = None
    JDirectAirlines: str | None = None
    FDirectAirlines: str | None = None
    YDirectAirlinesRaw: str | None = None
    WDirectAirlinesRaw: str | None = None
    JDirectAirlinesRaw: str | None = None
    FDirectAirlinesRaw: str | None = None

    # Direct flight flags
    YDirect: bool
    WDirect: bool
    JDirect: bool
    FDirect: bool
    YDirectRaw: bool | None = None
    WDirectRaw: bool | None = None
    JDirectRaw: bool | None = None
    FDirectRaw: bool | None = None

    # Metadata
    Source: str
    CreatedAt: str
    UpdatedAt: str
    AvailabilityTrips: list | None = None


class SearchResponse(BaseModel):
    """Response from search endpoint."""

    data: list[SearchResult]
    count: int
    hasMore: bool
    moreURL: str | None = None
    cursor: int


class TripDetails(BaseModel):
    """Trip details with flexible additional fields."""

    ID: str
    model_config = ConfigDict(extra="allow")
