"""Unit tests for data models."""

import pytest

from award_archive.models import (
    AvailabilityResponse,
    AvailabilityResult,
    Route,
    TripDetails,
)


class TestRoute:
    """Tests for Route model."""

    def test_create_route(self):
        """Should create a Route from valid data."""
        route = Route(
            ID="route-1",
            OriginAirport="SFO",
            OriginRegion="North America",
            DestinationAirport="NRT",
            DestinationRegion="Asia",
            NumDaysOut=30,
            Distance=5000,
            Source="aeroplan",
        )
        assert route.ID == "route-1"
        assert route.OriginAirport == "SFO"
        assert route.DestinationAirport == "NRT"


class TestAvailabilityResult:
    """Tests for AvailabilityResult model."""

    @pytest.fixture
    def sample_route(self):
        return Route(
            ID="route-1",
            OriginAirport="SFO",
            OriginRegion="North America",
            DestinationAirport="NRT",
            DestinationRegion="Asia",
            NumDaysOut=30,
            Distance=5000,
            Source="aeroplan",
        )

    def test_create_availability_result(self, sample_route):
        """Should create an AvailabilityResult from valid data."""
        result = AvailabilityResult(
            ID="avail-1",
            RouteID="route-1",
            Route=sample_route,
            Date="2025-03-15",
            ParsedDate="2025-03-15",
            YAvailable=True,
            WAvailable=False,
            JAvailable=True,
            FAvailable=False,
            YMileageCost="50000",
            WMileageCost=None,
            JMileageCost="85000",
            FMileageCost=None,
            YRemainingSeats=2,
            WRemainingSeats=0,
            JRemainingSeats=1,
            FRemainingSeats=0,
            YAirlines="AC",
            WAirlines=None,
            JAirlines="AC",
            FAirlines=None,
            YDirect=True,
            WDirect=False,
            JDirect=True,
            FDirect=False,
            Source="aeroplan",
            CreatedAt="2025-01-01T00:00:00Z",
            UpdatedAt="2025-01-01T00:00:00Z",
        )
        assert result.ID == "avail-1"
        assert result.JAvailable is True
        assert result.JMileageCost == "85000"

    def test_create_availability_result_with_all_fields(self, sample_route):
        """Should create an AvailabilityResult with all new API fields."""
        result = AvailabilityResult(
            ID="avail-1",
            RouteID="route-1",
            Route=sample_route,
            Date="2025-03-15",
            ParsedDate="2025-03-15T00:00:00Z",
            # Availability flags
            YAvailable=True,
            WAvailable=False,
            JAvailable=True,
            FAvailable=True,
            YAvailableRaw=True,
            WAvailableRaw=False,
            JAvailableRaw=True,
            FAvailableRaw=True,
            # Mileage costs
            YMileageCost="44000",
            WMileageCost="0",
            JMileageCost="80000",
            FMileageCost="140000",
            YMileageCostRaw=44000,
            WMileageCostRaw=0,
            JMileageCostRaw=80000,
            FMileageCostRaw=140000,
            # Direct mileage costs
            YDirectMileageCost=44000,
            WDirectMileageCost=0,
            JDirectMileageCost=80000,
            FDirectMileageCost=140000,
            YDirectMileageCostRaw=44000,
            WDirectMileageCostRaw=0,
            JDirectMileageCostRaw=80000,
            FDirectMileageCostRaw=140000,
            # Taxes
            TaxesCurrency="EUR",
            YTotalTaxes=14672,
            WTotalTaxes=0,
            JTotalTaxes=14672,
            FTotalTaxes=14672,
            YTotalTaxesRaw=14672,
            WTotalTaxesRaw=0,
            JTotalTaxesRaw=14672,
            FTotalTaxesRaw=14672,
            # Direct taxes
            YDirectTotalTaxes=14672,
            WDirectTotalTaxes=0,
            JDirectTotalTaxes=14672,
            FDirectTotalTaxes=14672,
            YDirectTotalTaxesRaw=14672,
            WDirectTotalTaxesRaw=0,
            JDirectTotalTaxesRaw=14672,
            FDirectTotalTaxesRaw=14672,
            # Remaining seats
            YRemainingSeats=9,
            WRemainingSeats=0,
            JRemainingSeats=3,
            FRemainingSeats=7,
            YRemainingSeatsRaw=9,
            WRemainingSeatsRaw=0,
            JRemainingSeatsRaw=3,
            FRemainingSeatsRaw=7,
            # Direct remaining seats
            YDirectRemainingSeats=9,
            WDirectRemainingSeats=0,
            JDirectRemainingSeats=3,
            FDirectRemainingSeats=7,
            YDirectRemainingSeatsRaw=9,
            WDirectRemainingSeatsRaw=0,
            JDirectRemainingSeatsRaw=3,
            FDirectRemainingSeatsRaw=7,
            # Airlines
            YAirlines="AC, LH, UA",
            WAirlines="",
            JAirlines="UA",
            FAirlines="AC, LH, UA",
            YAirlinesRaw="AC, LH, UA",
            WAirlinesRaw="",
            JAirlinesRaw="UA",
            FAirlinesRaw="AC, LH, UA",
            # Direct airlines
            YDirectAirlines="LH, UA",
            WDirectAirlines="",
            JDirectAirlines="UA",
            FDirectAirlines="LH",
            YDirectAirlinesRaw="LH, UA",
            WDirectAirlinesRaw="",
            JDirectAirlinesRaw="UA",
            FDirectAirlinesRaw="LH",
            # Direct flags
            YDirect=True,
            WDirect=False,
            JDirect=True,
            FDirect=True,
            YDirectRaw=True,
            WDirectRaw=False,
            JDirectRaw=True,
            FDirectRaw=True,
            # Metadata
            Source="united",
            CreatedAt="2025-01-22T14:32:09.856737Z",
            UpdatedAt="2025-12-22T14:32:00.127464Z",
            AvailabilityTrips=None,
        )
        # Verify new fields
        assert result.YMileageCostRaw == 44000
        assert result.TaxesCurrency == "EUR"
        assert result.YTotalTaxes == 14672
        assert result.YDirectMileageCost == 44000
        assert result.YDirectRemainingSeats == 9
        assert result.YDirectAirlines == "LH, UA"
        assert result.YDirectRaw is True


class TestAvailabilityResponse:
    """Tests for AvailabilityResponse model."""

    def test_create_empty_response(self):
        """Should create an empty response."""
        response = AvailabilityResponse(
            data=[],
            count=0,
            hasMore=False,
            cursor=0,
        )
        assert len(response.data) == 0
        assert response.hasMore is False

    def test_create_response_with_more_url(self):
        """Should create a response with moreURL field."""
        response = AvailabilityResponse(
            data=[],
            count=10,
            hasMore=True,
            moreURL="/partnerapi/availability?take=10&skip=10&cursor=123&source=united",
            cursor=123,
        )
        assert response.hasMore is True
        assert response.moreURL == "/partnerapi/availability?take=10&skip=10&cursor=123&source=united"
        assert response.cursor == 123


class TestTripDetails:
    """Tests for TripDetails model."""

    def test_allows_extra_fields(self):
        """Should allow extra fields due to ConfigDict(extra='allow')."""
        trip = TripDetails(
            ID="trip-1",
            extra_field="some_value",
            another_field=123,
        )
        assert trip.ID == "trip-1"
        assert trip.extra_field == "some_value"
        assert trip.another_field == 123

