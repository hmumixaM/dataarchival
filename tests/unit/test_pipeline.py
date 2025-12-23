"""Unit tests for data ingestion pipeline."""

import pandas as pd
import pytest

from award_archive.pipeline.seats_aero import flatten_availability_data


class TestFlattenAvailabilityData:
    """Tests for flatten_availability_data function."""

    @pytest.fixture
    def sample_availability_data(self):
        return [
            {
                "ID": "avail-001",
                "RouteID": "route-1",
                "Route": {
                    "ID": "route-1",
                    "OriginAirport": "SFO",
                    "OriginRegion": "North America",
                    "DestinationAirport": "NRT",
                    "DestinationRegion": "Asia",
                    "NumDaysOut": 60,
                    "Distance": 5000,
                    "Source": "aeroplan",
                },
                "Date": "2025-03-15",
                "ParsedDate": "2025-03-15T00:00:00Z",
                # Availability flags
                "YAvailable": True,
                "WAvailable": False,
                "JAvailable": True,
                "FAvailable": False,
                "YAvailableRaw": True,
                "WAvailableRaw": False,
                "JAvailableRaw": True,
                "FAvailableRaw": False,
                # Mileage costs
                "YMileageCost": "50000",
                "WMileageCost": "0",
                "JMileageCost": "85000",
                "FMileageCost": "0",
                "YMileageCostRaw": 50000,
                "WMileageCostRaw": 0,
                "JMileageCostRaw": 85000,
                "FMileageCostRaw": 0,
                # Direct mileage costs
                "YDirectMileageCost": 50000,
                "WDirectMileageCost": 0,
                "JDirectMileageCost": 85000,
                "FDirectMileageCost": 0,
                "YDirectMileageCostRaw": 50000,
                "WDirectMileageCostRaw": 0,
                "JDirectMileageCostRaw": 85000,
                "FDirectMileageCostRaw": 0,
                # Taxes
                "TaxesCurrency": "USD",
                "YTotalTaxes": 5600,
                "WTotalTaxes": 0,
                "JTotalTaxes": 5600,
                "FTotalTaxes": 0,
                "YTotalTaxesRaw": 5600,
                "WTotalTaxesRaw": 0,
                "JTotalTaxesRaw": 5600,
                "FTotalTaxesRaw": 0,
                # Direct taxes
                "YDirectTotalTaxes": 5600,
                "WDirectTotalTaxes": 0,
                "JDirectTotalTaxes": 5600,
                "FDirectTotalTaxes": 0,
                "YDirectTotalTaxesRaw": 5600,
                "WDirectTotalTaxesRaw": 0,
                "JDirectTotalTaxesRaw": 5600,
                "FDirectTotalTaxesRaw": 0,
                # Remaining seats
                "YRemainingSeats": 2,
                "WRemainingSeats": 0,
                "JRemainingSeats": 1,
                "FRemainingSeats": 0,
                "YRemainingSeatsRaw": 2,
                "WRemainingSeatsRaw": 0,
                "JRemainingSeatsRaw": 1,
                "FRemainingSeatsRaw": 0,
                # Direct remaining seats
                "YDirectRemainingSeats": 2,
                "WDirectRemainingSeats": 0,
                "JDirectRemainingSeats": 1,
                "FDirectRemainingSeats": 0,
                "YDirectRemainingSeatsRaw": 2,
                "WDirectRemainingSeatsRaw": 0,
                "JDirectRemainingSeatsRaw": 1,
                "FDirectRemainingSeatsRaw": 0,
                # Airlines
                "YAirlines": "AC",
                "WAirlines": "",
                "JAirlines": "AC",
                "FAirlines": "",
                "YAirlinesRaw": "AC",
                "WAirlinesRaw": "",
                "JAirlinesRaw": "AC",
                "FAirlinesRaw": "",
                # Direct airlines
                "YDirectAirlines": "AC",
                "WDirectAirlines": "",
                "JDirectAirlines": "AC",
                "FDirectAirlines": "",
                "YDirectAirlinesRaw": "AC",
                "WDirectAirlinesRaw": "",
                "JDirectAirlinesRaw": "AC",
                "FDirectAirlinesRaw": "",
                # Direct flags
                "YDirect": True,
                "WDirect": False,
                "JDirect": True,
                "FDirect": False,
                "YDirectRaw": True,
                "WDirectRaw": False,
                "JDirectRaw": True,
                "FDirectRaw": False,
                # Metadata
                "Source": "aeroplan",
                "CreatedAt": "2025-01-01T00:00:00Z",
                "UpdatedAt": "2025-01-01T00:00:00Z",
            }
        ]

    def test_returns_dataframe(self, sample_availability_data):
        """Should return a pandas DataFrame."""
        result = flatten_availability_data(sample_availability_data)
        assert isinstance(result, pd.DataFrame)

    def test_flattens_route_info(self, sample_availability_data):
        """Should flatten nested Route information."""
        result = flatten_availability_data(sample_availability_data)

        assert "OriginAirport" in result.columns
        assert "DestinationAirport" in result.columns
        assert "OriginRegion" in result.columns
        assert "DestinationRegion" in result.columns
        assert "NumDaysOut" in result.columns
        assert "Distance" in result.columns

        assert result["OriginAirport"].iloc[0] == "SFO"
        assert result["DestinationAirport"].iloc[0] == "NRT"
        assert result["NumDaysOut"].iloc[0] == 60

    def test_preserves_top_level_fields(self, sample_availability_data):
        """Should preserve all top-level fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["ID"].iloc[0] == "avail-001"
        assert result["RouteID"].iloc[0] == "route-1"
        assert result["Date"].iloc[0] == "2025-03-15"
        assert result["Source"].iloc[0] == "aeroplan"

    def test_preserves_availability_flags(self, sample_availability_data):
        """Should preserve cabin availability flags."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YAvailable"].iloc[0] == True  # noqa: E712 (numpy bool comparison)
        assert result["WAvailable"].iloc[0] == False  # noqa: E712
        assert result["JAvailable"].iloc[0] == True  # noqa: E712
        assert result["FAvailable"].iloc[0] == False  # noqa: E712

    def test_preserves_mileage_costs(self, sample_availability_data):
        """Should preserve mileage costs."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YMileageCost"].iloc[0] == "50000"
        assert result["WMileageCost"].iloc[0] == "0"
        assert result["JMileageCost"].iloc[0] == "85000"
        assert result["FMileageCost"].iloc[0] == "0"

    def test_preserves_remaining_seats(self, sample_availability_data):
        """Should preserve remaining seats counts."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YRemainingSeats"].iloc[0] == 2
        assert result["WRemainingSeats"].iloc[0] == 0
        assert result["JRemainingSeats"].iloc[0] == 1
        assert result["FRemainingSeats"].iloc[0] == 0

    def test_preserves_direct_flight_flags(self, sample_availability_data):
        """Should preserve direct flight flags."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YDirect"].iloc[0] == True  # noqa: E712 (numpy bool comparison)
        assert result["WDirect"].iloc[0] == False  # noqa: E712
        assert result["JDirect"].iloc[0] == True  # noqa: E712
        assert result["FDirect"].iloc[0] == False  # noqa: E712

    def test_preserves_raw_mileage_costs(self, sample_availability_data):
        """Should preserve raw mileage cost fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YMileageCostRaw"].iloc[0] == 50000
        assert result["WMileageCostRaw"].iloc[0] == 0
        assert result["JMileageCostRaw"].iloc[0] == 85000
        assert result["FMileageCostRaw"].iloc[0] == 0

    def test_preserves_taxes_fields(self, sample_availability_data):
        """Should preserve taxes fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["TaxesCurrency"].iloc[0] == "USD"
        assert result["YTotalTaxes"].iloc[0] == 5600
        assert result["WTotalTaxes"].iloc[0] == 0
        assert result["JTotalTaxes"].iloc[0] == 5600

    def test_preserves_direct_mileage_costs(self, sample_availability_data):
        """Should preserve direct mileage cost fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YDirectMileageCost"].iloc[0] == 50000
        assert result["JDirectMileageCost"].iloc[0] == 85000

    def test_preserves_direct_remaining_seats(self, sample_availability_data):
        """Should preserve direct remaining seats fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YDirectRemainingSeats"].iloc[0] == 2
        assert result["JDirectRemainingSeats"].iloc[0] == 1

    def test_preserves_raw_availability_flags(self, sample_availability_data):
        """Should preserve raw availability flags."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YAvailableRaw"].iloc[0] == True  # noqa: E712
        assert result["WAvailableRaw"].iloc[0] == False  # noqa: E712

    def test_preserves_direct_airlines(self, sample_availability_data):
        """Should preserve direct airlines fields."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YDirectAirlines"].iloc[0] == "AC"
        assert result["WDirectAirlines"].iloc[0] == ""

    def test_preserves_raw_direct_flags(self, sample_availability_data):
        """Should preserve raw direct flight flags."""
        result = flatten_availability_data(sample_availability_data)

        assert result["YDirectRaw"].iloc[0] == True  # noqa: E712
        assert result["WDirectRaw"].iloc[0] == False  # noqa: E712

    def test_handles_empty_list(self):
        """Should return empty DataFrame for empty input."""
        result = flatten_availability_data([])
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_handles_missing_route(self):
        """Should handle missing Route gracefully."""
        data = [
            {
                "ID": "avail-001",
                "RouteID": "route-1",
                # Route is missing
                "Date": "2025-03-15",
                "ParsedDate": "2025-03-15",
                "YAvailable": True,
                "WAvailable": False,
                "JAvailable": True,
                "FAvailable": False,
                "YMileageCost": None,
                "WMileageCost": None,
                "JMileageCost": None,
                "FMileageCost": None,
                "YRemainingSeats": 0,
                "WRemainingSeats": 0,
                "JRemainingSeats": 0,
                "FRemainingSeats": 0,
                "YAirlines": None,
                "WAirlines": None,
                "JAirlines": None,
                "FAirlines": None,
                "YDirect": False,
                "WDirect": False,
                "JDirect": False,
                "FDirect": False,
                "Source": "unknown",
                "CreatedAt": "2025-01-01T00:00:00Z",
                "UpdatedAt": "2025-01-01T00:00:00Z",
            }
        ]
        result = flatten_availability_data(data)

        # Should have None for route fields
        assert result["OriginAirport"].iloc[0] is None
        assert result["DestinationAirport"].iloc[0] is None

    def test_handles_multiple_records(self):
        """Should handle multiple records correctly."""
        data = [
            {
                "ID": f"avail-{i:03d}",
                "RouteID": f"route-{i}",
                "Route": {"OriginAirport": f"ORG{i}", "DestinationAirport": f"DST{i}"},
                "Date": f"2025-03-{15 + i:02d}",
                "ParsedDate": f"2025-03-{15 + i:02d}",
                "YAvailable": True,
                "WAvailable": False,
                "JAvailable": True,
                "FAvailable": False,
                "YMileageCost": None,
                "WMileageCost": None,
                "JMileageCost": None,
                "FMileageCost": None,
                "YRemainingSeats": i,
                "WRemainingSeats": 0,
                "JRemainingSeats": 0,
                "FRemainingSeats": 0,
                "YAirlines": None,
                "WAirlines": None,
                "JAirlines": None,
                "FAirlines": None,
                "YDirect": False,
                "WDirect": False,
                "JDirect": False,
                "FDirect": False,
                "Source": "test",
                "CreatedAt": "2025-01-01T00:00:00Z",
                "UpdatedAt": "2025-01-01T00:00:00Z",
            }
            for i in range(5)
        ]

        result = flatten_availability_data(data)

        assert len(result) == 5
        assert list(result["ID"]) == [
            "avail-000",
            "avail-001",
            "avail-002",
            "avail-003",
            "avail-004",
        ]
