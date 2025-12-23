"""Pydantic models for iPrefer hotel availability."""

from pydantic import BaseModel, Field


class RateCalendarResult(BaseModel):
    """Single day rate from the calendar API."""

    lowestRate: float | None = None
    lowestPointsRate: int | None = None
    available: bool = False


class RateCalendarResponse(BaseModel):
    """Response from rate calendar API."""

    count: int
    results: dict[str, RateCalendarResult] = Field(default_factory=dict)
    currency_code: str | None = None


class HotelItem(BaseModel):
    """Hotel link from directory listing."""

    site: str
    path: str
    title: str

    @property
    def url(self) -> str:
        """Full URL to hotel page."""
        return f"{self.site.rstrip('/')}/{self.path.lstrip('/')}"


class HotelDetails(BaseModel):
    """Hotel details from hotel page."""

    nid: int
    name: str | None = None
    url: str
    canonical_url: str | None = None
    code: str | None = None
    num_rooms: int | None = None
    country: str | None = None
    title: str | None = None
    description: str | None = None
    choice_points: str | int | None = None
    average_rate: str | None = None
    synxis_id: str | int | None = None
    book_with_points: bool | None = None
    book_with_choice: bool | None = None


class HotelAvailability(BaseModel):
    """Hotel availability for a specific date."""

    nid: int
    currency_code: str | None = None
    date: str
    is_points: bool
    lowest_rate: float | None = None
    lowest_points_rate: int | None = None
    available: bool = False

