"""iPrefer (Preferred Hotels) API client."""

import json
import logging

import backoff
import httpx
from bs4 import BeautifulSoup

from award_archive.models.iprefer import (
    HotelAvailability,
    HotelDetails,
    HotelItem,
    RateCalendarResponse,
)

log = logging.getLogger(__name__)

SITE = "https://preferredhotels.com"
DIRECTORY_URL = f"{SITE}/directory"
RATE_CALENDAR_URL = "https://ptgapis.com/rate-calendar/v2"


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, httpx.TimeoutException),
    max_tries=5,
    on_backoff=lambda details: log.warning(
        f"Hotel list request failed, retrying (attempt {details['tries']}/5)..."
    ),
)
def fetch_hotel_links(client: httpx.Client) -> list[HotelItem]:
    """Fetch list of all hotels from the directory.

    Args:
        client: httpx client for making requests

    Returns:
        List of HotelItem objects with links to hotel pages
    """
    log.info("Fetching hotel directory")
    response = client.get(DIRECTORY_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    hotel_containers = soup.find_all("div", class_="directory-card__button__container")

    hotel_links = [
        HotelItem(
            site=SITE,
            path=link.get("href", ""),
            title=(link.get("title", "") or "").strip(),
        )
        for container in hotel_containers
        if (link := container.find("a"))
    ]

    log.info(f"Found {len(hotel_links)} hotels in directory")
    return hotel_links


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, httpx.TimeoutException),
    max_tries=5,
    on_backoff=lambda details: log.warning(
        f"Hotel details request failed, retrying (attempt {details['tries']}/5)..."
    ),
)
def fetch_hotel_details(client: httpx.Client, url: str) -> HotelDetails:
    """Fetch details for a single hotel.

    Args:
        client: httpx client for making requests
        url: Full URL to hotel page

    Returns:
        HotelDetails object with hotel information
    """
    log.debug(f"Fetching details for {url}")
    response = client.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    details_json = soup.find_all("script", id="__NEXT_DATA__")

    if len(details_json) != 1:
        raise ValueError(f"Expected 1 __NEXT_DATA__ script, found {len(details_json)}")

    data = json.loads(details_json[0].text)

    page = data.get("props", {}).get("pageProps", {})
    content = page.get("nodeContent", {})

    return HotelDetails(
        nid=content.get("nid"),
        name=content.get("fieldDisplayTitle"),
        url=url,
        canonical_url=page.get("metaTags", {}).get("canonical_url"),
        code=content.get("fieldItemCode"),
        num_rooms=content.get("fieldNumRooms"),
        country=content.get("fieldCountryName"),
        title=page.get("metaTags", {}).get("title"),
        description=page.get("metaTags", {}).get("description"),
        choice_points=content.get("choicePointsValue"),
        average_rate=content.get("fieldAvgRateDisplay"),
        synxis_id=content.get("fieldSynxisId"),
        book_with_points=content.get("fieldIPreferBookWithPoints"),
        book_with_choice=content.get("participatesInChoicePoints"),
    )


@backoff.on_exception(
    backoff.expo,
    (httpx.HTTPError, httpx.TimeoutException),
    max_tries=5,
    on_backoff=lambda details: log.warning(
        f"Calendar request failed, retrying (attempt {details['tries']}/5)..."
    ),
)
def fetch_calendar(
    client: httpx.Client,
    nid: int,
    is_points: bool = True,
    adults: int = 1,
    children: int = 0,
) -> list[HotelAvailability]:
    """Fetch availability calendar for a hotel.

    Args:
        client: httpx client for making requests
        nid: Hotel NID from iPrefer
        is_points: If True, fetch points rates; if False, fetch cash rates
        adults: Number of adults
        children: Number of children

    Returns:
        List of HotelAvailability objects for each date
    """
    log.debug(f"Fetching {'points' if is_points else 'cash'} calendar for nid={nid}")

    params: dict = {"nid": nid, "adults": adults, "children": children}
    if is_points:
        params["rateCode"] = "IPPOINTS"

    response = client.get(RATE_CALENDAR_URL, params=params)
    response.raise_for_status()

    data = RateCalendarResponse.model_validate(response.json())

    availability_list = [
        HotelAvailability(
            nid=nid,
            currency_code=data.currency_code,
            date=date_str,
            is_points=is_points,
            lowest_rate=rate.lowestRate,
            lowest_points_rate=rate.lowestPointsRate,
            available=rate.available,
        )
        for date_str, rate in data.results.items()
    ]

    if len(availability_list) != data.count:
        log.warning(
            f"Count mismatch for nid={nid}: got {len(availability_list)}, expected {data.count}"
        )

    return availability_list

