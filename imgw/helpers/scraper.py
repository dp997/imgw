from collections.abc import Iterable
from typing import Union
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import AttributeValueList, Tag
from dlt.sources.helpers import requests

from imgw.utils import get_logger

logger = get_logger(__name__)


def is_visited(current_url: str, visited_dirs: set) -> bool:
    if current_url in visited_dirs:
        logger.debug("Already visited: %s", current_url)
        return True
    visited_dirs.add(current_url)
    return False


def fetch_and_parse(current_url: str) -> Union[BeautifulSoup, None]:
    try:
        response = requests.get(current_url)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")
    except requests.RequestException:
        logger.exception("Error fetching %s", current_url)
        return None


def extract_links(soup: BeautifulSoup) -> list:
    try:
        return soup.find_all("a")
    except Exception:
        logger.exception("Error extracting links")
        return []


def process_link(
    link: Tag,
    current_url: str,
    found_zip_links: set,
    visited_dirs: set,
) -> None:
    try:
        href = link.get("href")
        link_text = link.text.strip()
        if (
            not href
            or isinstance(href, AttributeValueList)
            or href.startswith("?")
            or href.startswith("#")
            or link_text == "Parent Directory"
        ):
            return
        absolute_link = urljoin(current_url, href)
        if href.lower().endswith(".zip"):
            if absolute_link not in found_zip_links:
                logger.debug("Found zip: %s", absolute_link)
                found_zip_links.add(absolute_link)
        elif href.endswith("/") and absolute_link != current_url:
            logger.debug("Going into directory: %s", href)
            scrape_directory_recursive(absolute_link, visited_dirs, found_zip_links)
        else:
            logger.debug("Ignoring link: %s (absolute: %s)", href, absolute_link)
    except Exception:
        logger.exception("Encountered an error while processing link")


def scrape_directory_recursive(
    current_url: str,
    visited_dirs: set[str],
    found_zip_links: set[str],
) -> None:
    if is_visited(current_url, visited_dirs):
        return
    logger.debug("Scraping: %s", current_url)
    soup = fetch_and_parse(current_url)
    if soup is None:
        return
    links = extract_links(soup)
    for link in links:
        process_link(link, current_url, found_zip_links, visited_dirs)


def find_zip_links(start_url: str) -> Iterable[str]:
    if not start_url.endswith("/"):
        start_url += "/"

    local_found_zip_links: set[str] = set()
    local_visited_dirs: set[str] = set()

    scrape_directory_recursive(start_url, local_visited_dirs, local_found_zip_links)

    yield from list(local_found_zip_links)
