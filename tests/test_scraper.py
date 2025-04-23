import unittest
from unittest.mock import MagicMock, patch

from bs4 import BeautifulSoup

from imgw.helpers.scraper import (
    extract_links,
    fetch_and_parse,
    find_zip_links,
    is_visited,
    process_link,
    scrape_directory_recursive,
)


class TestScraper(unittest.TestCase):
    def test_is_visited(self):
        visited_dirs = set()
        self.assertFalse(is_visited("https://example.com", visited_dirs))
        self.assertTrue(is_visited("https://example.com", visited_dirs))

    @patch("imgw.helpers.scraper.requests.get")
    def test_fetch_and_parse(self, mock_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"<html></html>"
        mock_get.return_value = mock_response
        soup = fetch_and_parse("https://example.com")
        self.assertIsInstance(soup, BeautifulSoup)

    # dont know how to mock dlt.sources.helpers.requests
    # @patch("imgw.helpers.scraper.requests.get")
    # def test_fetch_and_parse_exception(self, mock_get):
    #     mock_get.side_effect = Exception("Mocked exception")
    #     soup = fetch_and_parse("https://example.com", {}, 0)
    #     self.assertIsNone(soup)

    def test_extract_links(self):
        soup = BeautifulSoup("<html><a href='#'>Link</a></html>", "html.parser")
        links = extract_links(soup)
        self.assertEqual(len(links), 1)

    def test_process_link_zip(self):
        link = MagicMock()
        link.get.return_value = "file.zip"
        link.text.strip.return_value = "File"
        found_zip_links = set()
        process_link(link, "https://example.com", found_zip_links, set())
        self.assertEqual(len(found_zip_links), 1)

    def test_process_link_directory(self):
        link = MagicMock()
        link.get.return_value = "dir/"
        link.text.strip.return_value = "Directory"
        visited_dirs = set()
        found_zip_links = set()
        with patch("imgw.helpers.scraper.scrape_directory_recursive") as mock_scrape:
            process_link(link, "https://example.com", found_zip_links, visited_dirs)
            mock_scrape.assert_called_once()

    @patch("imgw.helpers.scraper.fetch_and_parse")
    @patch("imgw.helpers.scraper.extract_links")
    def test_scrape_directory_recursive(self, mock_extract_links, mock_fetch_and_parse):
        mock_fetch_and_parse.return_value = BeautifulSoup("<html></html>", "html.parser")
        mock_extract_links.return_value = []
        scrape_directory_recursive("https://example.com", set(), set())
        mock_fetch_and_parse.assert_called_once()

    def test_find_zip_links(self):
        with patch("imgw.helpers.scraper.scrape_directory_recursive") as mock_scrape:
            list(find_zip_links("https://example.com"))
            mock_scrape.assert_called_once()


if __name__ == "__main__":
    unittest.main()
