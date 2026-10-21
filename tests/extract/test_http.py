"""Offline tests for the HTTP-facing extract helpers."""

import pytest

from imgw.extract.helpers import extract
from imgw.extract.helpers.extract import ImgwZip, fetch_zip_data, get_json_data


class FakeResponse:
    def __init__(self, *, status_code: int = 200, payload=None, content: bytes = b"") -> None:
        self.status_code = status_code
        self._payload = payload
        self.content = content

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise extract.requests.HTTPError(response=self)

    def json(self):
        return self._payload


def test_fetch_zip_data_returns_named_resource(monkeypatch):
    response = FakeResponse(content=b"zip-bytes")
    monkeypatch.setattr(extract.requests, "get", lambda *args, **kwargs: response)

    result = fetch_zip_data("https://example.com/subdir/test.zip")

    assert isinstance(result, ImgwZip)
    assert result.filename == "test.zip"
    assert result.content == b"zip-bytes"


def test_fetch_zip_data_reraises_request_errors(monkeypatch):
    def fail(*args, **kwargs):
        raise extract.requests.RequestException("boom")

    monkeypatch.setattr(extract.requests, "get", fail)

    with pytest.raises(extract.requests.RequestException):
        fetch_zip_data("https://example.com/test.zip")


def test_get_json_data_yields_payload(monkeypatch):
    payload = [{"station": "Warszawa"}]
    monkeypatch.setattr(
        extract.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(payload=payload),
    )

    assert list(get_json_data("synop")) == [payload]


def test_get_json_data_skips_empty_404(monkeypatch):
    payload = {"status": False, "message": "No products were found"}
    monkeypatch.setattr(
        extract.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(status_code=404, payload=payload),
    )

    assert list(get_json_data("synop")) == [{}]


def test_get_json_data_reraises_other_404(monkeypatch):
    payload = {"status": False, "message": "something else"}
    monkeypatch.setattr(
        extract.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(status_code=404, payload=payload),
    )

    with pytest.raises(extract.requests.HTTPError):
        list(get_json_data("synop"))


def test_get_json_data_reraises_http_error_without_response(monkeypatch):
    def fail(*args, **kwargs):
        raise extract.requests.HTTPError()

    monkeypatch.setattr(extract.requests, "get", fail)

    with pytest.raises(extract.requests.HTTPError):
        list(get_json_data("synop"))
