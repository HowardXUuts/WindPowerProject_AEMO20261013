"""Tests for parsing the AEMO directory listing.

The first live run failed here: the listing returned HTTP 200, the filename
pattern matched nothing, and the job reported success having ingested zero
rows. These tests pin both halves of that fix — the timestamp width AEMO
actually uses, and an empty match set being an error rather than a no-op.
"""

from datetime import datetime

import pytest
from ingest_dispatchis import DISPATCHIS as PRICE
from ingest_scada import SCADA
from nemweb import NEM_TZ, ListingError, interval_from_filename, list_remote_files

# An IIS-style listing, as nemweb serves it. Note the 12-digit timestamps.
LISTING = """<html><head><title>nemweb.com.au - /Reports/Current/Dispatch_SCADA/</title>
</head><body><H1>nemweb.com.au - /Reports/Current/Dispatch_SCADA/</H1><hr>
<pre><A HREF="/Reports/Current/">[To Parent Directory]</A><br><br>
 Monday, October 13, 2026  2:00 PM  52428 <A HREF="/Reports/Current/Dispatch_SCADA/\
PUBLIC_DISPATCHSCADA_202610131400_0000000123456789.zip">\
PUBLIC_DISPATCHSCADA_202610131400_0000000123456789.zip</A><br>
 Monday, October 13, 2026  2:05 PM  52430 <A HREF="/Reports/Current/Dispatch_SCADA/\
PUBLIC_DISPATCHSCADA_202610131405_0000000123456790.zip">\
PUBLIC_DISPATCHSCADA_202610131405_0000000123456790.zip</A><br>
</pre><hr></body></html>"""


class FakeResponse:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None


class FakeClient:
    """Minimal stand-in for httpx.Client; these tests never touch the network."""

    def __init__(self, text: str):
        self._text = text

    def get(self, url: str) -> FakeResponse:
        return FakeResponse(self._text)


def test_finds_files_in_a_real_listing():
    names = list_remote_files(SCADA, FakeClient(LISTING))
    assert names == [
        "PUBLIC_DISPATCHSCADA_202610131400_0000000123456789.zip",
        "PUBLIC_DISPATCHSCADA_202610131405_0000000123456790.zip",
    ]


def test_each_name_appears_once_despite_being_in_href_and_text():
    # The file name occurs twice per row in the HTML; it must not be counted twice.
    assert len(list_remote_files(SCADA, FakeClient(LISTING))) == 2


def test_empty_match_set_is_an_error_not_an_empty_day():
    # The original bug: HTTP 200, zero matches, job green with nothing ingested.
    # Current/ always holds ~2 days, so no matches means the pattern is wrong.
    body = "<html><body><pre>[To Parent Directory]</pre></body></html>"
    with pytest.raises(ListingError, match="no PUBLIC_DISPATCHSCADA file names"):
        list_remote_files(SCADA, FakeClient(body))


def test_listing_error_quotes_the_body_for_diagnosis():
    with pytest.raises(ListingError, match="First 200 chars"):
        list_remote_files(PRICE, FakeClient("<html><body>nothing here</body></html>"))


def test_a_listing_for_the_other_report_does_not_match():
    with pytest.raises(ListingError):
        list_remote_files(PRICE, FakeClient(LISTING))


def test_twelve_digit_timestamp_is_the_dispatch_convention():
    name = "PUBLIC_DISPATCHSCADA_202610131405_0000000123456789.zip"
    assert interval_from_filename(SCADA, name) == datetime(2026, 10, 13, 14, 5, tzinfo=NEM_TZ)


def test_fourteen_digit_timestamp_still_parses():
    # Some NEMWeb reports add seconds; accept both rather than guessing.
    name = "PUBLIC_DISPATCHSCADA_20261013140530_0000000123456789.zip"
    assert interval_from_filename(SCADA, name) == datetime(
        2026, 10, 13, 14, 5, 30, tzinfo=NEM_TZ
    )


def test_price_report_uses_the_same_convention():
    name = "PUBLIC_DISPATCHIS_202610131405_0000000123456789.zip"
    assert interval_from_filename(PRICE, name) == datetime(2026, 10, 13, 14, 5, tzinfo=NEM_TZ)
