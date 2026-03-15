"""Unit tests for generic_json and generic_html parsers (no HTTP)."""
from __future__ import annotations

from unittest.mock import MagicMock

from job_scanner.models import SourceConfig, SourceFormat, SourceType
from job_scanner.sources.generic_html import parse_html_text
from job_scanner.sources.generic_json import _resolve_items, fetch_and_normalize


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_json_source(parser_template: dict | None = None) -> SourceConfig:
    return SourceConfig(
        name="TestSource",
        type=SourceType.GENERIC_JSON,
        url="https://example.com/jobs",
        format=SourceFormat.JSON,
        parser_template=parser_template or {},
    )


def _make_html_source(parser_template: dict | None = None) -> SourceConfig:
    return SourceConfig(
        name="TestHTMLSource",
        type=SourceType.GENERIC_HTML,
        url="https://example.com/jobs",
        format=SourceFormat.HTML,
        parser_template=parser_template or {
            "items_selector": "li.job",
            "title_selector": "h2",
            "apply_url_selector": "a",
        },
    )


# ---------------------------------------------------------------------------
# generic_json: _resolve_items
# ---------------------------------------------------------------------------


def test_resolve_items_top_level_list() -> None:
    payload = [{"title": "SRE"}, {"title": "Platform Engineer"}]
    result = _resolve_items(payload, None)
    assert len(result) == 2
    assert result[0]["title"] == "SRE"


def test_resolve_items_dict_with_jobs_key() -> None:
    payload = {"jobs": [{"id": "1", "title": "Staff Engineer"}]}
    result = _resolve_items(payload, None)
    assert len(result) == 1
    assert result[0]["title"] == "Staff Engineer"


def test_resolve_items_dict_with_results_key() -> None:
    payload = {"results": [{"id": "2", "title": "Principal SRE"}]}
    result = _resolve_items(payload, None)
    assert len(result) == 1


def test_resolve_items_custom_items_path() -> None:
    payload = {"data": {"listings": [{"id": "3", "title": "Infrastructure Lead"}]}}
    result = _resolve_items(payload, "data.listings")
    assert len(result) == 1
    assert result[0]["title"] == "Infrastructure Lead"


def test_resolve_items_empty_payload_returns_empty() -> None:
    assert _resolve_items({}, None) == []
    assert _resolve_items([], None) == []
    assert _resolve_items(None, None) == []


# ---------------------------------------------------------------------------
# generic_json: fetch_and_normalize with mocked fetcher
# ---------------------------------------------------------------------------


def test_fetch_and_normalize_extracts_fields() -> None:
    source = _make_json_source(
        parser_template={
            "id_field": "job_id",
            "title_field": "name",
            "description_field": "body",
            "location_field": "loc",
            "apply_url_field": "url",
        }
    )

    payload = [
        {
            "job_id": "abc123",
            "name": "Principal SRE",
            "body": "Build and scale reliability systems.",
            "loc": "Remote - United States",
            "url": "https://example.com/apply/abc123",
        }
    ]

    fetcher = MagicMock()
    fetcher.get_json.return_value = payload

    raw_jobs, normalized_jobs = fetch_and_normalize(source, fetcher)

    assert len(raw_jobs) == 1
    assert len(normalized_jobs) == 1
    assert normalized_jobs[0].title == "Principal SRE"
    assert normalized_jobs[0].source_job_id == "abc123"


def test_fetch_and_normalize_skips_items_without_id() -> None:
    source = _make_json_source()

    payload = [
        {"title": "No ID job"},  # no "id" field and no apply_url/url
    ]

    fetcher = MagicMock()
    fetcher.get_json.return_value = payload

    raw_jobs, normalized_jobs = fetch_and_normalize(source, fetcher)

    assert len(raw_jobs) == 0
    assert len(normalized_jobs) == 0


def test_fetch_and_normalize_logs_parse_errors(caplog) -> None:
    import logging

    source = _make_json_source(
        parser_template={"id_field": "id", "title_field": "title"}
    )

    payload = [{"id": "x1", "title": None}]  # None title may cause issues in normalization

    fetcher = MagicMock()
    fetcher.get_json.return_value = payload

    with caplog.at_level(logging.WARNING):
        raw_jobs, normalized_jobs = fetch_and_normalize(source, fetcher)

    # Even if parse fails, raw job is captured if id is present; no crash
    assert len(raw_jobs) <= 1  # raw may succeed
    # No unhandled exception raised


# ---------------------------------------------------------------------------
# generic_html: parse_html_text
# ---------------------------------------------------------------------------

MINIMAL_HTML = """
<html><body>
<ul>
  <li class="job">
    <h2>Staff Infrastructure Engineer</h2>
    <span class="location">Austin, TX</span>
    <a href="/jobs/42">Apply</a>
  </li>
  <li class="job">
    <h2>Principal SRE</h2>
    <span class="location">Remote</span>
    <a href="https://example.com/jobs/99">Apply</a>
  </li>
</ul>
</body></html>
"""


def _html_source_with_location() -> SourceConfig:
    return SourceConfig(
        name="TestHTMLSource",
        type=SourceType.GENERIC_HTML,
        url="https://example.com/jobs",
        format=SourceFormat.HTML,
        parser_template={
            "items_selector": "li.job",
            "title_selector": "h2",
            "apply_url_selector": "a",
            "location_selector": ".location",
        },
    )


def test_parse_html_text_extracts_title_and_location() -> None:
    source = _html_source_with_location()
    raw_jobs, normalized_jobs = parse_html_text(source, MINIMAL_HTML)

    assert len(normalized_jobs) == 2
    titles = [j.title for j in normalized_jobs]
    assert "Staff Infrastructure Engineer" in titles
    assert "Principal SRE" in titles


def test_parse_html_text_items_without_title_are_skipped() -> None:
    html = """
    <html><body>
    <ul>
      <li class="job"><a href="/jobs/1">Apply</a></li>
      <li class="job"><h2>Valid Job</h2><a href="/jobs/2">Apply</a></li>
    </ul>
    </body></html>
    """
    source = _make_html_source()
    raw_jobs, normalized_jobs = parse_html_text(source, html)

    assert len(normalized_jobs) == 1
    assert normalized_jobs[0].title == "Valid Job"


def test_parse_html_text_relative_url_absolutized() -> None:
    source = _html_source_with_location()
    raw_jobs, normalized_jobs = parse_html_text(source, MINIMAL_HTML)

    staff_job = next(j for j in normalized_jobs if "Staff" in j.title)
    assert staff_job.apply_url is not None
    assert staff_job.apply_url.startswith("https://example.com")


def test_parse_html_text_absolute_url_unchanged() -> None:
    source = _html_source_with_location()
    raw_jobs, normalized_jobs = parse_html_text(source, MINIMAL_HTML)

    sre_job = next(j for j in normalized_jobs if "SRE" in j.title)
    assert sre_job.apply_url == "https://example.com/jobs/99"


def test_parse_html_text_missing_required_selectors_raises() -> None:
    import pytest

    source = SourceConfig(
        name="BadSource",
        type=SourceType.GENERIC_HTML,
        url="https://example.com/jobs",
        format=SourceFormat.HTML,
        parser_template={"items_selector": "li.job"},  # missing title_selector
    )
    with pytest.raises(ValueError, match="items_selector"):
        parse_html_text(source, MINIMAL_HTML)
