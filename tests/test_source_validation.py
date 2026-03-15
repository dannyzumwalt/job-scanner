from job_scanner.models import SourceConfig, SourceFormat, SourceType
from job_scanner.source_validation import resolve_source_endpoint, validate_source, validate_source_template


class DummyFetcher:
    def get_json_with_meta(self, url, headers=None):
        _ = headers
        if "greenhouse" in url:
            return {"jobs": [{"id": 1, "title": "Staff SRE", "absolute_url": "https://example.com/jobs/1", "location": {"name": "Remote - United States"}, "content": "Compensation $300k-$360k"}]}, 200
        return {"jobs": []}, 200

    def get_text_with_meta(self, url, headers=None):
        _ = headers
        if "careers" in url:
            return (
                "<html><body><article class='job'><h2 class='t'>Principal Engineer</h2><a class='a' href='/jobs/1'>Apply</a></article></body></html>",
                200,
            )
        return "<rss></rss>", 200

    def post_json_with_meta(self, url, payload, headers=None):
        _ = payload
        _ = headers
        return {"data": {"jobsBoard": {"jobs": []}}}, 200


def test_resolve_source_endpoint_prefers_api_url() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.GENERIC_JSON,
        format=SourceFormat.JSON,
        enabled=True,
        url="https://example.com/jobs",
        api_url="https://example.com/api/jobs.json",
    )
    assert resolve_source_endpoint(source) == "https://example.com/api/jobs.json"


def test_validate_source_success() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.GREENHOUSE,
        format=SourceFormat.GREENHOUSE,
        enabled=True,
        url="https://boards.greenhouse.io/demo",
        expected_status=200,
    )
    result = validate_source(source, DummyFetcher())
    assert result.ok is True
    assert result.http_status == 200


def test_validate_source_strict_success_with_parse_count() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.GREENHOUSE,
        format=SourceFormat.GREENHOUSE,
        enabled=True,
        url="https://boards.greenhouse.io/demo",
        expected_status=200,
    )
    result = validate_source(source, DummyFetcher(), strict=True)
    assert result.ok is True
    assert result.schema_ok is True
    assert result.required_fields_ok is True
    assert result.parse_count >= 1


def test_validate_source_strict_fails_for_missing_html_template() -> None:
    source = SourceConfig(
        name="HTML",
        type=SourceType.GENERIC_HTML,
        format=SourceFormat.HTML,
        enabled=True,
        url="https://example.com/careers",
        expected_status=200,
    )
    result = validate_source(source, DummyFetcher(), strict=True)
    assert result.ok is False
    assert result.error_class == "template_validation_error"


def test_validate_source_strict_html_success() -> None:
    source = SourceConfig(
        name="HTML",
        type=SourceType.GENERIC_HTML,
        format=SourceFormat.HTML,
        enabled=True,
        url="https://example.com/careers",
        expected_status=200,
        parser_template={
            "items_selector": ".job",
            "title_selector": ".t",
            "apply_url_selector": ".a",
            "apply_url_attr": "href",
        },
    )
    result = validate_source(source, DummyFetcher(), strict=True)
    assert result.ok is True
    assert result.parse_count == 1


def test_validate_source_template_rejects_unknown_keys() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.GREENHOUSE,
        format=SourceFormat.GREENHOUSE,
        enabled=True,
        url="https://boards.greenhouse.io/demo",
        parser_template={"unknown_field": "x"},
    )
    ok, error, _ = validate_source_template(source)
    assert ok is False
    assert error is not None
