from job_scanner.models import SourceConfig, SourceFormat, SourceType
from job_scanner.source_validation import resolve_source_endpoint, validate_source


class DummyFetcher:
    def get_json_with_meta(self, url, headers=None):
        _ = headers
        return {"jobs": []}, 200

    def get_text_with_meta(self, url, headers=None):
        _ = headers
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
