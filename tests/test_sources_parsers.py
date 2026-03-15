import json
from pathlib import Path

from job_scanner.models import SourceConfig, SourceType
from job_scanner.sources.ashby import parse_ashby_job
from job_scanner.sources.generic_html import parse_html_text
from job_scanner.sources.generic_json import parse_generic_json_job
from job_scanner.sources.greenhouse import parse_greenhouse_job, resolve_greenhouse_endpoint
from job_scanner.sources.lever import parse_lever_job, resolve_lever_endpoint
from job_scanner.sources.rss import parse_rss_text


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_greenhouse_job_fixture() -> None:
    payload = json.loads((FIXTURES / "greenhouse_jobs.json").read_text(encoding="utf-8"))["jobs"][0]
    source = SourceConfig(name="Datadog", type=SourceType.GREENHOUSE, enabled=True, url="https://boards.greenhouse.io/datadog")

    job = parse_greenhouse_job(source, payload)

    assert job.title.startswith("Principal")
    assert job.is_remote is True
    assert job.estimated_total_comp_max is not None


def test_parse_lever_job_fixture() -> None:
    payload = json.loads((FIXTURES / "lever_jobs.json").read_text(encoding="utf-8"))[0]
    source = SourceConfig(name="Stripe", type=SourceType.LEVER, enabled=True, url="https://jobs.lever.co/stripe")

    job = parse_lever_job(source, payload)

    assert "Staff" in job.title
    assert job.is_remote is True
    assert job.estimated_total_comp_min is not None


def test_parse_ashby_job_fixture() -> None:
    payload = json.loads((FIXTURES / "ashby_jobs.json").read_text(encoding="utf-8"))["data"]["jobsBoard"]["jobs"][0]
    source = SourceConfig(name="Example", type=SourceType.ASHBY, enabled=True, url="https://jobs.ashbyhq.com/example")

    job = parse_ashby_job(source, payload)

    assert "Architect" in job.title
    assert job.is_remote is True
    assert job.estimated_total_comp_min is not None


def test_resolve_greenhouse_endpoint_prefers_explicit_api_url() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.GREENHOUSE,
        enabled=True,
        url="https://boards.greenhouse.io/demo",
        api_url="https://boards-api.greenhouse.io/v1/boards/custom/jobs?content=true",
    )
    assert resolve_greenhouse_endpoint(source) == source.api_url


def test_resolve_lever_endpoint_adds_mode_json_when_api_url_provided() -> None:
    source = SourceConfig(
        name="Demo",
        type=SourceType.LEVER,
        enabled=True,
        url="https://api.lever.co/v0/postings/demo",
    )
    assert resolve_lever_endpoint(source).endswith("mode=json")


def test_parse_generic_json_job_with_template_override() -> None:
    payload = json.loads((FIXTURES / "generic_json_jobs.json").read_text(encoding="utf-8"))["data"]["openings"][0]
    source = SourceConfig(
        name="Example Infra",
        type=SourceType.GENERIC_JSON,
        enabled=True,
        url="https://example.com/jobs.json",
        parser_template={
            "id_field": "job.uid",
            "title_field": "job.name",
            "description_field": "job.summary",
            "location_field": "job.city",
            "apply_url_field": "job.apply",
            "requisition_id_field": "job.req",
            "company_field": "employer",
            "base_min_field": "job.pay.base_min",
            "base_max_field": "job.pay.base_max",
            "bonus_percent_field": "job.pay.bonus_percent",
            "equity_field": "job.pay.equity",
            "salary_text_field": "job.summary",
        },
    )

    job = parse_generic_json_job(source, payload)
    assert job.title.startswith("Principal")
    assert job.company == "Example Infra"
    assert job.base_min == 260000
    assert job.bonus is not None
    assert job.equity == 60000


def test_parse_rss_feed_fixture() -> None:
    xml_text = (FIXTURES / "rss_jobs.xml").read_text(encoding="utf-8")
    source = SourceConfig(
        name="RSS Example",
        type=SourceType.RSS,
        enabled=True,
        url="https://example.com/jobs.rss",
    )
    raw_jobs, normalized_jobs = parse_rss_text(source, xml_text)
    assert len(raw_jobs) == 1
    assert len(normalized_jobs) == 1
    assert normalized_jobs[0].is_remote is True


def test_parse_generic_html_fixture() -> None:
    html = (FIXTURES / "html_jobs.html").read_text(encoding="utf-8")
    source = SourceConfig(
        name="HTML Example",
        type=SourceType.GENERIC_HTML,
        enabled=True,
        url="https://example.com/careers",
        parser_template={
            "items_selector": ".job-card",
            "title_selector": ".job-title",
            "apply_url_selector": ".apply-link",
            "apply_url_attr": "href",
            "description_selector": ".job-summary",
            "location_selector": ".job-location",
            "compensation_selector": ".job-compensation",
            "requisition_selector": ".job-id",
        },
    )

    raw_jobs, normalized_jobs = parse_html_text(source, html)
    assert len(raw_jobs) == 1
    assert len(normalized_jobs) == 1
    job = normalized_jobs[0]
    assert job.title.startswith("Principal")
    assert job.apply_url == "https://example.com/jobs/req-html-42"
    assert job.base_min is not None
