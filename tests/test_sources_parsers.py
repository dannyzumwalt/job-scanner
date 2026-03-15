import json
from pathlib import Path

from job_scanner.models import SourceConfig, SourceType
from job_scanner.sources.ashby import parse_ashby_job
from job_scanner.sources.greenhouse import parse_greenhouse_job
from job_scanner.sources.lever import parse_lever_job


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
