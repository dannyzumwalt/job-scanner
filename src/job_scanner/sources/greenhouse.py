from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace, extract_by_path, value_as_text
from .common import build_normalized_job


def _extract_slug(board_url: str) -> str:
    parsed = urlparse(board_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Cannot parse Greenhouse slug from {board_url}")
    return parts[-1]


def greenhouse_api_url(board_url: str) -> str:
    slug = _extract_slug(board_url)
    return f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"


def resolve_greenhouse_endpoint(source: SourceConfig) -> str:
    if source.api_url:
        return source.api_url

    parsed = urlparse(source.url)
    if "boards-api.greenhouse.io" in parsed.netloc:
        query = parse_qs(parsed.query)
        query["content"] = ["true"]
        normalized_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, normalized_query, parsed.fragment))

    return greenhouse_api_url(source.url)


def parse_greenhouse_job(source: SourceConfig, posting: dict) -> NormalizedJob:
    template = source.parser_template or {}
    title = compact_whitespace(value_as_text(extract_by_path(posting, template.get("title_field", "title"), default="")))
    description = value_as_text(extract_by_path(posting, template.get("description_field", "content"), default=""))
    location = compact_whitespace(value_as_text(extract_by_path(posting, template.get("location_field", "location.name"), default="")))

    apply_url = value_as_text(
        extract_by_path(posting, template.get("apply_url_field", "absolute_url"), default="")
        or posting.get("hosted_url")
    )
    requisition_id = value_as_text(
        extract_by_path(posting, template.get("requisition_id_field", "requisition_id"), default="")
    ) or None

    source_job_id = value_as_text(extract_by_path(posting, template.get("id_field", "id"), default=""))
    salary_text = value_as_text(
        extract_by_path(posting, template.get("salary_text_field", "salary"), default="")
    )
    company = value_as_text(extract_by_path(posting, template.get("company_field", "company"), default=source.name))

    return build_normalized_job(
        source,
        source_job_id=source_job_id,
        title=title,
        description=description,
        location=location,
        apply_url=apply_url or None,
        requisition_id=requisition_id,
        company=company or source.name,
        salary_text=salary_text or None,
        base_min_hint=extract_by_path(posting, template.get("base_min_field"), default=None),
        base_max_hint=extract_by_path(posting, template.get("base_max_field"), default=None),
        bonus_hint=extract_by_path(posting, template.get("bonus_field"), default=None),
        bonus_percent_hint=extract_by_path(posting, template.get("bonus_percent_field"), default=None),
        equity_hint=extract_by_path(posting, template.get("equity_field"), default=None),
        raw_payload=posting,
    )


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    endpoint = resolve_greenhouse_endpoint(source)
    data = fetcher.get_json(endpoint, headers=source.headers or None)
    postings = data.get("jobs") or []

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for posting in postings:
        source_job_id = str(posting.get("id") or "")
        if not source_job_id:
            continue
        raw_jobs.append(
            RawJob(
                source_name=source.name,
                source_type=source.type,
                source_url=source.url,
                source_job_id=source_job_id,
                payload=posting,
            )
        )
        try:
            normalized_jobs.append(parse_greenhouse_job(source, posting))
        except Exception:
            continue

    return raw_jobs, normalized_jobs
