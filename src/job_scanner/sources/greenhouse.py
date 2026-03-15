from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
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
    title = compact_whitespace(posting.get(template.get("title_field", "title")) or "")
    description = posting.get(template.get("description_field", "content")) or ""
    location = compact_whitespace((posting.get("location") or {}).get("name") or "")

    apply_url = posting.get(template.get("apply_url_field", "absolute_url")) or posting.get("hosted_url")
    requisition_id = str(posting.get(template.get("requisition_id_field", "requisition_id")) or "") or None

    return build_normalized_job(
        source,
        source_job_id=str(posting.get(template.get("id_field", "id"))),
        title=title,
        description=description,
        location=location,
        apply_url=apply_url,
        requisition_id=requisition_id,
        salary_text=posting.get(template.get("salary_text_field", "")) if template.get("salary_text_field") else None,
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
