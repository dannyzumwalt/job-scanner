from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
from .common import build_normalized_job


def _extract_site_slug(source_url: str) -> str:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Cannot parse Lever site slug from {source_url}")
    return parts[-1]


def lever_api_url(source_url: str) -> str:
    slug = _extract_site_slug(source_url)
    return f"https://api.lever.co/v0/postings/{slug}?mode=json"


def resolve_lever_endpoint(source: SourceConfig) -> str:
    if source.api_url:
        return source.api_url

    parsed = urlparse(source.url)
    if "api.lever.co" in parsed.netloc:
        query = parse_qs(parsed.query)
        query["mode"] = ["json"]
        normalized_query = urlencode(query, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, normalized_query, parsed.fragment))

    return lever_api_url(source.url)


def parse_lever_job(source: SourceConfig, posting: dict) -> NormalizedJob:
    template = source.parser_template or {}
    title = compact_whitespace(posting.get(template.get("title_field", "text")) or posting.get("title") or "")
    description = posting.get(template.get("description_field", "descriptionPlain")) or posting.get("description") or ""
    categories = posting.get("categories") or {}
    location = compact_whitespace(categories.get("location") or "")

    apply_url = posting.get(template.get("apply_url_field", "hostedUrl")) or posting.get("applyUrl")
    requisition_id = str(posting.get(template.get("requisition_id_field", "requisitionCode")) or "") or None

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
    endpoint = resolve_lever_endpoint(source)
    postings = fetcher.get_json(endpoint, headers=source.headers or None) or []

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
            normalized_jobs.append(parse_lever_job(source, posting))
        except Exception:
            continue

    return raw_jobs, normalized_jobs
