from __future__ import annotations

from urllib.parse import urlparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
from .common import build_normalized_job


def _extract_org_slug(source_url: str) -> str:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Cannot parse Ashby slug from {source_url}")
    return parts[-1]


def parse_ashby_job(source: SourceConfig, posting: dict) -> NormalizedJob:
    template = source.parser_template or {}
    title = compact_whitespace(posting.get(template.get("title_field", "title")) or "")
    description = posting.get(template.get("description_field", "descriptionHtml")) or posting.get("description") or ""
    location = compact_whitespace(
        posting.get("location")
        or (posting.get("locationName") if isinstance(posting.get("locationName"), str) else "")
        or ""
    )

    apply_url = posting.get(template.get("apply_url_field", "jobUrl")) or posting.get("applicationUrl")
    requisition_id = str(posting.get(template.get("requisition_id_field", "id")) or posting.get("jobId") or "") or None
    source_job_id = str(posting.get(template.get("id_field", "id")) or posting.get("jobId") or requisition_id or "")

    return build_normalized_job(
        source,
        source_job_id=source_job_id,
        title=title,
        description=description,
        location=location,
        apply_url=apply_url,
        requisition_id=requisition_id,
        salary_text=posting.get(template.get("salary_text_field", "")) if template.get("salary_text_field") else None,
        raw_payload=posting,
    )


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    slug = _extract_org_slug(source.url)

    # Ashby jobs boards expose a public non-user GraphQL endpoint.
    payload = {
        "operationName": "apiJobsBoardWithTeams",
        "variables": {"organizationHostedJobsPageName": slug},
        "query": "query apiJobsBoardWithTeams($organizationHostedJobsPageName: String!) { jobsBoard: organization(hostedJobsPageName: $organizationHostedJobsPageName) { jobs { id title descriptionHtml location locationName jobUrl applicationUrl } } }",
    }

    data = fetcher.post_json("https://jobs.ashbyhq.com/api/non-user-graphql", payload)
    postings = (
        (data or {}).get("data", {}).get("jobsBoard", {}).get("jobs")
        if isinstance(data, dict)
        else []
    )
    postings = postings or []

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for posting in postings:
        source_job_id = str(posting.get("id") or posting.get("jobId") or "")
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
            normalized_jobs.append(parse_ashby_job(source, posting))
        except Exception:
            continue

    return raw_jobs, normalized_jobs
