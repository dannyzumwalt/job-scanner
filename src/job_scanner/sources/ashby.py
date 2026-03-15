from __future__ import annotations

from urllib.parse import urlparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace, extract_by_path, value_as_text
from .common import build_normalized_job


def _extract_org_slug(source_url: str) -> str:
    parsed = urlparse(source_url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        raise ValueError(f"Cannot parse Ashby slug from {source_url}")
    return parts[-1]


def parse_ashby_job(source: SourceConfig, posting: dict) -> NormalizedJob:
    template = source.parser_template or {}
    title = compact_whitespace(value_as_text(extract_by_path(posting, template.get("title_field", "title"), default="")))
    description = value_as_text(
        extract_by_path(posting, template.get("description_field", "descriptionHtml"), default="")
        or extract_by_path(posting, "description", default="")
    )
    location = compact_whitespace(
        value_as_text(
            extract_by_path(posting, template.get("location_field", "location"), default="")
            or extract_by_path(posting, "locationName", default="")
        )
    )

    apply_url = value_as_text(
        extract_by_path(posting, template.get("apply_url_field", "jobUrl"), default="")
        or extract_by_path(posting, "applicationUrl", default="")
    )
    requisition_id = value_as_text(
        extract_by_path(posting, template.get("requisition_id_field", "id"), default="")
        or extract_by_path(posting, "jobId", default="")
    ) or None
    source_job_id = value_as_text(
        extract_by_path(posting, template.get("id_field", "id"), default="")
        or extract_by_path(posting, "jobId", default="")
        or requisition_id
        or ""
    )
    salary_text = value_as_text(extract_by_path(posting, template.get("salary_text_field", "salary"), default=""))
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
