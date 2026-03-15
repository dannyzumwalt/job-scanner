from __future__ import annotations

from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import (
    build_dedupe_key,
    build_job_hash,
    compact_whitespace,
    detect_role_family_tags,
    detect_seniority_hints,
    estimate_total_comp,
    extract_travel_percent,
    location_is_dfw,
    location_is_us,
    normalize_location,
    normalize_title,
    parse_comp_values_from_text,
    strip_html_tags,
)


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
    title = compact_whitespace(posting.get("title") or "")
    description = strip_html_tags(posting.get("content") or "")
    location = compact_whitespace((posting.get("location") or {}).get("name") or "")
    normalized_location = normalize_location(location)

    salary_min, salary_max = parse_comp_values_from_text(description)
    total_min, total_max = estimate_total_comp(salary_min, salary_max, None, None)

    raw_apply_url = posting.get("absolute_url") or posting.get("hosted_url")
    requisition_id = str(posting.get("requisition_id") or "") or None

    content_blob = f"{title} {description}"
    travel_percent = extract_travel_percent(content_blob)

    is_remote = "remote" in normalized_location or "remote" in content_blob.lower()
    is_hybrid = "hybrid" in normalized_location or "hybrid" in content_blob.lower()
    is_onsite = not is_remote and not is_hybrid

    dedupe_key = build_dedupe_key(
        company=source.name,
        normalized_title=normalize_title(title),
        normalized_location=normalized_location,
        apply_url=raw_apply_url,
        requisition_id=requisition_id,
    )

    hash_payload = {
        "title": title,
        "description": description,
        "location": location,
        "apply_url": raw_apply_url,
        "requisition_id": requisition_id,
        "salary_min": salary_min,
        "salary_max": salary_max,
    }

    return NormalizedJob(
        source_name=source.name,
        source_type=source.type,
        source_job_id=str(posting.get("id")),
        source_url=source.url,
        requisition_id=requisition_id,
        apply_url=raw_apply_url,
        company=source.name,
        title=title,
        normalized_title=normalize_title(title),
        description=description,
        location=location,
        normalized_location=normalized_location,
        country="US" if location_is_us(normalized_location) else None,
        is_remote=is_remote,
        is_hybrid=is_hybrid,
        is_onsite=is_onsite,
        dfw_match=location_is_dfw(normalized_location),
        us_match=location_is_us(normalized_location),
        travel_required=travel_percent is not None,
        travel_percent=travel_percent,
        base_min=salary_min,
        base_max=salary_max,
        estimated_total_comp_min=total_min,
        estimated_total_comp_max=total_max,
        compensation_confidence=0.55 if salary_min or salary_max else 0.15,
        role_family_tags=detect_role_family_tags(content_blob),
        seniority_hints=detect_seniority_hints(content_blob),
        dedupe_key=dedupe_key,
        job_hash=build_job_hash(hash_payload),
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
