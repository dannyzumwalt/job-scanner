from __future__ import annotations

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import extract_by_path, value_as_text
from .common import build_normalized_job


def _resolve_items(payload: object, items_path: str | None) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        if items_path:
            resolved = extract_by_path(payload, items_path, default=[])
            if isinstance(resolved, list):
                return [item for item in resolved if isinstance(item, dict)]
            return []

        # auto-discover common list keys
        for key in ("jobs", "items", "results", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]

    return []


def parse_generic_json_job(source: SourceConfig, item: dict) -> NormalizedJob:
    template = source.parser_template or {}

    source_job_id = value_as_text(extract_by_path(item, template.get("id_field", "id"), default=""))
    if not source_job_id:
        source_job_id = value_as_text(extract_by_path(item, template.get("alternate_id_field", "job_id"), default=""))
    if not source_job_id:
        source_job_id = value_as_text(extract_by_path(item, template.get("apply_url_field", "url"), default=""))

    title = value_as_text(extract_by_path(item, template.get("title_field", "title"), default=""))
    description = value_as_text(extract_by_path(item, template.get("description_field", "description"), default=""))
    location = value_as_text(extract_by_path(item, template.get("location_field", "location"), default=""))
    apply_url = value_as_text(extract_by_path(item, template.get("apply_url_field", "apply_url"), default=""))
    requisition_id = value_as_text(extract_by_path(item, template.get("requisition_id_field", "requisition_id"), default=""))
    company = value_as_text(extract_by_path(item, template.get("company_field", "company"), default=source.name))
    salary_text = value_as_text(extract_by_path(item, template.get("salary_text_field", "salary"), default=""))

    return build_normalized_job(
        source,
        source_job_id=source_job_id,
        title=title,
        description=description,
        location=location,
        apply_url=apply_url or None,
        requisition_id=requisition_id or None,
        company=company or source.name,
        salary_text=salary_text or None,
        base_min_hint=extract_by_path(item, template.get("base_min_field"), default=None),
        base_max_hint=extract_by_path(item, template.get("base_max_field"), default=None),
        bonus_hint=extract_by_path(item, template.get("bonus_field"), default=None),
        bonus_percent_hint=extract_by_path(item, template.get("bonus_percent_field"), default=None),
        equity_hint=extract_by_path(item, template.get("equity_field"), default=None),
        raw_payload=item,
    )


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    endpoint = source.api_url or source.url
    payload = fetcher.get_json(endpoint, headers=source.headers or None)
    items = _resolve_items(payload, (source.parser_template or {}).get("items_path"))

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []
    for item in items:
        source_job_id = value_as_text(extract_by_path(item, (source.parser_template or {}).get("id_field", "id"), default=""))
        if not source_job_id:
            source_job_id = value_as_text(item.get("url") or item.get("apply_url") or "")
        if not source_job_id:
            continue

        raw_jobs.append(
            RawJob(
                source_name=source.name,
                source_type=source.type,
                source_url=source.url,
                source_job_id=source_job_id,
                payload=item,
            )
        )
        try:
            normalized_jobs.append(parse_generic_json_job(source, item))
        except Exception:
            continue

    return raw_jobs, normalized_jobs
