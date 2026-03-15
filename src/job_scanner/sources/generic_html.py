from __future__ import annotations

from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
from .common import build_normalized_job


def _extract_text(node: Tag | None, *, attr: str | None = None) -> str:
    if node is None:
        return ""
    if attr:
        return compact_whitespace(str(node.get(attr) or ""))
    return compact_whitespace(node.get_text(" ", strip=True))


def parse_html_text(source: SourceConfig, html_text: str) -> tuple[list[RawJob], list[NormalizedJob]]:
    template = source.parser_template or {}

    items_selector = template.get("items_selector") or ""
    title_selector = template.get("title_selector") or ""
    apply_url_selector = template.get("apply_url_selector") or ""
    if not items_selector or not title_selector or not apply_url_selector:
        raise ValueError(
            "generic_html requires parser_template keys: items_selector, title_selector, apply_url_selector"
        )

    title_attr = template.get("title_attr")
    apply_url_attr = template.get("apply_url_attr", "href")
    description_selector = template.get("description_selector")
    location_selector = template.get("location_selector")
    requisition_selector = template.get("requisition_selector")
    compensation_selector = template.get("compensation_selector")
    company_selector = template.get("company_selector")
    source_job_id_selector = template.get("source_job_id_selector")
    source_job_id_attr = template.get("source_job_id_attr")

    soup = BeautifulSoup(html_text, "html.parser")
    items = soup.select(items_selector)

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for index, item in enumerate(items, start=1):
        if not isinstance(item, Tag):
            continue

        title_node = item.select_one(title_selector)
        apply_node = item.select_one(apply_url_selector)
        title = _extract_text(title_node, attr=title_attr)
        apply_url = _extract_text(apply_node, attr=apply_url_attr)
        if apply_url:
            apply_url = urljoin(source.url, apply_url)

        description = _extract_text(item.select_one(description_selector)) if description_selector else ""
        location = _extract_text(item.select_one(location_selector)) if location_selector else ""
        requisition_id = _extract_text(item.select_one(requisition_selector)) if requisition_selector else ""
        salary_text = _extract_text(item.select_one(compensation_selector)) if compensation_selector else ""
        company = _extract_text(item.select_one(company_selector)) if company_selector else source.name

        source_job_id = ""
        if source_job_id_selector:
            source_job_id = _extract_text(item.select_one(source_job_id_selector), attr=source_job_id_attr)
        if not source_job_id:
            source_job_id = requisition_id or apply_url or f"html-{index}"

        if not title:
            continue

        payload = {
            "title": title,
            "description": description,
            "location": location,
            "apply_url": apply_url,
            "requisition_id": requisition_id,
            "salary_text": salary_text,
            "company": company,
            "source_job_id": source_job_id,
        }

        raw_jobs.append(
            RawJob(
                source_name=source.name,
                source_type=source.type,
                source_url=source.url,
                source_job_id=source_job_id,
                payload=payload,
            )
        )
        try:
            normalized_jobs.append(
                build_normalized_job(
                    source,
                    source_job_id=source_job_id,
                    title=title,
                    description=description,
                    location=location,
                    apply_url=apply_url or None,
                    requisition_id=requisition_id or None,
                    salary_text=salary_text or None,
                    company=company or source.name,
                    raw_payload=payload,
                )
            )
        except Exception:
            continue

    return raw_jobs, normalized_jobs


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    endpoint = source.api_url or source.url
    html_text = fetcher.get_text(endpoint, headers=source.headers or None)
    return parse_html_text(source, html_text)
