from __future__ import annotations

from xml.etree import ElementTree

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
from .common import build_normalized_job


def _text(node, tag: str) -> str:
    if not tag:
        return ""
    child = node.find(tag)
    if child is None or child.text is None:
        return ""
    return compact_whitespace(child.text)


def parse_rss_text(source: SourceConfig, xml_text: str) -> tuple[list[RawJob], list[NormalizedJob]]:
    template = source.parser_template or {}
    item_tag = template.get("items_tag", "item")
    title_tag = template.get("title_field", "title")
    description_tag = template.get("description_field", "description")
    apply_url_tag = template.get("apply_url_field", "link")
    guid_tag = template.get("id_field", "guid")
    location_tag = template.get("location_field", "location")
    requisition_tag = template.get("requisition_id_field", "")
    salary_tag = template.get("salary_text_field", "")

    root = ElementTree.fromstring(xml_text)

    # default RSS path
    channel = root.find("channel")
    items = channel.findall(item_tag) if channel is not None else root.findall(item_tag)

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for item in items:
        title = _text(item, title_tag)
        description = _text(item, description_tag)
        apply_url = _text(item, apply_url_tag)
        guid = _text(item, guid_tag) or apply_url or title
        location = _text(item, location_tag)
        requisition_id = _text(item, requisition_tag) if requisition_tag else ""
        salary_text = _text(item, salary_tag) if salary_tag else ""

        if not guid:
            continue

        payload = {
            "title": title,
            "description": description,
            "link": apply_url,
            guid_tag or "guid": guid,
            "location": location,
        }
        if requisition_id:
            payload[requisition_tag] = requisition_id
        if salary_text:
            payload[salary_tag] = salary_text

        raw_jobs.append(
            RawJob(
                source_name=source.name,
                source_type=source.type,
                source_url=source.url,
                source_job_id=guid,
                payload=payload,
            )
        )

        try:
            normalized_jobs.append(
                build_normalized_job(
                    source,
                    source_job_id=guid,
                    title=title,
                    description=description,
                    location=location,
                    apply_url=apply_url or None,
                    requisition_id=requisition_id or None,
                    salary_text=salary_text or None,
                    raw_payload=payload,
                )
            )
        except Exception:
            continue

    return raw_jobs, normalized_jobs


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    endpoint = source.api_url or source.url
    xml_text = fetcher.get_text(endpoint, headers=source.headers or None)
    return parse_rss_text(source, xml_text)
