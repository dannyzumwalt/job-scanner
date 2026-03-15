from __future__ import annotations

from xml.etree import ElementTree

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig
from ..utils import compact_whitespace
from .common import build_normalized_job


def _text(node, tag: str) -> str:
    child = node.find(tag)
    if child is None or child.text is None:
        return ""
    return compact_whitespace(child.text)


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    endpoint = source.api_url or source.url
    xml_text = fetcher.get_text(endpoint, headers=source.headers or None)
    root = ElementTree.fromstring(xml_text)

    # default RSS path
    channel = root.find("channel")
    items = channel.findall("item") if channel is not None else root.findall("item")

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for item in items:
        title = _text(item, "title")
        description = _text(item, "description")
        apply_url = _text(item, "link")
        guid = _text(item, "guid") or apply_url or title
        location = _text(item, "location")

        if not guid:
            continue

        payload = {
            "title": title,
            "description": description,
            "link": apply_url,
            "guid": guid,
            "location": location,
        }

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
                    requisition_id=None,
                    raw_payload=payload,
                )
            )
        except Exception:
            continue

    return raw_jobs, normalized_jobs
