from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig, SourceType, SourceValidationResult
from . import ashby, generic, generic_html, generic_json, greenhouse, lever, rss


@dataclass
class SourceIngestionResult:
    source: SourceConfig
    endpoint: str
    preflight_ok: bool
    http_status: int | None
    latency_ms: int
    raw_jobs: list[RawJob]
    normalized_jobs: list[NormalizedJob]
    parse_count: int
    error: str | None = None
    error_class: str | None = None


def _fetch_single_source(
    source: SourceConfig,
    fetcher: HttpFetcher,
    preflight: SourceValidationResult | None = None,
) -> SourceIngestionResult:
    endpoint = (source.api_url or source.url) if preflight is None else preflight.endpoint
    started = time.perf_counter()
    preflight_ok = True if preflight is None else preflight.ok
    http_status: int | None = None if preflight is None else preflight.http_status

    if preflight is not None and not preflight.ok:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceIngestionResult(
            source=source,
            endpoint=endpoint,
            preflight_ok=False,
            http_status=http_status,
            latency_ms=latency_ms,
            raw_jobs=[],
            normalized_jobs=[],
            parse_count=0,
            error=preflight.error,
            error_class=preflight.error_class or "preflight_failed",
        )

    try:
        if source.type == SourceType.GREENHOUSE:
            raw_jobs, normalized_jobs = greenhouse.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.LEVER:
            raw_jobs, normalized_jobs = lever.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.ASHBY:
            raw_jobs, normalized_jobs = ashby.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.RSS:
            raw_jobs, normalized_jobs = rss.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.GENERIC_JSON:
            raw_jobs, normalized_jobs = generic_json.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.GENERIC_HTML:
            raw_jobs, normalized_jobs = generic_html.fetch_and_normalize(source, fetcher)
        else:
            raw_jobs, normalized_jobs = generic.fetch_and_normalize(source, fetcher)
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceIngestionResult(
            source=source,
            endpoint=endpoint,
            preflight_ok=preflight_ok,
            http_status=http_status,
            latency_ms=latency_ms,
            raw_jobs=raw_jobs,
            normalized_jobs=normalized_jobs,
            parse_count=len(normalized_jobs),
            error=None,
            error_class=None,
        )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceIngestionResult(
            source=source,
            endpoint=endpoint,
            preflight_ok=preflight_ok,
            http_status=http_status,
            latency_ms=latency_ms,
            raw_jobs=[],
            normalized_jobs=[],
            parse_count=0,
            error=str(exc),
            error_class=type(exc).__name__,
        )


def ingest_sources(
    sources: list[SourceConfig],
    fetcher: HttpFetcher,
    max_workers: int = 4,
    preflight_results: dict[str, SourceValidationResult] | None = None,
) -> list[SourceIngestionResult]:
    enabled_sources = [source for source in sources if source.enabled]
    if not enabled_sources:
        return []

    results: list[SourceIngestionResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_fetch_single_source, source, fetcher, (preflight_results or {}).get(source.name)): source
            for source in enabled_sources
        }
        for future in as_completed(futures):
            results.append(future.result())

    return sorted(results, key=lambda item: item.source.name.lower())
