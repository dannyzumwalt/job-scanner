from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig, SourceType
from . import ashby, generic, greenhouse, lever


@dataclass
class SourceIngestionResult:
    source: SourceConfig
    raw_jobs: list[RawJob]
    normalized_jobs: list[NormalizedJob]
    error: str | None = None


def _fetch_single_source(source: SourceConfig, fetcher: HttpFetcher) -> SourceIngestionResult:
    try:
        if source.type == SourceType.GREENHOUSE:
            raw_jobs, normalized_jobs = greenhouse.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.LEVER:
            raw_jobs, normalized_jobs = lever.fetch_and_normalize(source, fetcher)
        elif source.type == SourceType.ASHBY:
            raw_jobs, normalized_jobs = ashby.fetch_and_normalize(source, fetcher)
        else:
            raw_jobs, normalized_jobs = generic.fetch_and_normalize(source, fetcher)
        return SourceIngestionResult(
            source=source,
            raw_jobs=raw_jobs,
            normalized_jobs=normalized_jobs,
            error=None,
        )
    except Exception as exc:
        return SourceIngestionResult(source=source, raw_jobs=[], normalized_jobs=[], error=str(exc))


def ingest_sources(
    sources: list[SourceConfig],
    fetcher: HttpFetcher,
    max_workers: int = 4,
) -> list[SourceIngestionResult]:
    enabled_sources = [source for source in sources if source.enabled]
    if not enabled_sources:
        return []

    results: list[SourceIngestionResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_single_source, source, fetcher): source for source in enabled_sources}
        for future in as_completed(futures):
            results.append(future.result())

    return sorted(results, key=lambda item: item.source.name.lower())
