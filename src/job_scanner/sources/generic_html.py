from __future__ import annotations

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    _ = source
    _ = fetcher
    # Intentionally minimal for scale-up phase: generic HTML scraping remains opt-in and disabled by default.
    return [], []
