from __future__ import annotations

from ..http_client import HttpFetcher
from ..models import NormalizedJob, RawJob, SourceConfig


def fetch_and_normalize(source: SourceConfig, fetcher: HttpFetcher) -> tuple[list[RawJob], list[NormalizedJob]]:
    _ = source
    _ = fetcher
    # Generic HTML/RSS support is intentionally deferred in MVP.
    return [], []
