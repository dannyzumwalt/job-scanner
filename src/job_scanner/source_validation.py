from __future__ import annotations

import time

from .http_client import FetchError, HttpFetcher
from .models import SourceConfig, SourceType, SourceValidationResult
from .sources import ashby as ashby_source
from .sources import greenhouse as greenhouse_source
from .sources import lever as lever_source


ASHBY_VALIDATION_QUERY = {
    "operationName": "apiJobsBoardWithTeams",
    "variables": {"organizationHostedJobsPageName": ""},
    "query": "query apiJobsBoardWithTeams($organizationHostedJobsPageName: String!) { jobsBoard: organization(hostedJobsPageName: $organizationHostedJobsPageName) { jobs { id } } }",
}


def resolve_source_endpoint(source: SourceConfig) -> str:
    if source.type == SourceType.GREENHOUSE:
        return greenhouse_source.resolve_greenhouse_endpoint(source)
    if source.type == SourceType.LEVER:
        return lever_source.resolve_lever_endpoint(source)
    if source.type == SourceType.ASHBY:
        return "https://jobs.ashbyhq.com/api/non-user-graphql"
    return source.api_url or source.url


def validate_source(source: SourceConfig, fetcher: HttpFetcher) -> SourceValidationResult:
    endpoint = resolve_source_endpoint(source)
    started = time.perf_counter()

    try:
        if source.type == SourceType.ASHBY:
            slug = ashby_source._extract_org_slug(source.url)
            payload = dict(ASHBY_VALIDATION_QUERY)
            payload["variables"] = {"organizationHostedJobsPageName": slug}
            _, status = fetcher.post_json_with_meta(endpoint, payload, headers=source.headers or None)
            ok = status == source.expected_status
            latency_ms = int((time.perf_counter() - started) * 1000)
            return SourceValidationResult(
                source_name=source.name,
                source_type=source.type,
                endpoint=endpoint,
                ok=ok,
                http_status=status,
                latency_ms=latency_ms,
                error=None if ok else f"Unexpected status {status}",
                error_class=None if ok else "unexpected_status",
            )

        if source.type == SourceType.RSS:
            _, status = fetcher.get_text_with_meta(endpoint, headers=source.headers or None)
        else:
            _, status = fetcher.get_json_with_meta(endpoint, headers=source.headers or None)

        ok = status == source.expected_status
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            ok=ok,
            http_status=status,
            latency_ms=latency_ms,
            error=None if ok else f"Unexpected status {status}",
            error_class=None if ok else "unexpected_status",
        )
    except FetchError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            ok=False,
            http_status=exc.status_code,
            latency_ms=latency_ms,
            error=str(exc),
            error_class=exc.error_class,
        )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            ok=False,
            http_status=None,
            latency_ms=latency_ms,
            error=str(exc),
            error_class=type(exc).__name__,
        )
