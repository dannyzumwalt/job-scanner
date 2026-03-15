from __future__ import annotations

import time
from typing import Any

from .http_client import FetchError, HttpFetcher
from .models import SourceConfig, SourceType, SourceValidationResult
from .source_capabilities import get_capability
from .sources import ashby as ashby_source
from .sources import generic_html as generic_html_source
from .sources import generic_json as generic_json_source
from .sources import greenhouse as greenhouse_source
from .sources import lever as lever_source
from .sources import rss as rss_source


ASHBY_VALIDATION_QUERY = {
    "operationName": "apiJobsBoardWithTeams",
    "variables": {"organizationHostedJobsPageName": ""},
    "query": "query apiJobsBoardWithTeams($organizationHostedJobsPageName: String!) { jobsBoard: organization(hostedJobsPageName: $organizationHostedJobsPageName) { jobs { id } } }",
}


class _StaticPayloadFetcher:
    def __init__(self, *, json_payload: Any = None, text_payload: str = "", post_payload: Any = None) -> None:
        self._json_payload = json_payload
        self._text_payload = text_payload
        self._post_payload = post_payload if post_payload is not None else json_payload

    def get_json(self, url: str, headers: dict[str, str] | None = None) -> Any:
        _ = url
        _ = headers
        return self._json_payload

    def get_text(self, url: str, headers: dict[str, str] | None = None) -> str:
        _ = url
        _ = headers
        return self._text_payload

    def post_json(self, url: str, payload: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
        _ = url
        _ = payload
        _ = headers
        return self._post_payload


def resolve_source_endpoint(source: SourceConfig) -> str:
    if source.type == SourceType.GREENHOUSE:
        return greenhouse_source.resolve_greenhouse_endpoint(source)
    if source.type == SourceType.LEVER:
        return lever_source.resolve_lever_endpoint(source)
    if source.type == SourceType.ASHBY:
        return "https://jobs.ashbyhq.com/api/non-user-graphql"
    return source.api_url or source.url


def validate_source_template(source: SourceConfig) -> tuple[bool, str | None, list[str]]:
    capability = get_capability(source.type)
    template_keys = set(source.parser_template.keys())
    warnings: list[str] = []

    if template_keys and not capability.supports_parser_template:
        return False, f"{source.type.value} does not support parser_template overrides", warnings

    if not template_keys:
        if capability.required_parser_template_keys:
            missing = sorted(capability.required_parser_template_keys)
            return False, f"Missing required parser_template keys: {', '.join(missing)}", warnings
        return True, None, warnings

    unknown = sorted(template_keys - set(capability.allowed_parser_template_keys))
    if unknown:
        return (
            False,
            f"Unsupported parser_template keys for {source.type.value}: {', '.join(unknown)}",
            warnings,
        )

    missing_required = sorted(set(capability.required_parser_template_keys) - template_keys)
    if missing_required:
        return False, f"Missing required parser_template keys: {', '.join(missing_required)}", warnings

    return True, None, warnings


def _fetch_source_payload(
    source: SourceConfig,
    fetcher: HttpFetcher,
    endpoint: str,
) -> tuple[Any, int]:
    if source.type == SourceType.ASHBY:
        slug = ashby_source._extract_org_slug(source.url)
        payload = dict(ASHBY_VALIDATION_QUERY)
        payload["variables"] = {"organizationHostedJobsPageName": slug}
        return fetcher.post_json_with_meta(endpoint, payload, headers=source.headers or None)
    if source.type in (SourceType.RSS, SourceType.GENERIC_HTML):
        return fetcher.get_text_with_meta(endpoint, headers=source.headers or None)
    return fetcher.get_json_with_meta(endpoint, headers=source.headers or None)


def _run_schema_sample(source: SourceConfig, payload: Any) -> tuple[int, bool, bool, str | None, list[str]]:
    warnings: list[str] = []

    try:
        if source.type == SourceType.GREENHOUSE:
            parser_fetcher = _StaticPayloadFetcher(json_payload=payload)
            raw_jobs, normalized_jobs = greenhouse_source.fetch_and_normalize(source, parser_fetcher)  # type: ignore[arg-type]
        elif source.type == SourceType.LEVER:
            parser_fetcher = _StaticPayloadFetcher(json_payload=payload)
            raw_jobs, normalized_jobs = lever_source.fetch_and_normalize(source, parser_fetcher)  # type: ignore[arg-type]
        elif source.type == SourceType.ASHBY:
            parser_fetcher = _StaticPayloadFetcher(post_payload=payload)
            raw_jobs, normalized_jobs = ashby_source.fetch_and_normalize(source, parser_fetcher)  # type: ignore[arg-type]
        elif source.type == SourceType.RSS:
            if not isinstance(payload, str):
                return 0, False, False, "RSS validation payload was not XML text", warnings
            raw_jobs, normalized_jobs = rss_source.parse_rss_text(source, payload)
        elif source.type in (SourceType.GENERIC, SourceType.GENERIC_JSON):
            parser_fetcher = _StaticPayloadFetcher(json_payload=payload)
            raw_jobs, normalized_jobs = generic_json_source.fetch_and_normalize(source, parser_fetcher)  # type: ignore[arg-type]
        elif source.type == SourceType.GENERIC_HTML:
            if not isinstance(payload, str):
                return 0, False, False, "HTML validation payload was not text", warnings
            raw_jobs, normalized_jobs = generic_html_source.parse_html_text(source, payload)
        else:
            return 0, False, False, f"Unsupported source type for validation: {source.type.value}", warnings
    except Exception as exc:
        return 0, False, False, f"Schema parse failed: {exc}", warnings

    parse_count = len(normalized_jobs)
    schema_ok = True
    required_ok = True

    if raw_jobs and parse_count == 0:
        schema_ok = False
        warnings.append("Source returned jobs but parser produced zero normalized jobs")

    required_issues: list[str] = []
    for job in normalized_jobs[:3]:
        if not (job.title or "").strip():
            required_issues.append("title")
        if not (job.company or "").strip():
            required_issues.append("company")
        if not (job.source_job_id or "").strip():
            required_issues.append("source_job_id")
        if not (job.dedupe_key or "").strip():
            required_issues.append("dedupe_key")
    if required_issues:
        required_ok = False
        warnings.append(f"Missing required normalized fields: {', '.join(sorted(set(required_issues)))}")

    return parse_count, schema_ok, required_ok, None, warnings


def validate_source(source: SourceConfig, fetcher: HttpFetcher, *, strict: bool = False) -> SourceValidationResult:
    endpoint = resolve_source_endpoint(source)
    started = time.perf_counter()
    capability = get_capability(source.type)

    if not capability.supports_validation:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            strict=strict,
            ok=False,
            healthy=False,
            template_ok=False,
            schema_ok=False,
            required_fields_ok=False,
            parse_count=0,
            http_status=None,
            error_class="unsupported_validation",
            error=f"{source.type.value} does not support live source validation",
            latency_ms=latency_ms,
            warnings=[],
        )

    template_ok, template_error, template_warnings = validate_source_template(source)
    if strict and not template_ok:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            strict=strict,
            ok=False,
            healthy=False,
            template_ok=False,
            schema_ok=False,
            required_fields_ok=False,
            parse_count=0,
            http_status=None,
            error_class="template_validation_error",
            error=template_error,
            latency_ms=latency_ms,
            warnings=template_warnings,
        )

    try:
        payload, status = _fetch_source_payload(source, fetcher, endpoint)
        status_ok = status == source.expected_status
        parse_count = 0
        schema_ok = True
        required_ok = True
        parse_error: str | None = None
        warnings = list(template_warnings)

        if strict and status_ok:
            parse_count, schema_ok, required_ok, parse_error, parse_warnings = _run_schema_sample(source, payload)
            warnings.extend(parse_warnings)

        ok = status_ok and template_ok and schema_ok and required_ok and parse_error is None
        error = None
        error_class = None
        if not status_ok:
            error = f"Unexpected status {status}"
            error_class = "unexpected_status"
        elif not template_ok:
            error = template_error
            error_class = "template_validation_error"
        elif parse_error:
            error = parse_error
            error_class = "schema_validation_error"
        elif strict and (not schema_ok or not required_ok):
            error = "Schema/required-field validation failed"
            error_class = "schema_validation_error"

        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            strict=strict,
            ok=ok,
            healthy=ok,
            template_ok=template_ok,
            schema_ok=schema_ok,
            required_fields_ok=required_ok,
            parse_count=parse_count,
            http_status=status,
            latency_ms=latency_ms,
            error=error,
            error_class=error_class,
            warnings=warnings,
        )
    except FetchError as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            strict=strict,
            ok=False,
            healthy=False,
            template_ok=template_ok,
            schema_ok=False,
            required_fields_ok=False,
            parse_count=0,
            http_status=exc.status_code,
            latency_ms=latency_ms,
            error=str(exc),
            error_class=exc.error_class,
            warnings=template_warnings,
        )
    except Exception as exc:
        latency_ms = int((time.perf_counter() - started) * 1000)
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=endpoint,
            strict=strict,
            ok=False,
            healthy=False,
            template_ok=template_ok,
            schema_ok=False,
            required_fields_ok=False,
            parse_count=0,
            http_status=None,
            latency_ms=latency_ms,
            error=str(exc),
            error_class=type(exc).__name__,
            warnings=template_warnings,
        )
