from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import yaml

from .http_client import HttpFetcher
from .models import AppConfig, SourceConfig, SourceFormat, SourceType, SourceValidationResult
from .source_validation import validate_source


@dataclass(frozen=True)
class SourceCatalogEntry:
    name: str
    source_type: SourceType
    slug_or_url: str
    tags: tuple[str, ...]
    notes: str = ""
    compensation_tier: int = 2
    remote_friendly: bool = True
    parser_template: dict[str, str] | None = None


@dataclass
class DiscoveredSource:
    source: SourceConfig
    fit_score: int
    fit_reasons: list[str]
    validation: SourceValidationResult | None = None


def _gh(name: str, slug: str, tags: tuple[str, ...], notes: str, compensation_tier: int = 3) -> SourceCatalogEntry:
    return SourceCatalogEntry(
        name=name,
        source_type=SourceType.GREENHOUSE,
        slug_or_url=slug,
        tags=tags,
        notes=notes,
        compensation_tier=compensation_tier,
    )


def _lever(name: str, slug: str, tags: tuple[str, ...], notes: str, compensation_tier: int = 3) -> SourceCatalogEntry:
    return SourceCatalogEntry(
        name=name,
        source_type=SourceType.LEVER,
        slug_or_url=slug,
        tags=tags,
        notes=notes,
        compensation_tier=compensation_tier,
    )


def _ashby(name: str, slug: str, tags: tuple[str, ...], notes: str, compensation_tier: int = 3) -> SourceCatalogEntry:
    return SourceCatalogEntry(
        name=name,
        source_type=SourceType.ASHBY,
        slug_or_url=slug,
        tags=tags,
        notes=notes,
        compensation_tier=compensation_tier,
    )


def _json(
    name: str,
    url: str,
    tags: tuple[str, ...],
    notes: str,
    parser_template: dict[str, str],
    compensation_tier: int = 2,
) -> SourceCatalogEntry:
    return SourceCatalogEntry(
        name=name,
        source_type=SourceType.GENERIC_JSON,
        slug_or_url=url,
        tags=tags,
        notes=notes,
        compensation_tier=compensation_tier,
        parser_template=parser_template,
    )


def _rss(name: str, url: str, tags: tuple[str, ...], notes: str, compensation_tier: int = 2) -> SourceCatalogEntry:
    return SourceCatalogEntry(
        name=name,
        source_type=SourceType.RSS,
        slug_or_url=url,
        tags=tags,
        notes=notes,
        compensation_tier=compensation_tier,
    )


DEFAULT_SOURCE_CATALOG: tuple[SourceCatalogEntry, ...] = (
    _gh("Datadog", "datadog", ("infrastructure", "reliability", "platform", "observability"), "Infra-heavy engineering org."),
    _gh("Cloudflare", "cloudflare", ("network", "infrastructure", "reliability", "distributed systems"), "Network and edge infrastructure."),
    _gh("Airbnb", "airbnb", ("platform", "distributed systems", "data", "reliability"), "Large-scale platform engineering roles."),
    _gh("Figma", "figma", ("platform", "infrastructure", "data"), "High-comp technical IC roles."),
    _gh("Dropbox", "dropbox", ("infrastructure", "distributed systems", "platform"), "Infra and systems roles."),
    _gh("Lyft", "lyft", ("platform", "infrastructure", "reliability"), "Reliability and platform work."),
    _gh("Instacart", "instacart", ("data", "platform", "reliability"), "Data + infra opportunities."),
    _gh("Reddit", "reddit", ("distributed systems", "infrastructure", "reliability"), "Large-scale backend and reliability."),
    _gh("Coinbase", "coinbase", ("infrastructure", "security", "distributed systems"), "High-end IC opportunity set."),
    _gh("Affirm", "affirm", ("platform", "distributed systems", "data"), "Platform and backend scope."),
    _gh("MongoDB", "mongodb", ("database", "distributed systems", "infrastructure"), "Data infra and distributed systems."),
    _gh("Elastic", "elastic", ("search", "infrastructure", "distributed systems", "observability"), "Infra and distributed systems."),
    _gh("Twilio", "twilio", ("telecom", "network", "platform", "reliability"), "Telecom-adjacent infrastructure roles."),
    _gh("Robinhood", "robinhood", ("platform", "infrastructure", "reliability"), "Platform and SRE-like paths."),
    _gh("Grammarly", "grammarly", ("data", "platform", "infrastructure"), "Strong technical IC ladders."),
    _gh("OpenAI", "openai", ("infrastructure", "platform", "distributed systems", "ai"), "High-comp infrastructure and systems roles."),
    _gh("Plaid", "plaid", ("platform", "infrastructure", "reliability"), "Infra and production engineering roles."),
    _gh("Asana", "asana", ("platform", "infrastructure", "analytics"), "Platform and systems engineering."),
    _gh("Canva", "canva", ("platform", "distributed systems", "data"), "Global product infra footprint."),
    _gh("Duolingo", "duolingo", ("platform", "data", "reliability"), "Data and platform opportunities."),
    _gh("HubSpot", "hubspot", ("platform", "analytics", "reliability"), "Platform and ops-heavy engineering."),
    _gh("HashiCorp", "hashicorp", ("infrastructure", "cloud", "distributed systems", "reliability"), "Infra tooling and systems depth."),
    _gh("Toast", "toasttab", ("platform", "operations", "infrastructure"), "Operationally heavy systems engineering."),
    _gh("Wayfair", "wayfair", ("platform", "data", "operations"), "Data-driven operations and systems."),
    _gh("Coursera", "coursera", ("platform", "data", "reliability"), "Distributed systems and platform IC roles."),
    _gh("Okta", "okta", ("security", "platform", "infrastructure"), "Identity and platform engineering."),
    _gh("Square", "square", ("platform", "distributed systems", "reliability"), "Platform and reliability opportunities."),
    _gh("GitLab", "gitlab", ("remote", "platform", "infrastructure"), "Remote-friendly engineering organization."),
    _gh("Yelp", "yelp", ("data", "platform", "reliability"), "Platform and operational data roles."),
    _gh("TripAdvisor", "tripadvisor", ("platform", "data", "reliability"), "Systems and data-heavy roles."),
    _gh("Zendesk", "zendesk", ("platform", "operations", "analytics"), "Operationally aligned engineering."),
    _lever("Palantir", "palantir", ("distributed systems", "platform", "data"), "Data-intensive technical roles."),
    _lever("Spotify", "spotify", ("platform", "reliability", "data"), "Platform and backend IC roles."),
    _lever("Netflix", "netflix", ("distributed systems", "platform", "reliability"), "High-scale systems engineering."),
    _lever("Discord", "discord", ("distributed systems", "reliability", "platform"), "Large-scale realtime systems."),
    _lever("Brex", "brex", ("platform", "infrastructure", "reliability"), "Infra and backend opportunities."),
    _lever("Rippling", "rippling", ("platform", "operations", "reliability"), "Operationally deep backend work."),
    _lever("Chime", "chime", ("platform", "reliability", "distributed systems"), "Systems and reliability opportunities."),
    _lever("Postman", "postman", ("platform", "infrastructure", "developer tools"), "Platform and infra engineering."),
    _lever("Eventbrite", "eventbrite", ("platform", "operations", "data"), "Operations and platform roles."),
    _lever("Checkr", "checkr", ("platform", "operations", "data"), "Operational systems and platform."),
    _lever("Miro", "miro", ("platform", "distributed systems", "reliability"), "Large-scale collaboration systems."),
    _lever("Zapier", "zapier", ("remote", "automation", "platform"), "Automation-heavy remote org."),
    _lever("Nuro", "nuro", ("robotics", "infrastructure", "reliability"), "Reliability-oriented systems roles."),
    _lever("Samsara", "samsara", ("iot", "operations", "data", "infrastructure"), "Physical + digital ops systems."),
    _lever("Amplitude", "amplitude", ("analytics", "data", "platform"), "Product analytics infrastructure."),
    _ashby("Anthropic", "anthropic", ("ai", "infrastructure", "platform", "reliability"), "AI infra and platform roles."),
    _ashby("Ramp", "ramp", ("platform", "operations", "data"), "Operationally complex technical roles."),
    _ashby("Scale AI", "scaleai", ("ai", "data", "operations", "platform"), "AI data systems and infra."),
    _ashby("Linear", "linear", ("platform", "remote", "systems"), "Lean high-caliber IC team."),
    _ashby("Perplexity", "perplexity", ("ai", "infrastructure", "platform"), "AI-heavy systems opportunities."),
    _ashby("Cursor", "cursor", ("ai", "developer tools", "platform"), "AI tooling and systems engineering."),
    _json(
        "Remotive",
        "https://remotive.com/api/remote-jobs",
        ("remote", "aggregator", "multi-company", "infrastructure", "data"),
        "Remote job board API with many companies.",
        parser_template={
            "items_path": "jobs",
            "id_field": "id",
            "title_field": "title",
            "description_field": "description",
            "location_field": "candidate_required_location",
            "apply_url_field": "url",
            "company_field": "company_name",
            "salary_text_field": "salary",
        },
    ),
    _json(
        "RemoteOK API",
        "https://remoteok.com/api",
        ("remote", "aggregator", "multi-company", "distributed systems"),
        "Multi-company remote jobs via JSON API.",
        parser_template={
            "id_field": "id",
            "title_field": "position",
            "description_field": "description",
            "location_field": "location",
            "apply_url_field": "url",
            "company_field": "company",
            "salary_text_field": "salary",
        },
    ),
    _rss(
        "We Work Remotely RSS",
        "https://weworkremotely.com/categories/remote-programming-jobs.rss",
        ("remote", "aggregator", "multi-company", "engineering"),
        "Remote programming jobs from many companies.",
    ),
    _rss(
        "Remote OK RSS",
        "https://remoteok.com/remote-dev-jobs.rss",
        ("remote", "aggregator", "multi-company", "engineering"),
        "Remote development jobs RSS feed.",
    ),
)


FORMAT_BY_TYPE: dict[SourceType, SourceFormat] = {
    SourceType.GREENHOUSE: SourceFormat.GREENHOUSE,
    SourceType.LEVER: SourceFormat.LEVER,
    SourceType.ASHBY: SourceFormat.ASHBY,
    SourceType.GENERIC_JSON: SourceFormat.JSON,
    SourceType.RSS: SourceFormat.RSS,
}


STOP_WORDS = {
    "and",
    "the",
    "with",
    "for",
    "this",
    "that",
    "into",
    "from",
    "your",
    "role",
    "roles",
    "company",
    "companies",
    "engineering",
    "engineer",
    "technical",
    "level",
    "senior",
    "staff",
    "principal",
    "distinguished",
    "architect",
    "high",
    "value",
    "worth",
    "review",
    "preferred",
    "acceptable",
    "candidate",
}


SYNONYMS = {
    "sre": "reliability",
    "platform": "infrastructure",
    "distributed": "distributed systems",
    "cloud": "infrastructure",
    "telecommunications": "telecom",
    "networking": "network",
    "ops": "operations",
    "operation": "operations",
    "automation": "operations",
    "observability": "reliability",
    "analytics": "data",
}


def _tokenize(text: str) -> set[str]:
    tokens = {token.lower() for token in re.findall(r"[a-zA-Z][a-zA-Z0-9+\-]{1,}", text)}
    return {token for token in tokens if token not in STOP_WORDS}


def _normalize_term(term: str) -> str:
    return SYNONYMS.get(term, term)


def _collect_profile_terms(app_config: AppConfig, criteria_markdown_path: str | Path | None = None) -> set[str]:
    terms: set[str] = set()

    role = app_config.profile.role_preferences
    for item in role.target_role_families + role.positive_keywords + role.target_levels:
        terms.update(_tokenize(item))

    if app_config.profile.work_preferences.remote_preferred:
        terms.add("remote")
    if app_config.profile.work_preferences.dfw_acceptable:
        terms.update({"dallas", "fort", "worth", "dfw"})

    if criteria_markdown_path:
        path = Path(criteria_markdown_path)
        if path.exists():
            text = path.read_text(encoding="utf-8")
            for line in text.splitlines():
                stripped = line.strip()
                if stripped.startswith("-") or stripped.startswith("###"):
                    terms.update(_tokenize(stripped.lstrip("-# ")))

    return {_normalize_term(term) for term in terms}


def _resolve_source_url(entry: SourceCatalogEntry) -> str:
    if entry.source_type == SourceType.GREENHOUSE:
        return f"https://boards.greenhouse.io/{entry.slug_or_url.strip('/')}"
    if entry.source_type == SourceType.LEVER:
        return f"https://jobs.lever.co/{entry.slug_or_url.strip('/')}"
    if entry.source_type == SourceType.ASHBY:
        return f"https://jobs.ashbyhq.com/{entry.slug_or_url.strip('/')}"
    return entry.slug_or_url


def _source_key(source_type: SourceType, url: str) -> str:
    return f"{source_type.value}:{url.strip().rstrip('/').lower()}"


def _build_source_config(entry: SourceCatalogEntry, *, priority: int) -> SourceConfig:
    return SourceConfig(
        name=entry.name,
        type=entry.source_type,
        format=FORMAT_BY_TYPE.get(entry.source_type, SourceFormat.AUTO),
        enabled=False,
        priority=priority,
        expected_status=200,
        url=_resolve_source_url(entry),
        notes=entry.notes,
        parser_template=entry.parser_template or {},
    )


def _score_entry(entry: SourceCatalogEntry, profile_terms: set[str], app_config: AppConfig) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    candidate_terms = {_normalize_term(token) for token in _tokenize(" ".join(entry.tags))}

    overlap = sorted(candidate_terms.intersection(profile_terms))
    if overlap:
        score += min(50, 8 * len(overlap))
        reasons.append(f"Profile alignment: {', '.join(overlap[:4])}")

    if entry.compensation_tier >= 3:
        score += 20
        reasons.append("Historically strong senior compensation bands")
    elif entry.compensation_tier == 2:
        score += 12

    if app_config.profile.work_preferences.remote_preferred and entry.remote_friendly:
        score += 10
        reasons.append("Remote-friendly hiring patterns")

    if entry.source_type in {SourceType.GREENHOUSE, SourceType.LEVER, SourceType.ASHBY}:
        score += 10
    elif entry.source_type in {SourceType.GENERIC_JSON, SourceType.RSS}:
        score += 8
        reasons.append("Multi-company job board feed")

    if {"network", "telecom"}.intersection(candidate_terms):
        score += 6
        reasons.append("Network/telecom relevance")

    score = max(0, min(100, score))
    return score, reasons[:4]


def _validate_discovered_sources(
    discovered: list[DiscoveredSource],
    app_config: AppConfig,
    *,
    strict: bool,
) -> list[DiscoveredSource]:
    if not discovered:
        return discovered

    fetcher = HttpFetcher(
        timeout_seconds=app_config.profile.ingestion.request_timeout_seconds,
        retries=app_config.profile.ingestion.request_retries,
        retry_backoff_seconds=app_config.profile.ingestion.retry_backoff_seconds,
        min_request_interval_seconds=app_config.profile.ingestion.min_request_interval_seconds,
    )

    try:
        max_workers = max(1, app_config.profile.ingestion.max_workers)
        results: list[DiscoveredSource] = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {
                pool.submit(validate_source, item.source, fetcher, strict=strict): item
                for item in discovered
            }
            for future in as_completed(futures):
                item = futures[future]
                validation = future.result()
                results.append(
                    DiscoveredSource(
                        source=item.source,
                        fit_score=item.fit_score,
                        fit_reasons=item.fit_reasons,
                        validation=validation,
                    )
                )
        return sorted(results, key=lambda item: (-item.fit_score, item.source.name.lower()))
    finally:
        fetcher.close()


def discover_sources(
    app_config: AppConfig,
    *,
    limit: int = 40,
    min_fit_score: int = 35,
    include_types: set[SourceType] | None = None,
    include_existing: bool = False,
    validate_endpoints: bool = False,
    strict_validation: bool = False,
    only_healthy: bool = True,
    criteria_markdown_path: str | Path | None = None,
    catalog: tuple[SourceCatalogEntry, ...] = DEFAULT_SOURCE_CATALOG,
) -> list[DiscoveredSource]:
    profile_terms = _collect_profile_terms(app_config, criteria_markdown_path)

    existing_by_name = {source.name.strip().lower() for source in app_config.sources}
    existing_by_url = {_source_key(source.type, source.url) for source in app_config.sources}

    discovered: list[DiscoveredSource] = []
    for idx, entry in enumerate(catalog, start=1):
        if include_types and entry.source_type not in include_types:
            continue
        source = _build_source_config(entry, priority=200 + idx)
        key = _source_key(source.type, source.url)

        if not include_existing and (source.name.strip().lower() in existing_by_name or key in existing_by_url):
            continue

        fit_score, reasons = _score_entry(entry, profile_terms, app_config)
        if fit_score < min_fit_score:
            continue

        discovered.append(DiscoveredSource(source=source, fit_score=fit_score, fit_reasons=reasons))

    discovered.sort(key=lambda item: (-item.fit_score, item.source.name.lower()))

    if validate_endpoints:
        validation_pool = discovered[: max(limit * 3, limit)]
        discovered = _validate_discovered_sources(validation_pool, app_config, strict=strict_validation)
        if only_healthy:
            discovered = [item for item in discovered if item.validation and item.validation.healthy]

    return discovered[:limit]


def _source_to_yaml_record(source: SourceConfig) -> dict:
    payload = source.model_dump(mode="json")
    record = {
        "name": payload["name"],
        "type": payload["type"],
        "format": payload["format"],
        "enabled": payload["enabled"],
        "priority": payload["priority"],
        "expected_status": payload["expected_status"],
        "url": payload["url"],
    }
    if payload.get("api_url"):
        record["api_url"] = payload["api_url"]
    if payload.get("notes"):
        record["notes"] = payload["notes"]
    if payload.get("headers"):
        record["headers"] = payload["headers"]
    if payload.get("parser_template"):
        record["parser_template"] = payload["parser_template"]
    return record


def write_discovered_sources(
    output_path: str | Path,
    discovered: list[DiscoveredSource],
    *,
    enable: bool = False,
) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    sources: list[dict] = []
    for index, item in enumerate(discovered, start=1):
        source = item.source.model_copy(update={"enabled": enable, "priority": 200 + index})
        sources.append(_source_to_yaml_record(source))

    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"sources": sources}, fh, sort_keys=False)
    return str(path)


def append_discovered_sources(
    sources_path: str | Path,
    discovered: list[DiscoveredSource],
    *,
    enable: bool = False,
) -> dict[str, int | str]:
    path = Path(sources_path)
    if path.exists():
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh) or {}
    else:
        raw = {}

    existing_entries = raw.get("sources") or []
    if not isinstance(existing_entries, list):
        raise ValueError("sources config must contain a top-level 'sources' list")

    existing_names = {
        str(entry.get("name", "")).strip().lower()
        for entry in existing_entries
        if isinstance(entry, dict)
    }
    existing_urls = {
        _source_key(SourceType(str(entry.get("type"))), str(entry.get("url", "")))
        for entry in existing_entries
        if isinstance(entry, dict) and entry.get("type") and entry.get("url")
    }

    current_priorities = [
        int(entry.get("priority", 100))
        for entry in existing_entries
        if isinstance(entry, dict)
    ]
    next_priority = (max(current_priorities) if current_priorities else 100) + 1

    added = 0
    skipped = 0

    for item in discovered:
        source = item.source.model_copy(update={"enabled": enable, "priority": next_priority})
        name_key = source.name.strip().lower()
        url_key = _source_key(source.type, source.url)
        if name_key in existing_names or url_key in existing_urls:
            skipped += 1
            continue

        existing_entries.append(_source_to_yaml_record(source))
        existing_names.add(name_key)
        existing_urls.add(url_key)
        next_priority += 1
        added += 1

    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump({"sources": existing_entries}, fh, sort_keys=False)

    return {
        "path": str(path),
        "added": added,
        "skipped": skipped,
        "total": len(existing_entries),
    }
