from __future__ import annotations

from typing import Any

from ..models import NormalizedJob, SourceConfig
from ..utils import (
    build_dedupe_key,
    build_job_hash,
    compact_whitespace,
    detect_role_family_tags,
    detect_seniority_hints,
    estimate_total_comp,
    extract_travel_percent,
    location_is_dfw,
    location_is_us,
    normalize_location,
    normalize_title,
    parse_comp_values_from_text,
    strip_html_tags,
)


def parse_comp_and_confidence(primary_text: str, fallback_text: str = "") -> tuple[int | None, int | None, int | None, int | None, float]:
    salary_min, salary_max = parse_comp_values_from_text(primary_text)
    if salary_min is None and salary_max is None and fallback_text:
        salary_min, salary_max = parse_comp_values_from_text(fallback_text)

    total_min, total_max = estimate_total_comp(salary_min, salary_max, None, None)

    confidence = 0.15
    if salary_min is not None or salary_max is not None:
        confidence = 0.65
        if salary_min is not None and salary_max is not None and salary_min != salary_max:
            confidence = 0.8

    return salary_min, salary_max, total_min, total_max, confidence


def build_normalized_job(
    source: SourceConfig,
    *,
    source_job_id: str,
    title: str,
    description: str,
    location: str,
    apply_url: str | None,
    requisition_id: str | None,
    company: str | None = None,
    salary_text: str | None = None,
    ingest_mode: str = "live",
    import_batch_id: int | None = None,
    parse_confidence: float | None = None,
    data_quality_flags: list[str] | None = None,
    raw_payload: dict[str, Any] | None = None,
) -> NormalizedJob:
    clean_title = compact_whitespace(title)
    clean_description = strip_html_tags(description)
    clean_location = compact_whitespace(location)

    normalized_location = normalize_location(clean_location)
    content_blob = f"{clean_title} {clean_description}"
    travel_percent = extract_travel_percent(content_blob)

    is_remote = "remote" in normalized_location or "remote" in content_blob.lower()
    is_hybrid = "hybrid" in normalized_location or "hybrid" in content_blob.lower()
    is_onsite = not is_remote and not is_hybrid

    comp_text = salary_text or content_blob
    base_min, base_max, total_min, total_max, comp_confidence = parse_comp_and_confidence(comp_text, clean_description)

    explicit_quality_flags = list(data_quality_flags or [])
    if base_min is None and base_max is None:
        explicit_quality_flags.append("comp_missing")
    if not clean_location:
        explicit_quality_flags.append("location_missing")

    parse_conf = parse_confidence if parse_confidence is not None else comp_confidence

    dedupe_key = build_dedupe_key(
        company=(company or source.name),
        normalized_title=normalize_title(clean_title),
        normalized_location=normalized_location,
        apply_url=apply_url,
        requisition_id=requisition_id,
    )

    hash_payload = raw_payload or {
        "title": clean_title,
        "description": clean_description,
        "location": clean_location,
        "apply_url": apply_url,
        "requisition_id": requisition_id,
        "base_min": base_min,
        "base_max": base_max,
    }

    return NormalizedJob(
        source_name=source.name,
        source_type=source.type,
        source_job_id=source_job_id,
        source_url=source.url,
        requisition_id=requisition_id,
        apply_url=apply_url,
        company=company or source.name,
        title=clean_title,
        normalized_title=normalize_title(clean_title),
        description=clean_description,
        location=clean_location,
        normalized_location=normalized_location,
        country="US" if location_is_us(normalized_location) else None,
        is_remote=is_remote,
        is_hybrid=is_hybrid,
        is_onsite=is_onsite,
        dfw_match=location_is_dfw(normalized_location),
        us_match=location_is_us(normalized_location),
        travel_required=travel_percent is not None,
        travel_percent=travel_percent,
        base_min=base_min,
        base_max=base_max,
        estimated_total_comp_min=total_min,
        estimated_total_comp_max=total_max,
        compensation_confidence=comp_confidence,
        role_family_tags=detect_role_family_tags(content_blob),
        seniority_hints=detect_seniority_hints(content_blob),
        ingest_mode="import" if ingest_mode == "import" else "live",
        import_batch_id=import_batch_id,
        data_quality_flags=sorted(set(explicit_quality_flags)),
        parse_confidence=max(0.0, min(1.0, parse_conf)),
        dedupe_key=dedupe_key,
        job_hash=build_job_hash(hash_payload),
    )
