from __future__ import annotations

from typing import Any

from ..models import NormalizedJob, SourceConfig
from ..utils import (
    MIN_REASONABLE_COMPONENT,
    build_dedupe_key,
    build_job_hash,
    compact_whitespace,
    detect_role_family_tags,
    detect_seniority_hints,
    estimate_total_comp,
    extract_bonus_amount,
    extract_bonus_percent,
    extract_equity_amount,
    extract_travel_percent,
    location_is_dfw,
    location_is_us,
    normalize_location,
    normalize_title,
    parse_comp_values_from_text,
    parse_money_value,
    parse_percent_value,
    strip_html_tags,
)


def _normalize_range(low: int | None, high: int | None) -> tuple[int | None, int | None]:
    if low is None and high is None:
        return None, None
    if low is None:
        low = high
    if high is None:
        high = low
    if low is not None and high is not None and low > high:
        return high, low
    return low, high


def _bonus_from_percent(percent: float | None, base_min: int | None, base_max: int | None) -> int | None:
    if percent is None:
        return None
    if base_min is None and base_max is None:
        return None
    base_ref = base_max or base_min or 0
    return int((percent / 100.0) * base_ref)


def parse_comp_and_confidence(
    primary_text: str,
    fallback_text: str = "",
    *,
    base_min_hint: Any = None,
    base_max_hint: Any = None,
    bonus_hint: Any = None,
    bonus_percent_hint: Any = None,
    equity_hint: Any = None,
) -> tuple[
    int | None,
    int | None,
    int | None,
    int | None,
    int | None,
    int | None,
    float,
    list[str],
]:
    quality_flags: list[str] = []

    text_min, text_max = parse_comp_values_from_text(primary_text)
    if text_min is None and text_max is None and fallback_text:
        text_min, text_max = parse_comp_values_from_text(fallback_text)

    base_min_structured = parse_money_value(base_min_hint)
    base_max_structured = parse_money_value(base_max_hint)

    base_min = base_min_structured if base_min_structured is not None else text_min
    base_max = base_max_structured if base_max_structured is not None else text_max
    base_min, base_max = _normalize_range(base_min, base_max)

    bonus = parse_money_value(bonus_hint, min_value=MIN_REASONABLE_COMPONENT)
    bonus_percent = parse_percent_value(bonus_percent_hint)
    if bonus is None:
        bonus = extract_bonus_amount(primary_text) or extract_bonus_amount(fallback_text)
    if bonus is None:
        if bonus_percent is None:
            bonus_percent = extract_bonus_percent(primary_text) or extract_bonus_percent(fallback_text)
        bonus = _bonus_from_percent(bonus_percent, base_min, base_max)
        if bonus_percent is not None and bonus is None:
            quality_flags.append("bonus_pct_without_base")

    equity = parse_money_value(equity_hint, min_value=MIN_REASONABLE_COMPONENT)
    if equity is None:
        equity = extract_equity_amount(primary_text) or extract_equity_amount(fallback_text)

    total_min, total_max = estimate_total_comp(base_min, base_max, bonus, equity)

    has_structured_base = base_min_structured is not None or base_max_structured is not None
    has_structured_bonus = parse_money_value(bonus_hint, min_value=MIN_REASONABLE_COMPONENT) is not None
    has_structured_equity = parse_money_value(equity_hint, min_value=MIN_REASONABLE_COMPONENT) is not None
    has_text_range = text_min is not None and text_max is not None and text_min != text_max
    has_text_single = (text_min is not None or text_max is not None) and not has_text_range

    if has_structured_base and base_min is not None and base_max is not None:
        confidence = 0.92
    elif has_structured_base:
        confidence = 0.85
    elif has_text_range:
        confidence = 0.74
    elif has_text_single:
        confidence = 0.62
    else:
        confidence = 0.25

    if has_structured_bonus or has_structured_equity:
        confidence += 0.04
    elif bonus is not None or equity is not None:
        confidence += 0.02

    if base_min is None and base_max is None:
        quality_flags.append("comp_missing")
    elif total_min is None or total_max is None:
        quality_flags.append("comp_partial")
    if bonus is None and bonus_percent is not None:
        quality_flags.append("bonus_unresolved")

    return base_min, base_max, bonus, equity, total_min, total_max, max(0.0, min(0.98, confidence)), quality_flags


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
    base_min_hint: Any = None,
    base_max_hint: Any = None,
    bonus_hint: Any = None,
    bonus_percent_hint: Any = None,
    equity_hint: Any = None,
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
    base_min, base_max, bonus, equity, total_min, total_max, comp_confidence, comp_quality_flags = parse_comp_and_confidence(
        comp_text,
        clean_description,
        base_min_hint=base_min_hint,
        base_max_hint=base_max_hint,
        bonus_hint=bonus_hint,
        bonus_percent_hint=bonus_percent_hint,
        equity_hint=equity_hint,
    )

    explicit_quality_flags = list(data_quality_flags or [])
    explicit_quality_flags.extend(comp_quality_flags)
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
        "bonus": bonus,
        "equity": equity,
        "estimated_total_comp_min": total_min,
        "estimated_total_comp_max": total_max,
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
        bonus=bonus,
        equity=equity,
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
