from __future__ import annotations

from job_scanner.models import NormalizedJob, SourceType
from job_scanner.utils import build_dedupe_key, build_job_hash, normalize_location, normalize_title


def make_job(
    *,
    source_name: str = "ExampleCo",
    source_type: SourceType = SourceType.GREENHOUSE,
    source_job_id: str = "1",
    title: str = "Principal Infrastructure Engineer",
    description: str = "Build reliability and analytics systems",
    location: str = "Remote - United States",
    apply_url: str = "https://example.com/jobs/1",
    requisition_id: str | None = None,
    is_remote: bool = True,
    is_hybrid: bool = False,
    is_onsite: bool = False,
    dfw_match: bool = False,
    us_match: bool = True,
    travel_percent: int | None = None,
    base_min: int | None = 250000,
    base_max: int | None = 300000,
    estimated_total_comp_min: int | None = 320000,
    estimated_total_comp_max: int | None = 420000,
    role_family_tags: list[str] | None = None,
    seniority_hints: list[str] | None = None,
    parse_confidence: float = 1.0,
    data_quality_flags: list[str] | None = None,
) -> NormalizedJob:
    normalized_title = normalize_title(title)
    normalized_location = normalize_location(location)
    dedupe_key = build_dedupe_key(
        company=source_name,
        normalized_title=normalized_title,
        normalized_location=normalized_location,
        apply_url=apply_url,
        requisition_id=requisition_id,
    )

    payload = {
        "title": title,
        "description": description,
        "location": location,
        "apply_url": apply_url,
        "requisition_id": requisition_id,
    }

    return NormalizedJob(
        source_name=source_name,
        source_type=source_type,
        source_job_id=source_job_id,
        source_url="https://example.com/jobs",
        requisition_id=requisition_id,
        apply_url=apply_url,
        company=source_name,
        title=title,
        normalized_title=normalized_title,
        description=description,
        location=location,
        normalized_location=normalized_location,
        country="US",
        is_remote=is_remote,
        is_hybrid=is_hybrid,
        is_onsite=is_onsite,
        dfw_match=dfw_match,
        us_match=us_match,
        travel_required=travel_percent is not None,
        travel_percent=travel_percent,
        base_min=base_min,
        base_max=base_max,
        estimated_total_comp_min=estimated_total_comp_min,
        estimated_total_comp_max=estimated_total_comp_max,
        compensation_confidence=0.8,
        role_family_tags=role_family_tags or ["infrastructure", "analytics", "reliability"],
        seniority_hints=seniority_hints or ["principal"],
        parse_confidence=parse_confidence,
        data_quality_flags=data_quality_flags or [],
        dedupe_key=dedupe_key,
        job_hash=build_job_hash(payload),
    )
