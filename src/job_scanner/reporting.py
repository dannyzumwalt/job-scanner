from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .models import MatchCategory


def _comp_range_text(job: dict[str, Any]) -> str:
    low = job.get("estimated_total_comp_min")
    high = job.get("estimated_total_comp_max")
    if low and high:
        return f"${low:,.0f} - ${high:,.0f}"
    if low:
        return f"From ${low:,.0f}"
    if high:
        return f"Up to ${high:,.0f}"
    return "Not listed"


def _work_mode_text(job: dict[str, Any]) -> str:
    if job.get("is_remote"):
        return "Remote"
    if job.get("is_hybrid"):
        return "Hybrid"
    return "Onsite"


def _category_buckets(
    scored_jobs: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    top = []
    potential = []
    rejected = []
    for job in scored_jobs:
        category = job["category"]
        if category in (MatchCategory.STRONG, MatchCategory.GOOD):
            top.append(job)
        elif category == MatchCategory.POSSIBLE:
            potential.append(job)
        else:
            rejected.append(job)
    return top, potential, rejected


def build_market_notes(scored_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    remote_count = sum(1 for job in scored_jobs if job.get("is_remote"))
    dfw_count = sum(1 for job in scored_jobs if job.get("dfw_match"))
    comp_listed_count = sum(
        1
        for job in scored_jobs
        if job.get("estimated_total_comp_min") is not None or job.get("estimated_total_comp_max") is not None
    )
    strong_matches = sum(
        1 for job in scored_jobs if job.get("category") in (MatchCategory.STRONG, MatchCategory.GOOD)
    )

    title_histogram: dict[str, int] = {}
    company_histogram: dict[str, int] = {}
    for job in scored_jobs:
        title_histogram[job["title"]] = title_histogram.get(job["title"], 0) + 1
        company_histogram[job["company"]] = company_histogram.get(job["company"], 0) + 1

    most_common_titles = sorted(title_histogram.items(), key=lambda item: item[1], reverse=True)[:5]
    most_common_companies = sorted(company_histogram.items(), key=lambda item: item[1], reverse=True)[:5]

    return {
        "total_jobs_scanned": len(scored_jobs),
        "with_listed_compensation": comp_listed_count,
        "remote_jobs": remote_count,
        "dfw_jobs": dfw_count,
        "strong_matches": strong_matches,
        "most_common_titles": most_common_titles,
        "most_common_companies": most_common_companies,
    }


def _source_health_summary(source_health: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(source_health)
    success = sum(1 for item in source_health if item.get("status") == "success")
    failed = sum(1 for item in source_health if item.get("status") != "success")
    success_rate = 0.0 if total == 0 else round((success / total) * 100.0, 1)
    return {
        "total": total,
        "success": success,
        "failed": failed,
        "success_rate": success_rate,
    }


def render_markdown_report(
    scored_jobs: list[dict[str, Any]],
    *,
    scan_id: int,
    source_health: list[dict[str, Any]] | None = None,
    trend_notes: list[str] | None = None,
    top_matches_target: int = 10,
    potential_matches_target: int = 10,
    reject_list_max: int = 30,
    generated_at: datetime | None = None,
) -> str:
    generated = generated_at or datetime.now(UTC)
    top, potential, rejected = _category_buckets(scored_jobs)

    top = top[: max(1, top_matches_target)]
    potential = potential[: max(1, potential_matches_target)]
    rejected = rejected[: max(1, reject_list_max)]

    notes = build_market_notes(scored_jobs)
    health = _source_health_summary(source_health or [])

    lines: list[str] = []
    lines.append("# Job Scan Report")
    lines.append("")
    lines.append(f"Scan ID: {scan_id}")
    lines.append(f"Date of scan: {generated.isoformat()}")
    lines.append("")

    lines.append("## Top Matches")
    lines.append("")
    if not top:
        lines.append("No top matches in this scan.")
    for job in top:
        lines.append(f"### {job['title']}")
        lines.append(f"Company: {job['company']}")
        lines.append(f"Location: {job.get('location') or 'Not listed'}")
        lines.append(f"Work mode: {_work_mode_text(job)}")
        lines.append(f"Estimated total compensation: {_comp_range_text(job)}")
        lines.append(f"Score: {job['display_score']}/10 ({job['total_score']:.1f}/100)")
        lines.append(f"Apply URL: {job.get('apply_url') or 'N/A'}")
        if job.get("reasons"):
            lines.append("Why this role fits:")
            for reason in job["reasons"][:3]:
                lines.append(f"- {reason}")
        if job.get("concerns"):
            lines.append("Potential concerns:")
            for concern in job["concerns"][:2]:
                lines.append(f"- {concern}")
        lines.append("")

    lines.append("## Potential Matches (Needs Review)")
    lines.append("")
    if not potential:
        lines.append("No potential matches in this scan.")
    for job in potential:
        lines.append(f"### {job['title']}")
        lines.append(f"Company: {job['company']}")
        lines.append(f"Location: {job.get('location') or 'Not listed'}")
        lines.append(f"Estimated total compensation: {_comp_range_text(job)}")
        lines.append(f"Reason it might fit: {', '.join(job.get('reasons', [])[:2]) or 'Partial fit on role criteria'}")
        lines.append(
            f"Potential concerns: {', '.join(job.get('concerns', [])[:2]) or 'Needs deeper manual review'}"
        )
        lines.append(f"Score: {job['display_score']}/10 ({job['total_score']:.1f}/100)")
        lines.append("")

    lines.append("## Rejected / Low Priority")
    lines.append("")
    if not rejected:
        lines.append("No rejected roles in this scan.")
    for job in rejected:
        reason = ", ".join(job.get("concerns", [])[:1]) or "Low composite score"
        lines.append(f"- {job['title']} @ {job['company']}: {reason}")

    lines.append("")
    lines.append("## Market Notes")
    lines.append("")
    lines.append(f"- total jobs scanned: {notes['total_jobs_scanned']}")
    lines.append(f"- number with listed compensation: {notes['with_listed_compensation']}")
    lines.append(f"- number remote: {notes['remote_jobs']}")
    lines.append(f"- number DFW: {notes['dfw_jobs']}")
    lines.append(f"- number strong matches: {notes['strong_matches']}")
    lines.append(f"- most common titles: {', '.join([title for title, _ in notes['most_common_titles']]) or 'N/A'}")
    lines.append(
        f"- most common companies: {', '.join([company for company, _ in notes['most_common_companies']]) or 'N/A'}"
    )

    if trend_notes:
        lines.append("")
        lines.append("## Trend Notes")
        lines.append("")
        for note in trend_notes:
            lines.append(f"- {note}")

    lines.append("")
    lines.append("## Source Health")
    lines.append("")
    lines.append(
        f"- sources checked: {health['total']} | success: {health['success']} | failed: {health['failed']} | success rate: {health['success_rate']}%"
    )
    for item in (source_health or []):
        status = item.get("status", "unknown")
        http_status = item.get("http_status")
        latency = item.get("latency_ms", 0)
        parsed = item.get("parse_count", 0)
        suffix = ""
        if item.get("error_message"):
            suffix = f" | error: {item['error_message']}"
        lines.append(
            f"- {item.get('source_name', 'unknown')}: {status} | http={http_status} | parsed={parsed} | latency_ms={latency}{suffix}"
        )

    return "\n".join(lines).strip() + "\n"


def write_reports(
    report_dir: str,
    scan_id: int,
    scored_jobs: list[dict[str, Any]],
    *,
    source_health: list[dict[str, Any]] | None = None,
    trend_notes: list[str] | None = None,
    top_matches_target: int = 10,
    potential_matches_target: int = 10,
    reject_list_max: int = 30,
) -> dict[str, str]:
    out_dir = Path(report_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(UTC)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    markdown = render_markdown_report(
        scored_jobs,
        scan_id=scan_id,
        source_health=source_health,
        trend_notes=trend_notes,
        top_matches_target=top_matches_target,
        potential_matches_target=potential_matches_target,
        reject_list_max=reject_list_max,
        generated_at=now,
    )

    latest_md = out_dir / "latest_report.md"
    ts_md = out_dir / f"report_{timestamp}.md"
    latest_csv = out_dir / "latest_report.csv"
    ts_csv = out_dir / f"report_{timestamp}.csv"
    latest_json = out_dir / "latest_report.json"
    ts_json = out_dir / f"report_{timestamp}.json"

    latest_md.write_text(markdown, encoding="utf-8")
    ts_md.write_text(markdown, encoding="utf-8")

    export_rows: list[dict[str, Any]] = []
    for job in scored_jobs:
        export_rows.append(
            {
                "scan_id": scan_id,
                "normalized_job_id": job["normalized_job_id"],
                "title": job["title"],
                "company": job["company"],
                "location": job.get("location"),
                "is_remote": job.get("is_remote"),
                "is_hybrid": job.get("is_hybrid"),
                "is_onsite": job.get("is_onsite"),
                "ingest_mode": job.get("ingest_mode"),
                "parse_confidence": job.get("parse_confidence"),
                "estimated_total_comp_min": job.get("estimated_total_comp_min"),
                "estimated_total_comp_max": job.get("estimated_total_comp_max"),
                "total_score": job["total_score"],
                "display_score": job["display_score"],
                "category": job["category"].value,
                "recommended_action": job["recommended_action"],
                "apply_url": job.get("apply_url"),
                "is_new": job.get("is_new"),
                "reasons": " | ".join(job.get("reasons", [])),
                "concerns": " | ".join(job.get("concerns", [])),
            }
        )

    fieldnames = list(export_rows[0].keys()) if export_rows else []

    for csv_path in (latest_csv, ts_csv):
        with csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(export_rows)

    json_payload = {
        "scan_id": scan_id,
        "generated_at": now.isoformat(),
        "jobs": export_rows,
        "market_notes": build_market_notes(scored_jobs),
        "trend_notes": trend_notes or [],
        "source_health": source_health or [],
    }

    latest_json.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")
    ts_json.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")

    return {
        "latest_markdown": str(latest_md),
        "timestamped_markdown": str(ts_md),
        "latest_csv": str(latest_csv),
        "timestamped_csv": str(ts_csv),
        "latest_json": str(latest_json),
        "timestamped_json": str(ts_json),
    }
