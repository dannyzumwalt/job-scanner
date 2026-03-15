import json
from datetime import UTC, datetime
from pathlib import Path

from job_scanner.models import MatchCategory
from job_scanner.reporting import write_reports


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _sample_rows() -> list[dict]:
    return [
        {
            "normalized_job_id": 1,
            "title": "Principal Infrastructure Analytics Engineer",
            "company": "ExampleCo",
            "location": "Remote - United States",
            "is_remote": True,
            "is_hybrid": False,
            "is_onsite": False,
            "ingest_mode": "live",
            "parse_confidence": 0.92,
            "estimated_total_comp_min": 340000,
            "estimated_total_comp_max": 440000,
            "total_score": 95.5,
            "display_score": 9.6,
            "category": MatchCategory.STRONG,
            "recommended_action": "pursue",
            "apply_url": "https://example.com/jobs/1",
            "is_new": True,
            "reasons": [
                "Compensation potential aligns with target band",
                "Infrastructure/reliability signal is strong",
            ],
            "concerns": ["Low extraction confidence reduced score"],
            "dfw_match": False,
        },
        {
            "normalized_job_id": 2,
            "title": "Staff SRE (Remote)",
            "company": "ExampleCloud",
            "location": "Remote - United States",
            "is_remote": True,
            "is_hybrid": False,
            "is_onsite": False,
            "ingest_mode": "live",
            "parse_confidence": 0.87,
            "estimated_total_comp_min": 300000,
            "estimated_total_comp_max": 390000,
            "total_score": 88.2,
            "display_score": 8.8,
            "category": MatchCategory.GOOD,
            "recommended_action": "pursue",
            "apply_url": "https://example.com/jobs/2",
            "is_new": False,
            "reasons": ["Role seniority matches target IC level"],
            "concerns": [],
            "dfw_match": False,
        },
        {
            "normalized_job_id": 3,
            "title": "Senior Platform Engineer",
            "company": "ExampleData",
            "location": "Dallas, TX",
            "is_remote": False,
            "is_hybrid": True,
            "is_onsite": False,
            "ingest_mode": "live",
            "parse_confidence": 0.76,
            "estimated_total_comp_min": 250000,
            "estimated_total_comp_max": 320000,
            "total_score": 68.0,
            "display_score": 6.8,
            "category": MatchCategory.POSSIBLE,
            "recommended_action": "review manually",
            "apply_url": "https://example.com/jobs/3",
            "is_new": True,
            "reasons": ["Role includes meaningful analytics/data responsibilities"],
            "concerns": ["Needs deeper manual review"],
            "dfw_match": True,
        },
        {
            "normalized_job_id": 4,
            "title": "Sales Engineer",
            "company": "ExampleBiz",
            "location": "Austin, TX",
            "is_remote": False,
            "is_hybrid": False,
            "is_onsite": True,
            "ingest_mode": "live",
            "parse_confidence": 0.9,
            "estimated_total_comp_min": 140000,
            "estimated_total_comp_max": 180000,
            "total_score": 38.0,
            "display_score": 3.8,
            "category": MatchCategory.REJECT,
            "recommended_action": "reject",
            "apply_url": "https://example.com/jobs/4",
            "is_new": False,
            "reasons": [],
            "concerns": ["Compensation appears below hard floor"],
            "dfw_match": False,
        },
    ]


def test_sample_report_artifacts_are_reproducible(tmp_path: Path) -> None:
    generated_at = datetime(2026, 3, 15, 19, 30, 0, tzinfo=UTC)
    source_health = [
        {"source_name": "ExampleCo", "status": "success", "http_status": 200, "parse_count": 2, "latency_ms": 120},
        {"source_name": "ExampleCloud", "status": "failed", "http_status": 404, "parse_count": 0, "latency_ms": 95, "error_message": "Not Found"},
    ]
    health_gate = {
        "strict": True,
        "healthy_sources": 1,
        "total_live_sources": 2,
        "required_min": 2,
        "gate_passed": False,
    }
    trend_notes = ["Remote share: 75.0% (+5.0 pts vs prior scan)"]

    reports = write_reports(
        str(tmp_path),
        99,
        _sample_rows(),
        source_health=source_health,
        health_gate=health_gate,
        trend_notes=trend_notes,
        top_matches_target=2,
        potential_matches_target=1,
        reject_list_max=2,
        generated_at=generated_at,
    )

    latest_md = Path(reports["latest_markdown"]).read_text(encoding="utf-8")
    latest_csv = Path(reports["latest_csv"]).read_text(encoding="utf-8")
    latest_json = Path(reports["latest_json"]).read_text(encoding="utf-8")

    assert latest_md == (FIXTURES / "sample_report.md").read_text(encoding="utf-8")
    assert latest_csv == (FIXTURES / "sample_report.csv").read_text(encoding="utf-8")
    assert json.loads(latest_json) == json.loads((FIXTURES / "sample_report.json").read_text(encoding="utf-8"))
