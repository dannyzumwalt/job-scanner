import json
import sqlite3
from pathlib import Path

from job_scanner.config import load_app_config
from job_scanner.pipeline import diff_latest_scan, run_scan


FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _prepare_tmp_project(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    (root / "data" / "raw").mkdir(parents=True)
    (root / "data" / "processed").mkdir(parents=True)
    (root / "data" / "reports").mkdir(parents=True)

    profile_yaml = """
profile_name: test
primary_geography: United States
compensation:
  target_total_comp_min: 300000
  target_total_comp_max: 400000
  hard_floor_total_comp: 250000
work_preferences:
  remote_preferred: true
  remote_first_preferred: true
  dfw_acceptable: true
  allowed_locations: ["Dallas", "Fort Worth", "DFW"]
  max_travel_percent_preferred: 10
  reject_travel_percent_over: 20
role_preferences:
  target_levels: ["senior", "staff", "principal", "distinguished", "architect"]
  target_role_families: ["infrastructure", "operations", "analytics", "reliability", "network", "systems"]
  positive_keywords: ["principal", "staff", "architect", "infrastructure", "reliability", "analytics"]
  negative_keywords: ["contract", "sales", "internship", "entry level", "junior"]
scoring_weights:
  compensation_fit: 0.18
  role_seniority_fit: 0.14
  technical_domain_fit: 0.12
  analytics_data_fit: 0.10
  infrastructure_reliability_fit: 0.12
  remote_fit: 0.08
  location_fit: 0.08
  travel_fit: 0.08
  leadership_autonomy_fit: 0.05
  title_relevance: 0.05
scoring_rules:
  strong_match_min: 90
  good_match_min: 75
  possible_match_min: 60
  compensation_absent_penalty: 8
  low_compensation_penalty: 25
  heavy_travel_penalty: 25
  non_dfw_onsite_penalty: 18
  disallowed_role_penalty: 35
  infra_analytics_boost: 10
  senior_title_boost: 8
  remote_boost: 8
ingestion:
  request_timeout_seconds: 5
  request_retries: 0
  retry_backoff_seconds: 0
  min_request_interval_seconds: 0
  max_workers: 2
""".strip()

    sources_yaml = """
sources:
  - name: Datadog
    type: greenhouse
    enabled: true
    url: https://boards.greenhouse.io/datadog
  - name: Stripe
    type: lever
    enabled: true
    url: https://jobs.lever.co/stripe
""".strip()

    (root / "config" / "search_profile.yaml").write_text(profile_yaml, encoding="utf-8")
    (root / "config" / "sources.yaml").write_text(sources_yaml, encoding="utf-8")
    return root


def test_scan_report_and_diff_flow(tmp_path: Path, monkeypatch) -> None:
    root = _prepare_tmp_project(tmp_path)

    greenhouse_payload = json.loads((FIXTURES / "greenhouse_jobs.json").read_text(encoding="utf-8"))
    lever_payload = json.loads((FIXTURES / "lever_jobs.json").read_text(encoding="utf-8"))

    def first_get_json(self, url, headers=None):
        _ = self
        _ = headers
        if "greenhouse" in url:
            return greenhouse_payload
        if "lever" in url:
            return lever_payload
        return {}

    monkeypatch.setattr("job_scanner.http_client.HttpFetcher.get_json", first_get_json)
    monkeypatch.setattr(
        "job_scanner.http_client.HttpFetcher.get_json_with_meta",
        lambda self, url, headers=None: (first_get_json(self, url, headers), 200),
    )
    monkeypatch.setattr("job_scanner.http_client.HttpFetcher.post_json", lambda self, url, payload, headers=None: {})
    monkeypatch.setattr(
        "job_scanner.http_client.HttpFetcher.post_json_with_meta",
        lambda self, url, payload, headers=None: ({}, 200),
    )

    config = load_app_config(root)
    result_one = run_scan(config, generate_report=True)

    assert result_one["raw_count"] >= 3
    assert result_one["normalized_count"] >= 2
    assert Path(result_one["reports"]["latest_markdown"]).exists()

    markdown = Path(result_one["reports"]["latest_markdown"]).read_text(encoding="utf-8")
    assert "## Top Matches" in markdown
    assert "## Potential Matches (Needs Review)" in markdown
    assert "## Rejected / Low Priority" in markdown

    def second_get_json(self, url, headers=None):
        _ = self
        _ = headers
        if "greenhouse" in url:
            return {"jobs": greenhouse_payload["jobs"][:1]}
        if "lever" in url:
            modified = list(lever_payload)
            modified[0] = dict(modified[0])
            modified[0]["descriptionPlain"] = (
                modified[0]["descriptionPlain"] + " Updated requirements and ownership scope."
            )
            return modified
        return {}

    monkeypatch.setattr("job_scanner.http_client.HttpFetcher.get_json", second_get_json)
    monkeypatch.setattr(
        "job_scanner.http_client.HttpFetcher.get_json_with_meta",
        lambda self, url, headers=None: (second_get_json(self, url, headers), 200),
    )

    result_two = run_scan(config, generate_report=True)
    assert result_two["scan_id"] > result_one["scan_id"]

    diff = diff_latest_scan(config, since="last")
    assert diff["current_scan_id"] == result_two["scan_id"]
    assert len(diff["removed_jobs"]) >= 1

    conn = sqlite3.connect(config.db_path)
    try:
        scan_count = conn.execute("SELECT COUNT(*) FROM scans WHERE status = 'completed'").fetchone()[0]
        assert scan_count >= 2
    finally:
        conn.close()
