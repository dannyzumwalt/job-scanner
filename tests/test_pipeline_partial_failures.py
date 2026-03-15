from pathlib import Path

from job_scanner.config import load_app_config
from job_scanner.pipeline import run_scan


def _prepare_tmp_project(tmp_path: Path) -> Path:
    root = tmp_path / "project_partial"
    (root / "config").mkdir(parents=True)
    (root / "data" / "raw").mkdir(parents=True)
    (root / "data" / "processed").mkdir(parents=True)
    (root / "data" / "reports").mkdir(parents=True)

    (root / "config" / "search_profile.yaml").write_text(
        """
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
  target_levels: ["senior", "staff", "principal", "architect"]
  target_role_families: ["infrastructure", "analytics", "reliability"]
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
scan_profiles:
  quick:
    max_sources: 5
    validate_sources: true
    resume_enabled: false
    include_source_types: []
  deep:
    max_sources: 5
    validate_sources: true
    resume_enabled: true
    include_source_types: []
reporting:
  top_matches_target: 5
  potential_matches_target: 5
  reject_list_max: 10
  trend_lookback_scans: 3
""".strip(),
        encoding="utf-8",
    )

    (root / "config" / "sources.yaml").write_text(
        """
sources:
  - name: GoodSource
    type: greenhouse
    format: greenhouse
    enabled: true
    priority: 10
    expected_status: 200
    url: https://boards.greenhouse.io/good
  - name: BadSource
    type: greenhouse
    format: greenhouse
    enabled: true
    priority: 11
    expected_status: 200
    url: https://boards.greenhouse.io/bad
""".strip(),
        encoding="utf-8",
    )

    return root


def test_scan_survives_partial_source_failure(tmp_path: Path, monkeypatch) -> None:
    root = _prepare_tmp_project(tmp_path)

    def fake_get_json_with_meta(self, url, headers=None):
        _ = self
        _ = headers
        if "bad" in url:
            return {"jobs": []}, 404
        return {
            "jobs": [
                {
                    "id": 1,
                    "title": "Principal Infrastructure Engineer",
                    "absolute_url": "https://example.com/jobs/1",
                    "location": {"name": "Remote - United States"},
                    "content": "Compensation: $300k-$420k",
                }
            ]
        }, 200

    monkeypatch.setattr("job_scanner.http_client.HttpFetcher.get_json_with_meta", fake_get_json_with_meta)
    monkeypatch.setattr("job_scanner.http_client.HttpFetcher.get_json", lambda self, url, headers=None: fake_get_json_with_meta(self, url, headers)[0])

    config = load_app_config(root)
    result = run_scan(config, profile_name="quick")

    assert result["scored_count"] >= 1
    assert "BadSource" in result["source_errors"]

    report_path = Path(result["reports"]["latest_markdown"])
    assert report_path.exists()
    report_text = report_path.read_text(encoding="utf-8")
    assert "## Source Health" in report_text
