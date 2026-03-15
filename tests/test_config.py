from pathlib import Path

import pytest

from job_scanner.config import load_app_config, load_search_profile, load_sources


ROOT = Path(__file__).resolve().parents[1]


def test_load_search_profile_default_file() -> None:
    profile = load_search_profile(ROOT / "config" / "search_profile.yaml")
    assert profile.compensation.target_total_comp_min == 300000
    assert profile.work_preferences.remote_preferred is True
    assert profile.scan_profiles.deep.strict_source_validation is True
    assert profile.scan_profiles.deep.min_healthy_sources == 15


def test_load_sources_default_file() -> None:
    sources = load_sources(ROOT / "config" / "sources.yaml.sample")
    assert len(sources) >= 3
    assert any(source.type.value in ("greenhouse", "lever", "ashby") for source in sources)


def test_invalid_profile_raises(tmp_path: Path) -> None:
    path = tmp_path / "bad_profile.yaml"
    path.write_text("scoring_weights:\n  compensation_fit: bad\n", encoding="utf-8")

    with pytest.raises(ValueError):
        load_search_profile(path)


def test_load_app_config_uses_sample_sources_when_local_missing(tmp_path: Path) -> None:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)

    (root / "config" / "search_profile.yaml").write_text("profile_name: test\n", encoding="utf-8")
    (root / "config" / "sources.yaml.sample").write_text(
        """
sources:
  - name: Example
    type: greenhouse
    enabled: true
    url: https://boards.greenhouse.io/example
""".strip(),
        encoding="utf-8",
    )

    config = load_app_config(root)
    assert config.sources[0].name == "Example"
    assert config.sources_path.endswith("config/sources.yaml")
