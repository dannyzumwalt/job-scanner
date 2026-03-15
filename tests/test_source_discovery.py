from __future__ import annotations

import textwrap
from pathlib import Path

import yaml

from job_scanner.config import load_app_config
from job_scanner.models import SourceType, SourceValidationResult
from job_scanner.source_discovery import SourceCatalogEntry, append_discovered_sources, discover_sources


def _prepare_project(tmp_path: Path, profile_yaml: str, sources_yaml: str) -> Path:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    (root / "data" / "raw").mkdir(parents=True)
    (root / "data" / "processed").mkdir(parents=True)
    (root / "data" / "reports").mkdir(parents=True)
    (root / "config" / "search_profile.yaml").write_text(textwrap.dedent(profile_yaml).strip(), encoding="utf-8")
    (root / "config" / "sources.yaml").write_text(textwrap.dedent(sources_yaml).strip(), encoding="utf-8")
    return root


def test_discover_sources_excludes_existing_by_default(tmp_path: Path) -> None:
    root = _prepare_project(
        tmp_path,
        profile_yaml="profile_name: test",
        sources_yaml="""
        sources:
          - name: Existing Co
            type: greenhouse
            enabled: true
            url: https://boards.greenhouse.io/existingco
        """,
    )
    config = load_app_config(root)

    catalog = (
        SourceCatalogEntry(
            name="Existing Co",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="existingco",
            tags=("infrastructure", "reliability"),
        ),
        SourceCatalogEntry(
            name="New Infra Co",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="newinfraco",
            tags=("infrastructure", "reliability", "operations"),
        ),
    )

    discovered = discover_sources(
        config,
        limit=10,
        min_fit_score=0,
        validate_endpoints=False,
        include_existing=False,
        catalog=catalog,
        criteria_markdown_path=None,
    )

    names = [item.source.name for item in discovered]
    assert "Existing Co" not in names
    assert "New Infra Co" in names


def test_discovery_uses_criteria_markdown_terms(tmp_path: Path) -> None:
    root = _prepare_project(
        tmp_path,
        profile_yaml="""
        profile_name: test
        role_preferences:
          target_levels: []
          target_role_families: []
          positive_keywords: []
          negative_keywords: []
        """,
        sources_yaml="sources: []",
    )
    criteria = root / "ai-job-scan.md"
    criteria.write_text("- telecom infrastructure reliability", encoding="utf-8")
    config = load_app_config(root)

    catalog = (
        SourceCatalogEntry(
            name="Telecom Infra",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="telecominfra",
            tags=("telecom", "network", "reliability", "infrastructure"),
        ),
        SourceCatalogEntry(
            name="Marketing Board",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="marketingboard",
            tags=("marketing", "brand", "sales"),
        ),
    )

    discovered = discover_sources(
        config,
        limit=10,
        min_fit_score=0,
        validate_endpoints=False,
        include_existing=True,
        catalog=catalog,
        criteria_markdown_path=criteria,
    )

    assert discovered[0].source.name == "Telecom Infra"
    assert discovered[0].fit_score > discovered[1].fit_score


def test_discovery_validation_can_filter_healthy(monkeypatch, tmp_path: Path) -> None:
    root = _prepare_project(
        tmp_path,
        profile_yaml="profile_name: test",
        sources_yaml="sources: []",
    )
    config = load_app_config(root)

    catalog = (
        SourceCatalogEntry(
            name="Healthy",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="healthy",
            tags=("infrastructure",),
        ),
        SourceCatalogEntry(
            name="Broken",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="broken",
            tags=("infrastructure",),
        ),
    )

    def fake_validate(source, fetcher, strict=False):
        _ = fetcher
        _ = strict
        ok = source.name == "Healthy"
        return SourceValidationResult(
            source_name=source.name,
            source_type=source.type,
            endpoint=source.url,
            strict=False,
            ok=ok,
            healthy=ok,
            template_ok=True,
            schema_ok=True,
            required_fields_ok=True,
            parse_count=1 if ok else 0,
            http_status=200 if ok else 404,
            latency_ms=5,
            error=None if ok else "not found",
            error_class=None if ok else "http_status_error",
            warnings=[],
        )

    monkeypatch.setattr("job_scanner.source_discovery.validate_source", fake_validate)

    discovered = discover_sources(
        config,
        limit=10,
        min_fit_score=0,
        validate_endpoints=True,
        strict_validation=False,
        only_healthy=True,
        include_existing=True,
        catalog=catalog,
        criteria_markdown_path=None,
    )

    assert [item.source.name for item in discovered] == ["Healthy"]


def test_discovery_can_filter_by_source_type(tmp_path: Path) -> None:
    root = _prepare_project(
        tmp_path,
        profile_yaml="profile_name: test",
        sources_yaml="sources: []",
    )
    config = load_app_config(root)

    catalog = (
        SourceCatalogEntry(
            name="ATS One",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="atsone",
            tags=("infrastructure",),
        ),
        SourceCatalogEntry(
            name="Board Feed",
            source_type=SourceType.RSS,
            slug_or_url="https://example.com/jobs.rss",
            tags=("remote", "aggregator"),
        ),
    )

    discovered = discover_sources(
        config,
        limit=10,
        min_fit_score=0,
        include_types={SourceType.RSS},
        validate_endpoints=False,
        include_existing=True,
        catalog=catalog,
        criteria_markdown_path=None,
    )

    assert [item.source.name for item in discovered] == ["Board Feed"]


def test_append_discovered_sources_adds_new_and_skips_duplicates(tmp_path: Path) -> None:
    root = _prepare_project(
        tmp_path,
        profile_yaml="profile_name: test",
        sources_yaml="""
        sources:
          - name: Existing Co
            type: greenhouse
            enabled: true
            url: https://boards.greenhouse.io/existingco
            priority: 10
        """,
    )
    config = load_app_config(root)

    catalog = (
        SourceCatalogEntry(
            name="Existing Co",
            source_type=SourceType.GREENHOUSE,
            slug_or_url="existingco",
            tags=("infrastructure",),
        ),
        SourceCatalogEntry(
            name="New Co",
            source_type=SourceType.LEVER,
            slug_or_url="newco",
            tags=("infrastructure", "reliability"),
        ),
    )

    discovered = discover_sources(
        config,
        limit=10,
        min_fit_score=0,
        validate_endpoints=False,
        include_existing=True,
        catalog=catalog,
        criteria_markdown_path=None,
    )

    result = append_discovered_sources(config.sources_path, discovered, enable=False)
    assert result["added"] == 1
    assert result["skipped"] == 1

    payload = yaml.safe_load(Path(config.sources_path).read_text(encoding="utf-8"))
    names = [entry["name"] for entry in payload["sources"]]
    assert "Existing Co" in names
    assert "New Co" in names
