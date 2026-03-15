from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from .models import AppConfig, SearchProfile, SourcesFile


def _read_yaml(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping object: {path}")
    return data


def load_search_profile(path: str | Path) -> SearchProfile:
    source = Path(path)
    raw = _read_yaml(source)
    try:
        return SearchProfile.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid search profile config at {source}: {exc}") from exc


def load_sources(path: str | Path):
    source = Path(path)
    raw = _read_yaml(source)
    try:
        parsed = SourcesFile.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"Invalid sources config at {source}: {exc}") from exc
    return parsed.sources


def load_app_config(root_dir: str | Path | None = None) -> AppConfig:
    root = Path(root_dir or os.getcwd()).resolve()

    profile_path = Path(os.getenv("JOB_SCANNER_PROFILE_PATH", root / "config" / "search_profile.yaml"))
    default_sources_path = root / "config" / "sources.yaml"
    sample_sources_path = root / "config" / "sources.yaml.sample"
    env_sources_path = os.getenv("JOB_SCANNER_SOURCES_PATH")
    if env_sources_path:
        load_sources_path = Path(env_sources_path)
        configured_sources_path = load_sources_path
    else:
        configured_sources_path = default_sources_path
        if default_sources_path.exists():
            load_sources_path = default_sources_path
        elif sample_sources_path.exists():
            load_sources_path = sample_sources_path
        else:
            load_sources_path = default_sources_path
    db_path = Path(os.getenv("JOB_SCANNER_DB_PATH", root / "data" / "processed" / "job_scanner.db"))
    report_dir = Path(os.getenv("JOB_SCANNER_REPORT_DIR", root / "data" / "reports"))
    raw_dir = Path(root / "data" / "raw")
    processed_dir = Path(root / "data" / "processed")

    report_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    profile = load_search_profile(profile_path)
    sources = load_sources(load_sources_path)

    return AppConfig(
        root_dir=str(root),
        db_path=str(db_path),
        search_profile_path=str(profile_path),
        sources_path=str(configured_sources_path),
        report_dir=str(report_dir),
        raw_dir=str(raw_dir),
        processed_dir=str(processed_dir),
        profile=profile,
        sources=sources,
    )
