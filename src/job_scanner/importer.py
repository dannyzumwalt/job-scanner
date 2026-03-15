from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .models import NormalizedJob, RawJob, SourceConfig, SourceFormat, SourceType
from .sources.common import build_normalized_job
from .utils import compact_whitespace


def _pick(row: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return compact_whitespace(str(row[key]))
    return ""


def _normalize_row_to_job(
    row: dict[str, Any],
    source: SourceConfig,
    import_batch_id: int,
    row_index: int,
) -> tuple[RawJob, NormalizedJob] | None:
    title = _pick(row, ["title", "job_title", "position"])
    if not title:
        return None

    company = _pick(row, ["company", "company_name", "employer"]) or source.name
    location = _pick(row, ["location", "city", "region", "work_location"])
    description = _pick(row, ["description", "summary", "details"])
    apply_url = _pick(row, ["apply_url", "url", "job_url", "link"])
    requisition_id = _pick(row, ["requisition_id", "job_id", "id", "external_id"])
    salary_text = _pick(row, ["salary", "compensation", "salary_range", "pay_range"])
    base_min = _pick(row, ["base_min", "salary_min", "min_salary", "base_salary_min"])
    base_max = _pick(row, ["base_max", "salary_max", "max_salary", "base_salary_max"])
    bonus = _pick(row, ["bonus", "target_bonus", "annual_bonus"])
    bonus_percent = _pick(row, ["bonus_percent", "target_bonus_percent"])
    equity = _pick(row, ["equity", "equity_value", "rsu", "stock"])

    source_job_id = requisition_id or apply_url or f"import-row-{row_index}"

    raw = RawJob(
        source_name=source.name,
        source_type=source.type,
        source_url=source.url,
        source_job_id=source_job_id,
        payload=row,
    )

    quality_flags: list[str] = []
    if not description:
        quality_flags.append("description_missing")
    if not location:
        quality_flags.append("location_missing")

    normalized = build_normalized_job(
        source,
        source_job_id=source_job_id,
        title=title,
        description=description,
        location=location,
        apply_url=apply_url or None,
        requisition_id=requisition_id or None,
        company=company,
        salary_text=salary_text or None,
        base_min_hint=base_min or None,
        base_max_hint=base_max or None,
        bonus_hint=bonus or None,
        bonus_percent_hint=bonus_percent or None,
        equity_hint=equity or None,
        ingest_mode="import",
        import_batch_id=import_batch_id,
        data_quality_flags=quality_flags,
        parse_confidence=0.8,
        raw_payload=row,
    )

    return raw, normalized


def _read_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("jobs", "items", "results", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    raise ValueError("JSON import file must be a list of objects or contain jobs/items/results/data list")


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        return [dict(row) for row in reader]


def import_file_to_jobs(
    file_path: str,
    import_format: str,
    import_batch_id: int,
    source_name: str = "Manual Import",
) -> tuple[list[RawJob], list[NormalizedJob]]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Import file not found: {path}")

    normalized_format = import_format.lower().strip()
    if normalized_format == "auto":
        if path.suffix.lower() in {".csv"}:
            normalized_format = "csv"
        elif path.suffix.lower() in {".json"}:
            normalized_format = "json"
        else:
            raise ValueError("Could not infer import format; use --format csv|json")

    if normalized_format == "csv":
        rows = _read_csv(path)
    elif normalized_format == "json":
        rows = _read_json(path)
    else:
        raise ValueError("Import format must be csv, json, or auto")

    source = SourceConfig(
        name=source_name,
        type=SourceType.IMPORT,
        enabled=True,
        url=f"file://{path}",
        api_url=None,
        format=SourceFormat.JSON,
        parser_template={},
        priority=0,
        expected_status=200,
        notes="Manual import source",
    )

    raw_jobs: list[RawJob] = []
    normalized_jobs: list[NormalizedJob] = []

    for idx, row in enumerate(rows, start=1):
        converted = _normalize_row_to_job(row, source, import_batch_id, idx)
        if converted is None:
            continue
        raw, normalized = converted
        raw_jobs.append(raw)
        normalized_jobs.append(normalized)

    return raw_jobs, normalized_jobs
