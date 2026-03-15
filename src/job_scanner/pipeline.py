from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .dedupe import dedupe_jobs
from .http_client import HttpFetcher
from .models import AppConfig, NormalizedJob, RawJob
from .reporting import write_reports
from .scoring import JobScorer
from .sources import SourceIngestionResult, ingest_sources
from .storage import Storage


def _serialize_raw_job(job: RawJob) -> dict[str, Any]:
    return {
        "source_name": job.source_name,
        "source_type": job.source_type.value,
        "source_url": job.source_url,
        "source_job_id": job.source_job_id,
        "payload": job.payload,
        "fetched_at": job.fetched_at.isoformat(),
    }


def _write_raw_snapshot(raw_dir: str, scan_id: int, jobs: list[RawJob]) -> str:
    path = Path(raw_dir) / f"scan_{scan_id}_raw.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for job in jobs:
            fh.write(json.dumps(_serialize_raw_job(job), sort_keys=True, default=str) + "\n")
    return str(path)


def run_scan(app_config: AppConfig, *, generate_report: bool = True) -> dict[str, Any]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    storage.upsert_sources(app_config.sources)

    scan_id, started_at = storage.start_scan()

    fetcher = HttpFetcher(
        timeout_seconds=app_config.profile.ingestion.request_timeout_seconds,
        retries=app_config.profile.ingestion.request_retries,
        retry_backoff_seconds=app_config.profile.ingestion.retry_backoff_seconds,
        min_request_interval_seconds=app_config.profile.ingestion.min_request_interval_seconds,
    )

    try:
        ingestion_results: list[SourceIngestionResult] = ingest_sources(
            app_config.sources,
            fetcher,
            max_workers=app_config.profile.ingestion.max_workers,
        )

        all_raw_jobs: list[RawJob] = []
        all_normalized_jobs: list[NormalizedJob] = []
        source_errors: dict[str, str] = {}

        for result in ingestion_results:
            all_raw_jobs.extend(result.raw_jobs)
            all_normalized_jobs.extend(result.normalized_jobs)
            if result.error:
                storage.update_source_fetch_status(result.source.name, "error", result.error)
                source_errors[result.source.name] = result.error
            else:
                storage.update_source_fetch_status(result.source.name, "ok", None)

        storage.insert_raw_jobs(scan_id, [_serialize_raw_job(job) for job in all_raw_jobs])
        raw_snapshot_path = _write_raw_snapshot(app_config.raw_dir, scan_id, all_raw_jobs)

        deduped_jobs = dedupe_jobs(all_normalized_jobs)
        stored_jobs = storage.upsert_normalized_jobs(scan_id, deduped_jobs)

        active_keys = [stored.normalized.dedupe_key for stored in stored_jobs]
        inactive_marked = storage.mark_inactive_jobs(scan_id, active_keys)

        scorer = JobScorer(app_config.profile)
        scored_jobs = [(stored, scorer.score(stored.normalized)) for stored in stored_jobs]
        storage.insert_scores(scan_id, scored_jobs)

        storage.complete_scan(
            scan_id,
            total_raw=len(all_raw_jobs),
            total_normalized=len(deduped_jobs),
            total_scored=len(scored_jobs),
            inactive_marked=inactive_marked,
        )

        reports = {}
        if generate_report:
            scan_rows = storage.get_scored_jobs_for_scan(scan_id)
            reports = write_reports(app_config.report_dir, scan_id, scan_rows)

        completed_at = datetime.now(UTC)
        duration_seconds = (completed_at - started_at).total_seconds()

        return {
            "scan_id": scan_id,
            "started_at": started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_seconds": round(duration_seconds, 2),
            "raw_count": len(all_raw_jobs),
            "normalized_count": len(deduped_jobs),
            "scored_count": len(scored_jobs),
            "inactive_marked": inactive_marked,
            "source_errors": source_errors,
            "raw_snapshot": raw_snapshot_path,
            "reports": reports,
        }
    except Exception as exc:
        storage.fail_scan(scan_id, str(exc))
        raise
    finally:
        fetcher.close()
        storage.close()


def generate_report_for_latest_scan(app_config: AppConfig) -> dict[str, Any]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    try:
        latest_scan_id = storage.get_latest_scan_id()
        if latest_scan_id is None:
            raise RuntimeError("No completed scan found. Run `scan` first.")

        scan_rows = storage.get_scored_jobs_for_scan(latest_scan_id)
        reports = write_reports(app_config.report_dir, latest_scan_id, scan_rows)
        return {
            "scan_id": latest_scan_id,
            "report_count": len(scan_rows),
            "reports": reports,
        }
    finally:
        storage.close()


def list_top_jobs(app_config: AppConfig, *, top_n: int = 25) -> list[dict[str, Any]]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    try:
        return storage.list_top_jobs(limit=top_n)
    finally:
        storage.close()


def diff_latest_scan(app_config: AppConfig, *, since: str = "last") -> dict[str, Any]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    try:
        latest_scan_id = storage.get_latest_scan_id()
        if latest_scan_id is None:
            raise RuntimeError("No completed scan found. Run `scan` first.")

        baseline_scan_id = storage.resolve_baseline_scan_id(latest_scan_id, since)
        if baseline_scan_id is None:
            raise RuntimeError("Could not find a baseline scan to compare against")

        diff = storage.get_scan_diff(latest_scan_id, baseline_scan_id)
        return diff.model_dump()
    finally:
        storage.close()
