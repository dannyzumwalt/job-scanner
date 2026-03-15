from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .dedupe import dedupe_jobs
from .http_client import HttpFetcher
from .importer import import_file_to_jobs
from .locking import scan_lock
from .models import AppConfig, NormalizedJob, RawJob, ScanProfileSettings, SourceConfig, SourceType, SourceValidationResult
from .reporting import write_reports
from .scoring import JobScorer
from .source_validation import validate_source
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


def _resolve_scan_profile(profile: AppConfig, profile_name: str) -> ScanProfileSettings:
    if profile_name == "quick":
        return profile.profile.scan_profiles.quick
    if profile_name == "deep":
        return profile.profile.scan_profiles.deep
    raise ValueError(f"Unknown scan profile: {profile_name}")


def _filter_sources_for_profile(sources: list[SourceConfig], settings: ScanProfileSettings) -> list[SourceConfig]:
    selected = [source for source in sources if source.enabled]
    if settings.include_source_types:
        allowed = set(settings.include_source_types)
        selected = [source for source in selected if source.type in allowed]

    selected.sort(key=lambda source: (source.priority, source.name.lower()))

    if settings.max_sources is not None:
        selected = selected[: settings.max_sources]
    return selected


def _eligible_sources_without_limit(sources: list[SourceConfig], settings: ScanProfileSettings) -> list[SourceConfig]:
    selected = [source for source in sources if source.enabled]
    if settings.include_source_types:
        allowed = set(settings.include_source_types)
        selected = [source for source in selected if source.type in allowed]
    selected.sort(key=lambda source: (source.priority, source.name.lower()))
    return selected


def _collect_source_runs_payload(results: list[SourceIngestionResult], started_at: datetime) -> list[dict[str, Any]]:
    completed = datetime.now(UTC).isoformat()
    rows: list[dict[str, Any]] = []
    for result in results:
        rows.append(
            {
                "source_name": result.source.name,
                "source_type": result.source.type.value,
                "endpoint": result.endpoint,
                "status": "success" if not result.error else "failed",
                "preflight_ok": result.preflight_ok,
                "http_status": result.http_status,
                "raw_count": len(result.raw_jobs),
                "normalized_count": len(result.normalized_jobs),
                "parse_count": result.parse_count,
                "error_class": result.error_class,
                "error_message": result.error,
                "latency_ms": result.latency_ms,
                "started_at": started_at.isoformat(),
                "completed_at": completed,
            }
        )
    return rows


def _build_trend_notes(storage: Storage, current_scan_id: int, lookback_scans: int) -> list[str]:
    ids = storage.get_recent_completed_scan_ids(limit=max(lookback_scans, 2))
    if len(ids) < 2:
        return []

    current_id = current_scan_id
    if current_id not in ids:
        ids.insert(0, current_id)

    snapshots = [storage.get_market_snapshot(scan_id) for scan_id in ids[:2]]
    current = snapshots[0]
    previous = snapshots[1]

    notes: list[str] = []

    def pct(part: int, total: int) -> float:
        if total <= 0:
            return 0.0
        return round((part / total) * 100.0, 1)

    current_remote_pct = pct(current["remote_count"], current["total"])
    previous_remote_pct = pct(previous["remote_count"], previous["total"])
    delta_remote = round(current_remote_pct - previous_remote_pct, 1)
    notes.append(f"Remote share: {current_remote_pct}% ({delta_remote:+.1f} pts vs prior scan)")

    current_comp_pct = pct(current["comp_count"], current["total"])
    previous_comp_pct = pct(previous["comp_count"], previous["total"])
    delta_comp = round(current_comp_pct - previous_comp_pct, 1)
    notes.append(f"Listings with compensation: {current_comp_pct}% ({delta_comp:+.1f} pts vs prior scan)")

    delta_strong = current["strong_count"] - previous["strong_count"]
    notes.append(f"Strong-match count: {current['strong_count']} ({delta_strong:+d} vs prior scan)")

    return notes


def _build_health_gate(
    *,
    healthy_sources: int,
    total_live_sources: int,
    strict: bool,
    required_min: int,
) -> dict[str, Any]:
    effective_required_min = required_min if strict else 0
    gate_passed = healthy_sources >= effective_required_min if strict else True
    return {
        "strict": strict,
        "healthy_sources": healthy_sources,
        "total_live_sources": total_live_sources,
        "required_min": effective_required_min,
        "gate_passed": gate_passed,
    }


def _health_gate_from_validation_results(
    results: list[SourceValidationResult],
    *,
    strict: bool,
    required_min: int,
) -> dict[str, Any]:
    live_results = [item for item in results if item.source_type != SourceType.IMPORT]
    healthy = sum(1 for item in live_results if item.healthy)
    return _build_health_gate(
        healthy_sources=healthy,
        total_live_sources=len(live_results),
        strict=strict,
        required_min=required_min,
    )


def _health_gate_from_source_runs(
    source_runs: list[dict[str, Any]],
    *,
    strict: bool,
    required_min: int,
) -> dict[str, Any]:
    live_runs = [item for item in source_runs if item.get("source_type") != SourceType.IMPORT.value]
    healthy = sum(
        1
        for item in live_runs
        if item.get("status") == "success" and bool(item.get("preflight_ok", False))
    )
    return _build_health_gate(
        healthy_sources=healthy,
        total_live_sources=len(live_runs),
        strict=strict,
        required_min=required_min,
    )


def validate_sources_for_profile(
    app_config: AppConfig,
    *,
    only_enabled: bool = True,
    profile_name: str = "deep",
    strict: bool | None = None,
    min_healthy: int | None = None,
) -> dict[str, Any]:
    settings = _resolve_scan_profile(app_config, profile_name)
    candidates = _filter_sources_for_profile(app_config.sources, settings)
    if not only_enabled:
        disabled = [source for source in app_config.sources if not source.enabled]
        candidates.extend(disabled)
    strict_mode = settings.strict_source_validation if strict is None else strict
    required_min = settings.min_healthy_sources if min_healthy is None else max(0, min_healthy)

    timeout = (
        settings.request_timeout_seconds_override
        if settings.request_timeout_seconds_override is not None
        else app_config.profile.ingestion.request_timeout_seconds
    )

    fetcher = HttpFetcher(
        timeout_seconds=timeout,
        retries=app_config.profile.ingestion.request_retries,
        retry_backoff_seconds=app_config.profile.ingestion.retry_backoff_seconds,
        min_request_interval_seconds=app_config.profile.ingestion.min_request_interval_seconds,
    )
    try:
        results = [validate_source(source, fetcher, strict=strict_mode) for source in candidates]
        return {
            "results": results,
            "strict": strict_mode,
            "health_gate": _health_gate_from_validation_results(
                results,
                strict=strict_mode,
                required_min=required_min,
            ),
        }
    finally:
        fetcher.close()


def run_scan(
    app_config: AppConfig,
    *,
    generate_report: bool = True,
    profile_name: str = "deep",
    resume: bool | None = None,
) -> dict[str, Any]:
    lock_path = str(Path(app_config.processed_dir) / "scan.lock")
    with scan_lock(lock_path):
        storage = Storage(app_config.db_path)
        storage.init_db()
        storage.upsert_sources(app_config.sources)

        settings = _resolve_scan_profile(app_config, profile_name)
        sources_to_scan = _filter_sources_for_profile(app_config.sources, settings)
        full_eligible_sources = _eligible_sources_without_limit(app_config.sources, settings)

        resume_enabled = settings.resume_enabled if resume is None else resume
        resumed_skipped_sources: list[str] = []
        if resume_enabled:
            latest_failed_scan_id = storage.get_latest_failed_scan_id()
            if latest_failed_scan_id is not None:
                successful_sources = storage.get_successful_sources_for_scan(latest_failed_scan_id)
                if successful_sources:
                    resumed_skipped_sources = sorted(successful_sources)
                    sources_to_scan = [source for source in sources_to_scan if source.name not in successful_sources]

        scan_id, started_at = storage.start_scan()

        timeout_seconds = (
            settings.request_timeout_seconds_override
            if settings.request_timeout_seconds_override is not None
            else app_config.profile.ingestion.request_timeout_seconds
        )

        fetcher = HttpFetcher(
            timeout_seconds=timeout_seconds,
            retries=app_config.profile.ingestion.request_retries,
            retry_backoff_seconds=app_config.profile.ingestion.retry_backoff_seconds,
            min_request_interval_seconds=app_config.profile.ingestion.min_request_interval_seconds,
        )

        try:
            preflight_map: dict[str, SourceValidationResult] = {}
            strict_validation = settings.strict_source_validation
            if settings.validate_sources:
                for source in sources_to_scan:
                    preflight_map[source.name] = validate_source(source, fetcher, strict=strict_validation)

            ingestion_results: list[SourceIngestionResult] = ingest_sources(
                sources_to_scan,
                fetcher,
                max_workers=app_config.profile.ingestion.max_workers,
                preflight_results=preflight_map,
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

            storage.insert_source_runs(scan_id, _collect_source_runs_payload(ingestion_results, started_at))

            storage.insert_raw_jobs(scan_id, [_serialize_raw_job(job) for job in all_raw_jobs])
            raw_snapshot_path = _write_raw_snapshot(app_config.raw_dir, scan_id, all_raw_jobs)

            deduped_jobs = dedupe_jobs(all_normalized_jobs)
            stored_jobs = storage.upsert_normalized_jobs(scan_id, deduped_jobs)

            active_keys = [stored.normalized.dedupe_key for stored in stored_jobs]
            can_mark_inactive = (
                len(sources_to_scan) >= len(full_eligible_sources) and len(resumed_skipped_sources) == 0
            )
            inactive_marked = (
                storage.mark_inactive_jobs(scan_id, active_keys, ingest_mode="live") if can_mark_inactive else 0
            )

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

            source_health = storage.get_source_runs(scan_id)
            health_gate = _health_gate_from_source_runs(
                source_health,
                strict=strict_validation,
                required_min=settings.min_healthy_sources,
            )
            trend_notes = _build_trend_notes(
                storage,
                scan_id,
                lookback_scans=app_config.profile.reporting.trend_lookback_scans,
            )

            reports = {}
            if generate_report:
                scan_rows = storage.get_scored_jobs_for_scan(scan_id)
                reports = write_reports(
                    app_config.report_dir,
                    scan_id,
                    scan_rows,
                    source_health=source_health,
                    health_gate=health_gate,
                    trend_notes=trend_notes,
                    top_matches_target=app_config.profile.reporting.top_matches_target,
                    potential_matches_target=app_config.profile.reporting.potential_matches_target,
                    reject_list_max=app_config.profile.reporting.reject_list_max,
                )

            completed_at = datetime.now(UTC)
            duration_seconds = (completed_at - started_at).total_seconds()

            return {
                "scan_id": scan_id,
                "scan_profile": profile_name,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_seconds": round(duration_seconds, 2),
                "raw_count": len(all_raw_jobs),
                "normalized_count": len(deduped_jobs),
                "scored_count": len(scored_jobs),
                "inactive_marked": inactive_marked,
                "source_errors": source_errors,
                "health_gate": health_gate,
                "resumed_skipped_sources": resumed_skipped_sources,
                "raw_snapshot": raw_snapshot_path,
                "reports": reports,
            }
        except Exception as exc:
            storage.fail_scan(scan_id, str(exc))
            raise
        finally:
            fetcher.close()
            storage.close()


def run_import(
    app_config: AppConfig,
    *,
    file_path: str,
    import_format: str = "auto",
    source_name: str = "Manual Import",
    generate_report: bool = True,
) -> dict[str, Any]:
    lock_path = str(Path(app_config.processed_dir) / "scan.lock")
    with scan_lock(lock_path):
        storage = Storage(app_config.db_path)
        storage.init_db()

        scan_id, started_at = storage.start_scan()
        batch_id = storage.create_import_batch(file_path, import_format)

        try:
            raw_jobs, normalized_jobs = import_file_to_jobs(
                file_path=file_path,
                import_format=import_format,
                import_batch_id=batch_id,
                source_name=source_name,
            )

            storage.insert_raw_jobs(scan_id, [_serialize_raw_job(job) for job in raw_jobs])
            raw_snapshot_path = _write_raw_snapshot(app_config.raw_dir, scan_id, raw_jobs)

            deduped_jobs = dedupe_jobs(normalized_jobs)
            stored_jobs = storage.upsert_normalized_jobs(scan_id, deduped_jobs)

            scorer = JobScorer(app_config.profile)
            scored_jobs = [(stored, scorer.score(stored.normalized)) for stored in stored_jobs]
            storage.insert_scores(scan_id, scored_jobs)

            storage.insert_source_runs(
                scan_id,
                [
                    {
                        "source_name": source_name,
                        "source_type": "import",
                        "endpoint": f"file://{file_path}",
                        "status": "success",
                        "preflight_ok": True,
                        "http_status": 200,
                        "raw_count": len(raw_jobs),
                        "normalized_count": len(deduped_jobs),
                        "parse_count": len(deduped_jobs),
                        "error_class": None,
                        "error_message": None,
                        "latency_ms": 0,
                        "started_at": started_at.isoformat(),
                        "completed_at": datetime.now(UTC).isoformat(),
                    }
                ],
            )

            storage.complete_import_batch(batch_id, row_count=len(raw_jobs))
            storage.complete_scan(
                scan_id,
                total_raw=len(raw_jobs),
                total_normalized=len(deduped_jobs),
                total_scored=len(scored_jobs),
                inactive_marked=0,
            )

            reports = {}
            source_health = storage.get_source_runs(scan_id)
            health_gate = _health_gate_from_source_runs(
                source_health,
                strict=False,
                required_min=0,
            )
            if generate_report:
                rows = storage.get_scored_jobs_for_scan(scan_id)
                reports = write_reports(
                    app_config.report_dir,
                    scan_id,
                    rows,
                    source_health=source_health,
                    health_gate=health_gate,
                    trend_notes=_build_trend_notes(
                        storage,
                        scan_id,
                        lookback_scans=app_config.profile.reporting.trend_lookback_scans,
                    ),
                    top_matches_target=app_config.profile.reporting.top_matches_target,
                    potential_matches_target=app_config.profile.reporting.potential_matches_target,
                    reject_list_max=app_config.profile.reporting.reject_list_max,
                )

            completed_at = datetime.now(UTC)
            return {
                "scan_id": scan_id,
                "import_batch_id": batch_id,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat(),
                "duration_seconds": round((completed_at - started_at).total_seconds(), 2),
                "raw_count": len(raw_jobs),
                "normalized_count": len(deduped_jobs),
                "scored_count": len(scored_jobs),
                "raw_snapshot": raw_snapshot_path,
                "health_gate": health_gate,
                "reports": reports,
            }
        except Exception as exc:
            storage.fail_import_batch(batch_id, str(exc))
            storage.fail_scan(scan_id, str(exc))
            raise
        finally:
            storage.close()


def generate_report_for_latest_scan(app_config: AppConfig) -> dict[str, Any]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    try:
        latest_scan_id = storage.get_latest_scan_id()
        if latest_scan_id is None:
            raise RuntimeError("No completed scan found. Run `scan` first.")

        scan_rows = storage.get_scored_jobs_for_scan(latest_scan_id)
        source_health = storage.get_source_runs(latest_scan_id)
        deep_settings = app_config.profile.scan_profiles.deep
        health_gate = _health_gate_from_source_runs(
            source_health,
            strict=deep_settings.strict_source_validation,
            required_min=deep_settings.min_healthy_sources,
        )
        reports = write_reports(
            app_config.report_dir,
            latest_scan_id,
            scan_rows,
            source_health=source_health,
            health_gate=health_gate,
            trend_notes=_build_trend_notes(
                storage,
                latest_scan_id,
                lookback_scans=app_config.profile.reporting.trend_lookback_scans,
            ),
            top_matches_target=app_config.profile.reporting.top_matches_target,
            potential_matches_target=app_config.profile.reporting.potential_matches_target,
            reject_list_max=app_config.profile.reporting.reject_list_max,
        )
        return {
            "scan_id": latest_scan_id,
            "report_count": len(scan_rows),
            "health_gate": health_gate,
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


def cleanup_data(
    app_config: AppConfig,
    *,
    keep_scans: int = 8,
    keep_reports: int = 12,
) -> dict[str, int]:
    storage = Storage(app_config.db_path)
    storage.init_db()
    try:
        pruned_scans = storage.prune_scans(keep_scans=keep_scans)
    finally:
        storage.close()

    report_dir = Path(app_config.report_dir)
    report_patterns = ["report_*.md", "report_*.csv", "report_*.json"]
    deleted_reports = 0
    for pattern in report_patterns:
        files = sorted(report_dir.glob(pattern), key=lambda p: p.name, reverse=True)
        for path in files[keep_reports:]:
            path.unlink(missing_ok=True)
            deleted_reports += 1

    raw_dir = Path(app_config.raw_dir)
    raw_files = sorted(raw_dir.glob("scan_*_raw.jsonl"), key=lambda p: p.name, reverse=True)
    deleted_raw = 0
    for path in raw_files[keep_scans:]:
        path.unlink(missing_ok=True)
        deleted_raw += 1

    return {
        "pruned_scans": pruned_scans,
        "deleted_report_files": deleted_reports,
        "deleted_raw_snapshots": deleted_raw,
    }
