from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .migrations import CURRENT_SCHEMA_VERSION, migrate
from .models import MatchCategory, NormalizedJob, ScoreResult, ScanDiff, SourceConfig


@dataclass
class StoredJob:
    job_id: int
    normalized: NormalizedJob
    is_new: bool


class Storage:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.conn.close()

    def init_db(self) -> None:
        migrate(self.conn)

    def schema_version(self) -> int:
        row = self.conn.execute("SELECT MAX(version) AS version FROM schema_migrations").fetchone()
        if not row or row["version"] is None:
            return 0
        return int(row["version"])

    def schema_is_current(self) -> bool:
        return self.schema_version() >= CURRENT_SCHEMA_VERSION

    def upsert_sources(self, sources: list[SourceConfig]) -> None:
        now = datetime.now(UTC).isoformat()
        for source in sources:
            self.conn.execute(
                """
                INSERT INTO sources (
                    name, type, url, api_url, format, parser_template_json, priority, expected_status,
                    enabled, notes, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    type = excluded.type,
                    url = excluded.url,
                    api_url = excluded.api_url,
                    format = excluded.format,
                    parser_template_json = excluded.parser_template_json,
                    priority = excluded.priority,
                    expected_status = excluded.expected_status,
                    enabled = excluded.enabled,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    source.name,
                    source.type.value,
                    source.url,
                    source.api_url,
                    source.format.value,
                    json.dumps(source.parser_template or {}, sort_keys=True),
                    source.priority,
                    source.expected_status,
                    int(source.enabled),
                    source.notes,
                    now,
                    now,
                ),
            )
        self.conn.commit()

    def update_source_fetch_status(self, source_name: str, status: str, error: str | None = None) -> None:
        now = datetime.now(UTC).isoformat()
        self.conn.execute(
            """
            UPDATE sources
               SET last_status = ?,
                   last_error = ?,
                   last_fetched_at = ?,
                   updated_at = ?
             WHERE name = ?
            """,
            (status, error, now, now, source_name),
        )
        self.conn.commit()

    def start_scan(self) -> tuple[int, datetime]:
        started_at = datetime.now(UTC)
        cur = self.conn.execute(
            "INSERT INTO scans (started_at, status) VALUES (?, ?)",
            (started_at.isoformat(), "running"),
        )
        self.conn.commit()
        return int(cur.lastrowid), started_at

    def complete_scan(
        self,
        scan_id: int,
        *,
        total_raw: int,
        total_normalized: int,
        total_scored: int,
        inactive_marked: int,
    ) -> None:
        completed_at = datetime.now(UTC).isoformat()
        self.conn.execute(
            """
            UPDATE scans
               SET completed_at = ?,
                   status = ?,
                   total_raw = ?,
                   total_normalized = ?,
                   total_scored = ?,
                   inactive_marked = ?
             WHERE id = ?
            """,
            (completed_at, "completed", total_raw, total_normalized, total_scored, inactive_marked, scan_id),
        )
        self.conn.commit()

    def fail_scan(self, scan_id: int, error: str) -> None:
        completed_at = datetime.now(UTC).isoformat()
        self.conn.execute(
            "UPDATE scans SET completed_at = ?, status = ?, error = ? WHERE id = ?",
            (completed_at, "failed", error, scan_id),
        )
        self.conn.commit()

    def get_latest_failed_scan_id(self) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM scans WHERE status = 'failed' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return int(row["id"]) if row else None

    def get_successful_sources_for_scan(self, scan_id: int) -> set[str]:
        rows = self.conn.execute(
            "SELECT source_name FROM source_runs WHERE scan_id = ? AND status = 'success'",
            (scan_id,),
        ).fetchall()
        return {str(row["source_name"]) for row in rows}

    def insert_source_runs(self, scan_id: int, source_runs: list[dict[str, Any]]) -> None:
        if not source_runs:
            return
        rows = [
            (
                scan_id,
                item["source_name"],
                item["source_type"],
                item["endpoint"],
                item["status"],
                int(item.get("preflight_ok", False)),
                item.get("http_status"),
                int(item.get("raw_count", 0)),
                int(item.get("normalized_count", 0)),
                int(item.get("parse_count", 0)),
                item.get("error_class"),
                item.get("error_message"),
                int(item.get("latency_ms", 0)),
                item["started_at"],
                item["completed_at"],
            )
            for item in source_runs
        ]
        self.conn.executemany(
            """
            INSERT INTO source_runs (
                scan_id, source_name, source_type, endpoint, status, preflight_ok, http_status,
                raw_count, normalized_count, parse_count, error_class, error_message, latency_ms,
                started_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def get_source_runs(self, scan_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT source_name, source_type, endpoint, status, preflight_ok, http_status,
                   raw_count, normalized_count, parse_count, error_class, error_message, latency_ms,
                   started_at, completed_at
              FROM source_runs
             WHERE scan_id = ?
             ORDER BY source_name ASC
            """,
            (scan_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def insert_raw_jobs(self, scan_id: int, raw_jobs: list[dict[str, Any]]) -> None:
        if not raw_jobs:
            return
        rows = [
            (
                scan_id,
                job["source_name"],
                job["source_type"],
                job["source_url"],
                job["source_job_id"],
                json.dumps(job["payload"], sort_keys=True, default=str),
                job["fetched_at"],
            )
            for job in raw_jobs
        ]
        self.conn.executemany(
            """
            INSERT INTO raw_jobs (
                scan_id, source_name, source_type, source_url, source_job_id, payload_json, fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

    def upsert_normalized_jobs(self, scan_id: int, jobs: list[NormalizedJob]) -> list[StoredJob]:
        now = datetime.now(UTC).isoformat()
        stored: list[StoredJob] = []

        if not jobs:
            return stored

        dedupe_keys = [job.dedupe_key for job in jobs]
        placeholders = ",".join(["?"] * len(dedupe_keys))
        existing_rows = self.conn.execute(
            f"SELECT id, dedupe_key FROM normalized_jobs WHERE dedupe_key IN ({placeholders})",
            dedupe_keys,
        ).fetchall()
        existing_map = {row["dedupe_key"]: int(row["id"]) for row in existing_rows}

        for job in jobs:
            existing_id = existing_map.get(job.dedupe_key)

            common_payload = (
                job.source_name,
                job.source_type.value,
                job.source_job_id,
                job.source_url,
                job.requisition_id,
                job.apply_url,
                job.company,
                job.title,
                job.normalized_title,
                job.description,
                job.location,
                job.normalized_location,
                job.country,
                int(job.is_remote),
                int(job.is_hybrid),
                int(job.is_onsite),
                int(job.dfw_match),
                int(job.us_match),
                int(job.travel_required),
                job.travel_percent,
                job.base_min,
                job.base_max,
                job.bonus,
                job.equity,
                job.estimated_total_comp_min,
                job.estimated_total_comp_max,
                job.compensation_confidence,
                json.dumps(job.role_family_tags),
                json.dumps(job.seniority_hints),
                job.duplicate_count,
                job.job_hash,
                job.ingest_mode,
                job.import_batch_id,
                json.dumps(job.data_quality_flags),
                job.parse_confidence,
            )

            if existing_id is not None:
                self.conn.execute(
                    """
                    UPDATE normalized_jobs
                       SET source_name = ?,
                           source_type = ?,
                           source_job_id = ?,
                           source_url = ?,
                           requisition_id = ?,
                           apply_url = ?,
                           company = ?,
                           title = ?,
                           normalized_title = ?,
                           description = ?,
                           location = ?,
                           normalized_location = ?,
                           country = ?,
                           is_remote = ?,
                           is_hybrid = ?,
                           is_onsite = ?,
                           dfw_match = ?,
                           us_match = ?,
                           travel_required = ?,
                           travel_percent = ?,
                           base_min = ?,
                           base_max = ?,
                           bonus = ?,
                           equity = ?,
                           estimated_total_comp_min = ?,
                           estimated_total_comp_max = ?,
                           compensation_confidence = ?,
                           role_family_tags_json = ?,
                           seniority_hints_json = ?,
                           duplicate_count = ?,
                           job_hash = ?,
                           ingest_mode = ?,
                           import_batch_id = ?,
                           data_quality_flags_json = ?,
                           parse_confidence = ?,
                           last_seen = ?,
                           is_active = ?,
                           last_scan_id = ?
                     WHERE dedupe_key = ?
                    """,
                    (*common_payload, now, 1, scan_id, job.dedupe_key),
                )
                stored.append(StoredJob(job_id=existing_id, normalized=job, is_new=False))
            else:
                self.conn.execute(
                    """
                    INSERT INTO normalized_jobs (
                        source_name, source_type, source_job_id, source_url, requisition_id, apply_url,
                        company, title, normalized_title, description, location, normalized_location, country,
                        is_remote, is_hybrid, is_onsite, dfw_match, us_match, travel_required, travel_percent,
                        base_min, base_max, bonus, equity, estimated_total_comp_min, estimated_total_comp_max,
                        compensation_confidence, role_family_tags_json, seniority_hints_json, duplicate_count,
                        job_hash, ingest_mode, import_batch_id, data_quality_flags_json, parse_confidence,
                        first_seen, last_seen, is_active, last_scan_id, dedupe_key
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?
                    )
                    """,
                    (*common_payload, now, now, 1, scan_id, job.dedupe_key),
                )
                job_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                stored.append(StoredJob(job_id=job_id, normalized=job, is_new=True))

        self.conn.commit()
        return stored

    def mark_inactive_jobs(self, scan_id: int, active_dedupe_keys: list[str], ingest_mode: str = "live") -> int:
        placeholders = ",".join(["?"] * len(active_dedupe_keys))
        if active_dedupe_keys:
            query = f"""
                UPDATE normalized_jobs
                   SET is_active = 0,
                       last_scan_id = ?
                 WHERE is_active = 1
                   AND ingest_mode = ?
                   AND dedupe_key NOT IN ({placeholders})
            """
            args = [scan_id, ingest_mode, *active_dedupe_keys]
        else:
            query = (
                "UPDATE normalized_jobs SET is_active = 0, last_scan_id = ? "
                "WHERE is_active = 1 AND ingest_mode = ?"
            )
            args = [scan_id, ingest_mode]

        cur = self.conn.execute(query, args)
        self.conn.commit()
        return cur.rowcount

    def insert_scores(self, scan_id: int, scored_jobs: list[tuple[StoredJob, ScoreResult]]) -> None:
        now = datetime.now(UTC).isoformat()

        self.conn.execute("DELETE FROM score_results WHERE scan_id = ?", (scan_id,))
        self.conn.execute("DELETE FROM scan_jobs WHERE scan_id = ?", (scan_id,))

        score_rows = []
        snapshot_rows = []
        for stored, score in scored_jobs:
            score_rows.append(
                (
                    scan_id,
                    stored.job_id,
                    score.total_score,
                    score.display_score,
                    score.category.value,
                    score.recommended_action,
                    json.dumps(score.dimension_scores, sort_keys=True),
                    json.dumps(score.reasons),
                    json.dumps(score.concerns),
                    now,
                )
            )
            snapshot_rows.append(
                (
                    scan_id,
                    stored.job_id,
                    score.total_score,
                    score.category.value,
                    score.recommended_action,
                    stored.normalized.job_hash,
                    int(stored.is_new),
                )
            )

        self.conn.executemany(
            """
            INSERT INTO score_results (
                scan_id, normalized_job_id, total_score, display_score, category, recommended_action,
                dimension_scores_json, reasons_json, concerns_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            score_rows,
        )

        self.conn.executemany(
            """
            INSERT INTO scan_jobs (
                scan_id, normalized_job_id, total_score, category, recommended_action, job_hash, is_new
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            snapshot_rows,
        )

        self.conn.commit()

    def create_import_batch(self, source_file: str, import_format: str) -> int:
        now = datetime.now(UTC).isoformat()
        cur = self.conn.execute(
            """
            INSERT INTO import_batches (created_at, source_file, import_format, status)
            VALUES (?, ?, ?, ?)
            """,
            (now, source_file, import_format, "running"),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def complete_import_batch(self, batch_id: int, row_count: int) -> None:
        now = datetime.now(UTC).isoformat()
        self.conn.execute(
            "UPDATE import_batches SET completed_at = ?, row_count = ?, status = ? WHERE id = ?",
            (now, row_count, "completed", batch_id),
        )
        self.conn.commit()

    def fail_import_batch(self, batch_id: int, error: str) -> None:
        now = datetime.now(UTC).isoformat()
        self.conn.execute(
            "UPDATE import_batches SET completed_at = ?, status = ?, error = ? WHERE id = ?",
            (now, "failed", error, batch_id),
        )
        self.conn.commit()

    def get_latest_scan_id(self) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM scans WHERE status = 'completed' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return int(row["id"]) if row else None

    def get_previous_scan_id(self, scan_id: int) -> int | None:
        row = self.conn.execute(
            "SELECT id FROM scans WHERE status = 'completed' AND id < ? ORDER BY id DESC LIMIT 1",
            (scan_id,),
        ).fetchone()
        return int(row["id"]) if row else None

    def resolve_baseline_scan_id(self, current_scan_id: int, since: str) -> int | None:
        if since == "last":
            return self.get_previous_scan_id(current_scan_id)

        try:
            timestamp = datetime.fromisoformat(since)
        except ValueError:
            raise ValueError("--since must be 'last' or an ISO timestamp")

        row = self.conn.execute(
            """
            SELECT id
              FROM scans
             WHERE status = 'completed'
               AND started_at <= ?
               AND id < ?
             ORDER BY started_at DESC
             LIMIT 1
            """,
            (timestamp.isoformat(), current_scan_id),
        ).fetchone()
        return int(row["id"]) if row else None

    def get_scored_jobs_for_scan(self, scan_id: int) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
                nj.id AS normalized_job_id,
                nj.source_name,
                nj.source_type,
                nj.source_job_id,
                nj.source_url,
                nj.requisition_id,
                nj.apply_url,
                nj.company,
                nj.title,
                nj.normalized_title,
                nj.description,
                nj.location,
                nj.normalized_location,
                nj.country,
                nj.is_remote,
                nj.is_hybrid,
                nj.is_onsite,
                nj.dfw_match,
                nj.us_match,
                nj.travel_required,
                nj.travel_percent,
                nj.base_min,
                nj.base_max,
                nj.bonus,
                nj.equity,
                nj.estimated_total_comp_min,
                nj.estimated_total_comp_max,
                nj.compensation_confidence,
                nj.role_family_tags_json,
                nj.seniority_hints_json,
                nj.ingest_mode,
                nj.import_batch_id,
                nj.data_quality_flags_json,
                nj.parse_confidence,
                nj.duplicate_count,
                nj.job_hash,
                nj.first_seen,
                nj.last_seen,
                nj.is_active,
                sr.total_score,
                sr.display_score,
                sr.category,
                sr.recommended_action,
                sr.dimension_scores_json,
                sr.reasons_json,
                sr.concerns_json,
                sj.is_new
            FROM score_results sr
            JOIN normalized_jobs nj ON nj.id = sr.normalized_job_id
            JOIN scan_jobs sj ON sj.normalized_job_id = sr.normalized_job_id AND sj.scan_id = sr.scan_id
            WHERE sr.scan_id = ?
            ORDER BY sr.total_score DESC, nj.company ASC
            """,
            (scan_id,),
        ).fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["role_family_tags"] = json.loads(item.pop("role_family_tags_json") or "[]")
            item["seniority_hints"] = json.loads(item.pop("seniority_hints_json") or "[]")
            item["dimension_scores"] = json.loads(item.pop("dimension_scores_json") or "{}")
            item["reasons"] = json.loads(item.pop("reasons_json") or "[]")
            item["concerns"] = json.loads(item.pop("concerns_json") or "[]")
            item["data_quality_flags"] = json.loads(item.pop("data_quality_flags_json") or "[]")
            item["category"] = MatchCategory(item["category"])
            item["is_new"] = bool(item["is_new"])
            item["is_remote"] = bool(item["is_remote"])
            item["is_hybrid"] = bool(item["is_hybrid"])
            item["is_onsite"] = bool(item["is_onsite"])
            item["dfw_match"] = bool(item["dfw_match"])
            item["us_match"] = bool(item["us_match"])
            item["travel_required"] = bool(item["travel_required"])
            item["is_active"] = bool(item["is_active"])
            result.append(item)

        return result

    def list_top_jobs(
        self,
        limit: int = 25,
        scan_id: int | None = None,
        min_score: float | None = None,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        selected_scan_id = scan_id or self.get_latest_scan_id()
        if selected_scan_id is None:
            return []
        jobs = self.get_scored_jobs_for_scan(selected_scan_id)
        if min_score is not None:
            jobs = [j for j in jobs if j["display_score"] >= min_score]
        if category is not None:
            jobs = [j for j in jobs if j["category"].value == category]
        return jobs[:limit]

    def get_scan_diff(self, current_scan_id: int, baseline_scan_id: int) -> ScanDiff:
        current_rows = self.conn.execute(
            """
            SELECT sj.normalized_job_id, sj.total_score, sj.job_hash, nj.title, nj.company, nj.apply_url
              FROM scan_jobs sj
              JOIN normalized_jobs nj ON nj.id = sj.normalized_job_id
             WHERE sj.scan_id = ?
            """,
            (current_scan_id,),
        ).fetchall()
        baseline_rows = self.conn.execute(
            """
            SELECT sj.normalized_job_id, sj.total_score, sj.job_hash, nj.title, nj.company, nj.apply_url
              FROM scan_jobs sj
              JOIN normalized_jobs nj ON nj.id = sj.normalized_job_id
             WHERE sj.scan_id = ?
            """,
            (baseline_scan_id,),
        ).fetchall()

        current_map = {int(row["normalized_job_id"]): row for row in current_rows}
        baseline_map = {int(row["normalized_job_id"]): row for row in baseline_rows}

        new_ids = sorted(set(current_map) - set(baseline_map))
        removed_ids = sorted(set(baseline_map) - set(current_map))
        shared_ids = sorted(set(current_map).intersection(set(baseline_map)))

        changed: list[dict[str, Any]] = []
        for job_id in shared_ids:
            curr = current_map[job_id]
            base = baseline_map[job_id]
            score_delta = float(curr["total_score"]) - float(base["total_score"])
            if curr["job_hash"] != base["job_hash"] or abs(score_delta) >= 10.0:
                changed.append(
                    {
                        "normalized_job_id": job_id,
                        "company": curr["company"],
                        "title": curr["title"],
                        "apply_url": curr["apply_url"],
                        "score_delta": round(score_delta, 2),
                    }
                )

        return ScanDiff(
            current_scan_id=current_scan_id,
            baseline_scan_id=baseline_scan_id,
            new_jobs=[
                {
                    "normalized_job_id": job_id,
                    "company": current_map[job_id]["company"],
                    "title": current_map[job_id]["title"],
                    "apply_url": current_map[job_id]["apply_url"],
                }
                for job_id in new_ids
            ],
            removed_jobs=[
                {
                    "normalized_job_id": job_id,
                    "company": baseline_map[job_id]["company"],
                    "title": baseline_map[job_id]["title"],
                    "apply_url": baseline_map[job_id]["apply_url"],
                }
                for job_id in removed_ids
            ],
            changed_jobs=changed,
        )

    def get_recent_completed_scan_ids(self, limit: int = 5) -> list[int]:
        rows = self.conn.execute(
            "SELECT id FROM scans WHERE status = 'completed' ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [int(row["id"]) for row in rows]

    def get_market_snapshot(self, scan_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN nj.is_remote = 1 THEN 1 ELSE 0 END) AS remote_count,
                SUM(CASE WHEN nj.dfw_match = 1 THEN 1 ELSE 0 END) AS dfw_count,
                SUM(CASE WHEN nj.estimated_total_comp_min IS NOT NULL OR nj.estimated_total_comp_max IS NOT NULL THEN 1 ELSE 0 END) AS comp_count,
                SUM(CASE WHEN sr.category IN ('strong', 'good') THEN 1 ELSE 0 END) AS strong_count
            FROM score_results sr
            JOIN normalized_jobs nj ON nj.id = sr.normalized_job_id
            WHERE sr.scan_id = ?
            """,
            (scan_id,),
        ).fetchone()
        if not row:
            return {
                "scan_id": scan_id,
                "total": 0,
                "remote_count": 0,
                "dfw_count": 0,
                "comp_count": 0,
                "strong_count": 0,
            }
        return {
            "scan_id": scan_id,
            "total": int(row["total"] or 0),
            "remote_count": int(row["remote_count"] or 0),
            "dfw_count": int(row["dfw_count"] or 0),
            "comp_count": int(row["comp_count"] or 0),
            "strong_count": int(row["strong_count"] or 0),
        }

    def get_scan_summary(self, scan_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM scans WHERE id = ?",
            (scan_id,),
        ).fetchone()
        return dict(row) if row else {}

    def prune_scans(self, keep_scans: int) -> int:
        if keep_scans < 1:
            raise ValueError("keep_scans must be >= 1")

        rows = self.conn.execute(
            "SELECT id FROM scans WHERE status = 'completed' ORDER BY id DESC"
        ).fetchall()
        keep_ids = [int(row["id"]) for row in rows[:keep_scans]]
        prune_ids = [int(row["id"]) for row in rows[keep_scans:]]

        if not prune_ids:
            return 0

        placeholders = ",".join("?" for _ in prune_ids)
        self.conn.execute(f"DELETE FROM scans WHERE id IN ({placeholders})", prune_ids)
        self.conn.commit()
        return len(prune_ids)
