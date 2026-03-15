from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

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
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL,
                url TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                notes TEXT,
                last_status TEXT,
                last_error TEXT,
                last_fetched_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                total_raw INTEGER NOT NULL DEFAULT 0,
                total_normalized INTEGER NOT NULL DEFAULT 0,
                total_scored INTEGER NOT NULL DEFAULT 0,
                inactive_marked INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );

            CREATE TABLE IF NOT EXISTS raw_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT NOT NULL,
                source_job_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS normalized_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT UNIQUE NOT NULL,
                source_name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_job_id TEXT NOT NULL,
                source_url TEXT NOT NULL,
                requisition_id TEXT,
                apply_url TEXT,
                company TEXT NOT NULL,
                title TEXT NOT NULL,
                normalized_title TEXT NOT NULL,
                description TEXT NOT NULL,
                location TEXT,
                normalized_location TEXT,
                country TEXT,
                is_remote INTEGER NOT NULL,
                is_hybrid INTEGER NOT NULL,
                is_onsite INTEGER NOT NULL,
                dfw_match INTEGER NOT NULL,
                us_match INTEGER NOT NULL,
                travel_required INTEGER NOT NULL,
                travel_percent INTEGER,
                base_min INTEGER,
                base_max INTEGER,
                bonus INTEGER,
                equity INTEGER,
                estimated_total_comp_min INTEGER,
                estimated_total_comp_max INTEGER,
                compensation_confidence REAL NOT NULL,
                role_family_tags_json TEXT NOT NULL,
                seniority_hints_json TEXT NOT NULL,
                duplicate_count INTEGER NOT NULL DEFAULT 1,
                job_hash TEXT NOT NULL,
                first_seen TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_scan_id INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS score_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_id INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
                normalized_job_id INTEGER NOT NULL REFERENCES normalized_jobs(id) ON DELETE CASCADE,
                total_score REAL NOT NULL,
                display_score REAL NOT NULL,
                category TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                dimension_scores_json TEXT NOT NULL,
                reasons_json TEXT NOT NULL,
                concerns_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scan_jobs (
                scan_id INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
                normalized_job_id INTEGER NOT NULL REFERENCES normalized_jobs(id) ON DELETE CASCADE,
                total_score REAL NOT NULL,
                category TEXT NOT NULL,
                recommended_action TEXT NOT NULL,
                job_hash TEXT NOT NULL,
                is_new INTEGER NOT NULL,
                PRIMARY KEY (scan_id, normalized_job_id)
            );
            """
        )
        self.conn.commit()

    def upsert_sources(self, sources: list[SourceConfig]) -> None:
        now = datetime.now(UTC).isoformat()
        for source in sources:
            self.conn.execute(
                """
                INSERT INTO sources (name, type, url, enabled, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    type = excluded.type,
                    url = excluded.url,
                    enabled = excluded.enabled,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (source.name, source.type.value, source.url, int(source.enabled), source.notes, now, now),
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

        for job in jobs:
            existing = self.conn.execute(
                "SELECT id, first_seen FROM normalized_jobs WHERE dedupe_key = ?",
                (job.dedupe_key,),
            ).fetchone()

            update_payload = (
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
                now,
                1,
                scan_id,
                job.dedupe_key,
            )

            if existing:
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
                           last_seen = ?,
                           is_active = ?,
                           last_scan_id = ?
                     WHERE dedupe_key = ?
                    """,
                    update_payload,
                )
                stored.append(StoredJob(job_id=int(existing["id"]), normalized=job, is_new=False))
            else:
                insert_payload = (
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
                    now,
                    now,
                    1,
                    scan_id,
                    job.dedupe_key,
                )
                self.conn.execute(
                    """
                    INSERT INTO normalized_jobs (
                        source_name, source_type, source_job_id, source_url, requisition_id, apply_url,
                        company, title, normalized_title, description, location, normalized_location, country,
                        is_remote, is_hybrid, is_onsite, dfw_match, us_match, travel_required, travel_percent,
                        base_min, base_max, bonus, equity, estimated_total_comp_min, estimated_total_comp_max,
                        compensation_confidence, role_family_tags_json, seniority_hints_json, duplicate_count,
                        job_hash, first_seen, last_seen, is_active, last_scan_id, dedupe_key
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?
                    )
                    """,
                    insert_payload,
                )
                job_id = int(self.conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                stored.append(StoredJob(job_id=job_id, normalized=job, is_new=True))

        self.conn.commit()
        return stored

    def mark_inactive_jobs(self, scan_id: int, active_dedupe_keys: list[str]) -> int:
        placeholders = ",".join(["?"] * len(active_dedupe_keys))
        if active_dedupe_keys:
            query = f"""
                UPDATE normalized_jobs
                   SET is_active = 0,
                       last_scan_id = ?
                 WHERE is_active = 1
                   AND dedupe_key NOT IN ({placeholders})
            """
            args = [scan_id, *active_dedupe_keys]
        else:
            query = "UPDATE normalized_jobs SET is_active = 0, last_scan_id = ? WHERE is_active = 1"
            args = [scan_id]

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

    def list_top_jobs(self, limit: int = 25, scan_id: int | None = None) -> list[dict[str, Any]]:
        selected_scan_id = scan_id or self.get_latest_scan_id()
        if selected_scan_id is None:
            return []
        return self.get_scored_jobs_for_scan(selected_scan_id)[:limit]

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

    def get_scan_summary(self, scan_id: int) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM scans WHERE id = ?",
            (scan_id,),
        ).fetchone()
        return dict(row) if row else {}
