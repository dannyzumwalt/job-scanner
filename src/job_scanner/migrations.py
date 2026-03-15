from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

CURRENT_SCHEMA_VERSION = 5


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row[1] == column for row in rows)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _ensure_meta_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def _applied_versions(conn: sqlite3.Connection) -> set[int]:
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


def _mark_applied(conn: sqlite3.Connection, version: int) -> None:
    now = datetime.now(UTC).isoformat()
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        (version, now),
    )


def _migration_v1(conn: sqlite3.Connection) -> None:
    conn.executescript(
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


def _migration_v2(conn: sqlite3.Connection) -> None:
    source_columns = {
        "api_url": "TEXT",
        "format": "TEXT NOT NULL DEFAULT 'auto'",
        "parser_template_json": "TEXT NOT NULL DEFAULT '{}'",
        "priority": "INTEGER NOT NULL DEFAULT 100",
        "expected_status": "INTEGER NOT NULL DEFAULT 200",
    }
    for column, column_type in source_columns.items():
        if not _column_exists(conn, "sources", column):
            conn.execute(f"ALTER TABLE sources ADD COLUMN {column} {column_type}")


def _migration_v3(conn: sqlite3.Connection) -> None:
    normalized_columns = {
        "ingest_mode": "TEXT NOT NULL DEFAULT 'live'",
        "import_batch_id": "INTEGER",
        "data_quality_flags_json": "TEXT NOT NULL DEFAULT '[]'",
        "parse_confidence": "REAL NOT NULL DEFAULT 1.0",
    }
    for column, column_type in normalized_columns.items():
        if not _column_exists(conn, "normalized_jobs", column):
            conn.execute(f"ALTER TABLE normalized_jobs ADD COLUMN {column} {column_type}")



def _migration_v4(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS source_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER NOT NULL REFERENCES scans(id) ON DELETE CASCADE,
            source_name TEXT NOT NULL,
            source_type TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            status TEXT NOT NULL,
            preflight_ok INTEGER NOT NULL DEFAULT 0,
            http_status INTEGER,
            raw_count INTEGER NOT NULL DEFAULT 0,
            normalized_count INTEGER NOT NULL DEFAULT 0,
            parse_count INTEGER NOT NULL DEFAULT 0,
            error_class TEXT,
            error_message TEXT,
            latency_ms INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            completed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_source_runs_scan_id ON source_runs(scan_id)"
    )



def _migration_v5(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS import_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT NOT NULL,
            completed_at TEXT,
            source_file TEXT NOT NULL,
            import_format TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            error TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scans_status_id ON scans(status, id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_scan_jobs_scan_id ON scan_jobs(scan_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_normalized_active ON normalized_jobs(is_active)")


MIGRATIONS = {
    1: _migration_v1,
    2: _migration_v2,
    3: _migration_v3,
    4: _migration_v4,
    5: _migration_v5,
}


def migrate(conn: sqlite3.Connection) -> None:
    _ensure_meta_tables(conn)
    applied = _applied_versions(conn)

    for version in sorted(MIGRATIONS):
        if version in applied:
            continue
        MIGRATIONS[version](conn)
        _mark_applied(conn, version)

    # Backfill parser_template_json for rows missing value when upgraded from old schema.
    if _table_exists(conn, "sources") and _column_exists(conn, "sources", "parser_template_json"):
        conn.execute(
            "UPDATE sources SET parser_template_json = ? WHERE parser_template_json IS NULL OR parser_template_json = ''",
            (json.dumps({}),),
        )

    conn.commit()
