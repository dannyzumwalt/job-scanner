from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .config import load_app_config
from .pipeline import (
    cleanup_data,
    diff_latest_scan,
    generate_report_for_latest_scan,
    list_top_jobs,
    run_import,
    run_scan,
    validate_sources_for_profile,
)

app = typer.Typer(help="Local-first job listing scanner and scorer")
sources_app = typer.Typer(help="Source management commands")
app.add_typer(sources_app, name="sources")
console = Console()


def _load_config(root: str | None = None):
    return load_app_config(root_dir=Path(root) if root else None)


@app.command()
def scan(
    root: str | None = typer.Option(None, help="Project root path"),
    no_report: bool = typer.Option(False, help="Skip report generation after scan"),
    profile: str = typer.Option("deep", "--profile", help="Scan profile: quick|deep"),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume from last failed scan when possible"),
) -> None:
    """Fetch, normalize, dedupe, score, and persist jobs."""
    config = _load_config(root)
    result = run_scan(config, generate_report=not no_report, profile_name=profile, resume=resume)

    console.print(f"Scan complete: [bold]{result['scan_id']}[/bold] (profile={result['scan_profile']})")
    console.print(f"Duration: {result['duration_seconds']}s")
    console.print(
        f"Raw: {result['raw_count']} | Normalized: {result['normalized_count']} | Scored: {result['scored_count']}"
    )
    console.print(f"Inactive marked: {result['inactive_marked']}")
    console.print(f"Raw snapshot: {result['raw_snapshot']}")

    if result.get("resumed_skipped_sources"):
        console.print("\nResumed from prior failed scan; skipped sources:")
        for source in result["resumed_skipped_sources"]:
            console.print(f"- {source}")

    if result["source_errors"]:
        console.print("\nSource errors:", style="yellow")
        for source_name, error in result["source_errors"].items():
            console.print(f"- {source_name}: {error}", style="yellow")

    health_gate = result.get("health_gate") or {}
    if health_gate:
        gate_status = "PASSED" if health_gate.get("gate_passed") else "FAILED"
        console.print(
            (
                "\nSource health gate: "
                f"{gate_status} | healthy={health_gate.get('healthy_sources', 0)}"
                f"/{health_gate.get('total_live_sources', 0)}"
                f" | required_min={health_gate.get('required_min', 0)}"
                f" | strict={health_gate.get('strict', False)}"
            ),
            style="green" if health_gate.get("gate_passed") else "yellow",
        )

    reports = result.get("reports") or {}
    if reports:
        console.print("\nReports generated:")
        for name, path in reports.items():
            console.print(f"- {name}: {path}")


@sources_app.command("validate")
def validate_sources(
    root: str | None = typer.Option(None, help="Project root path"),
    profile: str = typer.Option("deep", "--profile", help="Scan profile context: quick|deep"),
    include_disabled: bool = typer.Option(False, "--include-disabled", help="Include disabled sources in validation"),
    strict: bool | None = typer.Option(None, "--strict/--no-strict", help="Override strict validation mode"),
    min_healthy: int | None = typer.Option(None, "--min-healthy", help="Override required healthy live source count"),
) -> None:
    """Preflight source endpoints and schema reachability."""
    config = _load_config(root)
    bundle = validate_sources_for_profile(
        config,
        only_enabled=not include_disabled,
        profile_name=profile,
        strict=strict,
        min_healthy=min_healthy,
    )
    results = bundle["results"]
    health_gate = bundle["health_gate"]
    strict_mode = bundle["strict"]

    table = Table(title=f"Source Validation ({profile}, strict={strict_mode})")
    table.add_column("Source")
    table.add_column("Type")
    table.add_column("HTTP")
    table.add_column("Parsed")
    table.add_column("Status")
    table.add_column("Latency ms")
    table.add_column("Endpoint")

    failures = 0
    for item in results:
        status_text = "ok" if item.ok else "failed"
        if not item.ok:
            failures += 1
        table.add_row(
            item.source_name,
            item.source_type.value,
            str(item.http_status or "-"),
            str(item.parse_count),
            status_text,
            str(item.latency_ms),
            item.endpoint,
        )

    console.print(table)
    console.print(
        (
            f"Health gate: {'PASSED' if health_gate['gate_passed'] else 'FAILED'}"
            f" | healthy={health_gate['healthy_sources']}/{health_gate['total_live_sources']}"
            f" | required_min={health_gate['required_min']}"
        ),
        style="green" if health_gate["gate_passed"] else "yellow",
    )
    if failures:
        console.print(f"Validation failures: {failures}", style="yellow")
    else:
        console.print("All validated sources are healthy.", style="green")


@app.command("import")
def import_jobs(
    file: str = typer.Option(..., "--file", help="Path to CSV/JSON import file"),
    format: str = typer.Option("auto", "--format", help="Import format: auto|csv|json"),
    source_name: str = typer.Option("Manual Import", "--source-name", help="Logical source label"),
    root: str | None = typer.Option(None, help="Project root path"),
    no_report: bool = typer.Option(False, help="Skip report generation after import"),
) -> None:
    """Import jobs from CSV/JSON and process through the standard pipeline."""
    config = _load_config(root)
    result = run_import(
        config,
        file_path=file,
        import_format=format,
        source_name=source_name,
        generate_report=not no_report,
    )

    console.print(f"Import complete: scan [bold]{result['scan_id']}[/bold], batch {result['import_batch_id']}")
    console.print(
        f"Raw: {result['raw_count']} | Normalized: {result['normalized_count']} | Scored: {result['scored_count']}"
    )
    console.print(f"Raw snapshot: {result['raw_snapshot']}")
    health_gate = result.get("health_gate") or {}
    if health_gate:
        console.print(
            (
                f"Source health gate: {'PASSED' if health_gate['gate_passed'] else 'FAILED'}"
                f" | healthy={health_gate['healthy_sources']}/{health_gate['total_live_sources']}"
                f" | required_min={health_gate['required_min']}"
            ),
            style="green" if health_gate.get("gate_passed") else "yellow",
        )
    reports = result.get("reports") or {}
    if reports:
        console.print("\nReports generated:")
        for name, path in reports.items():
            console.print(f"- {name}: {path}")


@app.command()
def report(root: str | None = typer.Option(None, help="Project root path")) -> None:
    """Generate report files from the latest completed scan."""
    config = _load_config(root)
    result = generate_report_for_latest_scan(config)

    console.print(f"Report generated for scan [bold]{result['scan_id']}[/bold]")
    health_gate = result.get("health_gate") or {}
    if health_gate:
        console.print(
            (
                f"Source health gate: {'PASSED' if health_gate['gate_passed'] else 'FAILED'}"
                f" | healthy={health_gate['healthy_sources']}/{health_gate['total_live_sources']}"
                f" | required_min={health_gate['required_min']}"
            ),
            style="green" if health_gate.get("gate_passed") else "yellow",
        )
    for name, path in result["reports"].items():
        console.print(f"- {name}: {path}")


@app.command("list")
def list_command(
    top: int = typer.Option(25, "--top", help="Number of top-ranked jobs to display"),
    root: str | None = typer.Option(None, help="Project root path"),
) -> None:
    """Show top current listings in terminal."""
    config = _load_config(root)
    jobs = list_top_jobs(config, top_n=top)

    if not jobs:
        console.print("No scored jobs available. Run `scan` first.", style="yellow")
        return

    table = Table(title=f"Top {len(jobs)} Job Matches")
    table.add_column("Score")
    table.add_column("Category")
    table.add_column("Title")
    table.add_column("Company")
    table.add_column("Location")
    table.add_column("Mode")
    table.add_column("Ingest")

    for job in jobs:
        mode = "Remote" if job["is_remote"] else ("Hybrid" if job["is_hybrid"] else "Onsite")
        table.add_row(
            f"{job['display_score']}/10",
            job["category"].value,
            job["title"],
            job["company"],
            job.get("location") or "N/A",
            mode,
            job.get("ingest_mode") or "live",
        )

    console.print(table)


@app.command()
def diff(
    since: str = typer.Option("last", "--since", help="Baseline: 'last' or ISO timestamp"),
    root: str | None = typer.Option(None, help="Project root path"),
) -> None:
    """Show new, removed, and changed listings vs a prior scan."""
    config = _load_config(root)
    result = diff_latest_scan(config, since=since)

    console.print(
        f"Comparing scan {result['current_scan_id']} against baseline {result['baseline_scan_id']}"
    )
    console.print(f"New jobs: {len(result['new_jobs'])}")
    console.print(f"Removed jobs: {len(result['removed_jobs'])}")
    console.print(f"Changed jobs: {len(result['changed_jobs'])}")
    console.print(json.dumps(result, indent=2))


@app.command()
def cleanup(
    keep_scans: int = typer.Option(8, "--keep-scans", help="Keep this many most recent completed scans"),
    keep_reports: int = typer.Option(12, "--keep-reports", help="Keep this many timestamped report files per type"),
    root: str | None = typer.Option(None, help="Project root path"),
) -> None:
    """Prune old scan records and historical report/raw artifacts."""
    config = _load_config(root)
    result = cleanup_data(config, keep_scans=keep_scans, keep_reports=keep_reports)

    console.print("Cleanup complete")
    console.print(f"- pruned scans: {result['pruned_scans']}")
    console.print(f"- deleted report files: {result['deleted_report_files']}")
    console.print(f"- deleted raw snapshots: {result['deleted_raw_snapshots']}")


def run() -> None:
    app()


if __name__ == "__main__":
    run()
