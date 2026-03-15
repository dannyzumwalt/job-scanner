from __future__ import annotations

import json
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .config import load_app_config
from .pipeline import diff_latest_scan, generate_report_for_latest_scan, list_top_jobs, run_scan

app = typer.Typer(help="Local-first job listing scanner and scorer")
console = Console()


def _load_config(root: str | None = None):
    return load_app_config(root_dir=Path(root) if root else None)


@app.command()
def scan(
    root: str | None = typer.Option(None, help="Project root path"),
    no_report: bool = typer.Option(False, help="Skip report generation after scan"),
) -> None:
    """Fetch, normalize, dedupe, score, and persist jobs."""
    config = _load_config(root)
    result = run_scan(config, generate_report=not no_report)

    console.print(f"Scan complete: [bold]{result['scan_id']}[/bold]")
    console.print(f"Duration: {result['duration_seconds']}s")
    console.print(
        f"Raw: {result['raw_count']} | Normalized: {result['normalized_count']} | Scored: {result['scored_count']}"
    )
    console.print(f"Inactive marked: {result['inactive_marked']}")
    console.print(f"Raw snapshot: {result['raw_snapshot']}")

    if result["source_errors"]:
        console.print("\nSource errors:", style="yellow")
        for source_name, error in result["source_errors"].items():
            console.print(f"- {source_name}: {error}", style="yellow")

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

    for job in jobs:
        mode = "Remote" if job["is_remote"] else ("Hybrid" if job["is_hybrid"] else "Onsite")
        table.add_row(
            f"{job['display_score']}/10",
            job["category"].value,
            job["title"],
            job["company"],
            job.get("location") or "N/A",
            mode,
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


def run() -> None:
    app()


if __name__ == "__main__":
    run()
