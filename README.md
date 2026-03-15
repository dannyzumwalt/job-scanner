# Job Scanner

A local-first, CLI-first job listing scanner that ingests jobs from structured sources, normalizes and deduplicates records, scores listings against your preferences, and generates ranked reports.

## What it does

- Pulls job listings from configurable source adapters (Greenhouse, Lever, Ashby, generic JSON, RSS).
- Supports manual CSV/JSON import flow for aggregator exports.
- Stores raw payloads and normalized records in SQLite.
- Maintains scan history and tracks first-seen/last-seen jobs.
- Scores jobs on a 0-100 weighted model and exposes a report-friendly 0-10 score.
- Exports ranked outputs to Markdown, CSV, and JSON.
- Supports scan diffs (`new`, `removed`, `changed`) against the previous or timestamp baseline.
- Tracks per-source run health (status, HTTP code, parse count, latency, error class/message).

## Project layout

- `config/search_profile.yaml`: scoring and preference profile.
- `config/sources.yaml`: enabled sources and source metadata.
- `src/job_scanner/`: scanner package and CLI.
- `data/processed/job_scanner.db`: SQLite database.
- `data/reports/`: latest and timestamped report files.
- `tests/`: unit and integration tests with fixtures.

## Setup (macOS)

1. Create and activate virtualenv:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -U pip
pip install -e .
```

3. Optional dev test tools:

```bash
pip install -e .[dev]
```

## Configuration

Edit these files without changing code:

- `config/search_profile.yaml`
- `config/sources.yaml`

Important knobs:

- target compensation range and hard floor
- remote/DFW/travel preferences
- include/exclude keywords
- scoring weights and penalties/boosts
- scan profiles (`quick` and `deep`)
- report targets (top matches/potential/reject limits and trend lookback)
- source enablement and URLs

Source config supports both:

- `url`: board URL (for slug-based API derivation)
- `api_url` (optional): explicit API endpoint override when board URL does not map cleanly
- `format`: `auto|greenhouse|lever|ashby|json|rss|html`
- `parser_template`: per-source field mapping overrides
- `priority`, `expected_status`: scheduling and validation controls

## CLI usage

Run via module:

```bash
python -m job_scanner scan --profile deep
python -m job_scanner scan --profile quick
python -m job_scanner sources validate --profile deep
python -m job_scanner import --file ./exports/linkedin_jobs.csv --format csv
python -m job_scanner report
python -m job_scanner list --top 25
python -m job_scanner diff --since last
python -m job_scanner cleanup --keep-scans 8 --keep-reports 12
```

Or via installed script:

```bash
job-scanner scan --profile deep
job-scanner sources validate
job-scanner import --file ./exports/jobs.json --format json
job-scanner report
job-scanner list --top 25
job-scanner diff --since 2026-03-15T12:00:00
job-scanner cleanup --keep-scans 8 --keep-reports 12
```

## Output files

Each scan writes:

- Raw snapshot: `data/raw/scan_<id>_raw.jsonl`
- Database rows: `raw_jobs`, `normalized_jobs`, `score_results`, `scan_jobs`, `source_runs`, `import_batches`
- Reports:
  - `data/reports/latest_report.md`
  - `data/reports/latest_report.csv`
  - `data/reports/latest_report.json`
  - timestamped historical versions for each format

## How to add a source

1. Add a new source entry in `config/sources.yaml`.
2. If it is a new source type, add a module in `src/job_scanner/sources/` with `fetch_and_normalize()`.
3. Register the source type in `src/job_scanner/sources/__init__.py`.
4. Add parser tests and fixtures in `tests/`.

If a source returns 404 in scan output:

1. Set `enabled: false` for that source immediately.
2. Add a verified `api_url` if available.
3. Re-run `python -m job_scanner scan`.

## Weekly automation (local)

Example weekly deep scan via cron (Sunday at 07:00 local time):

```cron
0 7 * * 0 cd /Users/danny/Projects/job-scanner && /bin/zsh -lc 'source .venv/bin/activate && python -m job_scanner scan --profile deep'
```

Recommended weekly sequence:

1. `python -m job_scanner sources validate --profile deep`
2. `python -m job_scanner scan --profile deep`
3. `python -m job_scanner diff --since last`
4. `python -m job_scanner cleanup --keep-scans 8 --keep-reports 12`

## Known limitations

- Ashby boards may require per-company slug verification.
- Generic HTML scraping remains opt-in and minimal by design.
- Compensation parsing is conservative and estimate confidence can be low when ranges are absent.
- Material change detection uses content hash and score delta threshold.
- Some company job board URLs do not expose stable public APIs; these sources may require explicit `api_url` or disabling.

## Future enhancements

- Email and Slack alerts.
- Manual CSV import from external job trackers.
- Browser automation fallback for hard-to-parse pages.
- Optional LLM-assisted semantic scoring.
- Application tracking and resume tailoring workflow.
