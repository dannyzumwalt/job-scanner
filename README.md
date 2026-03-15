# Job Scanner

A local-first, CLI-first job listing scanner that ingests jobs from structured sources, normalizes and deduplicates records, scores listings against your preferences, and generates ranked reports.

Version: `1.0.0`

## What it does

- Pulls job listings from configurable source adapters (Greenhouse, Lever, Ashby, generic JSON, RSS, opt-in generic HTML).
- Supports manual CSV/JSON import flow for aggregator exports.
- Stores raw payloads and normalized records in SQLite.
- Maintains scan history and tracks first-seen/last-seen jobs.
- Scores jobs on a 0-100 weighted model and exposes a report-friendly 0-10 score.
- Exports ranked outputs to Markdown, CSV, and JSON.
- Supports scan diffs (`new`, `removed`, `changed`) against the previous or timestamp baseline.
- Tracks per-source run health (status, HTTP code, parse count, latency, error class/message).
- Enforces profile-driven source health gates (deep profile default: `>=15` healthy live sources).

## Project layout

- `config/search_profile.yaml`: scoring and preference profile.
- `config/sources.yaml.sample`: tracked starter source catalog.
- `config/sources.yaml`: local source config (untracked/private).
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

4. Create local sources config:

```bash
cp config/sources.yaml.sample config/sources.yaml
```

## Configuration

Edit these files without changing code:

- `config/search_profile.yaml`
- `config/sources.yaml` (local, untracked)

Notes:

- If `config/sources.yaml` is missing, the app automatically falls back to `config/sources.yaml.sample`.
- `sources discover --append` writes to `config/sources.yaml` so your local source list can stay private.

Important knobs:

- target compensation range and hard floor
- remote/DFW/travel preferences
- include/exclude keywords
- scoring weights and penalties/boosts
- scan profiles (`quick` and `deep`)
- strict validation policy and minimum healthy live source threshold
- report targets (top matches/potential/reject limits and trend lookback)
- source enablement and URLs

Source config supports both:

- `url`: board URL (for slug-based API derivation)
- `api_url` (optional): explicit API endpoint override when board URL does not map cleanly
- `format`: `auto|greenhouse|lever|ashby|json|rss|html`
- `parser_template`: per-source field mapping overrides
- strict parser-template contracts by source type
- `priority`, `expected_status`: scheduling and validation controls

`scan_profiles` supports:

- `strict_source_validation` (deep defaults to `true`)
- `min_healthy_sources` (deep defaults to `15`)

## CLI usage

Run via module:

```bash
python -m job_scanner scan --profile deep
python -m job_scanner scan --profile quick
python -m job_scanner sources validate --profile deep
python -m job_scanner sources validate --profile deep --strict --min-healthy 15
python -m job_scanner sources discover --limit 60
python -m job_scanner sources discover --limit 60 --validate --all
python -m job_scanner sources discover --append --enable --limit 20
python -m job_scanner sources discover --output config/sources.discovered.yaml --limit 40
python -m job_scanner sources discover --type generic_json --type rss --limit 20
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
job-scanner sources validate --strict --min-healthy 15
job-scanner sources discover --limit 60
job-scanner sources discover --limit 60 --validate --all
job-scanner sources discover --append --enable --limit 20
job-scanner sources discover --output config/sources.discovered.yaml --limit 40
job-scanner sources discover --type generic_json --type rss --limit 20
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
  - report metadata includes source health gate fields: `healthy_sources`, `required_min`, `gate_passed`

## How to add a source

1. Add a new source entry in `config/sources.yaml`.
2. Run `python -m job_scanner sources validate --profile deep --strict`.
3. Confirm parser-template keys are valid for that source type.
4. Enable source only after strict validation passes.
5. Add/update parser tests and fixtures in `tests/` if needed.

Generic HTML template keys (required):

- `items_selector`
- `title_selector`
- `apply_url_selector`

Generic HTML template keys (optional):

- `title_attr`, `apply_url_attr`
- `description_selector`, `location_selector`
- `requisition_selector`, `compensation_selector`
- `company_selector`, `source_job_id_selector`, `source_job_id_attr`

If a source returns 404 in scan output:

1. Set `enabled: false` for that source immediately.
2. Add a verified `api_url` if available.
3. Re-run `python -m job_scanner scan`.

## Source discovery at scale

Use discovery to avoid hand-curating every company:

1. Run ranked discovery first (default is `--no-validate`): `python -m job_scanner sources discover --limit 80`
   - This is the recommended first pass when quickly expanding coverage.
   - For multi-company board feeds only: `python -m job_scanner sources discover --type generic_json --type rss --limit 20`
2. Optionally run endpoint checks on discovered candidates:
   - `python -m job_scanner sources discover --limit 80 --validate --all`
   - `python -m job_scanner sources discover --limit 80 --validate --only-healthy`
3. Optionally write a standalone discovery file for review:
   - `python -m job_scanner sources discover --output config/sources.discovered.yaml --limit 40`
4. Append top candidates to `config/sources.yaml`:
   - `python -m job_scanner sources discover --append --enable --limit 25`
5. Re-run strict validation before deep scans.

Discovery options:

- `--type`: filter catalog by source type (`greenhouse`, `lever`, `ashby`, `generic_json`, `rss`, etc.).
- `--validate`: check live endpoint and parser health for candidates.
- `--only-healthy`: when validating, keep only healthy candidates in output.
- `--criteria-markdown`: markdown keyword source (defaults to `ai-job-scan.md`).

Current built-in discovery catalog focuses on:

- ATS company feeds: Greenhouse, Lever, Ashby.
- Multi-company structured feeds: generic JSON + RSS.
- It does not include direct LinkedIn/Indeed scraping.

Recommended weekly source expansion loop:

1. `python -m job_scanner sources discover --limit 60`
2. `python -m job_scanner sources discover --append --limit 20`
3. `python -m job_scanner sources validate --profile deep --strict`
4. `python -m job_scanner scan --profile deep`

Discovery scoring uses:

- `config/search_profile.yaml` role and location preferences.
- Optional markdown criteria file (defaults to `ai-job-scan.md`) for extra keyword signals.
- A built-in catalog that includes ATS feeds and multi-company structured feeds (JSON/RSS).

## Weekly automation (local)

Example weekly deep scan via cron (Sunday at 07:00 local time):

```cron
0 7 * * 0 cd /Users/danny/Projects/job-scanner && /bin/zsh -lc 'source .venv/bin/activate && python -m job_scanner scan --profile deep'
```

Recommended weekly sequence:

1. `python -m job_scanner sources validate --profile deep --strict`
2. `python -m job_scanner scan --profile deep`
3. `python -m job_scanner diff --since last`
4. `python -m job_scanner cleanup --keep-scans 8 --keep-reports 12`

If the health gate fails (healthy sources below required minimum), review source failures and keep the scan as informational until source health recovers.

## Troubleshooting validation failures

- `404 Not Found`: board slug or endpoint is invalid for that provider; update `api_url` or disable source.
- `template_validation_error`: parser-template keys are unsupported or missing required keys.
- `schema_validation_error`: endpoint is reachable but payload shape is not compatible with parser settings.
- `unexpected_status`: endpoint returned a different HTTP status than `expected_status`.

Use `--strict` validation before enabling new sources in deep profile.

## Sample report fixture

Deterministic sample output for v1 regression checks is stored in:

- `tests/fixtures/sample_report.md`
- `tests/fixtures/sample_report.json`
- `tests/fixtures/sample_report.csv`

## Known limitations

- Ashby boards may require per-company slug verification.
- Generic HTML parsing is static and selector-driven; dynamic JS-rendered sites are out of scope.
- Compensation parsing is conservative; unknown bonus/equity values reduce confidence.
- Material change detection uses content hash and score delta threshold.
- Some company job board URLs do not expose stable public APIs; these sources may require explicit `api_url` or disabling.

## Future enhancements

- Email and Slack alerts.
- Manual CSV import from external job trackers.
- Browser automation fallback for hard-to-parse pages.
- Optional LLM-assisted semantic scoring.
- Application tracking and resume tailoring workflow.
