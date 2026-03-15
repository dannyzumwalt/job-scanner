# Codex Build Spec: Automated Job Listing Scanner

## Goal

Build a local-first job scanning tool that I can run periodically to collect job listings from selected sources, normalize them, score them against my criteria, and generate a concise ranked report.

This should be designed for **ongoing personal use**, easy maintenance, and future extension.

---

## My Current Search Preferences

These must live in a clearly editable config file so I can update them without touching code.

- **Target total compensation:** $300k-$400k minimum
- **Work arrangement:** Remote preferred
- **Location:** DFW area acceptable
- **Travel:** No travel preferred, limited travel acceptable
- **Primary geography:** United States
- **Role level:** Senior / Staff / Principal / Distinguished / Architect / high-end technical individual contributor roles
- **Role family preference:** infrastructure, operations, analytics, reliability, network, systems, data-driven technical leadership

---

## Candidate Profile to Match Against

Use this profile to score fit:

- BS in Data Analytics
- Network engineering SME background
- Telecom / infrastructure operations experience
- Vendor defect management
- Incident / outage analysis
- Data analytics and reporting
- Automation / scripting
- Dashboarding / operational intelligence
- Cross-functional technical leadership
- Best fit is likely some combination of:
  - Principal Engineer
  - Staff Engineer
  - Systems Architect
  - Reliability / SRE leadership
  - Operational Intelligence Engineer
  - Infrastructure Analytics Engineer
  - Technical strategy roles with strong technical depth

---

## Project Outcome

Create a working project that does all of the following:

1. Pull job listings from selected sources
2. Store raw and normalized data locally
3. De-duplicate listings
4. Score each listing against my preferences
5. Produce:
   - a ranked markdown report
   - a CSV export
   - a JSON export
6. Preserve prior scans so I can compare changes over time
7. Mark new listings since last run
8. Flag listings that are strong matches
9. Flag listings that are near-matches but have concerns
10. Exclude obvious junk automatically

---

## Preferred Tech Stack

Default to:

- **Python 3.12+**
- `requests` / `httpx`
- `beautifulsoup4` if scraping HTML is needed
- `pydantic` for schemas
- `sqlite` for local storage
- `typer` or `argparse` for CLI
- `jinja2` optional for report templates
- `pytest` for tests

Keep dependencies modest.

Do **not** make this a heavyweight web app unless there is a very good reason.

A clean CLI-first project is preferred.

---

## Architecture Requirements

Build the project with a clean structure like:

```text
job_scanner/
  README.md
  requirements.txt
  pyproject.toml
  .env.example
  config/
    search_profile.yaml
    sources.yaml
  data/
    raw/
    processed/
    reports/
  src/
    job_scanner/
      __init__.py
      main.py
      config.py
      models.py
      storage.py
      scoring.py
      reporting.py
      dedupe.py
      utils.py
      sources/
        __init__.py
        greenhouse.py
        lever.py
        ashby.py
        generic.py
  tests/
```
Use modular source connectors so more can be added later.

## Source Strategy
Implement source ingestion in phases.
### Phase 1 sources
Support the easiest high-value sources first:
* Greenhouse-hosted job boards
* Lever-hosted job boards
* Ashby-hosted job boards if practical
* Generic company careers pages via configurable RSS/JSON/HTML parsing if feasible

### Important constraints
* Prefer official/public company job feeds and structured endpoints where available
* Avoid brittle scraping where possible
* Make the project easy to extend with additional connectors later
* Build rate limiting and polite request handling

### Source Config
Allow me to define sources in a config file, for example

```yaml
sources:
  - name: Datadog
    type: greenhouse
    enabled: true
    url: "https://boards.greenhouse.io/datadog"
  - name: Cloudflare
    type: greenhouse
    enabled: true
    url: "https://boards.greenhouse.io/cloudflare"
  - name: Stripe
    type: jobs_api
    enabled: false
    url: "..."
```
## Scoring Logic
Create a transparent weighted scoring system.
### Required scoring dimensions
Each job should receive a score from 0 to 100.
At minimum score these areas:
* Compensation fit
* Role seniority fit
* Technical domain fit
* Analytics / data fit
* Infrastructure / reliability fit
* Remote fit
* Location fit
* Travel fit
* Leadership / autonomy fit
* Title relevance

### Example outcome categories
* **90-100**: Strong match
* **75-89**: Good match
* **60-74**: Possible match
* **Below 60**: Low priority / reject

### Scoring behavior
* Penalize clearly low compensation
* Penalize heavy travel
* Penalize non-DFW onsite requirements unless exceptionally strong
* Penalize sales engineering, contract, junior, and people-manager-first roles
* Boost roles combining infrastructure + analytics + systems thinking
* Boost staff/principal/distinguished/architect roles
* Boost remote roles

### Explainability
Each scored listing should also include:
* top reasons it matched
* top concerns
* recommended action:
  * pursue
  * review manually
  * reject


## Compensation Handling
Compensation data is messy, so handle it carefully.
### Requirements
* Parse listed salary ranges where available
* Infer comp fit conservatively when only partial data is provided
* Keep separate fields for:
  * base_min
  * base_max
  * bonus
  * equity
  * estimated_total_comp_min
  * estimated_total_comp_max
* If total comp is not directly listed, do not invent precise numbers
* Use a confidence score for compensation estimates

### Compensation decision rules
* Strong penalty if clear max comp is below target
* Moderate penalty if comp is absent but title/company suggests high upside
* Strong boost if the range likely supports $300k-$400k+


## De-duplication
Jobs often appear multiple times.
Implement de-duplication based on a combination of:
* company
* normalized title
* normalized location
* posting URL
* requisition ID if present

Keep original raw records but mark duplicates in normalized storage.

## Persistence
Use SQLite as the default local database.
Need tables for:
* sources
* raw_jobs
* normalized_jobs
* scans
* score_results

Requirements:
* preserve scan history
* track first_seen
* track last_seen
* mark removed or expired jobs when they disappear from source
* detect newly added jobs since last run


## CLI Requirements
Build a simple CLI with commands like
```bash
python -m job_scanner scan
python -m job_scanner report
python -m job_scanner list --top 25
python -m job_scanner diff --since last
```

Desired CLI behaviors:
### scan
* fetch all enabled sources
* normalize and store data
* score jobs
* print a concise summary

### report
* generate markdown, CSV, and JSON reports

### list
* show top-ranked current listings in terminal

### diff
* show new, removed, and materially changed listings since previous scan


## Reporting Requirements
Generate a markdown report with sections:
### Job Scan Report
Date of scan
### Top Matches
For each:
* title
* company
* location
* remote/onsite status
* compensation info
* score
* reasons it matched
* concerns
* apply URL

### Worth Reviewing
Same fields, but lower score band
### Rejected / Low Priority
Brief line items with short reason
### Market Notes
Simple counts such as:
* total jobs scanned
* number with listed compensation
* number remote
* number DFW
* number strong matches
* most common titles
* most common companies

Also generate:
* latest_report.md
* timestamped historical report
* CSV export
* JSON export


## Config Requirements
Create editable configs for:
### config/search_profile.yaml
Should include:
* target comp
* work arrangement preference
* location preference
* travel preference
* target role families
* target keywords
* exclude keywords
* weighting values for scoring

### config/sources.yaml
Should include:
* enabled sources
* source type
* company name
* source url
* notes


## Keywords and Heuristics
Seed the project with practical keyword logic.
### Positive signals
* principal
* staff
* distinguished
* architect
* infrastructure
* reliability
* sre
* observability
* operations
* network
* data
* analytics
* platform
* distributed systems
* systems engineering
* automation

### Negative signals
* contract
* temporary
* internship
* sales
* solutions consultant
* customer success
* heavy travel
* entry level
* manager only
* director of people
* recruiter

Keep this editable in config.

## Testing
Add tests for at least:
* salary parsing
* title normalization
* de-duplication
* scoring logic
* config loading
* one example source parser

Use pytest.

## Documentation
Create a README.md that includes:
* purpose
* setup
* virtualenv instructions
* config editing
* how to add a source
* how to run a scan
* how reports work
* known limitations
* future enhancements


## Future-Proofing
Design the code so the following can be added later without major refactor:
* email alerts
* Slack alerts
* LinkedIn/manual import CSV support
* browser automation for hard-to-parse sites
* LLM-assisted semantic scoring
* resume tailoring against selected jobs
* cover letter generation
* application tracker

Do not build all of these now unless easy.

## Implementation Style
I want Codex to behave like a strong senior engineer.
### Expectations
* Start by creating a plan
* Then scaffold the project
* Then implement a minimum viable vertical slice
* Then improve iteratively
* Keep commits or changes logically grouped
* Do not over-engineer
* Prefer readable, boring, maintainable code
* Add comments only where useful
* Keep parsing logic defensive


## Minimum Viable Version Definition
The first complete version is successful if it can:
1. Load my config
2. Read at least 2-3 source types
3. Pull live listings from several target companies
4. Normalize and store results in SQLite
5. Score listings
6. Produce a useful markdown report
7. Show top matches in terminal


## Deliverables
Produce:
1. Full project files
2. Working CLI
3. Example config files populated with sensible starter content
4. README
5. Tests
6. Sample report output from a test run


## Nice-to-Have Starter Source List
Populate starter source config with a reasonable mix of companies where high-end technical IC roles may appear, such as:
* Datadog
* Cloudflare
* Snowflake
* Stripe
* Google
* Amazon
* Microsoft
* Cisco
* Palo Alto Networks
* CrowdStrike
* NVIDIA
* HashiCorp / IBM if relevant
* major telecom or infrastructure-adjacent employers
* selected DFW companies with strong technical roles

Use what is practical given source support.

## Final Instruction
Do not just describe the solution.
Actually build the project.
Start with:
1. a concise implementation plan
2. project scaffolding
3. MVP implementation
4. instructions for how I run it locally on Mac

When in doubt, choose simplicity and maintainability.
