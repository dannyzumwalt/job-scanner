import json
from pathlib import Path

from job_scanner.importer import import_file_to_jobs


def test_import_csv_to_jobs(tmp_path: Path) -> None:
    csv_file = tmp_path / "jobs.csv"
    csv_file.write_text(
        "title,company,location,description,apply_url,salary\n"
        "Principal Infrastructure Engineer,Acme,Remote US,Build reliability,https://acme/jobs/1,$320k-$420k\n",
        encoding="utf-8",
    )

    raw_jobs, normalized_jobs = import_file_to_jobs(str(csv_file), "csv", import_batch_id=1)

    assert len(raw_jobs) == 1
    assert len(normalized_jobs) == 1
    assert normalized_jobs[0].ingest_mode == "import"
    assert normalized_jobs[0].import_batch_id == 1


def test_import_json_to_jobs(tmp_path: Path) -> None:
    json_file = tmp_path / "jobs.json"
    json_file.write_text(
        json.dumps(
            [
                {
                    "title": "Staff SRE",
                    "company": "Acme",
                    "location": "Remote US",
                    "description": "Operate distributed systems",
                    "apply_url": "https://acme/jobs/2",
                }
            ]
        ),
        encoding="utf-8",
    )

    raw_jobs, normalized_jobs = import_file_to_jobs(str(json_file), "json", import_batch_id=2)

    assert len(raw_jobs) == 1
    assert len(normalized_jobs) == 1
    assert normalized_jobs[0].title == "Staff SRE"
