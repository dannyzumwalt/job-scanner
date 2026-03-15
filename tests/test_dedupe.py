from job_scanner.dedupe import dedupe_jobs

from .helpers import make_job


def test_dedupe_groups_by_key_and_sets_duplicate_count() -> None:
    one = make_job(source_job_id="1", apply_url="https://example.com/jobs/1")
    two = make_job(source_job_id="2", apply_url="https://example.com/jobs/1")

    deduped = dedupe_jobs([one, two])

    assert len(deduped) == 1
    assert deduped[0].duplicate_count == 2
