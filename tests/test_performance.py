import time

from job_scanner.models import SearchProfile
from job_scanner.scoring import JobScorer

from .helpers import make_job


def test_scoring_10k_jobs_performance() -> None:
    scorer = JobScorer(SearchProfile())
    jobs = [
        make_job(
            source_job_id=str(i),
            apply_url=f"https://example.com/jobs/{i}",
            title="Principal Infrastructure Engineer" if i % 2 == 0 else "Staff SRE",
        )
        for i in range(10_000)
    ]

    started = time.perf_counter()
    for job in jobs:
        scorer.score(job)
    elapsed = time.perf_counter() - started

    # Practical guardrail for local runs, not a micro-benchmark.
    assert elapsed < 20.0
