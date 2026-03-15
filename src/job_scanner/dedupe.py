from __future__ import annotations

from collections import defaultdict

from .models import NormalizedJob


def dedupe_jobs(jobs: list[NormalizedJob]) -> list[NormalizedJob]:
    grouped: dict[str, list[NormalizedJob]] = defaultdict(list)
    for job in jobs:
        grouped[job.dedupe_key].append(job)

    deduped: list[NormalizedJob] = []
    for _, matches in grouped.items():
        winner = max(matches, key=lambda item: len(item.description or ""))
        winner.duplicate_count = len(matches)
        deduped.append(winner)

    return deduped
