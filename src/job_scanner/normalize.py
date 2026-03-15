from __future__ import annotations

from .models import NormalizedJob
from .utils import normalize_location, normalize_title


def normalize_job_fields(job: NormalizedJob) -> NormalizedJob:
    job.normalized_title = normalize_title(job.title)
    job.normalized_location = normalize_location(job.location)
    return job
