from job_scanner.models import MatchCategory, SearchProfile
from job_scanner.scoring import JobScorer

from .helpers import make_job


def test_scoring_strong_match_for_target_role() -> None:
    scorer = JobScorer(SearchProfile())
    job = make_job(
        title="Principal Infrastructure Analytics Engineer",
        description="Remote role for reliability, observability, and automation.",
        is_remote=True,
        estimated_total_comp_min=320000,
        estimated_total_comp_max=430000,
    )

    result = scorer.score(job)

    assert result.total_score >= 90
    assert result.category == MatchCategory.STRONG
    assert result.recommended_action == "pursue"


def test_scoring_reject_for_low_comp_and_high_travel() -> None:
    scorer = JobScorer(SearchProfile())
    job = make_job(
        title="Sales Engineer",
        description="Pre-sales support role with 40% travel",
        is_remote=False,
        is_onsite=True,
        dfw_match=False,
        estimated_total_comp_min=140000,
        estimated_total_comp_max=180000,
        travel_percent=40,
        role_family_tags=["operations"],
    )

    result = scorer.score(job)

    assert result.total_score < 60
    assert result.category == MatchCategory.REJECT
    assert result.recommended_action == "reject"
