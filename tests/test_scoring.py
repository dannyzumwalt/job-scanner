from job_scanner.models import MatchCategory, SearchProfile, ScoringRules
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


def test_scoring_good_match() -> None:
    scorer = JobScorer(SearchProfile())
    job = make_job(
        title="Senior Infrastructure Engineer",
        description="Systems reliability engineering",
        is_remote=True,
        role_family_tags=["infrastructure"],
        seniority_hints=["senior"],
        estimated_total_comp_min=300_000,
        estimated_total_comp_max=380_000,
    )

    result = scorer.score(job)

    assert result.category == MatchCategory.GOOD
    assert 75.0 <= result.total_score < 90.0


def test_scoring_possible_match() -> None:
    scorer = JobScorer(SearchProfile())
    job = make_job(
        title="Software Engineer",
        description="Backend development",
        is_remote=True,
        role_family_tags=["infrastructure"],
        seniority_hints=[],
        estimated_total_comp_min=220_000,
        estimated_total_comp_max=280_000,
    )

    result = scorer.score(job)

    assert result.category == MatchCategory.POSSIBLE
    assert 60.0 <= result.total_score < 75.0


def _moderate_job(**kwargs):
    """A job scoring ~63 pts — well below the 100-pt clamp so penalties/boosts are visible."""
    defaults = dict(
        title="Infrastructure Manager",
        description="Systems management role",
        is_remote=False,
        is_onsite=True,
        dfw_match=True,
        us_match=True,
        role_family_tags=["infrastructure"],
        seniority_hints=[],
        estimated_total_comp_min=260_000,
        estimated_total_comp_max=310_000,
    )
    defaults.update(kwargs)
    return make_job(**defaults)


def test_compensation_absent_penalty() -> None:
    scorer = JobScorer(SearchProfile())
    rules = SearchProfile().scoring_rules

    job_with = _moderate_job(source_job_id="1")
    job_without = _moderate_job(
        source_job_id="2",
        estimated_total_comp_min=None,
        estimated_total_comp_max=None,
        base_min=None,
        base_max=None,
    )

    result_with = scorer.score(job_with)
    result_without = scorer.score(job_without)

    assert result_without.total_score < result_with.total_score
    assert any("Compensation not listed" in c for c in result_without.concerns)
    assert result_with.total_score - result_without.total_score >= rules.compensation_absent_penalty


def test_low_compensation_penalty() -> None:
    scorer = JobScorer(SearchProfile())
    floor = SearchProfile().compensation.hard_floor_total_comp

    job_above = _moderate_job(
        source_job_id="1",
        estimated_total_comp_min=310_000,
        estimated_total_comp_max=380_000,
    )
    job_below = _moderate_job(
        source_job_id="2",
        estimated_total_comp_min=150_000,
        estimated_total_comp_max=200_000,
    )
    assert job_below.estimated_total_comp_max is not None
    assert job_below.estimated_total_comp_max < floor

    result_above = scorer.score(job_above)
    result_below = scorer.score(job_below)

    assert result_below.total_score < result_above.total_score
    assert any("below hard floor" in c for c in result_below.concerns)


def test_heavy_travel_penalty() -> None:
    scorer = JobScorer(SearchProfile())
    rules = SearchProfile().scoring_rules

    job_no_travel = _moderate_job(source_job_id="1")
    job_heavy_travel = _moderate_job(source_job_id="2", travel_percent=50)

    result_no = scorer.score(job_no_travel)
    result_heavy = scorer.score(job_heavy_travel)

    assert result_heavy.total_score < result_no.total_score
    assert result_no.total_score - result_heavy.total_score >= rules.heavy_travel_penalty
    assert any("Travel" in c for c in result_heavy.concerns)


def test_infra_analytics_boost() -> None:
    scorer = JobScorer(SearchProfile())
    rules = SearchProfile().scoring_rules

    job_no_boost = _moderate_job(source_job_id="1", role_family_tags=["infrastructure"])
    job_with_boost = _moderate_job(
        source_job_id="2",
        role_family_tags=["infrastructure", "analytics"],
    )

    result_no = scorer.score(job_no_boost)
    result_with = scorer.score(job_with_boost)

    assert result_with.total_score >= result_no.total_score + rules.infra_analytics_boost


def test_senior_title_boost() -> None:
    scorer = JobScorer(SearchProfile())
    rules = SearchProfile().scoring_rules

    job_no_boost = _moderate_job(source_job_id="1", title="Infrastructure Manager")
    job_with_boost = _moderate_job(source_job_id="2", title="Staff Infrastructure Manager")

    result_no = scorer.score(job_no_boost)
    result_with = scorer.score(job_with_boost)

    assert result_with.total_score >= result_no.total_score + rules.senior_title_boost


def test_remote_boost() -> None:
    scorer = JobScorer(SearchProfile())

    job_onsite = _moderate_job(source_job_id="1")
    job_remote = _moderate_job(source_job_id="2", is_remote=True, is_onsite=False)

    result_onsite = scorer.score(job_onsite)
    result_remote = scorer.score(job_remote)

    assert result_remote.total_score > result_onsite.total_score


def test_parse_confidence_penalty() -> None:
    scorer = JobScorer(SearchProfile())

    job_high_conf = _moderate_job(source_job_id="1", parse_confidence=1.0)
    job_low_conf = _moderate_job(source_job_id="2", parse_confidence=0.50)

    result_high = scorer.score(job_high_conf)
    result_low = scorer.score(job_low_conf)

    expected_penalty = (0.75 - 0.50) * 20.0
    assert result_low.total_score < result_high.total_score
    assert result_high.total_score - result_low.total_score >= expected_penalty


def test_seniority_hint_scoring() -> None:
    scorer = JobScorer(SearchProfile())

    job = make_job(
        title="Infrastructure Engineer",
        seniority_hints=["staff"],
    )

    result = scorer.score(job)

    assert result.dimension_scores["role_seniority_fit"] >= 78.0


def test_configurable_dimension_thresholds() -> None:
    high_bar_rules = ScoringRules(positive_dimension_min=85, negative_dimension_max=40)
    profile = SearchProfile(scoring_rules=high_bar_rules)
    scorer = JobScorer(profile)

    job = make_job()
    result = scorer.score(job)

    for reason in result.reasons:
        matching_dim = next(
            (dim for dim, score in result.dimension_scores.items() if score >= 85.0),
            None,
        )
        assert matching_dim is not None, f"Reason listed but no dimension >= 85: {reason}"
