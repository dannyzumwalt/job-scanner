from __future__ import annotations

from .models import MatchCategory, NormalizedJob, ScoreResult, SearchProfile
from .utils import clamp_score


class JobScorer:
    def __init__(self, profile: SearchProfile) -> None:
        self.profile = profile

    def score(self, job: NormalizedJob) -> ScoreResult:
        dimensions = {
            "compensation_fit": self._score_compensation(job),
            "role_seniority_fit": self._score_role_seniority(job),
            "technical_domain_fit": self._score_technical_domain(job),
            "analytics_data_fit": self._score_analytics_data(job),
            "infrastructure_reliability_fit": self._score_infrastructure_reliability(job),
            "remote_fit": self._score_remote(job),
            "location_fit": self._score_location(job),
            "travel_fit": self._score_travel(job),
            "leadership_autonomy_fit": self._score_leadership(job),
            "title_relevance": self._score_title_relevance(job),
        }

        weights = self.profile.scoring_weights.model_dump()
        weighted_score = sum(dimensions[name] * weights[name] for name in dimensions)

        reasons: list[str] = []
        concerns: list[str] = []
        adjustment = 0.0

        text_blob = f"{job.title} {job.description}".lower()
        rules = self.profile.scoring_rules

        if job.estimated_total_comp_max is None and job.estimated_total_comp_min is None:
            adjustment -= rules.compensation_absent_penalty
            concerns.append("Compensation not listed; upside is uncertain")

        floor = self.profile.compensation.hard_floor_total_comp
        if job.estimated_total_comp_max is not None and job.estimated_total_comp_max < floor:
            adjustment -= rules.low_compensation_penalty
            concerns.append("Compensation appears below hard floor")

        if job.travel_percent is not None and job.travel_percent > self.profile.work_preferences.reject_travel_percent_over:
            adjustment -= rules.heavy_travel_penalty
            concerns.append("Travel requirement appears too high")

        if job.is_onsite and not job.dfw_match and not job.is_remote:
            adjustment -= rules.non_dfw_onsite_penalty
            concerns.append("Onsite requirement appears outside DFW preference")

        if self._contains_any(text_blob, self.profile.role_preferences.negative_keywords):
            adjustment -= rules.disallowed_role_penalty
            concerns.append("Role contains negative fit keywords")

        has_infra = "infrastructure" in job.role_family_tags or "reliability" in job.role_family_tags
        has_analytics = "analytics" in job.role_family_tags
        if has_infra and has_analytics:
            adjustment += rules.infra_analytics_boost
            reasons.append("Strong infrastructure plus analytics signal")

        if self._contains_any(job.title.lower(), ["staff", "principal", "distinguished", "architect"]):
            adjustment += rules.senior_title_boost
            reasons.append("Title suggests senior technical IC level")

        if job.is_remote:
            adjustment += rules.remote_boost
            reasons.append("Remote-compatible role")

        total_score = clamp_score(weighted_score + adjustment)
        display_score = round(total_score / 10.0, 1)

        top_dimensions = sorted(dimensions.items(), key=lambda item: item[1], reverse=True)[:3]
        for name, value in top_dimensions:
            if value >= 75:
                reasons.append(f"{name.replace('_', ' ').title()} is strong")

        low_dimensions = sorted(dimensions.items(), key=lambda item: item[1])[:2]
        for name, value in low_dimensions:
            if value <= 45:
                concerns.append(f"{name.replace('_', ' ').title()} is weak")

        reasons = self._dedupe_messages(reasons)
        concerns = self._dedupe_messages(concerns)

        category = self._category_from_score(total_score)
        recommended_action = self._action_from_category(category)

        return ScoreResult(
            total_score=round(total_score, 2),
            display_score=display_score,
            category=category,
            recommended_action=recommended_action,
            dimension_scores={k: round(v, 2) for k, v in dimensions.items()},
            reasons=reasons,
            concerns=concerns,
        )

    def _score_compensation(self, job: NormalizedJob) -> float:
        target_min = self.profile.compensation.target_total_comp_min
        target_max = self.profile.compensation.target_total_comp_max
        hard_floor = self.profile.compensation.hard_floor_total_comp

        est_min = job.estimated_total_comp_min
        est_max = job.estimated_total_comp_max

        if est_min is None and est_max is None:
            return 45.0
        if est_max is not None and est_max < hard_floor:
            return 10.0
        if est_min is not None and est_min >= target_min:
            return 95.0 if (est_max or est_min) >= target_max else 88.0
        if est_max is not None and est_max >= target_min:
            return 78.0
        return 40.0

    def _score_role_seniority(self, job: NormalizedJob) -> float:
        title = job.title.lower()
        if any(token in title for token in ("distinguished", "principal", "staff", "architect")):
            return 95.0
        if "senior" in title or "lead" in title:
            return 78.0
        if any(token in title for token in ("junior", "entry", "intern")):
            return 15.0
        return 55.0

    def _score_technical_domain(self, job: NormalizedJob) -> float:
        preferred = set(self.profile.role_preferences.target_role_families)
        if not preferred:
            return 50.0
        overlap = len(preferred.intersection(set(job.role_family_tags)))
        if overlap >= 3:
            return 95.0
        if overlap == 2:
            return 82.0
        if overlap == 1:
            return 68.0
        return 40.0

    def _score_analytics_data(self, job: NormalizedJob) -> float:
        tags = set(job.role_family_tags)
        description = job.description.lower()
        if "analytics" in tags or "data" in description or "observability" in description:
            return 85.0
        return 45.0

    def _score_infrastructure_reliability(self, job: NormalizedJob) -> float:
        tags = set(job.role_family_tags)
        title = job.title.lower()
        if "infrastructure" in tags and "reliability" in tags:
            return 95.0
        if "sre" in title or "reliability" in title or "infrastructure" in title:
            return 85.0
        if "platform" in title:
            return 72.0
        return 40.0

    def _score_remote(self, job: NormalizedJob) -> float:
        if job.is_remote:
            return 100.0
        if job.is_hybrid:
            return 70.0
        return 35.0

    def _score_location(self, job: NormalizedJob) -> float:
        if job.is_remote:
            return 100.0
        if job.dfw_match:
            return 90.0
        if job.us_match:
            return 60.0
        return 20.0

    def _score_travel(self, job: NormalizedJob) -> float:
        if job.travel_percent is None:
            return 78.0
        if job.travel_percent <= self.profile.work_preferences.max_travel_percent_preferred:
            return 95.0
        if job.travel_percent <= self.profile.work_preferences.reject_travel_percent_over:
            return 55.0
        return 10.0

    def _score_leadership(self, job: NormalizedJob) -> float:
        blob = f"{job.title} {job.description}".lower()
        if any(token in blob for token in ("technical leadership", "technical strategy", "ownership", "architect")):
            return 88.0
        if any(token in blob for token in ("lead", "mentor", "cross-functional")):
            return 72.0
        return 55.0

    def _score_title_relevance(self, job: NormalizedJob) -> float:
        title = job.title.lower()
        targets = [token.lower() for token in self.profile.role_preferences.target_levels]
        if any(target in title for target in targets):
            return 90.0
        if any(token in title for token in ("engineer", "sre", "architect")):
            return 65.0
        return 40.0

    def _category_from_score(self, score: float) -> MatchCategory:
        rules = self.profile.scoring_rules
        if score >= rules.strong_match_min:
            return MatchCategory.STRONG
        if score >= rules.good_match_min:
            return MatchCategory.GOOD
        if score >= rules.possible_match_min:
            return MatchCategory.POSSIBLE
        return MatchCategory.REJECT

    @staticmethod
    def _action_from_category(category: MatchCategory) -> str:
        if category in (MatchCategory.STRONG, MatchCategory.GOOD):
            return "pursue"
        if category == MatchCategory.POSSIBLE:
            return "review manually"
        return "reject"

    @staticmethod
    def _contains_any(text: str, tokens: list[str]) -> bool:
        return any(token.lower() in text for token in tokens)

    @staticmethod
    def _dedupe_messages(items: list[str]) -> list[str]:
        deduped: list[str] = []
        seen = set()
        for item in items:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped
