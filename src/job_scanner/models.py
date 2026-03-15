from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class MatchCategory(str, Enum):
    STRONG = "strong"
    GOOD = "good"
    POSSIBLE = "possible"
    REJECT = "reject"


class SourceType(str, Enum):
    GREENHOUSE = "greenhouse"
    LEVER = "lever"
    ASHBY = "ashby"
    GENERIC = "generic"


class CompensationPreferences(BaseModel):
    target_total_comp_min: int = 300_000
    target_total_comp_max: int = 400_000
    hard_floor_total_comp: int = 250_000


class WorkPreferences(BaseModel):
    remote_preferred: bool = True
    remote_first_preferred: bool = True
    dfw_acceptable: bool = True
    allowed_locations: list[str] = Field(default_factory=list)
    max_travel_percent_preferred: int = 10
    reject_travel_percent_over: int = 20


class RolePreferences(BaseModel):
    target_levels: list[str] = Field(default_factory=list)
    target_role_families: list[str] = Field(default_factory=list)
    positive_keywords: list[str] = Field(default_factory=list)
    negative_keywords: list[str] = Field(default_factory=list)


class ScoringWeights(BaseModel):
    compensation_fit: float = 0.18
    role_seniority_fit: float = 0.14
    technical_domain_fit: float = 0.12
    analytics_data_fit: float = 0.10
    infrastructure_reliability_fit: float = 0.12
    remote_fit: float = 0.08
    location_fit: float = 0.08
    travel_fit: float = 0.08
    leadership_autonomy_fit: float = 0.05
    title_relevance: float = 0.05

    @model_validator(mode="after")
    def validate_total_weight(self) -> "ScoringWeights":
        total = sum(self.model_dump().values())
        if not (0.99 <= total <= 1.01):
            raise ValueError(f"scoring_weights must sum to ~1.0, found {total:.4f}")
        return self


class ScoringRules(BaseModel):
    strong_match_min: int = 90
    good_match_min: int = 75
    possible_match_min: int = 60
    compensation_absent_penalty: int = 8
    low_compensation_penalty: int = 25
    heavy_travel_penalty: int = 25
    non_dfw_onsite_penalty: int = 18
    disallowed_role_penalty: int = 35
    infra_analytics_boost: int = 10
    senior_title_boost: int = 8
    remote_boost: int = 8


class IngestionConfig(BaseModel):
    request_timeout_seconds: float = 25.0
    request_retries: int = 2
    retry_backoff_seconds: float = 0.5
    min_request_interval_seconds: float = 0.25
    max_workers: int = 4


class SearchProfile(BaseModel):
    profile_name: str = "default"
    primary_geography: str = "United States"
    compensation: CompensationPreferences = Field(default_factory=CompensationPreferences)
    work_preferences: WorkPreferences = Field(default_factory=WorkPreferences)
    role_preferences: RolePreferences = Field(default_factory=RolePreferences)
    scoring_weights: ScoringWeights = Field(default_factory=ScoringWeights)
    scoring_rules: ScoringRules = Field(default_factory=ScoringRules)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)


class SourceConfig(BaseModel):
    name: str
    type: SourceType
    enabled: bool = True
    url: str
    notes: str = ""
    headers: dict[str, str] = Field(default_factory=dict)


class SourcesFile(BaseModel):
    sources: list[SourceConfig]


class AppConfig(BaseModel):
    root_dir: str
    db_path: str
    search_profile_path: str
    sources_path: str
    report_dir: str
    raw_dir: str
    processed_dir: str
    profile: SearchProfile
    sources: list[SourceConfig]


class RawJob(BaseModel):
    source_name: str
    source_type: SourceType
    source_url: str
    source_job_id: str
    payload: dict[str, Any]
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class NormalizedJob(BaseModel):
    source_name: str
    source_type: SourceType
    source_job_id: str
    source_url: str

    requisition_id: str | None = None
    apply_url: str | None = None

    company: str
    title: str
    normalized_title: str
    description: str

    location: str | None = None
    normalized_location: str | None = None
    country: str | None = None

    is_remote: bool = False
    is_hybrid: bool = False
    is_onsite: bool = False
    dfw_match: bool = False
    us_match: bool = True

    travel_required: bool = False
    travel_percent: int | None = None

    base_min: int | None = None
    base_max: int | None = None
    bonus: int | None = None
    equity: int | None = None
    estimated_total_comp_min: int | None = None
    estimated_total_comp_max: int | None = None
    compensation_confidence: float = 0.0

    role_family_tags: list[str] = Field(default_factory=list)
    seniority_hints: list[str] = Field(default_factory=list)

    dedupe_key: str
    duplicate_count: int = 1
    job_hash: str = ""


class ScoreResult(BaseModel):
    total_score: float
    display_score: float
    category: MatchCategory
    recommended_action: Literal["pursue", "review manually", "reject"]
    dimension_scores: dict[str, float]
    reasons: list[str] = Field(default_factory=list)
    concerns: list[str] = Field(default_factory=list)


class ScoredJob(BaseModel):
    normalized_job_id: int
    scan_id: int
    normalized: NormalizedJob
    score: ScoreResult
    is_new: bool = False


class ScanSummary(BaseModel):
    scan_id: int
    started_at: datetime
    completed_at: datetime | None = None
    status: Literal["running", "completed", "failed"] = "running"
    raw_count: int = 0
    normalized_count: int = 0
    scored_count: int = 0
    inactive_marked: int = 0
    report_path: str | None = None


class ScanDiff(BaseModel):
    current_scan_id: int
    baseline_scan_id: int
    new_jobs: list[dict[str, Any]] = Field(default_factory=list)
    removed_jobs: list[dict[str, Any]] = Field(default_factory=list)
    changed_jobs: list[dict[str, Any]] = Field(default_factory=list)
