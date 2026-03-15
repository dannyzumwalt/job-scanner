from __future__ import annotations

from dataclasses import dataclass

from .models import SourceType


@dataclass(frozen=True)
class SourceCapability:
    supports_live_fetch: bool
    supports_validation: bool
    supports_parser_template: bool
    structured_feed: bool
    allowed_parser_template_keys: frozenset[str]
    required_parser_template_keys: frozenset[str] = frozenset()


COMMON_TEMPLATE_KEYS = frozenset(
    {
        "id_field",
        "alternate_id_field",
        "title_field",
        "description_field",
        "location_field",
        "apply_url_field",
        "requisition_id_field",
        "company_field",
        "salary_text_field",
        "base_min_field",
        "base_max_field",
        "bonus_field",
        "bonus_percent_field",
        "equity_field",
    }
)


CAPABILITIES: dict[SourceType, SourceCapability] = {
    SourceType.GREENHOUSE: SourceCapability(True, True, True, True, COMMON_TEMPLATE_KEYS),
    SourceType.LEVER: SourceCapability(True, True, True, True, COMMON_TEMPLATE_KEYS),
    SourceType.ASHBY: SourceCapability(True, True, True, True, COMMON_TEMPLATE_KEYS),
    SourceType.GENERIC: SourceCapability(True, True, True, True, COMMON_TEMPLATE_KEYS.union({"items_path"})),
    SourceType.GENERIC_JSON: SourceCapability(True, True, True, True, COMMON_TEMPLATE_KEYS.union({"items_path"})),
    SourceType.RSS: SourceCapability(
        True,
        True,
        True,
        True,
        frozenset(
            {
                "items_tag",
                "id_field",
                "title_field",
                "description_field",
                "location_field",
                "apply_url_field",
                "requisition_id_field",
                "salary_text_field",
            }
        ),
    ),
    SourceType.GENERIC_HTML: SourceCapability(
        True,
        True,
        True,
        False,
        frozenset(
            {
                "items_selector",
                "title_selector",
                "title_attr",
                "apply_url_selector",
                "apply_url_attr",
                "description_selector",
                "location_selector",
                "requisition_selector",
                "compensation_selector",
                "company_selector",
                "source_job_id_selector",
                "source_job_id_attr",
            }
        ),
        required_parser_template_keys=frozenset({"items_selector", "title_selector", "apply_url_selector"}),
    ),
    SourceType.IMPORT: SourceCapability(
        False,
        False,
        True,
        True,
        frozenset(),
    ),
}


def get_capability(source_type: SourceType) -> SourceCapability:
    return CAPABILITIES[source_type]
