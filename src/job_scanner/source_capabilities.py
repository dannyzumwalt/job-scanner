from __future__ import annotations

from dataclasses import dataclass

from .models import SourceType


@dataclass(frozen=True)
class SourceCapability:
    supports_live_fetch: bool
    supports_validation: bool
    supports_parser_template: bool
    structured_feed: bool


CAPABILITIES: dict[SourceType, SourceCapability] = {
    SourceType.GREENHOUSE: SourceCapability(True, True, True, True),
    SourceType.LEVER: SourceCapability(True, True, True, True),
    SourceType.ASHBY: SourceCapability(True, True, True, True),
    SourceType.GENERIC: SourceCapability(True, True, True, True),
    SourceType.GENERIC_JSON: SourceCapability(True, True, True, True),
    SourceType.RSS: SourceCapability(True, True, True, True),
    SourceType.GENERIC_HTML: SourceCapability(True, True, True, False),
    SourceType.IMPORT: SourceCapability(False, False, True, True),
}


def get_capability(source_type: SourceType) -> SourceCapability:
    return CAPABILITIES[source_type]
