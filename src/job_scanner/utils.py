from __future__ import annotations

import hashlib
import json
import re
from typing import Any

MONEY_TOKEN_RE = re.compile(r"\$?\s*(\d{2,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*([kKmM])?")
RANGE_SEP_RE = re.compile(r"\s*(?:-|to|–|—)\s*", re.IGNORECASE)
TRAVEL_RE = re.compile(r"(\d{1,2})\s*%\s*(?:travel|of travel)", re.IGNORECASE)
MIN_REASONABLE_COMP = 30_000
MAX_REASONABLE_COMP = 5_000_000


def compact_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def strip_html_tags(value: str | None) -> str:
    if not value:
        return ""
    text = re.sub(r"<[^>]+>", " ", value)
    return compact_whitespace(text)


def extract_by_path(payload: Any, path: str | None, default: Any = None) -> Any:
    if path is None or path == "":
        return default
    current = payload
    for token in path.split("."):
        token = token.strip()
        if token == "":
            continue
        if isinstance(current, dict):
            current = current.get(token)
        elif isinstance(current, list):
            try:
                index = int(token)
            except ValueError:
                return default
            if index < 0 or index >= len(current):
                return default
            current = current[index]
        else:
            return default
        if current is None:
            return default
    return current


def value_as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return compact_whitespace(value)
    if isinstance(value, (int, float)):
        return str(value)
    return compact_whitespace(str(value))


def coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = value_as_text(value).lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def normalize_title(title: str) -> str:
    clean = compact_whitespace(title).lower()
    clean = re.sub(r"[^a-z0-9\s/+&-]", "", clean)
    return clean


def normalize_location(location: str | None) -> str:
    clean = compact_whitespace(location).lower()
    if not clean:
        return ""
    replacements = {
        "dallas-fort worth": "dfw",
        "dallas fort worth": "dfw",
        "dallas-fortworth": "dfw",
        "fort worth": "fort worth",
        "remote - united states": "remote us",
        "remote, us": "remote us",
    }
    for raw, fixed in replacements.items():
        clean = clean.replace(raw, fixed)
    return re.sub(r"[^a-z0-9\s,/-]", "", clean)


def location_is_dfw(normalized_location: str) -> bool:
    return any(token in normalized_location for token in ("dfw", "dallas", "fort worth"))


def location_is_us(normalized_location: str) -> bool:
    if not normalized_location:
        return True
    us_tokens = ("united states", "usa", "us", "remote us")
    return any(token in normalized_location for token in us_tokens) or not any(
        token in normalized_location
        for token in ("canada", "uk", "europe", "germany", "india", "australia")
    )


def extract_travel_percent(text: str) -> int | None:
    match = TRAVEL_RE.search(text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _money_to_int(value: str, suffix: str | None) -> int:
    base = float(value.replace(",", ""))
    if suffix:
        if suffix.lower() == "k":
            base *= 1_000
        elif suffix.lower() == "m":
            base *= 1_000_000
    return int(base)


def parse_comp_values_from_text(text: str | None) -> tuple[int | None, int | None]:
    if not text:
        return None, None

    stripped = compact_whitespace(text)
    candidates = []
    for match in MONEY_TOKEN_RE.finditer(stripped):
        value = _money_to_int(match.group(1), match.group(2))
        if MIN_REASONABLE_COMP <= value <= MAX_REASONABLE_COMP:
            candidates.append(value)

    if not candidates:
        return None, None

    if len(candidates) == 1:
        value = candidates[0]
        return value, value

    # Prefer explicit range chunks when present.
    if RANGE_SEP_RE.search(stripped):
        return min(candidates), max(candidates)

    # Fallback: best-effort range from first two amounts.
    first_two = candidates[:2]
    return min(first_two), max(first_two)


def estimate_total_comp(
    base_min: int | None,
    base_max: int | None,
    bonus: int | None,
    equity: int | None,
) -> tuple[int | None, int | None]:
    if base_min is None and base_max is None:
        return None, None

    b_min = base_min or base_max or 0
    b_max = base_max or base_min or 0
    bonus_val = bonus or 0
    equity_val = equity or 0
    return b_min + bonus_val + equity_val, b_max + bonus_val + equity_val


def detect_seniority_hints(text: str) -> list[str]:
    low = text.lower()
    hints = []
    for token in ("staff", "principal", "distinguished", "architect", "senior", "lead", "sre"):
        if token in low:
            hints.append(token)
    return sorted(set(hints))


def detect_role_family_tags(text: str) -> list[str]:
    low = text.lower()
    tags = []
    family_tokens = {
        "infrastructure": ["infrastructure", "platform", "distributed systems"],
        "operations": ["operations", "incident", "outage", "production"],
        "analytics": ["analytics", "analysis", "dashboard", "observability", "data"],
        "reliability": ["reliability", "sre", "availability", "resilience"],
        "network": ["network", "telecom", "routing", "switching"],
        "systems": ["systems", "architecture", "kernel"],
        "automation": ["automation", "scripting", "python", "tooling"],
    }
    for tag, words in family_tokens.items():
        if any(word in low for word in words):
            tags.append(tag)
    return sorted(set(tags))


def build_dedupe_key(
    company: str,
    normalized_title: str,
    normalized_location: str,
    apply_url: str | None,
    requisition_id: str | None,
) -> str:
    key = "|".join(
        [
            company.lower().strip(),
            normalized_title.strip(),
            normalized_location.strip(),
            (apply_url or "").strip().lower(),
            (requisition_id or "").strip().lower(),
        ]
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_job_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def clamp_score(value: float, lower: float = 0.0, upper: float = 100.0) -> float:
    return max(lower, min(upper, value))
