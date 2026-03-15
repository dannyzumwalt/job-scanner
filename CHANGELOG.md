# Changelog

## Unreleased
- Added `sources discover` command to recommend new sources from a broader catalog using profile and criteria markdown alignment.
- Added discovery support for multi-company structured feeds (JSON/RSS) alongside ATS candidates.
- Added discovery write/append workflows for scaling `config/sources.yaml` without one-by-one manual entry.

## 1.0.0 - 2026-03-15
- Added strict source validation with schema/parse checks and parser-template contract enforcement.
- Added deep-profile source health gate support (`min_healthy_sources`, default `15`) with CLI/report visibility.
- Implemented constrained `generic_html` connector (template-driven static parsing, opt-in).
- Upgraded compensation extraction with structured base/bonus/equity hints plus confidence and quality flags.
- Added v1 fixture coverage for strict validation, generic connectors, health gate behavior, and sample report reproducibility.
