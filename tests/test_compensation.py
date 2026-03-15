from job_scanner.sources.common import parse_comp_and_confidence


def test_parse_comp_structured_components_high_confidence() -> None:
    base_min, base_max, bonus, equity, total_min, total_max, confidence, flags = parse_comp_and_confidence(
        "Compensation details available",
        base_min_hint=260000,
        base_max_hint=320000,
        bonus_hint=40000,
        equity_hint=80000,
    )

    assert (base_min, base_max) == (260000, 320000)
    assert bonus == 40000
    assert equity == 80000
    assert total_min == 380000
    assert total_max == 440000
    assert confidence >= 0.9
    assert "comp_missing" not in flags


def test_parse_comp_bonus_percent_falls_back_to_base() -> None:
    base_min, base_max, bonus, equity, total_min, total_max, confidence, flags = parse_comp_and_confidence(
        "Base salary $280k - $340k with bonus target 15%",
        bonus_percent_hint="15%",
    )

    assert (base_min, base_max) == (280000, 340000)
    assert bonus == 51000
    assert equity is None
    assert total_min == 331000
    assert total_max == 391000
    assert confidence > 0.7
    assert "bonus_unresolved" not in flags


def test_parse_comp_missing_marks_quality_flags() -> None:
    _, _, _, _, total_min, total_max, confidence, flags = parse_comp_and_confidence(
        "Compensation depends on experience",
    )
    assert total_min is None
    assert total_max is None
    assert confidence < 0.4
    assert "comp_missing" in flags
