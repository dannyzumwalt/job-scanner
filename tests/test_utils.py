from job_scanner.utils import (
    extract_bonus_amount,
    extract_bonus_percent,
    extract_equity_amount,
    extract_travel_percent,
    normalize_title,
    parse_comp_values_from_text,
    parse_money_value,
    parse_percent_value,
)


def test_parse_comp_values_from_text_range() -> None:
    low, high = parse_comp_values_from_text("Compensation range is $300k - $425k total")
    assert low == 300000
    assert high == 425000


def test_parse_comp_values_single_value() -> None:
    low, high = parse_comp_values_from_text("Salary: $350,000")
    assert low == 350000
    assert high == 350000


def test_parse_comp_ignores_unrealistic_values() -> None:
    low, high = parse_comp_values_from_text("Reference ID 123,456,789,012 and budget marker 9,999,999,999")
    assert low is None
    assert high is None


def test_extract_bonus_and_equity_values() -> None:
    text = "Comp includes target bonus 15% and equity $75k"
    assert extract_bonus_percent(text) == 15.0
    assert extract_equity_amount(text) == 75000


def test_extract_bonus_amount_from_text() -> None:
    text = "Expected annual bonus up to $30,000 depending on performance"
    assert extract_bonus_amount(text) == 30000


def test_parse_money_and_percent_values() -> None:
    assert parse_money_value("$320k") == 320000
    assert parse_percent_value("12.5%") == 12.5


def test_extract_travel_percent() -> None:
    assert extract_travel_percent("This role has 15% travel") == 15
    assert extract_travel_percent("No travel expected") is None


def test_normalize_title() -> None:
    assert normalize_title(" Principal, Infrastructure Engineer ") == "principal infrastructure engineer"
