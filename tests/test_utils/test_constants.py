from decimal import Decimal

from cpapacket.utils import constants


def test_deliverable_folders_include_required_keys() -> None:
    expected = {
        "pnl",
        "balance_sheet",
        "general_ledger",
        "payroll_summary",
        "officer_w2",
        "distributions",
        "contractor",
        "estimated_tax",
        "retained_earnings",
        "payroll_recon",
        "meta",
    }
    assert set(constants.DELIVERABLE_FOLDERS) == expected
    assert constants.DELIVERABLE_FOLDERS["meta"] == "_meta"


def test_decimal_threshold_constants_use_decimal_type() -> None:
    decimal_constants = (
        constants.BALANCE_EQUATION_TOLERANCE,
        constants.PAYROLL_RECON_TOLERANCE,
        constants.RETAINED_EARNINGS_TOLERANCE,
        constants.CONTRACTOR_1099_THRESHOLD,
        constants.MISCODE_HIGH_AMOUNT_THRESHOLD,
    )
    assert all(isinstance(value, Decimal) for value in decimal_constants)
    assert Decimal("600.00") == constants.CONTRACTOR_1099_THRESHOLD


def test_numeric_limits_are_positive_and_ordered() -> None:
    assert constants.RETRY_MAX_429 > 0
    assert constants.RETRY_MAX_5XX > 0
    assert constants.QBO_MAX_CONCURRENCY > 0
    assert constants.GUSTO_MAX_CONCURRENCY > 0
    assert constants.CACHE_TTL_HOURS > 0
    assert constants.MISCODE_ROUND_NUMBER_DIVISOR > 0
    assert constants.MISCODE_CONFIDENCE_HIGH >= constants.MISCODE_CONFIDENCE_MEDIUM
    assert constants.MISCODE_CONFIDENCE_MEDIUM >= constants.MISCODE_CONFIDENCE_LOW


def test_schema_versions_cover_deliverables() -> None:
    schema_keys = set(constants.SCHEMA_VERSIONS)
    folder_keys = set(constants.DELIVERABLE_FOLDERS) - {"meta", "officer_w2"}
    assert schema_keys == folder_keys
    assert all(version_map["csv"] == "1.0" for version_map in constants.SCHEMA_VERSIONS.values())
