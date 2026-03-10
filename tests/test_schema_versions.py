from __future__ import annotations

import ast
import inspect
import textwrap
from collections.abc import Callable

from cpapacket.deliverables import (
    balance_sheet,
    contractor_summary,
    distributions,
    general_ledger,
    payroll_recon,
    payroll_summary,
    pnl,
    tax_tracker,
)
from cpapacket.utils.constants import SCHEMA_VERSIONS
from cpapacket.writers import retained_earnings as retained_earnings_writer

_CSV_V1_HEADERS: dict[str, tuple[str, ...]] = {
    "pnl": ("section", "level", "row_type", "label", "amount", "path"),
    "balance_sheet": ("section", "level", "row_type", "label", "amount", "path"),
    "general_ledger": (
        "txn_id",
        "date",
        "transaction_type",
        "document_number",
        "account_name",
        "account_type",
        "payee",
        "memo",
        "debit",
        "credit",
        "signed_amount",
    ),
    "distributions": ("year", "distribution_total", "miscoded_candidate_count", "owner_keywords"),
    "payroll_summary": (
        "year",
        "run_count",
        "wages_total",
        "employee_taxes_total",
        "employer_taxes_total",
        "employee_retirement_deferral_total",
        "employer_retirement_contribution_total",
        "payroll_cost_total",
    ),
    "contractor": (
        "vendor_id",
        "display_name",
        "tax_id_on_file",
        "total_paid",
        "card_processor_total",
        "non_card_total",
        "requires_1099_review",
        "flags",
    ),
    "estimated_tax": ("jurisdiction", "due_date", "amount", "status", "paid_date", "last_updated"),
    "payroll_recon": ("year", "gusto_total", "qbo_total", "variance", "status", "tolerance"),
    "retained_earnings": (
        "year",
        "beginning_re",
        "net_income",
        "distributions",
        "expected_ending_re",
        "actual_ending_re",
        "difference",
        "status",
        "flags",
        "miscoded_distribution_count",
    ),
}


def _extract_fieldnames_from_callable(target: Callable[..., object]) -> tuple[str, ...]:
    source = textwrap.dedent(inspect.getsource(target))
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in {"write_rows", "write_rows_streaming"}:
            continue
        for keyword in node.keywords:
            if keyword.arg != "fieldnames":
                continue
            resolved = _resolve_fieldnames_node(keyword.value, target.__globals__)
            if resolved:
                return resolved
    raise AssertionError(f"No CSV fieldnames found for {target.__qualname__}")


def _resolve_fieldnames_node(
    node: ast.AST,
    namespace: dict[str, object],
) -> tuple[str, ...] | None:
    if isinstance(node, ast.List):
        values = [_literal_str(element) for element in node.elts]
        if all(value is not None for value in values):
            return tuple(value for value in values if value is not None)
        return None

    if isinstance(node, ast.Tuple):
        values = [_literal_str(element) for element in node.elts]
        if all(value is not None for value in values):
            return tuple(value for value in values if value is not None)
        return None

    if isinstance(node, ast.Name):
        value = namespace.get(node.id)
        if isinstance(value, tuple) and all(isinstance(item, str) for item in value):
            return value
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            return tuple(value)
        return None

    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "list"
        and len(node.args) == 1
    ):
        return _resolve_fieldnames_node(node.args[0], namespace)

    return None


def _literal_str(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def test_schema_versions_keys_match_expected_csv_contracts() -> None:
    assert set(SCHEMA_VERSIONS) == set(_CSV_V1_HEADERS)
    assert all(version_map.get("csv") == "1.0" for version_map in SCHEMA_VERSIONS.values())


def test_schema_version_v1_headers_match_actual_writer_column_order() -> None:
    actual_headers: dict[str, tuple[str, ...]] = {
        "pnl": _extract_fieldnames_from_callable(pnl._write_csv),
        "balance_sheet": _extract_fieldnames_from_callable(balance_sheet._write_csv),
        "general_ledger": _extract_fieldnames_from_callable(
            general_ledger.GeneralLedgerDeliverable.generate
        ),
        "distributions": _extract_fieldnames_from_callable(distributions._write_summary_csv),
        "payroll_summary": _extract_fieldnames_from_callable(
            payroll_summary._write_company_summary_csv
        ),
        "contractor": _extract_fieldnames_from_callable(contractor_summary._write_contractor_csv),
        "estimated_tax": _extract_fieldnames_from_callable(tax_tracker._write_tracker_csv),
        "payroll_recon": _extract_fieldnames_from_callable(payroll_recon._write_reconciliation_csv),
        "retained_earnings": _extract_fieldnames_from_callable(
            retained_earnings_writer.write_rollforward_csv
        ),
    }

    assert actual_headers == _CSV_V1_HEADERS
