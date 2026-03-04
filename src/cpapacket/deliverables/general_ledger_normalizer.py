"""QBO GeneralLedgerDetail normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from cpapacket.models.general_ledger import GeneralLedgerRow

_ZERO = Decimal("0.00")


def normalize_general_ledger_report(report_payload: Mapping[str, Any]) -> list[GeneralLedgerRow]:
    """Parse QBO GeneralLedgerDetail payload into canonical ledger rows."""
    rows_node = report_payload.get("Rows", {})
    rows = rows_node.get("Row", []) if isinstance(rows_node, Mapping) else []
    if not isinstance(rows, list):
        return []

    normalized: list[GeneralLedgerRow] = []
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            continue
        normalized.append(_normalize_row(index=index, row=raw))
    return normalized


def _normalize_row(*, index: int, row: Mapping[str, Any]) -> GeneralLedgerRow:
    txn_id = _required_text(row, "TxnId")
    txn_date = _parse_date(_required_text(row, "TxnDate"))
    transaction_type = (
        _first_text(row, ("TxnType", "TransactionType", "Description")) or "General Ledger"
    )
    document_number = _first_text(row, ("DocNum", "DocumentNumber")) or txn_id
    account_name = _required_text(row, "AccountName")
    account_type = _first_text(row, ("AccountType",)) or "Unknown"
    payee = _first_text(row, ("Payee", "Name", "EntityName"))
    memo = _first_text(row, ("Memo",))

    debit_raw = row.get("Debit")
    credit_raw = row.get("Credit")
    if debit_raw is not None or credit_raw is not None:
        debit = _parse_decimal_or_zero(debit_raw)
        credit = _parse_decimal_or_zero(credit_raw)
    else:
        amount = _parse_decimal_or_zero(row.get("Amount"))
        if amount >= _ZERO:
            debit = amount
            credit = _ZERO
        else:
            debit = _ZERO
            credit = abs(amount)

    try:
        return GeneralLedgerRow(
            txn_id=txn_id,
            date=txn_date,
            transaction_type=transaction_type,
            document_number=document_number,
            account_name=account_name,
            account_type=account_type,
            payee=payee,
            memo=memo,
            debit=debit,
            credit=credit,
        )
    except Exception as exc:
        raise ValueError(f"invalid GL row at index {index}: {exc}") from exc


def _required_text(row: Mapping[str, Any], key: str) -> str:
    raw = str(row.get(key, "")).strip()
    if not raw:
        raise ValueError(f"missing required field: {key}")
    return raw


def _first_text(row: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        raw = str(row.get(key, "")).strip()
        if raw:
            return raw
    return None


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid date value: {value!r}") from exc


def _parse_decimal_or_zero(value: Any) -> Decimal:
    if value in (None, ""):
        return _ZERO
    try:
        parsed = Decimal(str(value).replace(",", "").replace("$", "").strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"invalid decimal value: {value!r}")
    return parsed
