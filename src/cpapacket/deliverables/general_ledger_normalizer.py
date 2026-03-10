"""QBO GeneralLedgerDetail normalization helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Any

from cpapacket.models.general_ledger import GeneralLedgerRow

_ZERO = Decimal("0.00")


def normalize_general_ledger_report(report_payload: Mapping[str, Any]) -> list[GeneralLedgerRow]:
    """Parse QBO GeneralLedgerDetail payload into canonical ledger rows."""
    rows = _extract_row_list(report_payload.get("Rows"))
    if not rows:
        return []

    column_titles = _extract_column_titles(report_payload.get("Columns"))

    normalized: list[GeneralLedgerRow] = []
    for index, raw in enumerate(_iter_transaction_rows(rows=rows, column_titles=column_titles)):
        normalized.append(_normalize_row(index=index, row=raw))
    return normalized


def _normalize_row(*, index: int, row: Mapping[str, Any]) -> GeneralLedgerRow:
    txn_id = _resolve_txn_id(row)
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


def _resolve_txn_id(row: Mapping[str, Any]) -> str:
    explicit = _first_text(row, ("TxnId", "TransactionId", "Id"))
    if explicit:
        return explicit

    signature = "|".join(
        (
            _first_text(row, ("TxnDate",)) or "",
            _first_text(row, ("TxnType", "TransactionType", "Description")) or "",
            _first_text(row, ("DocNum", "DocumentNumber")) or "",
            _first_text(row, ("AccountName",)) or "",
            _first_text(row, ("AccountType",)) or "",
            _first_text(row, ("Payee", "Name", "EntityName")) or "",
            _first_text(row, ("Memo",)) or "",
            str(row.get("Amount", "")).strip(),
            str(row.get("Debit", "")).strip(),
            str(row.get("Credit", "")).strip(),
        )
    )
    digest = sha256(signature.encode("utf-8")).hexdigest()[:16]
    return f"derived-{digest}"


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


def _extract_row_list(node: Any) -> list[Mapping[str, Any]]:
    if isinstance(node, Mapping):
        rows = node.get("Row", [])
    elif isinstance(node, Sequence) and not isinstance(node, (str, bytes, bytearray)):
        rows = node
    else:
        return []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes, bytearray)):
        return []
    return [item for item in rows if isinstance(item, Mapping)]


def _extract_column_titles(node: Any) -> list[str]:
    if not isinstance(node, Mapping):
        return []
    columns = node.get("Column", [])
    if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes, bytearray)):
        return []
    titles: list[str] = []
    for column in columns:
        if not isinstance(column, Mapping):
            continue
        titles.append(str(column.get("ColTitle", "")).strip().lower())
    return titles


def _iter_transaction_rows(
    *,
    rows: list[Mapping[str, Any]],
    column_titles: list[str],
) -> list[Mapping[str, Any]]:
    output: list[Mapping[str, Any]] = []
    for row in rows:
        child_rows = _extract_row_list(row.get("Rows"))
        if child_rows:
            output.extend(_iter_transaction_rows(rows=child_rows, column_titles=column_titles))

        normalized = _normalize_report_row(row=row, column_titles=column_titles)
        if normalized is None:
            continue
        if not _looks_like_transaction_row(normalized):
            continue
        output.append(normalized)
    return output


def _normalize_report_row(
    *,
    row: Mapping[str, Any],
    column_titles: list[str],
) -> Mapping[str, Any] | None:
    if any(key in row for key in ("TxnDate", "AccountName", "Amount", "Debit", "Credit")):
        return row

    col_data = row.get("ColData")
    if not isinstance(col_data, Sequence) or isinstance(col_data, (str, bytes, bytearray)):
        return None

    mapped_values: dict[str, str] = {}
    for index, item in enumerate(col_data):
        if not isinstance(item, Mapping):
            continue
        title = column_titles[index] if index < len(column_titles) else ""
        value = str(item.get("value", "")).strip()
        if title:
            mapped_values[title] = value

    def _pick(*names: str) -> str:
        for name in names:
            value = mapped_values.get(name, "").strip()
            if value:
                return value
        return ""

    normalized: dict[str, str] = {
        "TxnDate": _pick("txn date", "date", "transaction date"),
        "TxnType": _pick("transaction type", "type"),
        "DocNum": _pick("num", "doc num", "document number", "document no."),
        "AccountName": _pick("account", "account name", "split", "split account"),
        "AccountType": _pick("account type"),
        "Payee": _pick("name", "payee"),
        "Memo": _pick("memo", "description", "memo/description"),
        "Amount": _pick("amount"),
        "Debit": _pick("debit"),
        "Credit": _pick("credit"),
    }

    explicit_txn_id = _first_text(row, ("TxnId", "TransactionId", "Id"))
    if explicit_txn_id:
        normalized["TxnId"] = explicit_txn_id
    else:
        for item in col_data:
            if not isinstance(item, Mapping):
                continue
            item_id = str(item.get("id", "")).strip()
            if item_id:
                normalized["TxnId"] = item_id
                break

    return {key: value for key, value in normalized.items() if value}


def _looks_like_transaction_row(row: Mapping[str, Any]) -> bool:
    has_date = bool(_first_text(row, ("TxnDate",)))
    has_account = bool(_first_text(row, ("AccountName",)))
    has_amount = any(str(row.get(key, "")).strip() for key in ("Amount", "Debit", "Credit"))
    return has_date and has_account and has_amount
