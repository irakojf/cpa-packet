"""Contractor summary helpers and deliverable orchestration."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Literal, Protocol, TypedDict, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import ensure_directory
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.deliverables.general_ledger import (
    GeneralLedgerMonthlySlice,
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.models.contractor import ContractorRecord
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.utils.constants import (
    CONTRACTOR_1099_THRESHOLD,
    DELIVERABLE_FOLDERS,
    SCHEMA_VERSIONS,
)
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter
from cpapacket.writers.pdf_writer import PdfTableRow, PdfTableSection, PdfWriter

_CENT = Decimal("0.01")
_ZERO = Decimal("0.00")
_CONTRACTOR_KEYWORDS = ("contract", "contractor", "subcontract", "cost of labor", "labor - cogs")
_CONTRACTOR_ACCOUNT_TYPES = {"expense", "costofgoodssold", "cogs"}
_CARD_PAYMENT_HINTS = (
    "stripe",
    "paypal",
    "credit card",
    "creditcard",
    "amex",
    "visa",
    "mastercard",
)


class _AccountProviders(Protocol):
    def get_accounts(self) -> dict[str, Any]:
        """Return QBO accounts query payload."""


class ContractorDataProvider(_AccountProviders, Protocol):
    """Provider interface required by ContractorSummaryDeliverable."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return one month of general-ledger report payload."""

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        """Return one month of general-ledger payload and its source marker."""

    def get_company_info(self) -> dict[str, Any]:
        """Return QBO company info payload."""


class _ContractorTotals(TypedDict):
    display_name: str
    total_paid_raw: Decimal
    card_processor_total_raw: Decimal


class ContractorSummaryDeliverable:
    """Deliverable orchestration for contractor 1099 review summary."""

    key = "contractor"
    folder = DELIVERABLE_FOLDERS["contractor"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, ctx: object) -> dict[str, Any]:
        provider = _resolve_account_provider(ctx)
        if provider is None:
            return {}

        detected_accounts = detect_contractor_accounts(providers=provider)
        selected_accounts = _prompt_selected_accounts(
            detected_accounts=detected_accounts,
            non_interactive=bool(getattr(ctx, "non_interactive", False)),
        )
        return {"selected_account_ids": [account["id"] for account in selected_accounts]}

    def is_current(self, _ctx: object) -> bool:
        return False

    def generate(
        self,
        ctx: RunContext,
        store: ContractorDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        company_name = _extract_company_name(store.get_company_info())
        detected_accounts = detect_contractor_accounts(providers=store)
        selected_accounts = _resolve_selected_accounts(
            detected_accounts=detected_accounts,
            prompts=prompts,
            non_interactive=ctx.non_interactive,
        )
        selected_account_names = {account["name"] for account in selected_accounts}

        monthly_slices = fetch_general_ledger_monthly_slices(
            year=ctx.year,
            provider=store,
        )
        gl_rows = merge_general_ledger_monthly_slices(monthly_slices)
        records = build_contractor_records(
            rows=gl_rows,
            selected_account_names=selected_account_names,
        )
        contractor_total_paid = sum((record.total_paid for record in records), _ZERO).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )
        selected_account_total = sum_selected_account_balances(
            rows=gl_rows,
            selected_account_names=selected_account_names,
        )
        reconciliation_mismatch = (contractor_total_paid - selected_account_total).quantize(
            _CENT, rounding=ROUND_HALF_UP
        )
        has_reconciliation_mismatch = reconciliation_mismatch != _ZERO

        warnings: list[str] = []
        if not detected_accounts:
            warnings.append("No contractor accounts detected; generated empty contractor summary.")
        elif not selected_accounts:
            warnings.append("No contractor accounts selected; generated empty contractor summary.")
        if selected_accounts and not records:
            warnings.append("No contractor payments found in selected accounts.")
        if has_reconciliation_mismatch:
            warnings.append(
                "Contractor reconciliation mismatch: "
                f"contractor_total_paid={format(contractor_total_paid, 'f')} "
                f"selected_account_total={format(selected_account_total, 'f')} "
                f"delta={format(reconciliation_mismatch, 'f')}"
            )

        artifacts = write_contractor_output_artifacts(
            ctx=ctx,
            records=records,
            detected_accounts=detected_accounts,
            selected_accounts=selected_accounts,
            monthly_slices=monthly_slices,
            gl_row_count=len(gl_rows),
            contractor_total_paid=contractor_total_paid,
            selected_account_total=selected_account_total,
            has_reconciliation_mismatch=has_reconciliation_mismatch,
            warnings=warnings,
            company_name=company_name,
        )
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=artifacts,
            warnings=warnings,
        )


def should_flag_for_1099_review(*, non_card_total: Decimal) -> bool:
    """Return whether non-card payments meet the 1099 review threshold."""
    normalized_total = non_card_total.quantize(_CENT, rounding=ROUND_HALF_UP)
    if normalized_total <= _ZERO:
        return False
    threshold = Decimal(str(CONTRACTOR_1099_THRESHOLD))
    return normalized_total >= threshold


def detect_contractor_accounts(*, providers: _AccountProviders) -> list[dict[str, str]]:
    """Identify contractor-related Expense/COGS accounts from QBO account payload."""
    payload = providers.get_accounts()
    accounts = _extract_accounts(payload)
    detected: list[dict[str, str]] = []
    seen_ids: set[str] = set()

    for account in accounts:
        account_id = str(account.get("Id", "")).strip()
        account_name = str(account.get("Name", "")).strip()
        account_type = str(account.get("AccountType", "")).strip()
        if not account_id or not account_name:
            continue
        if not _is_contractor_account_type(account_type):
            continue
        if not _contains_contractor_keyword(account_name):
            continue
        if account_id in seen_ids:
            continue

        seen_ids.add(account_id)
        detected.append(
            {
                "id": account_id,
                "name": account_name,
                "account_type": account_type,
            }
        )

    return sorted(detected, key=lambda account: account["name"].lower())


def build_contractor_records(
    *,
    rows: Sequence[GeneralLedgerRow],
    selected_account_names: set[str],
) -> list[ContractorRecord]:
    """Aggregate filtered GL rows into contractor records."""
    if not selected_account_names:
        return []

    totals: dict[str, _ContractorTotals] = {}

    for row in rows:
        account_name = row.account_name.strip()
        if selected_account_names and not _account_matches(account_name, selected_account_names):
            continue

        # Use signed amount so refunds/reversals offset payments.
        # For expense/COGS accounts: debit = payment, credit = refund.
        amount = (row.debit - row.credit).quantize(_CENT, rounding=ROUND_HALF_UP)
        if amount == _ZERO:
            continue

        display_name = (row.payee or "").strip() or "Unknown Vendor"
        vendor_id = _vendor_id_for_name(display_name)
        bucket = totals.setdefault(
            vendor_id,
            {
                "display_name": display_name,
                "total_paid_raw": _ZERO,
                "card_processor_total_raw": _ZERO,
            },
        )
        bucket["total_paid_raw"] += amount
        if _is_card_payment_row(row):
            bucket["card_processor_total_raw"] += amount

    records: list[ContractorRecord] = []
    for vendor_id, bucket in sorted(
        totals.items(),
        key=lambda item: item[1]["display_name"].lower(),
    ):
        total_paid_raw = bucket["total_paid_raw"].quantize(_CENT, rounding=ROUND_HALF_UP)
        total_paid = abs(total_paid_raw).quantize(_CENT, rounding=ROUND_HALF_UP)

        if total_paid == _ZERO:
            continue

        card_total_raw = bucket["card_processor_total_raw"].quantize(_CENT, rounding=ROUND_HALF_UP)
        card_total_candidate = card_total_raw if total_paid_raw >= _ZERO else -card_total_raw
        card_total = min(max(card_total_candidate, _ZERO), total_paid).quantize(
            _CENT,
            rounding=ROUND_HALF_UP,
        )
        non_card_total = (total_paid - card_total).quantize(_CENT, rounding=ROUND_HALF_UP)

        requires_review = should_flag_for_1099_review(non_card_total=non_card_total)
        flags = ["requires_1099_review"] if requires_review else []
        records.append(
            ContractorRecord(
                vendor_id=vendor_id,
                display_name=bucket["display_name"],
                tax_id_on_file=False,
                total_paid=total_paid,
                card_processor_total=card_total,
                non_card_total=non_card_total,
                requires_1099_review=requires_review,
                flags=flags,
            )
        )

    return records


def write_contractor_output_artifacts(
    *,
    ctx: RunContext,
    records: Sequence[ContractorRecord],
    detected_accounts: Sequence[dict[str, str]],
    selected_accounts: Sequence[dict[str, str]],
    monthly_slices: Sequence[GeneralLedgerMonthlySlice],
    gl_row_count: int,
    contractor_total_paid: Decimal,
    selected_account_total: Decimal,
    has_reconciliation_mismatch: bool,
    warnings: list[str],
    company_name: str = "Unknown Company",
) -> list[str]:
    """Write contractor CSV/PDF/JSON outputs and canonical metadata."""
    deliverable_dir = ensure_directory(ctx.out_dir / DELIVERABLE_FOLDERS["contractor"])
    meta_dir = ensure_directory(ctx.out_dir / "_meta")
    cpa_dir = ensure_directory(deliverable_dir / "cpa")
    dev_dir = ensure_directory(deliverable_dir / "dev")

    base_name = f"Contractor_1099_Review_{ctx.year}"
    csv_path = _resolve_output_path(
        cpa_dir / f"{base_name}.csv",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    pdf_path = _resolve_output_path(
        cpa_dir / f"{base_name}.pdf",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    json_path = (
        None
        if ctx.no_raw
        else _resolve_output_path(
            dev_dir / f"{base_name}.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
    )
    flagged_csv_path = _resolve_output_path(
        cpa_dir / f"flagged_for_review_{ctx.year}.csv",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )
    metadata_path = _resolve_output_path(
        meta_dir / f"{ContractorSummaryDeliverable.key}_metadata.json",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )

    _write_contractor_csv(path=csv_path, records=records, selected_accounts=selected_accounts)
    _write_flagged_review_csv(path=flagged_csv_path, records=records)
    _write_contractor_pdf(
        path=pdf_path,
        year=ctx.year,
        records=records,
        company_name=company_name,
    )
    raw_json_path = (
        None
        if json_path is None
        else JsonWriter().write_payload(
            json_path,
            payload=_build_raw_payload(
                year=ctx.year,
                records=records,
                detected_accounts=detected_accounts,
                selected_accounts=selected_accounts,
            ),
            no_raw=False,
            redact=ctx.redact,
        )
    )

    artifacts: list[Path] = [csv_path, flagged_csv_path, pdf_path]
    if raw_json_path is not None:
        artifacts.append(raw_json_path)

    metadata_inputs = _build_metadata_inputs(
        year=ctx.year,
        detected_accounts=detected_accounts,
        selected_accounts=selected_accounts,
        monthly_slices=monthly_slices,
        gl_row_count=gl_row_count,
        record_count=len(records),
        contractor_total_paid=contractor_total_paid,
        selected_account_total=selected_account_total,
        has_reconciliation_mismatch=has_reconciliation_mismatch,
        no_raw=ctx.no_raw,
        redact=ctx.redact,
    )
    metadata = DeliverableMetadata(
        deliverable=ContractorSummaryDeliverable.key,
        inputs=metadata_inputs,
        input_fingerprint=compute_input_fingerprint(metadata_inputs),
        schema_versions=SCHEMA_VERSIONS.get(ContractorSummaryDeliverable.key, {}),
        artifacts=[str(path) for path in artifacts],
        warnings=warnings,
        data_sources={"qbo": "general_ledger+accounts"},
    )
    write_deliverable_metadata(metadata_path, metadata)

    return [str(path) for path in artifacts] + [str(metadata_path)]


def _write_contractor_csv(
    *,
    path: Path,
    records: Sequence[ContractorRecord],
    selected_accounts: Sequence[dict[str, str]],
) -> None:
    selected_source_accounts = "|".join(account["name"] for account in selected_accounts)
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "vendor_id",
            "vendor_name",
            "tax_id_on_file",
            "total_paid",
            "card_processor_total",
            "non_card_total",
            "selected_source_accounts",
            "threshold",
            "flagged_for_1099_review",
            "review_note",
            "requires_1099_review",
            "flags",
        ],
        rows=[
            {
                "vendor_id": record.vendor_id,
                "vendor_name": record.display_name,
                "tax_id_on_file": str(record.tax_id_on_file).lower(),
                "total_paid": format(record.total_paid, "f"),
                "card_processor_total": format(record.card_processor_total, "f"),
                "non_card_total": format(record.non_card_total, "f"),
                "selected_source_accounts": selected_source_accounts,
                "threshold": format(CONTRACTOR_1099_THRESHOLD, "f"),
                "flagged_for_1099_review": str(record.requires_1099_review).lower(),
                "review_note": _review_note(record),
                "requires_1099_review": str(record.requires_1099_review).lower(),
                "flags": "|".join(record.flags),
            }
            for record in records
        ],
    )


def _write_flagged_review_csv(*, path: Path, records: Sequence[ContractorRecord]) -> None:
    flagged = [record for record in records if record.requires_1099_review]
    CsvWriter().write_rows(
        path,
        fieldnames=[
            "vendor_id",
            "display_name",
            "total_paid",
            "non_card_total",
            "requires_1099_review",
            "flags",
        ],
        rows=[
            {
                "vendor_id": record.vendor_id,
                "display_name": record.display_name,
                "total_paid": format(record.total_paid, "f"),
                "non_card_total": format(record.non_card_total, "f"),
                "requires_1099_review": str(record.requires_1099_review).lower(),
                "flags": "|".join(record.flags),
            }
            for record in flagged
        ],
    )


def _fmt_money(value: Decimal) -> str:
    """Format a Decimal as $X,XXX.XX."""
    return f"${value:,.2f}"


def _write_contractor_pdf(
    *,
    path: Path,
    year: int,
    records: Sequence[ContractorRecord],
    company_name: str = "Unknown Company",
) -> None:
    vendor_count = len(records)
    flagged_count = sum(1 for record in records if record.requires_1099_review)
    total_paid = sum((record.total_paid for record in records), _ZERO).quantize(
        _CENT, rounding=ROUND_HALF_UP
    )
    total_non_card = sum((record.non_card_total for record in records), _ZERO).quantize(
        _CENT, rounding=ROUND_HALF_UP
    )
    total_card = sum((record.card_processor_total for record in records), _ZERO).quantize(
        _CENT, rounding=ROUND_HALF_UP
    )

    summary_section = PdfTableSection(
        title="Summary",
        headers=("", ""),
        rows=[
            PdfTableRow(
                cells=(
                    "Methodology",
                    (
                        "This is a QBO-account-based 1099 review schedule, "
                        "not a final filing determination."
                    ),
                )
            ),
            PdfTableRow(cells=("Total Vendors", str(vendor_count))),
            PdfTableRow(cells=("Total Paid", _fmt_money(total_paid))),
            PdfTableRow(cells=("Card Processor Payments", _fmt_money(total_card))),
            PdfTableRow(cells=("Non-Card Payments", _fmt_money(total_non_card))),
            PdfTableRow(cells=("Flagged for 1099 Review", str(flagged_count))),
        ],
    )

    sorted_records = sorted(records, key=lambda r: r.display_name.lower())
    detail_rows: list[PdfTableRow] = []
    for record in sorted_records:
        status: Literal["mismatch"] | None = "mismatch" if record.requires_1099_review else None
        detail_rows.append(
            PdfTableRow(
                cells=(
                    record.display_name,
                    _fmt_money(record.total_paid),
                    _fmt_money(record.non_card_total),
                    "Yes" if record.requires_1099_review else "No",
                ),
                status=status,
            )
        )
    detail_rows.append(
        PdfTableRow(
            cells=("Total", _fmt_money(total_paid), _fmt_money(total_non_card), ""),
            row_type="total",
        )
    )

    detail_section = PdfTableSection(
        title="Contractor Detail",
        headers=("Vendor Name", "Total Paid", "Non-Card Total", "1099 Review"),
        rows=detail_rows,
    )

    PdfWriter().write_reconciliation_report(
        path,
        company_name=company_name,
        report_title="Contractor 1099 Review",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        sections=[summary_section, detail_section],
    )


def _review_note(record: ContractorRecord) -> str:
    if record.requires_1099_review:
        return "Meets non-card threshold; CPA review required."
    if record.non_card_total == _ZERO:
        return "Card-only or fully processor-paid activity."
    return "Below review threshold based on detected non-card activity."


def _build_raw_payload(
    *,
    year: int,
    records: Sequence[ContractorRecord],
    detected_accounts: Sequence[dict[str, str]],
    selected_accounts: Sequence[dict[str, str]],
) -> dict[str, Any]:
    return {
        "deliverable": ContractorSummaryDeliverable.key,
        "year": year,
        "detected_accounts": [dict(account) for account in detected_accounts],
        "selected_accounts": [dict(account) for account in selected_accounts],
        "records": [
            {
                "vendor_id": record.vendor_id,
                "display_name": record.display_name,
                "tax_id_on_file": record.tax_id_on_file,
                "total_paid": format(record.total_paid, "f"),
                "card_processor_total": format(record.card_processor_total, "f"),
                "non_card_total": format(record.non_card_total, "f"),
                "requires_1099_review": record.requires_1099_review,
                "flags": list(record.flags),
            }
            for record in records
        ],
    }


def _build_metadata_inputs(
    *,
    year: int,
    detected_accounts: Sequence[dict[str, str]],
    selected_accounts: Sequence[dict[str, str]],
    monthly_slices: Sequence[GeneralLedgerMonthlySlice],
    gl_row_count: int,
    record_count: int,
    contractor_total_paid: Decimal,
    selected_account_total: Decimal,
    has_reconciliation_mismatch: bool,
    no_raw: bool,
    redact: bool,
) -> dict[str, Any]:
    slice_hashes = [
        hashlib.sha256(
            json.dumps(slice_.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        for slice_ in monthly_slices
    ]
    return {
        "year": year,
        "no_raw": no_raw,
        "redact": redact,
        "detected_account_ids": [account["id"] for account in detected_accounts],
        "selected_account_ids": [account["id"] for account in selected_accounts],
        "slice_hashes": slice_hashes,
        "gl_row_count": gl_row_count,
        "record_count": record_count,
        "contractor_total_paid": format(contractor_total_paid, "f"),
        "selected_account_total": format(selected_account_total, "f"),
        "has_reconciliation_mismatch": has_reconciliation_mismatch,
    }


def sum_selected_account_balances(
    *,
    rows: Sequence[GeneralLedgerRow],
    selected_account_names: set[str],
) -> Decimal:
    """Return the selected-account total using vendor-level normalized nets."""
    if not selected_account_names:
        return _ZERO

    totals_by_vendor: dict[str, Decimal] = {}
    for row in rows:
        if not _account_matches(row.account_name.strip(), selected_account_names):
            continue
        amount = (row.debit - row.credit).quantize(_CENT, rounding=ROUND_HALF_UP)
        if amount == _ZERO:
            continue
        display_name = (row.payee or "").strip() or "Unknown Vendor"
        vendor_id = _vendor_id_for_name(display_name)
        totals_by_vendor[vendor_id] = totals_by_vendor.get(vendor_id, _ZERO) + amount

    total = sum((abs(amount) for amount in totals_by_vendor.values()), _ZERO)
    return total.quantize(_CENT, rounding=ROUND_HALF_UP)


def _resolve_selected_accounts(
    *,
    detected_accounts: Sequence[dict[str, str]],
    prompts: Mapping[str, Any],
    non_interactive: bool,
) -> list[dict[str, str]]:
    prompt_ids_raw = prompts.get("selected_account_ids")
    prompt_ids: list[str] | None = None
    if isinstance(prompt_ids_raw, list):
        prompt_ids = [str(item).strip() for item in prompt_ids_raw if str(item).strip()]
    elif isinstance(prompt_ids_raw, str):
        prompt_ids = [item.strip() for item in prompt_ids_raw.split(",") if item.strip()]

    if prompt_ids is not None:
        prompt_id_set = set(prompt_ids)
        return [account for account in detected_accounts if account["id"] in prompt_id_set]
    if non_interactive:
        return [dict(account) for account in detected_accounts]
    return [dict(account) for account in detected_accounts]


def _resolve_account_provider(ctx: object) -> _AccountProviders | None:
    direct = cast(Any, ctx)
    if hasattr(direct, "get_accounts"):
        return cast(_AccountProviders, direct)
    for attr in ("store", "data_store", "providers", "provider"):
        value = getattr(ctx, attr, None)
        if value is not None and hasattr(value, "get_accounts"):
            return cast(_AccountProviders, value)
    return None


def _prompt_selected_accounts(
    *,
    detected_accounts: Sequence[dict[str, str]],
    non_interactive: bool,
    input_fn: Callable[[str], str] | None = None,
) -> list[dict[str, str]]:
    if not detected_accounts:
        return []
    if non_interactive:
        return [dict(account) for account in detected_accounts]

    print("Detected contractor-related accounts:")
    for account in detected_accounts:
        print(f"- {account['id']}: {account['name']} [{account['account_type']}]")

    prompt_input = input_fn or input

    while True:
        confirm = prompt_input("Use detected accounts? [Y/n]: ").strip().lower()
        if confirm in {"", "y", "yes"}:
            return [dict(account) for account in detected_accounts]
        if confirm in {"n", "no"}:
            break

    manual_raw = prompt_input(
        "Enter account IDs to include (comma-separated, blank for none): "
    ).strip()
    if not manual_raw:
        return []
    selected_ids = {item.strip() for item in manual_raw.split(",") if item.strip()}
    return [dict(account) for account in detected_accounts if account["id"] in selected_ids]


def _extract_accounts(payload: dict[str, Any]) -> list[Mapping[str, Any]]:
    query_response = payload.get("QueryResponse")
    if isinstance(query_response, Mapping):
        raw_accounts = query_response.get("Account")
        if isinstance(raw_accounts, list):
            return [account for account in raw_accounts if isinstance(account, Mapping)]

    raw_accounts = payload.get("Account")
    if isinstance(raw_accounts, list):
        return [account for account in raw_accounts if isinstance(account, Mapping)]
    return []


def _extract_company_name(company_payload: Mapping[str, Any]) -> str:
    company_info = company_payload.get("CompanyInfo")
    if isinstance(company_info, Mapping):
        legal_name = company_info.get("LegalName")
        if isinstance(legal_name, str) and legal_name.strip():
            return legal_name.strip()
        company_name = company_info.get("CompanyName")
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
    return "Unknown Company"


def _account_matches(account_name: str, selected_names: set[str]) -> bool:
    """Check if a GL account name matches any selected account name.

    GL account names may include parent paths (e.g. 'Cost of goods sold:Cost of labor - COGS')
    while selected names use the short form ('Cost of labor - COGS').
    """
    if account_name in selected_names:
        return True
    return any(account_name.endswith(":" + name) for name in selected_names)


def _is_contractor_account_type(account_type: str) -> bool:
    normalized = "".join(ch for ch in account_type.strip().lower() if ch.isalnum())
    return normalized in _CONTRACTOR_ACCOUNT_TYPES


def _contains_contractor_keyword(account_name: str) -> bool:
    normalized = account_name.strip().lower()
    return any(keyword in normalized for keyword in _CONTRACTOR_KEYWORDS)


def _vendor_id_for_name(display_name: str) -> str:
    normalized = display_name.strip().lower()
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:12]


def _is_card_payment_row(row: GeneralLedgerRow) -> bool:
    text = " ".join(
        (
            row.account_name,
            row.account_type,
            row.transaction_type,
            row.document_number,
            row.memo or "",
            row.payee or "",
        )
    ).lower()
    compact = "".join(ch for ch in text if not ch.isspace())
    return any(token in text or token in compact for token in _CARD_PAYMENT_HINTS)


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(
        path,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )
