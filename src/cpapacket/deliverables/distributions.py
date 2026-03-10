"""Distributions deliverable orchestration helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import IO, Any, Protocol, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.deliverables.general_ledger import (
    fetch_general_ledger_monthly_slices,
    merge_general_ledger_monthly_slices,
)
from cpapacket.reconciliation.retained_earnings import (
    extract_distribution_activity_rows,
    extract_distribution_balance_from_balance_sheet,
    extract_distribution_total,
    integrate_miscoded_distributions,
)
from cpapacket.utils.constants import (
    DELIVERABLE_FOLDERS,
    RETAINED_EARNINGS_TOLERANCE,
    SCHEMA_VERSIONS,
)
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

_DEFAULT_OWNER_KEYWORDS = ("owner", "shareholder")


class DistributionsDataProvider(Protocol):
    """Provider interface for distribution deliverable inputs."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Fetch a monthly general-ledger payload."""

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        """Fetch a monthly general-ledger payload plus source marker."""

    def get_company_info(self) -> dict[str, Any]:
        """Return QBO company info payload."""

    def get_balance_sheet(self, year: int, as_of: str) -> dict[str, Any]:
        """Return QBO balance sheet payload."""


class DistributionsDeliverable:
    """Shareholder distributions summary and miscoding orchestration."""

    key = "distributions"
    folder = DELIVERABLE_FOLDERS["distributions"]
    required = True
    dependencies: list[str] = ["general_ledger", "balance_sheet", "prior_balance_sheet"]
    requires_gusto = False

    def gather_prompts(self, ctx: RunContext) -> dict[str, Any]:
        return {"owner_keywords": list(ctx.owner_keywords)}

    def is_current(self, _ctx: object) -> bool:
        # Incremental freshness checks are implemented in a follow-up bead.
        return False

    def generate(
        self,
        ctx: RunContext,
        store: DistributionsDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        owner_keywords = _resolve_owner_keywords(ctx=ctx, prompts=prompts)
        company_name = _extract_company_name(store.get_company_info())

        monthly_slices = fetch_general_ledger_monthly_slices(
            year=ctx.year,
            provider=store,
        )
        gl_rows = list(merge_general_ledger_monthly_slices(monthly_slices))
        activity_rows = extract_distribution_activity_rows(gl_rows)
        distribution_total = extract_distribution_total(gl_rows)
        prior_year = ctx.year - 1
        prior_balance_sheet = store.get_balance_sheet(prior_year, f"{prior_year}-12-31")
        current_balance_sheet = store.get_balance_sheet(ctx.year, f"{ctx.year}-12-31")
        prior_distribution_balance, _ = extract_distribution_balance_from_balance_sheet(
            prior_balance_sheet
        )
        current_distribution_balance, _ = extract_distribution_balance_from_balance_sheet(
            current_balance_sheet
        )
        balance_sheet_change = (
            (current_distribution_balance - prior_distribution_balance)
            .copy_abs()
            .quantize(Decimal("0.01"))
        )
        bridge_difference = (distribution_total - balance_sheet_change).quantize(Decimal("0.01"))
        bridge_status = (
            "Balanced"
            if bridge_difference.copy_abs() <= RETAINED_EARNINGS_TOLERANCE
            else "Review"
        )
        miscoded = integrate_miscoded_distributions(
            gl_rows=gl_rows,
            owner_keywords=owner_keywords,
            packet_root=ctx.out_dir,
            year=ctx.year,
        )

        deliverable_dir = ensure_directory(ctx.out_dir / self.folder)
        meta_dir = ensure_directory(ctx.out_dir / "_meta")
        cpa_dir = ensure_directory(deliverable_dir / "cpa")
        dev_dir = ensure_directory(deliverable_dir / "dev")

        base_name = f"distributions_summary_{ctx.year}"
        activity_csv_path = _resolve_output_path(
            cpa_dir / f"distribution_activity_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        bridge_csv_path = _resolve_output_path(
            cpa_dir / f"distribution_balance_bridge_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        summary_csv_path = _resolve_output_path(
            cpa_dir / f"{base_name}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        summary_pdf_path = _resolve_output_path(
            cpa_dir / f"{base_name}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        summary_json_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                dev_dir / f"{base_name}.json",
                on_conflict=ctx.on_conflict,
                non_interactive=ctx.non_interactive,
            )
        )
        metadata_path = _resolve_output_path(
            meta_dir / f"{self.key}_metadata.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )

        candidate_count = len(miscoded.candidates)
        _write_summary_csv(
            path=summary_csv_path,
            year=ctx.year,
            distribution_total=distribution_total,
            miscoded_candidate_count=candidate_count,
            owner_keywords=owner_keywords,
            balance_sheet_change=balance_sheet_change,
            bridge_status=bridge_status,
        )
        _write_summary_pdf(
            path=summary_pdf_path,
            year=ctx.year,
            distribution_total=distribution_total,
            miscoded_candidate_count=candidate_count,
            balance_sheet_change=balance_sheet_change,
            bridge_status=bridge_status,
            company_name=company_name,
        )
        _write_activity_csv(path=activity_csv_path, rows=activity_rows)
        _write_balance_bridge_csv(
            path=bridge_csv_path,
            year=ctx.year,
            prior_distribution_balance=prior_distribution_balance,
            current_distribution_balance=current_distribution_balance,
            balance_sheet_change=balance_sheet_change,
            gl_distribution_total=distribution_total,
            difference=bridge_difference,
            status=bridge_status,
        )
        if summary_json_path is not None:
            _write_summary_json(
                path=summary_json_path,
                year=ctx.year,
                distribution_total=distribution_total,
                miscoded_candidate_count=candidate_count,
                owner_keywords=owner_keywords,
                balance_sheet_change=balance_sheet_change,
                bridge_status=bridge_status,
            )

        warnings: list[str] = []
        if not ctx.owner_keywords and _using_default_owner_keywords(owner_keywords):
            warnings.append(
                "No owner keywords supplied; default owner/shareholder keywords were used."
            )

        artifacts = [
            summary_csv_path,
            summary_pdf_path,
            activity_csv_path,
            bridge_csv_path,
            miscoded.csv_path,
        ]
        if summary_json_path is not None:
            artifacts.append(summary_json_path)

        _write_metadata(
            path=metadata_path,
            key=self.key,
            artifacts=artifacts,
            warnings=warnings,
            inputs={
                "year": ctx.year,
                "owner_keywords": owner_keywords,
                "monthly_slice_count": len(monthly_slices),
                "gl_row_count": len(gl_rows),
                "distribution_total": f"{distribution_total:.2f}",
                "distribution_balance_sheet_change": f"{balance_sheet_change:.2f}",
                "miscoded_candidate_count": candidate_count,
                "no_raw": ctx.no_raw,
            },
        )

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(path) for path in artifacts],
            warnings=warnings,
        )


def _resolve_owner_keywords(*, ctx: RunContext, prompts: Mapping[str, Any]) -> list[str]:
    prompt_keywords = prompts.get("owner_keywords")
    values: list[str] = []

    if isinstance(prompt_keywords, list):
        values = [str(item).strip() for item in prompt_keywords if str(item).strip()]
    elif isinstance(prompt_keywords, str):
        values = [item.strip() for item in prompt_keywords.split(",") if item.strip()]

    if values:
        return values
    if ctx.owner_keywords:
        return list(ctx.owner_keywords)
    return list(_DEFAULT_OWNER_KEYWORDS)


def _using_default_owner_keywords(owner_keywords: list[str]) -> bool:
    return tuple(keyword.lower() for keyword in owner_keywords) == _DEFAULT_OWNER_KEYWORDS


def _write_summary_csv(
    *,
    path: Path,
    year: int,
    distribution_total: Decimal,
    miscoded_candidate_count: int,
    owner_keywords: list[str],
    balance_sheet_change: Decimal,
    bridge_status: str,
) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=[
            "year",
            "distribution_total",
            "distribution_balance_sheet_change",
            "bridge_status",
            "miscoded_candidate_count",
            "owner_keywords",
        ],
        rows=[
            {
                "year": year,
                "distribution_total": f"{distribution_total:.2f}",
                "distribution_balance_sheet_change": f"{balance_sheet_change:.2f}",
                "bridge_status": bridge_status,
                "miscoded_candidate_count": miscoded_candidate_count,
                "owner_keywords": "|".join(owner_keywords),
            }
        ],
    )


def _write_summary_json(
    *,
    path: Path,
    year: int,
    distribution_total: Decimal,
    miscoded_candidate_count: int,
    owner_keywords: list[str],
    balance_sheet_change: Decimal,
    bridge_status: str,
) -> None:
    payload = {
        "year": year,
        "distribution_total": f"{distribution_total:.2f}",
        "distribution_balance_sheet_change": f"{balance_sheet_change:.2f}",
        "bridge_status": bridge_status,
        "miscoded_candidate_count": miscoded_candidate_count,
        "owner_keywords": owner_keywords,
    }
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")


def _write_summary_pdf(
    *,
    path: Path,
    year: int,
    distribution_total: Decimal,
    miscoded_candidate_count: int,
    balance_sheet_change: Decimal,
    bridge_status: str,
    company_name: str = "Unknown Company",
) -> None:
    writer = PdfWriter()
    writer.write_report(
        path,
        company_name=company_name,
        report_title="Shareholder Distributions Summary",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        body_lines=[
            PdfBodyLine(text=f"Distribution Total: {distribution_total:.2f}", row_type="total"),
            PdfBodyLine(text=f"Balance-Sheet Change: {balance_sheet_change:.2f}"),
            PdfBodyLine(text=f"Bridge Status: {bridge_status}"),
            PdfBodyLine(text=f"Likely Miscoded Count: {miscoded_candidate_count}"),
        ],
    )


def _write_activity_csv(*, path: Path, rows: list[Any]) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=[
            "date",
            "txn_type",
            "doc_num",
            "payee",
            "account_name",
            "memo",
            "debit",
            "credit",
            "signed_amount",
            "classification",
        ],
        rows=[
            {
                "date": row.date.isoformat(),
                "txn_type": row.txn_type,
                "doc_num": row.doc_num,
                "payee": row.payee,
                "account_name": row.account_name,
                "memo": row.memo,
                "debit": f"{row.debit:.2f}",
                "credit": f"{row.credit:.2f}",
                "signed_amount": f"{row.signed_amount:.2f}",
                "classification": row.classification,
            }
            for row in rows
        ],
    )


def _write_balance_bridge_csv(
    *,
    path: Path,
    year: int,
    prior_distribution_balance: Decimal,
    current_distribution_balance: Decimal,
    balance_sheet_change: Decimal,
    gl_distribution_total: Decimal,
    difference: Decimal,
    status: str,
) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=[
            "year",
            "prior_distribution_balance",
            "current_distribution_balance",
            "balance_sheet_change",
            "gl_distribution_total",
            "difference",
            "status",
        ],
        rows=[
            {
                "year": year,
                "prior_distribution_balance": f"{prior_distribution_balance:.2f}",
                "current_distribution_balance": f"{current_distribution_balance:.2f}",
                "balance_sheet_change": f"{balance_sheet_change:.2f}",
                "gl_distribution_total": f"{gl_distribution_total:.2f}",
                "difference": f"{difference:.2f}",
                "status": status,
            }
        ],
    )


def _write_metadata(
    *,
    path: Path,
    key: str,
    artifacts: list[Path],
    warnings: list[str],
    inputs: Mapping[str, Any],
) -> None:
    canonical = json.dumps(dict(inputs), sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    payload = {
        "deliverable": key,
        "input_fingerprint": fingerprint,
        "inputs": dict(inputs),
        "schema_versions": SCHEMA_VERSIONS.get(key, {}),
        "artifacts": [str(item) for item in artifacts],
        "warnings": warnings,
    }
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(
        path,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )


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
