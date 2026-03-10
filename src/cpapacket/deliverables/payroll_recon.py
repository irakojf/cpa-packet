"""Payroll reconciliation deliverable orchestration (Gusto vs QBO)."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import ensure_directory
from cpapacket.core.metadata import (
    DeliverableMetadata,
    compute_input_fingerprint,
    write_deliverable_metadata,
)
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.reconciliation.payroll_recon import (
    PayrollReconciliation,
    fetch_gusto_reconciliation_total,
    fetch_qbo_payroll_total,
    reconcile_payroll_totals,
)
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter
from cpapacket.writers.pdf_writer import PdfTableRow, PdfWriter

_ZERO = Decimal("0.00")


class PayrollReconDataProvider(Protocol):
    """Provider interface required by PayrollReconDeliverable."""

    def get_payroll_runs(self, year: int) -> list[dict[str, Any]]:
        """Return annual payroll runs from Gusto."""

    def get_accounts(self) -> dict[str, Any]:
        """Return QBO account payload."""


class PayrollReconDeliverable:
    """Annual payroll reconciliation deliverable."""

    key = "payroll_recon"
    folder = DELIVERABLE_FOLDERS["payroll_recon"]
    required = True
    dependencies: list[str] = []
    requires_gusto = True

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        return False

    def generate(
        self,
        ctx: RunContext,
        store: PayrollReconDataProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        if not ctx.gusto_available:
            return DeliverableResult(
                deliverable_key=self.key,
                success=True,
                warnings=["Skipped payroll reconciliation; Gusto not connected."],
            )

        company_name = _extract_company_name(store)
        gusto_total = fetch_gusto_reconciliation_total(providers=store, year=ctx.year)
        qbo_total = fetch_qbo_payroll_total(providers=store)
        reconciliation = reconcile_payroll_totals(gusto_total=gusto_total, qbo_total=qbo_total)

        warnings: list[str] = []
        if qbo_total == _ZERO and gusto_total > _ZERO:
            warnings.append(
                "No matching QBO payroll accounts found; reconciliation may be incomplete."
            )
        if reconciliation.status == "MISMATCH":
            warnings.append(
                "Payroll reconciliation mismatch detected "
                f"(variance {reconciliation.variance:.2f})."
            )

        artifacts = write_payroll_recon_output_artifacts(
            ctx=ctx,
            reconciliation=reconciliation,
            warnings=warnings,
            company_name=company_name,
        )
        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=artifacts,
            warnings=warnings,
        )


def write_payroll_recon_output_artifacts(
    *,
    ctx: RunContext,
    reconciliation: PayrollReconciliation,
    warnings: list[str],
    company_name: str = "Unknown Company",
) -> list[str]:
    """Write payroll reconciliation CSV/PDF/JSON and canonical metadata."""
    deliverable_dir = ensure_directory(ctx.out_dir / DELIVERABLE_FOLDERS["payroll_recon"])
    meta_dir = ensure_directory(ctx.out_dir / "_meta")
    cpa_dir = ensure_directory(deliverable_dir / "cpa")
    dev_dir = ensure_directory(deliverable_dir / "dev")

    base_name = f"payroll_reconciliation_{ctx.year}"
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
    metadata_path = _resolve_output_path(
        meta_dir / f"{PayrollReconDeliverable.key}_metadata.json",
        on_conflict=ctx.on_conflict,
        non_interactive=ctx.non_interactive,
    )

    _write_reconciliation_csv(path=csv_path, year=ctx.year, reconciliation=reconciliation)
    _write_reconciliation_pdf(path=pdf_path, year=ctx.year, reconciliation=reconciliation, company_name=company_name)
    if json_path is not None:
        _write_reconciliation_json(path=json_path, year=ctx.year, reconciliation=reconciliation)

    artifacts: list[Path] = [csv_path, pdf_path]
    if json_path is not None:
        artifacts.append(json_path)

    metadata_inputs = {
        "year": ctx.year,
        "gusto_total": f"{reconciliation.gusto_total:.2f}",
        "qbo_total": f"{reconciliation.qbo_total:.2f}",
        "variance": f"{reconciliation.variance:.2f}",
        "status": reconciliation.status,
        "tolerance": f"{reconciliation.tolerance:.2f}",
        "no_raw": ctx.no_raw,
    }
    metadata = DeliverableMetadata(
        deliverable=PayrollReconDeliverable.key,
        inputs=metadata_inputs,
        input_fingerprint=compute_input_fingerprint(metadata_inputs),
        schema_versions=SCHEMA_VERSIONS[PayrollReconDeliverable.key],
        artifacts=[str(path) for path in artifacts],
        warnings=warnings,
    )
    write_deliverable_metadata(metadata_path, metadata)

    return [str(path) for path in artifacts]


def _write_reconciliation_csv(
    *,
    path: Path,
    year: int,
    reconciliation: PayrollReconciliation,
) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=["year", "gusto_total", "qbo_total", "variance", "status", "tolerance"],
        rows=[
            {
                "year": year,
                "gusto_total": f"{reconciliation.gusto_total:.2f}",
                "qbo_total": f"{reconciliation.qbo_total:.2f}",
                "variance": f"{reconciliation.variance:.2f}",
                "status": reconciliation.status,
                "tolerance": f"{reconciliation.tolerance:.2f}",
            }
        ],
    )


def _write_reconciliation_json(
    *,
    path: Path,
    year: int,
    reconciliation: PayrollReconciliation,
) -> None:
    writer = JsonWriter()
    writer.write_payload(
        path,
        payload={
            "year": year,
            "gusto_total": f"{reconciliation.gusto_total:.2f}",
            "qbo_total": f"{reconciliation.qbo_total:.2f}",
            "variance": f"{reconciliation.variance:.2f}",
            "status": reconciliation.status,
            "tolerance": f"{reconciliation.tolerance:.2f}",
        },
        no_raw=False,
    )


def _write_reconciliation_pdf(
    *,
    path: Path,
    year: int,
    reconciliation: PayrollReconciliation,
    company_name: str = "Unknown Company",
) -> None:
    status_token = "reconciled" if reconciliation.status == "RECONCILED" else "mismatch"
    writer = PdfWriter()
    writer.write_table_report(
        path,
        company_name=company_name,
        report_title="Payroll Reconciliation",
        date_range_label=f"{year}-01-01 to {year}-12-31",
        columns=["Year", "Gusto Total", "QBO Total", "Variance", "Status", "Tolerance"],
        rows=[
            PdfTableRow(
                cells=(
                    str(year),
                    f"{reconciliation.gusto_total:.2f}",
                    f"{reconciliation.qbo_total:.2f}",
                    f"{reconciliation.variance:.2f}",
                    reconciliation.status,
                    f"{reconciliation.tolerance:.2f}",
                ),
                row_type="total",
                status=cast("Any", status_token),
            )
        ],
    )


def _resolve_output_path(path: Path, *, on_conflict: str, non_interactive: bool) -> Path:
    normalized_conflict = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(
        path,
        on_conflict=normalized_conflict,
        non_interactive=non_interactive,
    )


def _extract_company_name(store: object) -> str:
    get_info = getattr(store, "get_company_info", None)
    if get_info is None:
        return "Unknown Company"
    try:
        payload = get_info()
    except Exception:
        return "Unknown Company"
    if not isinstance(payload, Mapping):
        return "Unknown Company"
    company_info = payload.get("CompanyInfo")
    if isinstance(company_info, Mapping):
        legal_name = company_info.get("LegalName")
        if isinstance(legal_name, str) and legal_name.strip():
            return legal_name.strip()
        company_name = company_info.get("CompanyName")
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
    return "Unknown Company"
