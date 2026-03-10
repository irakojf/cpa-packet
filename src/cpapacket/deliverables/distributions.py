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
    extract_distribution_total,
    integrate_miscoded_distributions,
)
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.pdf_writer import PdfBodyLine, PdfWriter

_DEFAULT_OWNER_KEYWORDS = ("owner", "shareholder")


class DistributionsDataProvider(Protocol):
    """Provider interface for distribution deliverable inputs."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Fetch a monthly general-ledger payload."""

    def get_company_info(self) -> dict[str, Any]:
        """Return QBO company info payload."""


class DistributionsDeliverable:
    """Shareholder distributions summary and miscoding orchestration."""

    key = "distributions"
    folder = DELIVERABLE_FOLDERS["distributions"]
    required = True
    dependencies: list[str] = ["general_ledger"]
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
        distribution_total = extract_distribution_total(gl_rows)
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
        )
        _write_summary_pdf(
            path=summary_pdf_path,
            year=ctx.year,
            distribution_total=distribution_total,
            miscoded_candidate_count=candidate_count,
            company_name=company_name,
        )
        if summary_json_path is not None:
            _write_summary_json(
                path=summary_json_path,
                year=ctx.year,
                distribution_total=distribution_total,
                miscoded_candidate_count=candidate_count,
                owner_keywords=owner_keywords,
            )

        warnings: list[str] = []
        if not ctx.owner_keywords and _using_default_owner_keywords(owner_keywords):
            warnings.append(
                "No owner keywords supplied; default owner/shareholder keywords were used."
            )

        artifacts = [summary_csv_path, summary_pdf_path, miscoded.csv_path]
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
) -> None:
    writer = CsvWriter()
    writer.write_rows(
        path,
        fieldnames=[
            "year",
            "distribution_total",
            "miscoded_candidate_count",
            "owner_keywords",
        ],
        rows=[
            {
                "year": year,
                "distribution_total": f"{distribution_total:.2f}",
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
) -> None:
    payload = {
        "year": year,
        "distribution_total": f"{distribution_total:.2f}",
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
            PdfBodyLine(text=f"Likely Miscoded Count: {miscoded_candidate_count}"),
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
