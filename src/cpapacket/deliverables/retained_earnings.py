"""Retained earnings deliverable orchestration for CPA-facing equity review."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import IO, Any, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write, ensure_directory
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.reconciliation.retained_earnings import (
    build_retained_earnings_rollforward,
    evaluate_re_structural_flags,
    integrate_miscoded_distributions,
    load_re_source_data,
)
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.retained_earnings import (
    write_cpa_notes,
    write_distribution_bridge_csv,
    write_distribution_bridge_detail_csv,
    write_equity_activity_csv,
    write_equity_tie_out_csv,
    write_rollforward_csv,
    write_rollforward_data_json,
    write_rollforward_pdf,
)


class RetainedEarningsDeliverable:
    """Generate CPA-facing equity review artifacts from cross-deliverable data."""

    key = "retained_earnings"
    folder = DELIVERABLE_FOLDERS["retained_earnings"]
    required = True
    dependencies: list[str] = ["pnl", "balance_sheet", "prior_balance_sheet", "distributions"]
    requires_gusto = False

    def gather_prompts(self, _ctx: RunContext) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        # Incremental freshness checks are handled by build orchestration.
        return False

    def generate(
        self,
        ctx: RunContext,
        store: Any,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts
        company_name = _extract_company_name(store)
        source = load_re_source_data(year=ctx.year, provider=store)
        structural_flags = evaluate_re_structural_flags(
            net_income=source.net_income,
            distributions_gl=source.distributions_gl,
            distributions_bs_change=source.distributions_bs_change,
            actual_ending_book_equity_bucket=source.actual_ending_book_equity_bucket,
            shareholder_receivable_ending_balance=source.shareholder_receivable_ending_balance,
            gl_rows=source.gl_rows,
        )
        rollforward = build_retained_earnings_rollforward(
            source=source,
            structural_flags=structural_flags,
        )

        miscoded = integrate_miscoded_distributions(
            gl_rows=source.gl_rows,
            owner_keywords=list(ctx.owner_keywords),
            packet_root=ctx.out_dir,
            year=ctx.year,
        )
        miscoded_count = len(miscoded.candidates)

        deliverable_dir = ensure_directory(ctx.out_dir / self.folder)
        meta_dir = ensure_directory(ctx.out_dir / "_meta")
        cpa_dir = ensure_directory(deliverable_dir / "cpa")
        dev_dir = ensure_directory(deliverable_dir / "dev")

        rollforward_csv_path = _resolve_output_path(
            cpa_dir / f"Book_Equity_Rollforward_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        rollforward_pdf_path = _resolve_output_path(
            cpa_dir / f"Book_Equity_Rollforward_{ctx.year}.pdf",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        tie_out_csv_path = _resolve_output_path(
            cpa_dir / f"Equity_Tie_Out_to_QBO_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        bridge_csv_path = _resolve_output_path(
            cpa_dir / f"Distribution_Bridge_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        bridge_detail_csv_path = _resolve_output_path(
            cpa_dir / f"Distribution_Bridge_Detail_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        shareholder_receivable_csv_path = _resolve_output_path(
            cpa_dir / f"Shareholder_Receivable_Activity_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        direct_equity_csv_path = _resolve_output_path(
            cpa_dir / f"Direct_Equity_Postings_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        cpa_notes_path = _resolve_output_path(
            cpa_dir / "CPA_NOTES.md",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        data_json_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                dev_dir / f"Equity_Review_{ctx.year}_data.json",
                on_conflict=ctx.on_conflict,
                non_interactive=ctx.non_interactive,
            )
        )
        metadata_path = _resolve_output_path(
            meta_dir / f"{self.key}_metadata.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )

        write_rollforward_csv(
            path=rollforward_csv_path,
            year=ctx.year,
            rollforward=rollforward,
            miscoded_distribution_count=miscoded_count,
        )
        write_rollforward_pdf(
            path=rollforward_pdf_path,
            year=ctx.year,
            rollforward=rollforward,
            miscoded_distribution_count=miscoded_count,
            company_name=company_name,
        )
        write_equity_tie_out_csv(path=tie_out_csv_path, rows=source.equity_tie_out_rows)
        write_distribution_bridge_csv(
            path=bridge_csv_path,
            year=ctx.year,
            bridge=source.distribution_balance_bridge,
        )
        write_distribution_bridge_detail_csv(
            path=bridge_detail_csv_path,
            rows=source.distribution_bridge_detail_rows,
        )
        write_equity_activity_csv(
            path=shareholder_receivable_csv_path,
            rows=source.shareholder_receivable_rows,
        )
        write_equity_activity_csv(
            path=direct_equity_csv_path,
            rows=source.direct_equity_rows,
        )
        write_cpa_notes(path=cpa_notes_path)
        if data_json_path is not None:
            write_rollforward_data_json(
                path=data_json_path,
                year=ctx.year,
                rollforward=rollforward,
                miscoded_distribution_count=miscoded_count,
                data_sources={
                    "prior_balance_sheet": "store",
                    "pnl": "store",
                    "general_ledger": "store",
                    "current_balance_sheet": "store",
                },
                equity_tie_out_rows=source.equity_tie_out_rows,
                distribution_bridge=source.distribution_balance_bridge,
            )

        warnings = list(structural_flags)
        if miscoded_count > 0:
            warnings.append(
                f"Detected {miscoded_count} likely miscoded distribution transaction(s)."
            )

        artifacts = [
            rollforward_csv_path,
            rollforward_pdf_path,
            tie_out_csv_path,
            bridge_csv_path,
            bridge_detail_csv_path,
            shareholder_receivable_csv_path,
            direct_equity_csv_path,
            cpa_notes_path,
            miscoded.csv_path,
        ]
        if data_json_path is not None:
            artifacts.append(data_json_path)
        _write_metadata(
            path=metadata_path,
            key=self.key,
            artifacts=artifacts,
            warnings=warnings,
            inputs={
                "year": ctx.year,
                "beginning_book_equity_bucket": f"{source.beginning_book_equity_bucket:.2f}",
                "net_income": f"{source.net_income:.2f}",
                "distribution_total_gl": f"{source.distributions_gl:.2f}",
                "distribution_total_bs_change": f"{source.distributions_bs_change:.2f}",
                "contributions_total_gl": f"{source.contributions:.2f}",
                "other_direct_book_equity_postings_total": (
                    f"{source.other_direct_equity_postings:.2f}"
                ),
                "actual_ending_book_equity_bucket": (
                    f"{source.actual_ending_book_equity_bucket:.2f}"
                ),
                "shareholder_receivable_ending_balance": (
                    f"{source.shareholder_receivable_ending_balance:.2f}"
                ),
                "gl_row_count": len(source.gl_rows),
                "miscoded_distribution_count": miscoded_count,
                "no_raw": ctx.no_raw,
            },
        )

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=[str(path) for path in artifacts],
            warnings=warnings,
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


def _extract_company_name(store: Any) -> str:
    get_info = getattr(store, "get_company_info", None)
    if get_info is None:
        return "Unknown Company"
    try:
        payload = get_info()
    except Exception:
        return "Unknown Company"
    company_info = payload.get("CompanyInfo") if isinstance(payload, Mapping) else None
    if isinstance(company_info, Mapping):
        legal_name = company_info.get("LegalName")
        if isinstance(legal_name, str) and legal_name.strip():
            return legal_name.strip()
        company_name = company_info.get("CompanyName")
        if isinstance(company_name, str) and company_name.strip():
            return company_name.strip()
    return "Unknown Company"
