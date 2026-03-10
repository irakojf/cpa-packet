"""Retained earnings rollforward deliverable orchestration."""

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
    write_rollforward_csv,
    write_rollforward_data_json,
    write_rollforward_pdf,
)


class RetainedEarningsDeliverable:
    """Generate retained earnings rollforward artifacts from cross-deliverable data."""

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
            distributions=source.distributions,
            actual_ending_re=source.actual_ending_retained_earnings,
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
        base_name = f"Retained_Earnings_Rollforward_{ctx.year}"

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
        data_json_path = (
            None
            if ctx.no_raw
            else _resolve_output_path(
                dev_dir / f"{base_name}_data.json",
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
            path=csv_path,
            year=ctx.year,
            rollforward=rollforward,
            miscoded_distribution_count=miscoded_count,
        )
        write_rollforward_pdf(
            path=pdf_path,
            year=ctx.year,
            rollforward=rollforward,
            miscoded_distribution_count=miscoded_count,
            company_name=company_name,
        )
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
            )

        warnings = list(structural_flags)
        if miscoded_count > 0:
            warnings.append(
                f"Detected {miscoded_count} likely miscoded distribution transaction(s)."
            )

        artifacts = [csv_path, pdf_path, miscoded.csv_path]
        if data_json_path is not None:
            artifacts.append(data_json_path)
        _write_metadata(
            path=metadata_path,
            key=self.key,
            artifacts=artifacts,
            warnings=warnings,
            inputs={
                "year": ctx.year,
                "beginning_re": f"{source.beginning_retained_earnings:.2f}",
                "net_income": f"{source.net_income:.2f}",
                "distributions": f"{source.distributions:.2f}",
                "actual_ending_re": f"{source.actual_ending_retained_earnings:.2f}",
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
