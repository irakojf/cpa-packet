"""General ledger deliverable orchestration helpers."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, IO, Protocol, cast

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.deliverables.general_ledger_normalizer import normalize_general_ledger_report
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.utils.constants import DELIVERABLE_FOLDERS, SCHEMA_VERSIONS
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter


class GeneralLedgerMonthProvider(Protocol):
    """Provider interface for fetching one month of general ledger data."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return QBO GeneralLedger payload for a specific month."""


@dataclass(frozen=True)
class GeneralLedgerMonthlySlice:
    """One fetched monthly ledger payload."""

    month: int
    payload: dict[str, Any]


class GeneralLedgerSliceError(RuntimeError):
    """Raised when monthly slicing fails for a specific month."""

    def __init__(
        self,
        *,
        year: int,
        failed_month: int,
        completed_slices: tuple[GeneralLedgerMonthlySlice, ...],
        cause: Exception,
    ) -> None:
        super().__init__(
            f"general ledger monthly slicing failed for {year}-{failed_month:02d}: {cause}"
        )
        self.year = year
        self.failed_month = failed_month
        self.completed_slices = completed_slices
        self.cause = cause


def merge_general_ledger_monthly_slices(
    slices: tuple[GeneralLedgerMonthlySlice, ...],
    *,
    normalizer: Callable[
        [dict[str, Any]], list[GeneralLedgerRow]
    ] = normalize_general_ledger_report,
) -> tuple[GeneralLedgerRow, ...]:
    """Merge monthly slices in month order and deduplicate rows.

    Deduplication key preference:
    1. ``txn_id`` when present.
    2. Composite hash of stable transaction fields when ``txn_id`` is blank.

    The first occurrence wins, so if the same transaction appears in adjacent
    months (date-window overlap), the earliest month slice is preserved.
    """
    merged: list[GeneralLedgerRow] = []
    seen_keys: set[str] = set()

    for slice_ in sorted(slices, key=lambda item: item.month):
        for row in normalizer(slice_.payload):
            key = _dedupe_key_for_row(row)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            merged.append(row)

    return tuple(merged)


def fetch_general_ledger_monthly_slices(
    *,
    year: int,
    provider: GeneralLedgerMonthProvider,
    start_month: int = 1,
    end_month: int = 12,
    progress_callback: Callable[[int], None] | None = None,
) -> tuple[GeneralLedgerMonthlySlice, ...]:
    """Fetch monthly general-ledger slices in order with resumable ranges.

    Pass ``start_month`` to resume after a prior partial failure (for example,
    retrying from the first failed month onward).
    """
    if start_month < 1 or start_month > 12:
        raise ValueError("start_month must be between 1 and 12")
    if end_month < 1 or end_month > 12:
        raise ValueError("end_month must be between 1 and 12")
    if start_month > end_month:
        raise ValueError("start_month must be <= end_month")

    completed: list[GeneralLedgerMonthlySlice] = []
    for month in range(start_month, end_month + 1):
        try:
            payload = provider.get_general_ledger(year, month)
        except Exception as exc:  # pragma: no cover - exercised in tests via raised error
            raise GeneralLedgerSliceError(
                year=year,
                failed_month=month,
                completed_slices=tuple(completed),
                cause=exc,
            ) from exc

        completed.append(GeneralLedgerMonthlySlice(month=month, payload=payload))
        if progress_callback is not None:
            progress_callback(month)

    return tuple(completed)


def _dedupe_key_for_row(row: GeneralLedgerRow) -> str:
    txn_id = row.txn_id.strip()
    if txn_id:
        return f"txn:{txn_id}"

    signature = "|".join(
        (
            row.date.isoformat(),
            row.transaction_type.strip(),
            row.document_number.strip(),
            row.account_name.strip(),
            format(row.debit, "f"),
            format(row.credit, "f"),
            (row.payee or "").strip(),
            (row.memo or "").strip(),
        )
    )
    return f"composite:{sha256(signature.encode('utf-8')).hexdigest()}"


class GeneralLedgerDeliverable:
    """Deliverable implementation for the full-year general ledger."""

    key = "general_ledger"
    folder = DELIVERABLE_FOLDERS["general_ledger"]
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, _ctx: object) -> dict[str, Any]:
        return {}

    def is_current(self, _ctx: object) -> bool:
        return False

    def generate(
        self,
        ctx: RunContext,
        provider: GeneralLedgerMonthProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        warnings: list[str] = []
        slices = fetch_general_ledger_monthly_slices(year=ctx.year, provider=provider)
        rows = merge_general_ledger_monthly_slices(slices)
        if not rows:
            warnings.append("General ledger normalized to zero rows.")

        deliverable_dir = ctx.out_dir / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = ctx.out_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)

        csv_path = _resolve_output_path(
            deliverable_dir / f"General_Ledger_{ctx.year}.csv",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )
        csv_writer = CsvWriter()
        csv_writer.write_rows_streaming(
            csv_path,
            fieldnames=[
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
            ],
            rows=_iter_csv_rows(rows),
            dedupe_id_field="txn_id",
        )

        json_path = JsonWriter().write_payload(
            deliverable_dir / f"General_Ledger_{ctx.year}_raw.json",
            payload=_build_raw_payload(ctx.year, slices),
            no_raw=ctx.no_raw,
            redact=ctx.redact,
        )

        metadata_path = _resolve_output_path(
            meta_dir / f"{self.key}_metadata.json",
            on_conflict=ctx.on_conflict,
            non_interactive=ctx.non_interactive,
        )

        metadata_artifacts = [csv_path]
        if json_path is not None:
            metadata_artifacts.append(json_path)

        _write_metadata(
            path=metadata_path,
            key=self.key,
            slices=slices,
            artifacts=metadata_artifacts,
            warnings=warnings,
            context_inputs={
                "year": ctx.year,
                "no_raw": ctx.no_raw,
                "redact": ctx.redact,
                "force": ctx.force,
            },
        )

        artifacts: list[str] = [str(csv_path)]
        if json_path is not None:
            artifacts.append(str(json_path))

        return DeliverableResult(
            deliverable_key=self.key,
            success=True,
            artifacts=artifacts,
            warnings=warnings,
        )


def _resolve_output_path(
    path: Path,
    *,
    on_conflict: str,
    non_interactive: bool,
) -> Path:
    normalized = None if on_conflict == "prompt" else on_conflict
    return resolve_output_path(path, on_conflict=normalized, non_interactive=non_interactive)


def _iter_csv_rows(rows: Sequence[GeneralLedgerRow]) -> Iterable[dict[str, str]]:
    for row in rows:
        yield {
            "txn_id": row.txn_id,
            "date": row.date.isoformat(),
            "transaction_type": row.transaction_type,
            "document_number": row.document_number,
            "account_name": row.account_name,
            "account_type": row.account_type,
            "payee": row.payee or "",
            "memo": row.memo or "",
            "debit": format(row.debit, "f"),
            "credit": format(row.credit, "f"),
            "signed_amount": format(row.signed_amount, "f"),
        }


def _build_raw_payload(year: int, slices: Sequence[GeneralLedgerMonthlySlice]) -> dict[str, Any]:
    return {
        "deliverable": "general_ledger",
        "year": year,
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "slices": [slice_.payload for slice_ in slices],
    }


def _write_metadata(
    *,
    path: Path,
    key: str,
    slices: Sequence[GeneralLedgerMonthlySlice],
    artifacts: Sequence[Path],
    warnings: list[str],
    context_inputs: Mapping[str, Any],
) -> None:
    metadata_inputs = {
        **dict(context_inputs),
        "slice_hashes": [
            sha256(json.dumps(slice_.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            for slice_ in slices
        ],
    }
    canonical = json.dumps(metadata_inputs, sort_keys=True, separators=(",", ":"))
    fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
    payload: dict[str, Any] = {
        "deliverable": key,
        "input_fingerprint": fingerprint,
        "schema_versions": SCHEMA_VERSIONS.get(key, {}),
        "artifacts": [str(item) for item in artifacts],
        "warnings": warnings,
    }
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")
