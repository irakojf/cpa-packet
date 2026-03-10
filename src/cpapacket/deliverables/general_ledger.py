"""General ledger deliverable orchestration helpers."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import IO, Any, Protocol, cast

import httpx

from cpapacket.core.context import RunContext
from cpapacket.core.filesystem import atomic_write
from cpapacket.core.limiter import ServiceLimiter
from cpapacket.core.retry import RetryPolicy
from cpapacket.deliverables.base import DeliverableResult
from cpapacket.deliverables.general_ledger_normalizer import normalize_general_ledger_report
from cpapacket.models.general_ledger import GeneralLedgerRow
from cpapacket.utils.constants import (
    BALANCE_EQUATION_TOLERANCE,
    DELIVERABLE_FOLDERS,
    SCHEMA_VERSIONS,
)
from cpapacket.utils.prompts import resolve_output_path
from cpapacket.writers.csv_writer import CsvWriter
from cpapacket.writers.json_writer import JsonWriter


class GeneralLedgerMonthProvider(Protocol):
    """Provider interface for fetching one month of general ledger data."""

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        """Return QBO GeneralLedger payload for a specific month."""

    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        """Return payload and source marker ("api" or "cache")."""


@dataclass(frozen=True)
class GeneralLedgerMonthlySlice:
    """One fetched monthly ledger payload."""

    month: int
    payload: dict[str, Any]
    source: str = "api"


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


_SERVICE_LIMITER = ServiceLimiter()
_RETRY_POLICY = RetryPolicy()


def merge_general_ledger_monthly_slices(
    slices: tuple[GeneralLedgerMonthlySlice, ...],
    *,
    normalizer: Callable[
        [dict[str, Any]], list[GeneralLedgerRow]
    ] = normalize_general_ledger_report,
) -> tuple[GeneralLedgerRow, ...]:
    """Merge monthly slices in month order and deduplicate rows.

    Deduplication uses a stable line-level signature rather than bare ``txn_id``.
    QBO report slices routinely emit multiple legitimate rows for the same
    transaction id across different accounts. Using only ``txn_id`` collapses
    those postings and leaves the merged ledger unbalanced.

    The first occurrence wins, so if the same line appears in adjacent months
    (date-window overlap), the earliest month slice is preserved.
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
    """Fetch monthly general-ledger slices with bounded concurrency.

    Months are fetched in chronological order but a ThreadPoolExecutor keeps up to
    ``QBO_MAX_CONCURRENCY`` slices in flight. ``ServiceLimiter`` gates QBO calls,
    failures report the month that errored plus every previously completed slice,
    and ``progress_callback`` receives sequential month numbers even when fetches
    finish out of order.
    """
    if start_month < 1 or start_month > 12:
        raise ValueError("start_month must be between 1 and 12")
    if end_month < 1 or end_month > 12:
        raise ValueError("end_month must be between 1 and 12")
    if start_month > end_month:
        raise ValueError("start_month must be <= end_month")

    months = list(range(start_month, end_month + 1))
    limit = _SERVICE_LIMITER.limit_for("qbo")
    month_iter = iter(months)

    def _pop_next_month() -> int | None:
        try:
            return next(month_iter)
        except StopIteration:
            return None

    next_progress_month = start_month
    pending_progress: set[int] = set()

    def _record_progress(month: int) -> None:
        nonlocal next_progress_month
        if progress_callback is None:
            return
        pending_progress.add(month)
        while next_progress_month in pending_progress:
            progress_callback(next_progress_month)
            pending_progress.remove(next_progress_month)
            next_progress_month += 1

    def _fetch_month(month: int) -> GeneralLedgerMonthlySlice:
        with _SERVICE_LIMITER.acquire("qbo"):
            payload, source = _fetch_month_payload(provider=provider, year=year, month=month)
        return GeneralLedgerMonthlySlice(
            month=month,
            payload=payload,
            source="cache" if source == "cache" else "api",
        )

    completed: dict[int, GeneralLedgerMonthlySlice] = {}
    pending: dict[Future[GeneralLedgerMonthlySlice], int] = {}

    with ThreadPoolExecutor(max_workers=limit) as executor:
        def _submit(next_month: int) -> None:
            future = executor.submit(_fetch_month, next_month)
            pending[future] = next_month

        initial_slots = min(limit, len(months))
        for _ in range(initial_slots):
            month = _pop_next_month()
            if month is None:
                break
            _submit(month)

        while pending:
            done, _ = wait(set(pending), return_when=FIRST_COMPLETED)
            for future in done:
                month = pending.pop(future)
                try:
                    slice_ = future.result()
                except Exception as exc:  # pragma: no cover - exercised in tests via raised error
                    lower_month_futures = {
                        remaining: remaining_month
                        for remaining, remaining_month in pending.items()
                        if remaining_month < month
                    }
                    for remaining, remaining_month in list(pending.items()):
                        if remaining_month >= month:
                            remaining.cancel()

                    while lower_month_futures:
                        done_lower, _ = wait(
                            set(lower_month_futures),
                            return_when=FIRST_COMPLETED,
                        )
                        for lower_future in done_lower:
                            lower_month = lower_month_futures.pop(lower_future)
                            pending.pop(lower_future, None)
                            try:
                                lower_slice = lower_future.result()
                            except Exception:
                                continue
                            completed[lower_month] = lower_slice
                            _record_progress(lower_month)

                    for remaining in list(pending):
                        remaining.cancel()
                    pending.clear()

                    completed_ordered = tuple(
                        completed[m] for m in sorted(completed) if m < month
                    )
                    raise GeneralLedgerSliceError(
                        year=year,
                        failed_month=month,
                        completed_slices=completed_ordered,
                        cause=exc,
                    ) from exc
                completed[month] = slice_
                _record_progress(month)
                next_month = _pop_next_month()
                if next_month is not None:
                    _submit(next_month)

    return tuple(completed[m] for m in sorted(completed))


def _dedupe_key_for_row(row: GeneralLedgerRow) -> str:
    signature = "|".join(
        (
            row.txn_id.strip(),
            row.date.isoformat(),
            row.transaction_type.strip(),
            row.document_number.strip(),
            row.account_name.strip(),
            row.account_type.strip(),
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

    def is_current(self, ctx: object) -> bool:
        if not isinstance(ctx, RunContext):
            return False
        if not ctx.incremental or ctx.force:
            return False

        metadata_path = ctx.out_dir / "_meta" / f"{self.key}_metadata.json"
        if not metadata_path.exists():
            return False

        try:
            metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False

        recorded_fingerprint = metadata_payload.get("input_fingerprint")
        if not isinstance(recorded_fingerprint, str) or not recorded_fingerprint:
            return False

        artifacts = metadata_payload.get("artifacts")
        if not isinstance(artifacts, list) or not artifacts:
            return False
        artifact_paths = [Path(item) for item in artifacts if isinstance(item, str) and item]
        if len(artifact_paths) != len(artifacts):
            return False
        if any(not artifact.exists() for artifact in artifact_paths):
            return False

        raw_path = ctx.out_dir / self.folder / "dev" / f"General_Ledger_{ctx.year}_raw.json"
        if not raw_path.exists():
            return False

        try:
            raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return False
        slices = raw_payload.get("slices")
        if not isinstance(slices, list):
            return False

        expected_inputs = _build_metadata_inputs(
            slices=slices,
            context_inputs={
                "year": ctx.year,
                "no_raw": ctx.no_raw,
                "redact": ctx.redact,
                "force": ctx.force,
            },
        )
        canonical = json.dumps(expected_inputs, sort_keys=True, separators=(",", ":"))
        expected_fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
        return expected_fingerprint == recorded_fingerprint

    def generate(
        self,
        ctx: RunContext,
        provider: GeneralLedgerMonthProvider,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        del prompts

        if self.is_current(ctx):
            metadata_path = ctx.out_dir / "_meta" / f"{self.key}_metadata.json"
            try:
                metadata_payload = json.loads(metadata_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                metadata_payload = {}
            artifacts_raw = metadata_payload.get("artifacts", [])
            cached_artifacts = [item for item in artifacts_raw if isinstance(item, str)]
            return DeliverableResult(
                deliverable_key=self.key,
                success=True,
                artifacts=cached_artifacts,
                warnings=[
                    "Skipped incremental run; existing general-ledger artifacts are current."
                ],
            )

        warnings: list[str] = []
        slices = fetch_general_ledger_monthly_slices(year=ctx.year, provider=provider)
        rows = merge_general_ledger_monthly_slices(slices)
        if not rows:
            warnings.append("General ledger normalized to zero rows.")

        deliverable_dir = ctx.out_dir / self.folder
        deliverable_dir.mkdir(parents=True, exist_ok=True)
        meta_dir = ctx.out_dir / "_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        cpa_dir = deliverable_dir / "cpa"
        cpa_dir.mkdir(parents=True, exist_ok=True)
        dev_dir = deliverable_dir / "dev"
        dev_dir.mkdir(parents=True, exist_ok=True)

        csv_path = _resolve_output_path(
            cpa_dir / f"General_Ledger_{ctx.year}.csv",
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
            dedupe_id_field=None,
        )

        json_path = JsonWriter().write_payload(
            dev_dir / f"General_Ledger_{ctx.year}_raw.json",
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
        private_metadata_path = (
            ctx.out_dir
            / "_meta"
            / "private"
            / "deliverables"
            / f"{self.key}_{ctx.year}_metadata.json"
        )
        _write_metadata(
            path=private_metadata_path,
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
    return cast(
        Path,
        resolve_output_path(path, on_conflict=normalized, non_interactive=non_interactive),
    )


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
    metadata_inputs = _build_metadata_inputs(
        slices=[slice_.payload for slice_ in slices],
        context_inputs=context_inputs,
    )
    canonical = json.dumps(metadata_inputs, sort_keys=True, separators=(",", ":"))
    fingerprint = sha256(canonical.encode("utf-8")).hexdigest()
    payload: dict[str, Any] = {
        "deliverable": key,
        "inputs": metadata_inputs,
        "input_fingerprint": fingerprint,
        "schema_versions": SCHEMA_VERSIONS.get(key, {}),
        "cached_months": [slice_.month for slice_ in slices if slice_.source == "cache"],
        "fresh_months": [slice_.month for slice_ in slices if slice_.source != "cache"],
        "artifacts": [str(item) for item in artifacts],
        "warnings": warnings,
    }
    with atomic_write(path, mode="w", encoding="utf-8") as handle:
        text_handle = cast(IO[str], handle)
        json.dump(payload, text_handle, indent=2, sort_keys=True)
        text_handle.write("\n")


def _build_metadata_inputs(
    *,
    slices: Sequence[dict[str, Any]],
    context_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **dict(context_inputs),
        "slice_hashes": [
            sha256(
                json.dumps(slice, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            for slice in slices
        ],
    }


def _fetch_month_payload(
    *,
    provider: GeneralLedgerMonthProvider,
    year: int,
    month: int,
) -> tuple[dict[str, Any], str]:
    retries_remaining = _RETRY_POLICY.max_5xx
    while True:
        try:
            if hasattr(provider, "get_general_ledger_with_source"):
                payload_with_source = provider.get_general_ledger_with_source(year, month)
                if (
                    isinstance(payload_with_source, tuple)
                    and len(payload_with_source) == 2
                    and isinstance(payload_with_source[0], dict)
                    and isinstance(payload_with_source[1], str)
                ):
                    return payload_with_source[0], payload_with_source[1]

            return provider.get_general_ledger(year, month), "api"
        except (TimeoutError, httpx.TimeoutException):
            if retries_remaining == 0:
                raise
            retries_remaining -= 1
