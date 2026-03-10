"""Estimated tax tracker initialization and persistence helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from platformdirs import user_config_dir

from cpapacket.core.filesystem import atomic_write
from cpapacket.models.tax import EstimatedTaxPayment, Jurisdiction, TaxDeadline

_APP_NAME = "cpapacket"
_QUARTERLY_DUE_DATES: tuple[tuple[str, int, int], ...] = (
    ("Q1", 4, 15),
    ("Q2", 6, 16),
    ("Q3", 9, 15),
    ("Q4", 1, 15),
)


@dataclass(frozen=True, slots=True)
class TaxTrackerInitResult:
    """Result payload for a successful tracker initialization."""

    tracker_path: Path
    deadlines_path: Path
    payment_count: int
    deadline_count: int


def config_root() -> Path:
    """Return the global cpapacket config root."""
    return Path(user_config_dir(_APP_NAME, _APP_NAME))


def tracker_file_path(*, year: int, root: Path | None = None) -> Path:
    base = root if root is not None else config_root()
    return base / f"tax_tracker_{year}.json"


def deadlines_file_path(*, year: int, root: Path | None = None) -> Path:
    base = root if root is not None else config_root()
    return base / f"tax_deadlines_{year}.json"


def generate_default_deadlines(*, year: int) -> list[TaxDeadline]:
    """Generate baseline filing and estimated-tax deadlines for Federal/NY/DE."""
    deadlines: list[TaxDeadline] = [
        TaxDeadline(
            jurisdiction="Federal",
            name="S-Corp Return (1120-S)",
            due_date=date(year, 3, 15),
            category="filing",
        ),
        TaxDeadline(
            jurisdiction="Federal",
            name="Individual Return",
            due_date=date(year, 4, 15),
            category="filing",
        ),
        TaxDeadline(
            jurisdiction="DE",
            name="Franchise Tax",
            due_date=date(year, 3, 1),
            category="filing",
        ),
    ]

    for quarter, month, day in _QUARTERLY_DUE_DATES:
        due_year = year + 1 if quarter == "Q4" else year
        due = date(due_year, month, day)
        deadlines.append(
            TaxDeadline(
                jurisdiction="Federal",
                name=f"Estimated Tax {quarter}",
                due_date=due,
                category="estimated_tax",
            )
        )
        deadlines.append(
            TaxDeadline(
                jurisdiction="NY",
                name=f"Estimated Tax {quarter}",
                due_date=due,
                category="estimated_tax",
            )
        )

    return deadlines


def initialize_tax_tracker(
    *,
    year: int,
    federal_quarterly_amount: Decimal,
    ny_quarterly_amount: Decimal,
    de_franchise_amount: Decimal,
    root: Path | None = None,
    overwrite: bool = False,
) -> TaxTrackerInitResult:
    """Create tracker/deadline config files using default jurisdiction schedules."""
    deadlines = generate_default_deadlines(year=year)
    payments = _build_default_payments(
        year=year,
        deadlines=deadlines,
        quarterly_amounts={
            "Federal": federal_quarterly_amount,
            "NY": ny_quarterly_amount,
            "DE": de_franchise_amount,
        },
    )
    tracker_path = tracker_file_path(year=year, root=root)
    deadlines_path = deadlines_file_path(year=year, root=root)

    if not overwrite and (tracker_path.exists() or deadlines_path.exists()):
        raise FileExistsError(
            f"Tax tracker files already exist for {year}: {tracker_path} and/or {deadlines_path}"
        )

    _write_json(tracker_path, [payment.model_dump(mode="json") for payment in payments])
    _write_json(deadlines_path, [deadline.model_dump(mode="json") for deadline in deadlines])

    return TaxTrackerInitResult(
        tracker_path=tracker_path,
        deadlines_path=deadlines_path,
        payment_count=len(payments),
        deadline_count=len(deadlines),
    )


def _build_default_payments(
    *,
    year: int,
    deadlines: list[TaxDeadline],
    quarterly_amounts: dict[Jurisdiction, Decimal],
) -> list[EstimatedTaxPayment]:
    now = datetime.now(UTC)
    payments: list[EstimatedTaxPayment] = []

    for deadline in deadlines:
        if deadline.category != "estimated_tax":
            continue
        amount = quarterly_amounts[deadline.jurisdiction]
        payments.append(
            EstimatedTaxPayment(
                jurisdiction=deadline.jurisdiction,
                due_date=deadline.due_date,
                amount=amount,
                status="not_paid",
                paid_date=None,
                last_updated=now,
            )
        )

    payments.append(
        EstimatedTaxPayment(
            jurisdiction="DE",
            due_date=date(year, 3, 1),
            amount=quarterly_amounts["DE"],
            status="not_paid",
            paid_date=None,
            last_updated=now,
        )
    )
    return payments


def _write_json(path: Path, payload: object) -> None:
    with atomic_write(path, mode="w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
