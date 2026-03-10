"""Deliverable registry and dependency-aware ordering helpers."""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Sequence

from cpapacket.deliverables.base import Deliverable

GeneralLedgerDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.general_ledger import (
        GeneralLedgerDeliverable as _GeneralLedgerDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    GeneralLedgerDeliverableCtor = None
else:
    GeneralLedgerDeliverableCtor = _GeneralLedgerDeliverable

PnlDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.pnl import PnlDeliverable as _PnlDeliverable
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    PnlDeliverableCtor = None
else:
    PnlDeliverableCtor = _PnlDeliverable

DistributionsDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.distributions import (
        DistributionsDeliverable as _DistributionsDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    DistributionsDeliverableCtor = None
else:
    DistributionsDeliverableCtor = _DistributionsDeliverable

PayrollSummaryDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.payroll_summary import (
        PayrollSummaryDeliverable as _PayrollSummaryDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    PayrollSummaryDeliverableCtor = None
else:
    PayrollSummaryDeliverableCtor = _PayrollSummaryDeliverable

ContractorSummaryDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.contractor_summary import (
        ContractorSummaryDeliverable as _ContractorSummaryDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    ContractorSummaryDeliverableCtor = None
else:
    ContractorSummaryDeliverableCtor = _ContractorSummaryDeliverable

PayrollReconDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.payroll_recon import (
        PayrollReconDeliverable as _PayrollReconDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    PayrollReconDeliverableCtor = None
else:
    PayrollReconDeliverableCtor = _PayrollReconDeliverable

TaxTrackerDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.tax_tracker import TaxTrackerDeliverable as _TaxTrackerDeliverable
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    TaxTrackerDeliverableCtor = None
else:
    TaxTrackerDeliverableCtor = _TaxTrackerDeliverable

BalanceSheetDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.balance_sheet import (
        BalanceSheetDeliverable as _BalanceSheetDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    BalanceSheetDeliverableCtor = None
else:
    BalanceSheetDeliverableCtor = _BalanceSheetDeliverable

PriorBalanceSheetDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.balance_sheet import (
        PriorBalanceSheetDeliverable as _PriorBalanceSheetDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    PriorBalanceSheetDeliverableCtor = None
else:
    PriorBalanceSheetDeliverableCtor = _PriorBalanceSheetDeliverable

RetainedEarningsDeliverableCtor: type[Deliverable] | None
try:
    from cpapacket.deliverables.retained_earnings import (
        RetainedEarningsDeliverable as _RetainedEarningsDeliverable,
    )
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    RetainedEarningsDeliverableCtor = None
else:
    RetainedEarningsDeliverableCtor = _RetainedEarningsDeliverable


def _build_default_registry() -> tuple[Deliverable, ...]:
    entries: list[Deliverable] = []
    if PnlDeliverableCtor is not None:
        entries.append(PnlDeliverableCtor())
    if BalanceSheetDeliverableCtor is not None:
        entries.append(BalanceSheetDeliverableCtor())
    if GeneralLedgerDeliverableCtor is not None:
        entries.append(GeneralLedgerDeliverableCtor())
    if PayrollSummaryDeliverableCtor is not None:
        entries.append(PayrollSummaryDeliverableCtor())
    if ContractorSummaryDeliverableCtor is not None:
        entries.append(ContractorSummaryDeliverableCtor())
    if DistributionsDeliverableCtor is not None:
        entries.append(DistributionsDeliverableCtor())
    if TaxTrackerDeliverableCtor is not None:
        entries.append(TaxTrackerDeliverableCtor())
    if PriorBalanceSheetDeliverableCtor is not None:
        entries.append(PriorBalanceSheetDeliverableCtor())
    if RetainedEarningsDeliverableCtor is not None:
        entries.append(RetainedEarningsDeliverableCtor())
    if PayrollReconDeliverableCtor is not None:
        entries.append(PayrollReconDeliverableCtor())
    return tuple(entries)


DELIVERABLE_REGISTRY: tuple[Deliverable, ...] = _build_default_registry()


def get_ordered_registry(
    *,
    filter_keys: Iterable[str] | None = None,
    registry: Sequence[Deliverable] | None = None,
) -> list[Deliverable]:
    """Return deliverables in dependency-safe topological order.

    When ``filter_keys`` is set, explicit keys are included along with all
    transitive dependencies required to run them safely.
    """
    source = list(registry if registry is not None else DELIVERABLE_REGISTRY)
    by_key = {item.key: item for item in source}

    include: set[str]
    if filter_keys is None:
        include = set(by_key)
    else:
        requested = {key.strip() for key in filter_keys if key and key.strip()}
        unknown = requested - set(by_key)
        if unknown:
            raise KeyError(f"Unknown deliverable key(s): {', '.join(sorted(unknown))}")

        include = set()
        stack = list(requested)
        while stack:
            key = stack.pop()
            if key in include:
                continue
            include.add(key)
            for dependency in by_key[key].dependencies:
                if dependency not in by_key:
                    raise KeyError(f"Deliverable '{key}' depends on missing key '{dependency}'")
                stack.append(dependency)

    indegree: dict[str, int] = {key: 0 for key in include}
    adjacency: dict[str, list[str]] = {key: [] for key in include}

    for key in include:
        deliverable = by_key[key]
        for dependency in deliverable.dependencies:
            if dependency not in include:
                continue
            adjacency[dependency].append(key)
            indegree[key] += 1

    declaration_rank = {deliverable.key: index for index, deliverable in enumerate(source)}
    queue = deque(
        sorted(
            (key for key, degree in indegree.items() if degree == 0),
            key=lambda item: declaration_rank[item],
        )
    )

    ordered_keys: list[str] = []
    while queue:
        key = queue.popleft()
        ordered_keys.append(key)
        for dependent in sorted(adjacency[key], key=lambda item: declaration_rank[item]):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(ordered_keys) != len(include):
        raise ValueError("Cycle detected in deliverable dependency graph")

    return [by_key[key] for key in ordered_keys]
