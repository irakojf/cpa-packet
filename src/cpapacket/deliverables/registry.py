"""Deliverable registry and dependency-aware ordering helpers."""

from __future__ import annotations

from collections import deque
from typing import Iterable, Sequence

from cpapacket.deliverables.base import Deliverable

try:
    from cpapacket.deliverables.pnl import PnlDeliverable
except Exception:  # pragma: no cover - best-effort bootstrap during early scaffolding
    PnlDeliverable = None  # type: ignore[assignment]


def _build_default_registry() -> tuple[Deliverable, ...]:
    entries: list[Deliverable] = []
    if PnlDeliverable is not None:
        entries.append(PnlDeliverable())
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
                    raise KeyError(
                        f"Deliverable '{key}' depends on missing key '{dependency}'"
                    )
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
            key=declaration_rank.get,
        )
    )

    ordered_keys: list[str] = []
    while queue:
        key = queue.popleft()
        ordered_keys.append(key)
        for dependent in sorted(adjacency[key], key=declaration_rank.get):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                queue.append(dependent)

    if len(ordered_keys) != len(include):
        raise ValueError("Cycle detected in deliverable dependency graph")

    return [by_key[key] for key in ordered_keys]
