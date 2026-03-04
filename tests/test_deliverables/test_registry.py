from __future__ import annotations

from dataclasses import dataclass

import pytest

from cpapacket.deliverables.registry import get_ordered_registry


@dataclass(frozen=True)
class _DummyDeliverable:
    key: str
    dependencies: list[str]
    folder: str = "x"
    required: bool = True
    requires_gusto: bool = False

    def gather_prompts(self, ctx: object) -> dict[str, object]:
        del ctx
        return {}

    def is_current(self, ctx: object) -> bool:
        del ctx
        return False

    def generate(self, ctx: object, store: object, prompts: dict[str, object]) -> object:
        del ctx, store, prompts
        return object()


def test_orders_dependencies_before_dependents() -> None:
    registry = [
        _DummyDeliverable(key="c", dependencies=["b"]),
        _DummyDeliverable(key="a", dependencies=[]),
        _DummyDeliverable(key="b", dependencies=["a"]),
    ]

    ordered = get_ordered_registry(registry=registry)

    assert [item.key for item in ordered] == ["a", "b", "c"]


def test_filter_keys_includes_dependency_closure() -> None:
    registry = [
        _DummyDeliverable(key="a", dependencies=[]),
        _DummyDeliverable(key="b", dependencies=["a"]),
        _DummyDeliverable(key="c", dependencies=["b"]),
    ]

    ordered = get_ordered_registry(registry=registry, filter_keys=["c"])

    assert [item.key for item in ordered] == ["a", "b", "c"]


def test_filter_keys_rejects_unknown_key() -> None:
    registry = [_DummyDeliverable(key="a", dependencies=[])]

    with pytest.raises(KeyError, match="Unknown deliverable key"):
        get_ordered_registry(registry=registry, filter_keys=["missing"])


def test_rejects_missing_dependency_definition() -> None:
    registry = [_DummyDeliverable(key="a", dependencies=["missing"])]

    with pytest.raises(KeyError, match="depends on missing key"):
        get_ordered_registry(registry=registry, filter_keys=["a"])


def test_detects_dependency_cycle() -> None:
    registry = [
        _DummyDeliverable(key="a", dependencies=["c"]),
        _DummyDeliverable(key="b", dependencies=["a"]),
        _DummyDeliverable(key="c", dependencies=["b"]),
    ]

    with pytest.raises(ValueError, match="Cycle detected"):
        get_ordered_registry(registry=registry)
