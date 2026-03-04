from __future__ import annotations

from dataclasses import dataclass
from inspect import Parameter, signature

import pytest

from cpapacket.deliverables.registry import DELIVERABLE_REGISTRY, get_ordered_registry


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


def test_registered_deliverables_define_required_protocol_fields() -> None:
    assert DELIVERABLE_REGISTRY, "Registry should include at least one deliverable"

    for deliverable in DELIVERABLE_REGISTRY:
        assert isinstance(deliverable.key, str) and deliverable.key
        assert isinstance(deliverable.folder, str) and deliverable.folder
        assert isinstance(deliverable.required, bool)
        assert isinstance(deliverable.dependencies, list)
        assert isinstance(deliverable.requires_gusto, bool)


def test_registered_deliverables_expose_supported_generate_signature() -> None:
    for deliverable in DELIVERABLE_REGISTRY:
        generate_signature = signature(deliverable.generate)
        params = list(generate_signature.parameters.values())
        param_names = [param.name for param in params]

        # Accept both the target Protocol shape and current deliverable-specific shape.
        protocol_style = param_names == ["ctx", "store", "prompts"]
        pnl_style = (
            param_names[:3] == ["report_payload", "output_root", "year"]
            and all(param.kind is Parameter.KEYWORD_ONLY for param in params[:3])
        )
        assert protocol_style or pnl_style, (
            f"{type(deliverable).__name__}.generate has unsupported signature: "
            f"{generate_signature}"
        )
