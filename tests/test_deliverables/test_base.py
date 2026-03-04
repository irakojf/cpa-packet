from __future__ import annotations

from cpapacket.deliverables.base import Deliverable, DeliverableResult


def test_deliverable_result_minimal_shape() -> None:
    result = DeliverableResult(deliverable_key="pnl", success=True)

    assert result.deliverable_key == "pnl"
    assert result.success is True
    assert result.artifacts == []
    assert result.warnings == []
    assert result.error is None


def test_deliverable_result_accepts_full_payload() -> None:
    result = DeliverableResult(
        deliverable_key="pnl",
        success=False,
        artifacts=["out/file.csv"],
        warnings=["missing optional data"],
        error="upstream timeout",
    )

    assert result.success is False
    assert result.artifacts == ["out/file.csv"]
    assert result.warnings == ["missing optional data"]
    assert result.error == "upstream timeout"


class _DummyDeliverable:
    key = "pnl"
    folder = "01_Year-End_Profit_and_Loss"
    required = True
    dependencies: list[str] = []
    requires_gusto = False

    def gather_prompts(self, ctx: object) -> dict[str, object]:
        del ctx
        return {}

    def is_current(self, ctx: object) -> bool:
        del ctx
        return False

    def generate(
        self,
        ctx: object,
        store: object,
        prompts: dict[str, object],
    ) -> DeliverableResult:
        del ctx, store, prompts
        return DeliverableResult(deliverable_key=self.key, success=True)


def test_runtime_protocol_accepts_conforming_implementation() -> None:
    deliverable = _DummyDeliverable()

    assert isinstance(deliverable, Deliverable)


def test_runtime_protocol_rejects_incomplete_implementation() -> None:
    class _Incomplete:
        key = "pnl"

    assert not isinstance(_Incomplete(), Deliverable)
