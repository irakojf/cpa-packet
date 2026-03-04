"""Base protocol and result types for deliverable implementations."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, Field


class DeliverableResult(BaseModel):
    """Standard result payload for a deliverable generation step."""

    deliverable_key: str
    success: bool
    artifacts: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None


@runtime_checkable
class Deliverable(Protocol):
    """Runtime-checkable protocol implemented by each deliverable module."""

    key: str
    folder: str
    required: bool
    dependencies: list[str]
    requires_gusto: bool

    def gather_prompts(self, ctx: Any) -> dict[str, Any]:
        """Collect any interactive or contextual prompts before generation."""

    def is_current(self, ctx: Any) -> bool:
        """Return True when outputs for this deliverable are still current."""

    def generate(
        self,
        ctx: Any,
        store: Any,
        prompts: dict[str, Any],
    ) -> DeliverableResult:
        """Generate deliverable outputs and return metadata about the run."""
