from __future__ import annotations

import json

from click.testing import CliRunner

from cpapacket.cli.main import _resolve_non_interactive, cli


class _FakeStream:
    def __init__(self, isatty: bool) -> None:
        self._isatty = isatty

    def isatty(self) -> bool:
        return self._isatty


def _parse_context(result) -> dict[str, object]:
    assert result.exit_code == 0
    return json.loads(result.output)


def test_cli_non_interactive_defaults_to_abort() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "context-debug"],
    )

    context = _parse_context(result)
    assert context["non_interactive"] is True
    assert context["on_conflict"] == "abort"


def test_cli_on_conflict_overwrite_is_respected() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["--year", "2025", "--on-conflict", "overwrite", "context-debug"],
    )

    context = _parse_context(result)
    assert context["on_conflict"] == "overwrite"


def test_resolve_non_interactive_uses_isatty_detection() -> None:
    assert _resolve_non_interactive(False, stdin=_FakeStream(isatty=False)) is True
    assert _resolve_non_interactive(False, stdin=_FakeStream(isatty=True)) is False
    assert _resolve_non_interactive(True, stdin=_FakeStream(isatty=True)) is True
