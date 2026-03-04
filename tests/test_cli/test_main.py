from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import cast

from click.testing import CliRunner

from cpapacket.cli import pnl as pnl_cli
from cpapacket.cli.main import build_run_context, cli
from cpapacket.clients.auth import OAuthToken
from cpapacket.core.context import RunContext
from cpapacket.deliverables.base import DeliverableResult


def test_build_run_context_defaults_on_conflict_by_interactive_mode() -> None:
    interactive_ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=False,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )
    non_interactive_ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=True,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )

    assert interactive_ctx.on_conflict == "prompt"
    assert non_interactive_ctx.on_conflict == "abort"


def test_build_run_context_uses_year_resolution_and_owner_keywords() -> None:
    ctx = build_run_context(
        year=None,
        out_dir=Path("/tmp/Acme_2024_CPA_Packet"),
        method="cash",
        non_interactive=True,
        on_conflict="copy",
        incremental=True,
        force=True,
        no_cache=True,
        no_raw=True,
        redact=True,
        include_debug=True,
        verbose=True,
        quiet=False,
        plain=True,
        owner_keywords_raw="Alex, Smith ,",
    )

    assert ctx.year == 2024
    assert ctx.year_source == "inferred"
    assert ctx.method == "cash"
    assert ctx.owner_keywords == ["Alex", "Smith"]
    assert ctx.incremental is True
    assert ctx.force is True


def test_cli_version_flag_exits_successfully() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() != ""


def test_context_debug_outputs_resolved_context_json() -> None:
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "--owner-keywords",
            "Alex,Smith",
            "context-debug",
        ],
    )
    assert result.exit_code == 0
    assert '"year": 2025' in result.output
    assert '"year_source": "explicit"' in result.output
    assert '"owner_keywords"' in result.output


def test_build_run_context_sets_gusto_available_true_when_token_present(
    monkeypatch,
) -> None:
    class _StoreWithToken:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def load_token(self) -> OAuthToken:
            return OAuthToken(
                access_token="access",
                refresh_token="refresh",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithToken)

    ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=False,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )

    assert ctx.gusto_available is True


def test_build_run_context_sets_gusto_available_false_when_token_missing(
    monkeypatch,
) -> None:
    class _StoreWithoutToken:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def load_token(self) -> None:
            return None

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithoutToken)

    ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=False,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )

    assert ctx.gusto_available is False


def test_build_run_context_sets_gusto_available_false_on_detection_error(
    monkeypatch,
) -> None:
    class _BrokenStore:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"
            raise RuntimeError("keyring unavailable")

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _BrokenStore)

    ctx = build_run_context(
        year=2025,
        out_dir=Path("/tmp"),
        method="accrual",
        non_interactive=False,
        on_conflict=None,
        incremental=False,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        owner_keywords_raw=None,
    )

    assert ctx.gusto_available is False


def test_cli_pnl_requires_qbo_env() -> None:
    runner = CliRunner()
    env = {k: v for k, v in os.environ.items() if not k.startswith("CPAPACKET_")}

    result = runner.invoke(cli, ["pnl"], env=env)

    assert result.exit_code == 1
    assert "Missing required environment variable: CPAPACKET_QBO_CLIENT_ID" in result.output


def test_context_debug_infers_year_from_packet_directory(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    packet_dir = tmp_path / "Acme_2024_CPA_Packet"
    packet_dir.mkdir()
    monkeypatch.chdir(packet_dir)

    result = runner.invoke(cli, ["context-debug"])

    assert result.exit_code == 0
    assert '"year": 2024' in result.output
    assert '"year_source": "inferred"' in result.output


def test_context_debug_default_year_before_october(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FakeDate(date):
        @classmethod
        def today(cls) -> date:
            return cls(2026, 9, 1)

    monkeypatch.setattr("cpapacket.core.context.date", _FakeDate)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(cli, ["context-debug"])

    assert result.exit_code == 0
    assert '"year": 2025' in result.output
    assert '"year_source": "default"' in result.output


def test_context_debug_default_year_october_and_later(
    monkeypatch,
    tmp_path: Path,
) -> None:
    class _FakeDate(date):
        @classmethod
        def today(cls) -> date:
            return cls(2026, 10, 1)

    monkeypatch.setattr("cpapacket.core.context.date", _FakeDate)
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(cli, ["context-debug"])

    assert result.exit_code == 0
    assert '"year": 2026' in result.output
    assert '"year_source": "default"' in result.output


def test_auth_qbo_login_prints_authorization_url_and_verifier(monkeypatch) -> None:
    class _FakeQboClient:
        def authorization_url(self, *, state: str) -> tuple[str, str]:
            assert state == "state-123"
            return "https://example.test/oauth", "verifier-abc"

    monkeypatch.setitem(
        build_run_context.__globals__,
        "_build_qbo_client",
        lambda realm_id=None: _FakeQboClient(),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "qbo", "login", "--state", "state-123"])

    assert result.exit_code == 0
    assert "https://example.test/oauth" in result.output
    assert "verifier-abc" in result.output


def test_auth_qbo_status_reports_missing_token(monkeypatch) -> None:
    class _StoreWithoutToken:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "qbo"

        def load_token(self) -> None:
            return None

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithoutToken)

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "qbo", "status"])

    assert result.exit_code == 0
    assert "not authenticated" in result.output


def test_auth_qbo_status_reports_active_and_expired(monkeypatch) -> None:
    tokens = [
        OAuthToken(
            access_token="active",
            refresh_token="refresh",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        ),
        OAuthToken(
            access_token="expired",
            refresh_token="refresh",
            expires_at=datetime.now(UTC) - timedelta(minutes=30),
        ),
    ]

    class _StoreWithSequence:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "qbo"

        def load_token(self) -> OAuthToken:
            return tokens.pop(0)

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithSequence)
    runner = CliRunner()

    active_result = runner.invoke(cli, ["auth", "qbo", "status"])
    expired_result = runner.invoke(cli, ["auth", "qbo", "status"])

    assert active_result.exit_code == 0
    assert "authenticated (active)" in active_result.output
    assert expired_result.exit_code == 0
    assert "authenticated (expired)" in expired_result.output


def test_auth_qbo_logout_clears_token(monkeypatch) -> None:
    cleared = {"called": False}

    class _Store:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "qbo"

        def clear_token(self) -> None:
            cleared["called"] = True

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _Store)

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "qbo", "logout"])

    assert result.exit_code == 0
    assert "token cleared" in result.output.lower()
    assert cleared["called"] is True


def test_auth_qbo_login_requires_code_verifier_with_code(monkeypatch) -> None:
    class _FakeQboClient:
        def authorization_url(self, *, state: str) -> tuple[str, str]:
            return "https://example.test/oauth", "verifier-abc"

    monkeypatch.setitem(
        build_run_context.__globals__,
        "_build_qbo_client",
        lambda realm_id=None: _FakeQboClient(),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "qbo", "login", "--code", "abc123"])

    assert result.exit_code != 0
    assert "--code-verifier is required" in result.output


def test_auth_gusto_login_prints_authorization_url_and_verifier(monkeypatch) -> None:
    class _FakeGustoClient:
        def authorization_url(self, *, state: str) -> tuple[str, str]:
            assert state == "state-456"
            return "https://example.test/gusto/oauth", "gusto-verifier"

    monkeypatch.setitem(
        build_run_context.__globals__,
        "_build_gusto_client",
        lambda: _FakeGustoClient(),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "gusto", "login", "--state", "state-456"])

    assert result.exit_code == 0
    assert "https://example.test/gusto/oauth" in result.output
    assert "gusto-verifier" in result.output


def test_auth_gusto_status_reports_not_configured(monkeypatch) -> None:
    class _StoreWithoutToken:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def load_token(self) -> None:
            return None

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithoutToken)

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "gusto", "status"])

    assert result.exit_code == 0
    assert "not configured" in result.output


def test_pnl_command_runs_deliverable_and_prints_artifacts(monkeypatch) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client

    class _FakeDeliverable:
        def generate(
            self,
            _ctx: object,
            _store: object,
            prompts: dict[str, object],
        ) -> DeliverableResult:
            assert prompts == {}
            return DeliverableResult(
                deliverable_key="pnl",
                success=True,
                artifacts=["/tmp/packet/Profit_and_Loss_2025.csv"],
                warnings=["P&L report normalized to zero rows."],
            )

    monkeypatch.setattr(pnl_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(pnl_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(pnl_cli, "PnlDeliverable", lambda: _FakeDeliverable())
    monkeypatch.setattr(pnl_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(cli, ["--year", "2025", "pnl"])

    assert result.exit_code == 0
    assert "P&L deliverable complete." in result.output
    assert "/tmp/packet/Profit_and_Loss_2025.csv" in result.output
    assert "WARNING: P&L report normalized to zero rows." in result.output


def test_pnl_command_surfaces_deliverable_errors(monkeypatch) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client

    class _FailingDeliverable:
        def generate(
            self,
            _ctx: object,
            _store: object,
            prompts: dict[str, object],
        ) -> DeliverableResult:
            assert prompts == {}
            return DeliverableResult(
                deliverable_key="pnl",
                success=False,
                error="QBO token missing",
            )

    monkeypatch.setattr(pnl_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(pnl_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(pnl_cli, "PnlDeliverable", lambda: _FailingDeliverable())
    monkeypatch.setattr(pnl_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(cli, ["--year", "2025", "pnl"])

    assert result.exit_code != 0
    assert "QBO token missing" in result.output


def test_pnl_command_uses_cash_method_when_flag_is_set(monkeypatch) -> None:
    class _FakeStore:
        def __init__(self, *, cache_dir: Path) -> None:
            self.cache_dir = cache_dir

    class _FakeProviders:
        def __init__(self, *, store: _FakeStore, qbo_client: object) -> None:
            self.store = store
            self.qbo_client = qbo_client

    class _CashAwareDeliverable:
        def generate(
            self,
            _ctx: object,
            _store: object,
            prompts: dict[str, object],
        ) -> DeliverableResult:
            assert prompts == {}
            ctx = cast(RunContext, _ctx)
            assert ctx.method == "cash"
            return DeliverableResult(
                deliverable_key="pnl",
                success=True,
                artifacts=["/tmp/packet/Profit_and_Loss_2025.csv"],
            )

    monkeypatch.setattr(pnl_cli, "SessionDataStore", _FakeStore)
    monkeypatch.setattr(pnl_cli, "DataProviders", _FakeProviders)
    monkeypatch.setattr(pnl_cli, "PnlDeliverable", lambda: _CashAwareDeliverable())
    monkeypatch.setattr(pnl_cli, "_build_qbo_client", lambda: object())

    runner = CliRunner()
    result = runner.invoke(cli, ["--year", "2025", "--method", "cash", "pnl"])

    assert result.exit_code == 0
    assert "P&L deliverable complete." in result.output
    assert "/tmp/packet/Profit_and_Loss_2025.csv" in result.output


def test_auth_gusto_status_reports_active_and_expired(monkeypatch) -> None:
    tokens = [
        OAuthToken(
            access_token="active",
            refresh_token="refresh",
            expires_at=datetime.now(UTC) + timedelta(minutes=30),
        ),
        OAuthToken(
            access_token="expired",
            refresh_token="refresh",
            expires_at=datetime.now(UTC) - timedelta(minutes=30),
        ),
    ]

    class _StoreWithSequence:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def load_token(self) -> OAuthToken:
            return tokens.pop(0)

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _StoreWithSequence)
    monkeypatch.setitem(build_run_context.__globals__, "_detect_gusto_availability", lambda: False)
    runner = CliRunner()

    active_result = runner.invoke(cli, ["auth", "gusto", "status"])
    expired_result = runner.invoke(cli, ["auth", "gusto", "status"])

    assert active_result.exit_code == 0
    assert "authenticated (active)" in active_result.output
    assert expired_result.exit_code == 0
    assert "authenticated (expired)" in expired_result.output


def test_auth_gusto_logout_clears_token(monkeypatch) -> None:
    cleared = {"called": False}

    class _Store:
        def __init__(self, provider_name: str) -> None:
            assert provider_name == "gusto"

        def clear_token(self) -> None:
            cleared["called"] = True

    monkeypatch.setitem(build_run_context.__globals__, "OAuthTokenStore", _Store)

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "gusto", "logout"])

    assert result.exit_code == 0
    assert "token cleared" in result.output.lower()
    assert cleared["called"] is True


def test_auth_gusto_login_requires_code_verifier_with_code(monkeypatch) -> None:
    class _FakeGustoClient:
        def authorization_url(self, *, state: str) -> tuple[str, str]:
            return "https://example.test/gusto/oauth", "gusto-verifier"

    monkeypatch.setitem(
        build_run_context.__globals__,
        "_build_gusto_client",
        lambda: _FakeGustoClient(),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["auth", "gusto", "login", "--code", "abc123"])

    assert result.exit_code != 0
    assert "--code-verifier is required" in result.output
