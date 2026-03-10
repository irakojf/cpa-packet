from __future__ import annotations

import importlib
import json
from datetime import date
from pathlib import Path

from click.testing import CliRunner

from cpapacket.cli.main import cli


def test_tax_init_non_interactive_creates_tracker_and_deadlines(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))

    result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )

    assert result.exit_code == 0
    tracker_path = config_root / "tax_tracker_2025.json"
    deadlines_path = config_root / "tax_deadlines_2025.json"
    assert tracker_path.exists()
    assert deadlines_path.exists()

    tracker_payload = json.loads(tracker_path.read_text(encoding="utf-8"))
    deadlines_payload = json.loads(deadlines_path.read_text(encoding="utf-8"))
    assert len(tracker_payload) == 9
    assert len(deadlines_payload) == 11
    assert any(item["jurisdiction"] == "DE" for item in deadlines_payload)


def test_tax_init_non_interactive_refuses_overwrite_by_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))

    first = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert first.exit_code == 0

    second = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert second.exit_code == 1
    assert "already exist for 2025" in second.output


def test_tax_init_interactive_accepts_custom_amounts(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    cli_module = importlib.import_module("cpapacket.cli.main")
    monkeypatch.setattr(cli_module, "_resolve_non_interactive", lambda *_: False)

    result = runner.invoke(
        cli,
        ["--year", "2025", "tax", "init"],
        input="1200.00\n800.00\n300.00\n",
    )
    assert result.exit_code == 0

    tracker_path = config_root / "tax_tracker_2025.json"
    tracker_payload = json.loads(tracker_path.read_text(encoding="utf-8"))
    federal_amounts = {
        item["amount"] for item in tracker_payload if item["jurisdiction"] == "Federal"
    }
    ny_amounts = {item["amount"] for item in tracker_payload if item["jurisdiction"] == "NY"}
    de_amounts = {item["amount"] for item in tracker_payload if item["jurisdiction"] == "DE"}
    assert federal_amounts == {"1200.00"}
    assert ny_amounts == {"800.00"}
    assert de_amounts == {"300.00"}


def test_tax_mark_paid_updates_matching_payment(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.tax_tracker.user_config_dir", lambda *_: str(config_root))

    init_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert init_result.exit_code == 0

    mark_result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "tax",
            "mark-paid",
            "--jurisdiction",
            "DE",
            "--due",
            "03/01/25",
            "--paid-date",
            "03/05/25",
        ],
    )
    assert mark_result.exit_code == 0

    tracker_payload = json.loads(
        (config_root / "tax_tracker_2025.json").read_text(encoding="utf-8")
    )
    matching = [
        item
        for item in tracker_payload
        if item["jurisdiction"] == "DE" and item["due_date"] == "2025-03-01"
    ]
    assert len(matching) == 1
    assert matching[0]["status"] == "paid"
    assert matching[0]["paid_date"] == "2025-03-05"


def test_tax_mark_paid_errors_when_payment_not_found(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.tax_tracker.user_config_dir", lambda *_: str(config_root))

    init_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert init_result.exit_code == 0

    mark_result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "tax",
            "mark-paid",
            "--jurisdiction",
            "NY",
            "--due",
            "01/01/25",
        ],
    )
    assert mark_result.exit_code == 1
    assert "No payment found for jurisdiction=NY due=2025-01-01" in mark_result.output


def test_tax_status_renders_dashboard_with_due_labels(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.cli.tax_tracker._today_utc_date", lambda: date(2025, 4, 20))

    init_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert init_result.exit_code == 0

    status_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "status"],
    )
    assert status_result.exit_code == 0
    assert "Estimated Tax Payment Status" in status_result.output
    assert "PAST DUE" in status_result.output
    assert "UPCOMING" in status_result.output


def test_tax_update_modifies_existing_payment_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.tax_tracker.user_config_dir", lambda *_: str(config_root))

    init_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert init_result.exit_code == 0

    update_result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "tax",
            "update",
            "--jurisdiction",
            "DE",
            "--due",
            "03/01/25",
            "--amount",
            "777.77",
            "--status",
            "paid",
            "--paid-date",
            "03/06/25",
            "--new-due",
            "03/02/25",
        ],
    )
    assert update_result.exit_code == 0
    assert "Updated payment:" in update_result.output

    tracker_payload = json.loads(
        (config_root / "tax_tracker_2025.json").read_text(encoding="utf-8")
    )
    matching = [
        item
        for item in tracker_payload
        if item["jurisdiction"] == "DE" and item["due_date"] == "2025-03-02"
    ]
    assert len(matching) == 1
    assert matching[0]["amount"] == "777.77"
    assert matching[0]["status"] == "paid"
    assert matching[0]["paid_date"] == "2025-03-06"


def test_tax_update_errors_when_payment_not_found(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runner = CliRunner()
    config_root = tmp_path / "cfg"
    monkeypatch.setattr("cpapacket.packet.tax_tracker.user_config_dir", lambda *_: str(config_root))
    monkeypatch.setattr("cpapacket.tax_tracker.user_config_dir", lambda *_: str(config_root))

    init_result = runner.invoke(
        cli,
        ["--year", "2025", "--non-interactive", "tax", "init"],
    )
    assert init_result.exit_code == 0

    update_result = runner.invoke(
        cli,
        [
            "--year",
            "2025",
            "--non-interactive",
            "tax",
            "update",
            "--jurisdiction",
            "NY",
            "--due",
            "01/01/25",
            "--amount",
            "12.34",
        ],
    )
    assert update_result.exit_code == 1
    assert "No payment found for jurisdiction=NY due=2025-01-01" in update_result.output
