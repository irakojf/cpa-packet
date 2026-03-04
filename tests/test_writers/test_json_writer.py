from __future__ import annotations

import json
from pathlib import Path

from cpapacket.writers.json_writer import JsonWriter


def test_json_writer_writes_raw_payload(tmp_path: Path) -> None:
    writer = JsonWriter()
    destination = tmp_path / "raw.json"
    payload = {"name": "Example Co", "amount": "100.00", "rows": [{"id": 1}, {"id": 2}]}

    result_path = writer.write_payload(destination, payload=payload)

    assert result_path == destination
    assert json.loads(destination.read_text(encoding="utf-8")) == payload
    assert not list(tmp_path.glob("*.tmp"))


def test_json_writer_skips_file_when_no_raw_enabled(tmp_path: Path) -> None:
    writer = JsonWriter()
    destination = tmp_path / "skip.json"

    result_path = writer.write_payload(destination, payload={"ok": True}, no_raw=True)

    assert result_path is None
    assert not destination.exists()


def test_json_writer_redacts_sensitive_fields(tmp_path: Path) -> None:
    writer = JsonWriter()
    destination = tmp_path / "redacted.json"
    payload = {
        "access_token": "token-123",
        "nested": {
            "refresh_token": "refresh-456",
            "api_key": "key-abc",
            "safe": "keep-me",
        },
        "records": [{"ssn": "111-22-3333"}, {"ein": "12-3456789"}],
    }

    writer.write_payload(destination, payload=payload, redact=True)
    written = json.loads(destination.read_text(encoding="utf-8"))

    assert written["access_token"] == "[REDACTED]"
    assert written["nested"]["refresh_token"] == "[REDACTED]"
    assert written["nested"]["api_key"] == "[REDACTED]"
    assert written["nested"]["safe"] == "keep-me"
    assert written["records"][0]["ssn"] == "[REDACTED]"
    assert written["records"][1]["ein"] == "[REDACTED]"


def test_json_writer_overwrites_existing_file_atomically(tmp_path: Path) -> None:
    writer = JsonWriter()
    destination = tmp_path / "overwrite.json"
    destination.write_text('{"old": true}\n', encoding="utf-8")

    writer.write_payload(destination, payload={"new": True})

    assert json.loads(destination.read_text(encoding="utf-8")) == {"new": True}
    assert not list(tmp_path.glob("*.tmp"))


def test_json_writer_handles_large_payload(tmp_path: Path) -> None:
    writer = JsonWriter()
    destination = tmp_path / "large.json"
    payload = {"items": [{"id": index, "value": f"entry-{index}"} for index in range(5000)]}

    writer.write_payload(destination, payload=payload)
    written = json.loads(destination.read_text(encoding="utf-8"))

    assert len(written["items"]) == 5000
    assert written["items"][0] == {"id": 0, "value": "entry-0"}
    assert written["items"][-1] == {"id": 4999, "value": "entry-4999"}
