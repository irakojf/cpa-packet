from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import respx

from cpapacket.core.context import RunContext
from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.deliverables.general_ledger import GeneralLedgerDeliverable


class _HttpQboClient:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        return self._client.request(method, endpoint, params=params, json=json_body)


class _FailingProvider:
    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        raise AssertionError(f"incremental skip failed; provider was called for {year}-{month:02d}")


class _SourceAwareFixtureProvider:
    def get_general_ledger_with_source(
        self,
        year: int,
        month: int,
    ) -> tuple[dict[str, Any], str]:
        fixture_path = Path("tests/fixtures/qbo") / f"general_ledger_{month:02d}.json"
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        source = "cache" if month % 2 == 0 else "api"
        return payload, source

    def get_general_ledger(self, year: int, month: int) -> dict[str, Any]:
        payload, _source = self.get_general_ledger_with_source(year, month)
        return payload


def _run_context(tmp_path: Path, *, incremental: bool) -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict="abort",
        incremental=incremental,
        force=False,
        no_cache=False,
        no_raw=False,
        redact=False,
        include_debug=False,
        verbose=False,
        quiet=False,
        plain=False,
        skip=[],
        owner_keywords=[],
        gusto_available=False,
    )


def _general_ledger_response(request: httpx.Request) -> httpx.Response:
    start_date = request.url.params["start_date"]
    month = int(start_date.split("-")[1])
    fixture_path = Path("tests/fixtures/qbo") / f"general_ledger_{month:02d}.json"
    payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    return httpx.Response(200, json=payload)


def _seed_outputs(tmp_path: Path) -> tuple[RunContext, GeneralLedgerDeliverable]:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    deliverable = GeneralLedgerDeliverable()

    with httpx.Client(base_url="https://api.example.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/GeneralLedger").mock(
                side_effect=_general_ledger_response
            )
            result = deliverable.generate(
                _run_context(tmp_path, incremental=False),
                providers,
                prompts={},
            )

    assert route.call_count == 12
    assert result.success
    return _run_context(tmp_path, incremental=True), deliverable


def test_general_ledger_incremental_skips_when_fingerprint_matches(tmp_path: Path) -> None:
    ctx_incremental, deliverable = _seed_outputs(tmp_path)

    assert deliverable.is_current(ctx_incremental)

    result = deliverable.generate(ctx_incremental, _FailingProvider(), prompts={})

    assert result.success
    assert result.artifacts
    assert "Skipped incremental run" in result.warnings[0]


def test_general_ledger_incremental_detects_raw_payload_drift(tmp_path: Path) -> None:
    ctx_incremental, deliverable = _seed_outputs(tmp_path)

    raw_path = tmp_path / "03_Full-Year_General_Ledger" / "dev" / "General_Ledger_2025_raw.json"
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    raw_payload["slices"][0]["Header"]["Time"] = "tampered"
    raw_path.write_text(f"{json.dumps(raw_payload, indent=2, sort_keys=True)}\n", encoding="utf-8")

    assert deliverable.is_current(ctx_incremental) is False


def test_general_ledger_metadata_tracks_cached_months_and_private_path(tmp_path: Path) -> None:
    deliverable = GeneralLedgerDeliverable()
    result = deliverable.generate(
        _run_context(tmp_path, incremental=False),
        _SourceAwareFixtureProvider(),
        prompts={},
    )

    assert result.success

    public_meta = tmp_path / "_meta" / "general_ledger_metadata.json"
    private_meta = (
        tmp_path / "_meta" / "private" / "deliverables" / "general_ledger_2025_metadata.json"
    )
    assert public_meta.exists()
    assert private_meta.exists()

    payload = json.loads(private_meta.read_text(encoding="utf-8"))
    assert payload["cached_months"] == [2, 4, 6, 8, 10, 12]
    assert payload["fresh_months"] == [1, 3, 5, 7, 9, 11]
