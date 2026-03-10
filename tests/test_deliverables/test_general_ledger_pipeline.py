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

_GOLDEN_CSV_PATH = Path("tests/fixtures/qbo/general_ledger_2025_golden.csv")


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


def _run_context(tmp_path: Path, *, on_conflict: str = "abort") -> RunContext:
    return RunContext(
        year=2025,
        year_source="explicit",
        out_dir=tmp_path,
        method="accrual",
        non_interactive=True,
        on_conflict=on_conflict,
        incremental=False,
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


def _run_deliverable(tmp_path: Path) -> tuple[respx.MockRoute, Path, Path, Path, Any]:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    with httpx.Client(base_url="https://api.example.test") as http_client:
        qbo_client = _HttpQboClient(http_client)
        providers = DataProviders(store=store, qbo_client=qbo_client, gusto_client=None)
        deliverable = GeneralLedgerDeliverable()

        with respx.mock(assert_all_called=True) as router:
            route = router.get("https://api.example.test/reports/GeneralLedger").mock(
                side_effect=_general_ledger_response
            )
            result = deliverable.generate(_run_context(tmp_path), providers, prompts={})

    csv_path = tmp_path / "03_Full-Year_General_Ledger" / "cpa" / "General_Ledger_2025.csv"
    raw_path = tmp_path / "03_Full-Year_General_Ledger" / "dev" / "General_Ledger_2025_raw.json"
    meta_path = tmp_path / "_meta" / "general_ledger_metadata.json"
    return route, csv_path, raw_path, meta_path, result


def test_general_ledger_pipeline_writes_csv_json_and_metadata(
    tmp_path: Path,
) -> None:
    route, csv_path, raw_path, meta_path, result = _run_deliverable(tmp_path)

    assert route.call_count == 12
    assert csv_path.exists()
    assert raw_path.exists()
    assert meta_path.exists()
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["deliverable"] == "general_ledger"
    assert str(csv_path) in metadata["artifacts"]
    assert str(raw_path) in metadata["artifacts"]
    assert metadata["cached_months"] == []
    raw_payload = json.loads(raw_path.read_text(encoding="utf-8"))
    assert raw_payload["year"] == 2025
    assert len(raw_payload["slices"]) == 12
    assert result.success


def test_general_ledger_metadata_tracks_cached_months(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    deliverable = GeneralLedgerDeliverable()
    context = _run_context(tmp_path)
    meta_path = tmp_path / "_meta" / "general_ledger_metadata.json"

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
            deliverable.generate(context, providers, prompts={})
            deliverable.generate(
                _run_context(tmp_path, on_conflict="overwrite"), providers, prompts={}
            )

    assert route.call_count == 12
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["cached_months"] == list(range(1, 13))
    assert metadata["fresh_months"] == []


def test_general_ledger_pipeline_matches_golden_csv_snapshot(tmp_path: Path) -> None:
    _, csv_path, _, _, result = _run_deliverable(tmp_path)

    expected = _GOLDEN_CSV_PATH.read_text(encoding="utf-8")
    actual = csv_path.read_text(encoding="utf-8")
    assert actual == expected
    assert result.success
