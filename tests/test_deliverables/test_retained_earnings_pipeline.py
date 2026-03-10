from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import respx

from cpapacket.data.providers import DataProviders
from cpapacket.data.store import SessionDataStore
from cpapacket.reconciliation.retained_earnings import (
    build_retained_earnings_rollforward,
    evaluate_re_structural_flags,
    load_re_source_data,
)


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


def _balance_sheet_payload(retained_earnings: str) -> dict[str, Any]:
    return {
        "Rows": {
            "Row": [
                {
                    "Header": {"ColData": [{"value": "Equity"}]},
                    "Rows": {
                        "Row": [
                            {
                                "ColData": [
                                    {"value": "Retained Earnings"},
                                    {"value": retained_earnings},
                                ]
                            }
                        ]
                    },
                }
            ]
        }
    }


def _pnl_payload(net_income: str) -> dict[str, Any]:
    return {
        "Rows": {
            "Row": [
                {
                    "Summary": {
                        "ColData": [
                            {"value": "Net Income"},
                            {"value": net_income},
                        ]
                    }
                }
            ]
        }
    }


def _gl_row(
    *,
    txn_id: str,
    account_name: str,
    account_type: str,
    amount: str,
    memo: str,
) -> dict[str, Any]:
    return {
        "TxnId": txn_id,
        "TxnDate": "2025-01-15",
        "TxnType": "Journal",
        "DocNum": f"DOC-{txn_id}",
        "AccountName": account_name,
        "AccountType": account_type,
        "Payee": "Owner",
        "Memo": memo,
        "Amount": amount,
    }


def test_retained_earnings_pipeline_balanced_uses_respx_and_cache(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def balance_sheet_response(request: httpx.Request) -> httpx.Response:
        as_of = request.url.params.get("as_of_date")
        if as_of == "2024-12-31":
            payload = _balance_sheet_payload("100.00")
        elif as_of == "2025-12-31":
            payload = _balance_sheet_payload("325.00")
        else:  # pragma: no cover
            payload = {"Rows": {"Row": []}}
        return httpx.Response(200, json=payload)

    def pnl_response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_pnl_payload("250.00"))

    def general_ledger_response(request: httpx.Request) -> httpx.Response:
        month = int(request.url.params["start_date"].split("-")[1])
        rows = (
            [
                _gl_row(
                    txn_id="DIST-1",
                    account_name="Shareholder Distributions",
                    account_type="Equity",
                    amount="25.00",
                    memo="owner distribution",
                )
            ]
            if month == 1
            else []
        )
        return httpx.Response(200, json={"Rows": {"Row": rows}})

    with httpx.Client(base_url="https://qbo.api.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        with respx.mock(assert_all_called=True) as router:
            balance_route = router.get("https://qbo.api.test/reports/BalanceSheet").mock(
                side_effect=balance_sheet_response
            )
            pnl_route = router.get("https://qbo.api.test/reports/ProfitAndLoss").mock(
                side_effect=pnl_response
            )
            ledger_route = router.get("https://qbo.api.test/reports/GeneralLedger").mock(
                side_effect=general_ledger_response
            )

            source = load_re_source_data(year=2025, provider=providers)
            flags = evaluate_re_structural_flags(
                net_income=source.net_income,
                distributions=source.distributions,
                actual_ending_re=source.actual_ending_retained_earnings,
                gl_rows=source.gl_rows,
            )
            rollforward = build_retained_earnings_rollforward(source=source, structural_flags=flags)

            # Second run should be fully served from SessionDataStore cache.
            load_re_source_data(year=2025, provider=providers)

    assert source.beginning_retained_earnings == 100
    assert source.net_income == 250
    assert source.distributions == 25
    assert source.actual_ending_retained_earnings == 325
    assert rollforward.status == "Balanced"
    assert rollforward.difference == 0
    assert rollforward.flags == []
    assert balance_route.call_count == 2
    assert pnl_route.call_count == 1
    assert ledger_route.call_count == 12


def test_retained_earnings_pipeline_mismatch_sets_structural_flags(
    tmp_path: Path,
) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    def balance_sheet_response(request: httpx.Request) -> httpx.Response:
        as_of = request.url.params.get("as_of_date")
        if as_of == "2024-12-31":
            payload = _balance_sheet_payload("100.00")
        elif as_of == "2025-12-31":
            payload = _balance_sheet_payload("-10.00")
        else:  # pragma: no cover
            payload = {"Rows": {"Row": []}}
        return httpx.Response(200, json=payload)

    def pnl_response(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_pnl_payload("250.00"))

    def general_ledger_response(request: httpx.Request) -> httpx.Response:
        month = int(request.url.params["start_date"].split("-")[1])
        if month == 1:
            rows = [
                _gl_row(
                    txn_id="DIST-2",
                    account_name="Shareholder Distributions",
                    account_type="Equity",
                    amount="300.00",
                    memo="owner distribution",
                ),
                _gl_row(
                    txn_id="RE-ADJ",
                    account_name="Retained Earnings",
                    account_type="Equity",
                    amount="20.00",
                    memo="direct re posting",
                ),
            ]
        else:
            rows = []
        return httpx.Response(200, json={"Rows": {"Row": rows}})

    with httpx.Client(base_url="https://qbo.api.test") as http_client:
        providers = DataProviders(
            store=store,
            qbo_client=_HttpQboClient(http_client),
            gusto_client=None,
        )
        with respx.mock(assert_all_called=True) as router:
            balance_route = router.get("https://qbo.api.test/reports/BalanceSheet").mock(
                side_effect=balance_sheet_response
            )
            pnl_route = router.get("https://qbo.api.test/reports/ProfitAndLoss").mock(
                side_effect=pnl_response
            )
            ledger_route = router.get("https://qbo.api.test/reports/GeneralLedger").mock(
                side_effect=general_ledger_response
            )

            source = load_re_source_data(year=2025, provider=providers)
            flags = evaluate_re_structural_flags(
                net_income=source.net_income,
                distributions=source.distributions,
                actual_ending_re=source.actual_ending_retained_earnings,
                gl_rows=source.gl_rows,
            )
            rollforward = build_retained_earnings_rollforward(source=source, structural_flags=flags)

    assert rollforward.status == "Mismatch"
    assert rollforward.difference == 60
    assert "basis_risk_distributions_exceed_net_income" in rollforward.flags
    assert "negative_ending_retained_earnings" in rollforward.flags
    assert "direct_retained_earnings_postings_detected" in rollforward.flags
    assert balance_route.call_count == 2
    assert pnl_route.call_count == 1
    assert ledger_route.call_count == 12
