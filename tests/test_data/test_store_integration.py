from __future__ import annotations

from pathlib import Path

import httpx
import respx

from cpapacket.data.store import SessionDataStore


def test_store_cache_hit_avoids_duplicate_http_calls(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    url = "https://api.example.test/reports/pnl"

    with respx.mock(assert_all_called=True) as mock:
        route = mock.get(url).mock(return_value=httpx.Response(200, json={"rows": [1, 2, 3]}))

        def fetcher() -> dict[str, object]:
            response = httpx.get(url, timeout=5.0)
            response.raise_for_status()
            return response.json()

        first, first_source = store.get_or_fetch("qbo:pnl:2025", fetcher)
        second, second_source = store.get_or_fetch("qbo:pnl:2025", fetcher)

    assert first == {"rows": [1, 2, 3]}
    assert second == first
    assert first_source == "api"
    assert second_source == "cache"
    assert route.call_count == 1


def test_store_force_flag_refetches_even_when_cache_is_warm(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    url = "https://api.example.test/reports/pnl"

    with respx.mock(assert_all_called=True) as mock:
        route = mock.get(url).mock(
            side_effect=[
                httpx.Response(200, json={"rows": ["stale"]}),
                httpx.Response(200, json={"rows": ["fresh"]}),
            ]
        )

        def fetcher() -> dict[str, object]:
            response = httpx.get(url, timeout=5.0)
            response.raise_for_status()
            return response.json()

        first, first_source = store.get_or_fetch("qbo:pnl:2025", fetcher)
        forced, forced_source = store.get_or_fetch("qbo:pnl:2025", fetcher, force=True)
        cached, cached_source = store.get_or_fetch("qbo:pnl:2025", fetcher)

    assert first == {"rows": ["stale"]}
    assert first_source == "api"
    assert forced == {"rows": ["fresh"]}
    assert forced_source == "api"
    assert cached == {"rows": ["fresh"]}
    assert cached_source == "cache"
    assert route.call_count == 2
