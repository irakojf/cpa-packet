from __future__ import annotations

import gzip
import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from cpapacket.data.store import SessionDataStore


def test_set_get_has_and_clear() -> None:
    store = SessionDataStore()

    assert store.has("qbo:pnl:2025") is False
    assert store.get("qbo:pnl:2025") is None

    payload = {"rows": 3}
    store.set("qbo:pnl:2025", payload)

    assert store.has("qbo:pnl:2025") is True
    assert store.get("qbo:pnl:2025") == payload

    store.clear()
    assert store.get("qbo:pnl:2025") is None


def test_get_or_fetch_caches_subsequent_calls() -> None:
    store = SessionDataStore()
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 42}

    first, first_source = store.get_or_fetch("qbo:pnl:2025", fetcher)
    second, second_source = store.get_or_fetch("qbo:pnl:2025", fetcher)

    assert first == {"value": 42}
    assert second == {"value": 42}
    assert first_source == "api"
    assert second_source == "cache"
    assert calls == 1


def test_get_or_fetch_coalesces_concurrent_requests() -> None:
    store = SessionDataStore()
    started = threading.Event()
    release = threading.Event()
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        started.set()
        release.wait(timeout=2)
        return {"value": 99}

    results: list[tuple[dict[str, int], str]] = []

    def worker() -> None:
        results.append(store.get_or_fetch("qbo:bs:2025", fetcher))

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start()
    started.wait(timeout=2)
    t2.start()
    time.sleep(0.05)
    release.set()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert calls == 1
    assert len(results) == 2
    assert sorted(source for _, source in results) == ["api", "cache"]
    assert all(value == {"value": 99} for value, _ in results)


def test_get_or_fetch_does_not_cache_failures() -> None:
    store = SessionDataStore()
    calls = 0

    def boom() -> dict[str, int]:
        nonlocal calls
        calls += 1
        raise RuntimeError("upstream failed")

    with pytest.raises(RuntimeError, match="upstream failed"):
        store.get_or_fetch("qbo:pnl:2025", boom)

    with pytest.raises(RuntimeError, match="upstream failed"):
        store.get_or_fetch("qbo:pnl:2025", boom)

    assert calls == 2


def test_get_or_fetch_allows_parallel_work_for_different_keys() -> None:
    store = SessionDataStore()
    calls = {"left": 0, "right": 0}
    gate = threading.Event()
    results: list[tuple[dict[str, int], str]] = []

    def fetch_left() -> dict[str, int]:
        calls["left"] += 1
        gate.wait(timeout=2)
        return {"value": 10}

    def fetch_right() -> dict[str, int]:
        calls["right"] += 1
        gate.wait(timeout=2)
        return {"value": 20}

    def worker_left() -> None:
        results.append(store.get_or_fetch("left-key", fetch_left))

    def worker_right() -> None:
        results.append(store.get_or_fetch("right-key", fetch_right))

    t1 = threading.Thread(target=worker_left)
    t2 = threading.Thread(target=worker_right)
    t1.start()
    t2.start()
    time.sleep(0.05)
    gate.set()
    t1.join(timeout=2)
    t2.join(timeout=2)

    assert calls == {"left": 1, "right": 1}
    assert len(results) == 2
    assert sorted(source for _, source in results) == ["api", "api"]
    assert sorted(value["value"] for value, _ in results) == [10, 20]


def test_set_writes_gzip_payload_and_meta(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)

    store.set("abc123", {"rows": 2, "source": "qbo"})

    payload_path = cache_dir / "abc123.json.gz"
    meta_path = cache_dir / "abc123.meta.json"
    assert payload_path.exists()
    assert meta_path.exists()

    with gzip.open(payload_path, mode="rt", encoding="utf-8") as handle:
        assert json.load(handle) == {"rows": 2, "source": "qbo"}

    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    assert metadata["cache_key"] == "abc123"
    assert metadata["ttl_seconds"] > 0
    assert str(metadata["payload_sha256"]).startswith("sha256:")


def test_store_warms_memory_cache_from_disk_on_init(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    seed = SessionDataStore(cache_dir=cache_dir)
    seed.set("warm-key", {"value": 7})

    warmed = SessionDataStore(cache_dir=cache_dir)
    assert warmed.has("warm-key") is True
    assert warmed.get("warm-key") == {"value": 7}


def test_expired_disk_entries_are_ignored(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"

    def old_now() -> datetime:
        return datetime(2026, 1, 1, 0, 0, tzinfo=UTC)

    def fresh_now() -> datetime:
        return datetime(2026, 1, 1, 3, 0, tzinfo=UTC)

    store = SessionDataStore(cache_dir=cache_dir, ttl_hours=1, now_provider=old_now)
    store.set("expiring-key", {"value": 1})

    reloaded = SessionDataStore(cache_dir=cache_dir, ttl_hours=1, now_provider=fresh_now)
    assert reloaded.get("expiring-key") is None

    value, source = reloaded.get_or_fetch("expiring-key", lambda: {"value": 2})
    assert value == {"value": 2}
    assert source == "api"


def test_integrity_mismatch_quarantines_and_refetches(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    seed = SessionDataStore(cache_dir=cache_dir)
    seed.set("bad-key", {"value": 1})

    meta_path = cache_dir / "bad-key.meta.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    metadata["payload_sha256"] = "sha256:deadbeef"
    meta_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    loaded = SessionDataStore(cache_dir=cache_dir)
    value, source = loaded.get_or_fetch("bad-key", lambda: {"value": 2})

    assert value == {"value": 2}
    assert source == "api"

    quarantine_dir = cache_dir / "quarantine"
    assert quarantine_dir.exists()
    quarantine_files = list(quarantine_dir.glob("bad-key__*.quarantine.json"))
    assert quarantine_files, "expected quarantine marker file"


def test_get_or_fetch_force_bypasses_memory_cache() -> None:
    store = SessionDataStore()
    store.set("force-key", {"value": 1})
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 2}

    value, source = store.get_or_fetch("force-key", fetcher, force=True)

    assert source == "api"
    assert value == {"value": 2}
    assert calls == 1
    assert store.get("force-key") == {"value": 2}


def test_get_or_fetch_force_bypasses_disk_cache_and_replaces_cached_value(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    seed = SessionDataStore(cache_dir=cache_dir)
    seed.set("force-disk-key", {"value": 1})

    store = SessionDataStore(cache_dir=cache_dir)
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 2}

    value, source = store.get_or_fetch("force-disk-key", fetcher, force=True)
    assert source == "api"
    assert value == {"value": 2}
    assert calls == 1

    reloaded = SessionDataStore(cache_dir=cache_dir)
    assert reloaded.get("force-disk-key") == {"value": 2}


def test_no_cache_keeps_memory_cache_but_skips_disk_persistence(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 7}

    first, first_source = store.get_or_fetch("nocache-key", fetcher, no_cache=True)
    second, second_source = store.get_or_fetch("nocache-key", fetcher)

    assert first == {"value": 7}
    assert first_source == "api"
    assert second == {"value": 7}
    assert second_source == "cache"
    assert calls == 1
    assert not (cache_dir / "nocache-key.json.gz").exists()
    assert not (cache_dir / "nocache-key.meta.json").exists()


def test_normal_mode_uses_disk_cache_across_store_instances(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    initial = SessionDataStore(cache_dir=cache_dir)
    first, first_source = initial.get_or_fetch("disk-key", lambda: {"value": 3})
    assert first_source == "api"
    assert first == {"value": 3}

    calls = 0

    def should_not_run() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 99}

    reloaded = SessionDataStore(cache_dir=cache_dir)
    second, second_source = reloaded.get_or_fetch("disk-key", should_not_run)

    assert second_source == "cache"
    assert second == {"value": 3}
    assert calls == 0


def test_get_or_fetch_force_bypasses_memory_and_disk_cache(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    seeded = SessionDataStore(cache_dir=cache_dir)
    seeded.set("force-key", {"value": "stale"})

    store = SessionDataStore(cache_dir=cache_dir)
    calls = 0

    def fetcher() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return {"value": "fresh"}

    value, source = store.get_or_fetch("force-key", fetcher, force=True)
    assert value == {"value": "fresh"}
    assert source == "api"
    assert calls == 1

    cached_value, cached_source = store.get_or_fetch("force-key", fetcher)
    assert cached_value == {"value": "fresh"}
    assert cached_source == "cache"
    assert calls == 1


def test_get_or_fetch_no_cache_uses_memory_without_disk_writes(tmp_path: Path) -> None:
    cache_dir = tmp_path / "_meta" / "private" / "cache"
    store = SessionDataStore(cache_dir=cache_dir)
    calls = 0

    def fetcher() -> dict[str, int]:
        nonlocal calls
        calls += 1
        return {"value": 123}

    value, source = store.get_or_fetch("no-cache-key", fetcher, no_cache=True)
    assert value == {"value": 123}
    assert source == "api"
    assert calls == 1

    assert store.get("no-cache-key") == {"value": 123}
    assert not (cache_dir / "no-cache-key.json.gz").exists()
    assert not (cache_dir / "no-cache-key.meta.json").exists()

    cached_value, cached_source = store.get_or_fetch("no-cache-key", fetcher, no_cache=True)
    assert cached_value == {"value": 123}
    assert cached_source == "cache"
    assert calls == 1
