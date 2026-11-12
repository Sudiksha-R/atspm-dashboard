from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable

import server


Builder = Callable[[dict[str, list[str]]], dict[str, Any]]


def _query(**values: str) -> dict[str, list[str]]:
    return {key: [value] for key, value in values.items() if value != ""}


def main() -> None:
    all_ids = ",".join(server.ORDERED_SIGNAL_IDS)
    all_days = ",".join(server.DEMO_DAYS)
    common = {
        "dateFrom": server.DEMO_DATE_FROM.isoformat(),
        "dateTo": server.DEMO_DATE_TO.isoformat(),
        "daysOfWeek": all_days,
        "hourFrom": "06:00",
        "hourTo": "18:00",
        "timeOfDayPreset": "custom",
    }
    window_without_preset = {
        key: value for key, value in common.items() if key != "timeOfDayPreset"
    }

    jobs: list[tuple[str, str, Builder, dict[str, list[str]]]] = [
        (
            "Dashboard",
            "/api/historical-dashboard",
            server.build_historical_dashboard_payload,
            _query(**common, phase="all"),
        ),
        (
            "Dashboard normalized",
            "/api/historical-dashboard",
            server.build_historical_dashboard_payload,
            _query(**common, phase="all", intersectionIds=all_ids),
        ),
        ("Ranking", "/api/ranking", server.build_ranking_payload, _query(**common)),
        (
            "Grouping Intersections",
            "/api/grouping-preview",
            server.build_grouping_preview_payload,
            _query(
                intersectionIds=all_ids,
                dateFrom=common["dateFrom"],
                dateTo=common["dateTo"],
                daysOfWeek=all_days,
                hourFrom=common["hourFrom"],
                hourTo=common["hourTo"],
                moe="avgWait",
                threshold="0.25",
            ),
        ),
        (
            "Correlation Cliques",
            "/api/correlation-cliques",
            server.build_correlation_cliques_payload,
            _query(**window_without_preset, moe="volume", threshold="0.60"),
        ),
        (
            "Bottleneck Detection",
            "/api/bottleneck-detection",
            server.build_bottleneck_detection_payload,
            _query(**window_without_preset, moe="aorPct", threshold="60"),
        ),
        (
            "Change Detection",
            "/api/change-detection",
            server.build_change_detection_payload,
            _query(
                signalIds=all_ids,
                moe="avgWait",
                granularity="hour",
                selectedDate="2026-05-18",
                selectedHour="06:00",
            ),
        ),
        (
            "Ad Hoc Queries",
            "/api/dashboard",
            server.build_dashboard_payload,
            _query(**common, phase="all"),
        ),
        (
            "Ad Hoc Queries normalized",
            "/api/dashboard",
            server.build_dashboard_payload,
            _query(**common, phase="all", intersectionIds=all_ids),
        ),
    ]
    for signal_id in server.ORDERED_SIGNAL_IDS:
        jobs.extend(
            [
                (
                    f"Intersection-wise Analysis · {signal_id}",
                    "/api/historical-run",
                    server.build_historical_payload,
                    _query(signalId=signal_id),
                ),
                (
                    f"Day-Hour View · {signal_id}",
                    "/api/day-hour",
                    server.build_day_hour_payload,
                    _query(**common, signalId=signal_id, metric="avgWait"),
                ),
            ]
        )

    manifest: dict[str, Any] = {
        "version": server.DEMO_CACHE_VERSION,
        "dateFrom": server.DEMO_DATE_FROM.isoformat(),
        "dateTo": server.DEMO_DATE_TO.isoformat(),
        "daysOfWeek": server.DEMO_DAYS,
        "intersectionIds": server.ORDERED_SIGNAL_IDS,
        "entries": [],
    }
    payload_by_signature: dict[tuple[str, str], dict[str, Any]] = {}

    for label, path, builder, query in jobs:
        signature = (builder.__name__, json.dumps(query, sort_keys=True))
        started = time.perf_counter()
        payload = payload_by_signature.get(signature)
        if payload is None:
            payload = builder(query)
            payload_by_signature[signature] = payload
        cache_path = server._store_persistent_api_payload(path, query, payload)
        elapsed = time.perf_counter() - started
        size = cache_path.stat().st_size
        manifest["entries"].append(
            {
                "label": label,
                "path": path,
                "query": query,
                "cacheFile": cache_path.name,
                "bytes": size,
                "buildSeconds": round(elapsed, 3),
            }
        )
        print(f"{label}: {elapsed:.3f}s -> {cache_path.name} ({size / 1024:.1f} KiB)", flush=True)

    manifest_path = Path(server.DEMO_CACHE_DIR) / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"Wrote manifest: {manifest_path}")


if __name__ == "__main__":
    main()
