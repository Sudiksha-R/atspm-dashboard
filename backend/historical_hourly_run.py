from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "atspm" / "src"))

from atspm import SignalDataProcessor  # noqa: E402

SIGNAL_ID = 1470
RUN_DATE = "2024-10-03"
SOURCE_CSV = ROOT / "atspm-2024-10-3_filtered.csv"
MAPPING_CSV = ROOT / "MAPPINGS_DET_INFO_OCT_2022.csv"
OUTPUT_DIR = ROOT / "derived" / f"historical_{SIGNAL_ID}_{RUN_DATE}"
HOURS_DIR = OUTPUT_DIR / "hours"
FILTERED_CSV = OUTPUT_DIR / f"signal_{SIGNAL_ID}_{RUN_DATE}_filtered.csv"
SQLITE_PATH = OUTPUT_DIR / "atspm_hourly_results.sqlite"
RUN_ID = f"signal-{SIGNAL_ID}-{RUN_DATE}-hourly"

RAW_COLUMNS = ["signalID", "timeStamp", "eventCode", "eventParam"]
AGGREGATION_TABLES = [
    "has_data",
    "arrival_on_green",
    "split_failures",
    "terminations",
    "timeline",
    "phase_wait",
    "coordination_agg",
]


def _prepare_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    HOURS_DIR.mkdir(parents=True, exist_ok=True)
    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()


def _load_signal_day() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(SOURCE_CSV, usecols=RAW_COLUMNS, chunksize=500_000):
        chunk = chunk[chunk["signalID"] == SIGNAL_ID].copy()
        if chunk.empty:
            continue

        timestamps = pd.to_datetime(chunk["timeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
        chunk = chunk[timestamps.dt.strftime("%Y-%m-%d") == RUN_DATE].copy()
        if chunk.empty:
            continue
        chunk["_parsed_timestamp"] = pd.to_datetime(chunk["timeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
        frames.append(chunk)

    if not frames:
        return pd.DataFrame(columns=[*RAW_COLUMNS, "_parsed_timestamp"])
    return pd.concat(frames, ignore_index=True).sort_values("_parsed_timestamp").reset_index(drop=True)


def _build_detector_config() -> pd.DataFrame:
    mapping = pd.read_csv(MAPPING_CSV)
    mapping["DeviceId"] = pd.to_numeric(mapping["SIIA_ID"], errors="coerce")
    mapping = mapping[mapping["DeviceId"] == SIGNAL_ID].copy()
    mapping["Phase"] = pd.to_numeric(mapping["phase"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    mapping["Parameter"] = pd.to_numeric(mapping["channel"], errors="coerce")
    mapping["distanceToStopbar"] = pd.to_numeric(mapping["distanceToStopbar"], errors="coerce").fillna(0)
    mapping["Function"] = mapping["distanceToStopbar"].apply(lambda value: "Advance" if value > 0 else "Presence")
    mapping = mapping.dropna(subset=["DeviceId", "Phase", "Parameter"])
    mapping["DeviceId"] = mapping["DeviceId"].astype(int)
    mapping["Phase"] = mapping["Phase"].astype(int)
    mapping["Parameter"] = mapping["Parameter"].astype(int)
    return mapping[["DeviceId", "Phase", "Parameter", "Function"]].drop_duplicates().reset_index(drop=True)


def _normalize_hour(hour_frame: pd.DataFrame) -> pd.DataFrame:
    normalized = hour_frame.rename(
        columns={
            "signalID": "DeviceId",
            "timeStamp": "TimeStamp",
            "eventCode": "EventId",
            "eventParam": "Parameter",
        }
    )[["TimeStamp", "DeviceId", "EventId", "Parameter"]].copy()
    normalized["TimeStamp"] = pd.to_datetime(normalized["TimeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
    return normalized.dropna(subset=["TimeStamp"]).sort_values("TimeStamp").reset_index(drop=True)


def _run_atspm(raw_data: pd.DataFrame, detector_config: pd.DataFrame) -> dict[str, pd.DataFrame]:
    params: dict[str, Any] = {
        "raw_data": raw_data,
        "detector_config": detector_config,
        "bin_size": 15,
        "remove_incomplete": True,
        "aggregations": [
            {"name": "has_data", "params": {"no_data_min": 5, "min_data_points": 3}},
            {"name": "arrival_on_green", "params": {"latency_offset_seconds": 0}},
            {
                "name": "split_failures",
                "params": {
                    "red_time": 5,
                    "red_occupancy_threshold": 0.80,
                    "green_occupancy_threshold": 0.80,
                    "by_approach": True,
                },
            },
            {"name": "terminations", "params": {}},
            {"name": "timeline", "params": {"maxtime": True, "min_duration": 1, "cushion_time": 1, "live": False}},
            {
                "name": "phase_wait",
                "params": {
                    "preempt_recovery_seconds": 120,
                    "assumed_cycle_length": 140,
                    "skip_multiplier": 1.5,
                },
            },
            {"name": "coordination_agg", "params": {}},
        ],
        "verbose": 0,
        "controller_type": "maxtime",
    }

    with SignalDataProcessor(**params) as processor:
        processor.load()
        processor.aggregate()
        return {
            table_name: processor.conn.query(f"SELECT * FROM {table_name}").df()
            for table_name in AGGREGATION_TABLES
        }


def _sqlite_safe(frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy()
    for column in safe.columns:
        if pd.api.types.is_datetime64_any_dtype(safe[column]):
            safe[column] = safe[column].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    return safe


def _write_frame(conn: sqlite3.Connection, table_name: str, frame: pd.DataFrame, hour: int, source_rows: int) -> int:
    if frame.empty:
        return 0
    next_frame = _sqlite_safe(frame)
    next_frame.insert(0, "source_rows", source_rows)
    next_frame.insert(0, "hour", hour)
    next_frame.insert(0, "run_id", RUN_ID)
    next_frame.to_sql(table_name, conn, if_exists="append", index=False)
    return len(next_frame)


def _write_metadata(conn: sqlite3.Connection, filtered_rows: int, detector_rows: int) -> None:
    metadata = pd.DataFrame(
        [
            {
                "run_id": RUN_ID,
                "signal_id": SIGNAL_ID,
                "run_date": RUN_DATE,
                "source_csv": str(SOURCE_CSV),
                "filtered_csv": str(FILTERED_CSV),
                "hours_dir": str(HOURS_DIR),
                "sqlite_path": str(SQLITE_PATH),
                "filtered_rows": filtered_rows,
                "detector_config_rows": detector_rows,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "note": "Derived outputs only. Original source CSVs are read but not modified.",
            }
        ]
    )
    metadata.to_sql("run_metadata", conn, if_exists="replace", index=False)


def main() -> None:
    _prepare_output_dir()
    signal_day = _load_signal_day()
    detector_config = _build_detector_config()

    signal_day[RAW_COLUMNS].to_csv(FILTERED_CSV, index=False)

    with sqlite3.connect(SQLITE_PATH) as conn:
        _write_metadata(conn, len(signal_day), len(detector_config))
        status_rows: list[dict[str, Any]] = []
        file_rows: list[dict[str, Any]] = []

        for hour in range(24):
            hour_frame = signal_day[signal_day["_parsed_timestamp"].dt.hour == hour].copy()
            hour_file = HOURS_DIR / f"hour_{hour:02d}.csv"
            hour_frame[RAW_COLUMNS].to_csv(hour_file, index=False)
            source_rows = len(hour_frame)
            file_rows.append(
                {
                    "run_id": RUN_ID,
                    "hour": hour,
                    "hour_file": str(hour_file),
                    "source_rows": source_rows,
                }
            )

            if source_rows == 0:
                status_rows.append(
                    {
                        "run_id": RUN_ID,
                        "hour": hour,
                        "status": "empty",
                        "source_rows": 0,
                        "message": "No event rows in this hour.",
                    }
                )
                continue

            try:
                normalized = _normalize_hour(hour_frame)
                tables = _run_atspm(normalized, detector_config)
                table_counts = {
                    table_name: _write_frame(conn, table_name, table_frame, hour, source_rows)
                    for table_name, table_frame in tables.items()
                }
                status_rows.append(
                    {
                        "run_id": RUN_ID,
                        "hour": hour,
                        "status": "ok",
                        "source_rows": source_rows,
                        "message": jsonish(table_counts),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                status_rows.append(
                    {
                        "run_id": RUN_ID,
                        "hour": hour,
                        "status": "failed",
                        "source_rows": source_rows,
                        "message": f"{type(exc).__name__}: {exc}",
                    }
                )

        pd.DataFrame(file_rows).to_sql("hour_files", conn, if_exists="replace", index=False)
        pd.DataFrame(status_rows).to_sql("hourly_run_status", conn, if_exists="replace", index=False)

    print(f"Wrote filtered day CSV: {FILTERED_CSV}")
    print(f"Wrote hourly CSVs: {HOURS_DIR}")
    print(f"Wrote SQLite results: {SQLITE_PATH}")


def jsonish(payload: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in payload.items())


if __name__ == "__main__":
    main()
