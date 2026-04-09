from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "atspm" / "src"))

from atspm import SignalDataProcessor  # noqa: E402

SIGNAL_ID = 1470
MAPPING_CSV = ROOT / "MAPPINGS_DET_INFO_OCT_2022.csv"
OUTPUT_DIR = ROOT / "derived" / f"historical_{SIGNAL_ID}_all_days"
DAYS_DIR = OUTPUT_DIR / "days"
SQLITE_PATH = OUTPUT_DIR / "atspm_hourly_results_all_days.sqlite"
RUN_ID = f"signal-{SIGNAL_ID}-october-hourly"

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


def _raw_files() -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for path in sorted(ROOT.glob("atspm-2024-10-*_filtered.csv")):
        stem = path.stem.replace("atspm-", "").replace("_filtered", "")
        year, month, day = stem.split("-")
        parsed.append((f"{year}-{int(month):02d}-{int(day):02d}", path))
    return parsed


def _prepare_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DAYS_DIR.mkdir(parents=True, exist_ok=True)
    if SQLITE_PATH.exists():
        SQLITE_PATH.unlink()


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


def _load_signal_day(source_csv: Path, run_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for chunk in pd.read_csv(source_csv, usecols=RAW_COLUMNS, chunksize=500_000):
        chunk = chunk[chunk["signalID"] == SIGNAL_ID].copy()
        if chunk.empty:
            continue
        chunk["_parsed_timestamp"] = pd.to_datetime(chunk["timeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
        chunk = chunk[chunk["_parsed_timestamp"].dt.strftime("%Y-%m-%d") == run_date].copy()
        if chunk.empty:
            continue
        frames.append(chunk)

    if not frames:
        return pd.DataFrame(columns=[*RAW_COLUMNS, "_parsed_timestamp"])
    return pd.concat(frames, ignore_index=True).sort_values("_parsed_timestamp").reset_index(drop=True)


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
    params = {
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


def _write_frame(
    conn: sqlite3.Connection,
    table_name: str,
    frame: pd.DataFrame,
    run_date: str,
    hour: int,
    source_rows: int,
) -> int:
    if frame.empty:
        return 0
    next_frame = _sqlite_safe(frame)
    next_frame.insert(0, "source_rows", source_rows)
    next_frame.insert(0, "hour", hour)
    next_frame.insert(0, "run_date", run_date)
    next_frame.insert(0, "run_id", RUN_ID)
    next_frame.to_sql(table_name, conn, if_exists="append", index=False)
    return len(next_frame)


def _write_metadata(conn: sqlite3.Connection, detector_rows: int) -> None:
    metadata = pd.DataFrame(
        [
            {
                "run_id": RUN_ID,
                "signal_id": SIGNAL_ID,
                "days_dir": str(DAYS_DIR),
                "sqlite_path": str(SQLITE_PATH),
                "detector_config_rows": detector_rows,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "available_days": len(_raw_files()),
                "note": "Derived outputs only. Original source CSVs are read but not modified.",
            }
        ]
    )
    metadata.to_sql("run_metadata", conn, if_exists="replace", index=False)


def _jsonish(payload: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in payload.items())


def main() -> None:
    _prepare_output_dir()
    detector_config = _build_detector_config()

    with sqlite3.connect(SQLITE_PATH) as conn:
        _write_metadata(conn, len(detector_config))
        daily_rows: list[dict[str, object]] = []
        file_rows: list[dict[str, object]] = []
        status_rows: list[dict[str, object]] = []

        for run_date, source_csv in _raw_files():
            signal_day = _load_signal_day(source_csv, run_date)
            day_dir = DAYS_DIR / run_date
            hours_dir = day_dir / "hours"
            day_dir.mkdir(parents=True, exist_ok=True)
            hours_dir.mkdir(parents=True, exist_ok=True)
            filtered_csv = day_dir / f"signal_{SIGNAL_ID}_{run_date}_filtered.csv"
            signal_day[RAW_COLUMNS].to_csv(filtered_csv, index=False)

            daily_rows.append(
                {
                    "run_id": RUN_ID,
                    "run_date": run_date,
                    "source_csv": str(source_csv),
                    "filtered_csv": str(filtered_csv),
                    "source_rows": len(signal_day),
                }
            )

            for hour in range(24):
                hour_frame = signal_day[signal_day["_parsed_timestamp"].dt.hour == hour].copy()
                hour_file = hours_dir / f"hour_{hour:02d}.csv"
                hour_frame[RAW_COLUMNS].to_csv(hour_file, index=False)
                source_rows = len(hour_frame)
                file_rows.append(
                    {
                        "run_id": RUN_ID,
                        "run_date": run_date,
                        "hour": hour,
                        "hour_file": str(hour_file),
                        "source_rows": source_rows,
                    }
                )

                if source_rows == 0:
                    status_rows.append(
                        {
                            "run_id": RUN_ID,
                            "run_date": run_date,
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
                        table_name: _write_frame(conn, table_name, table_frame, run_date, hour, source_rows)
                        for table_name, table_frame in tables.items()
                    }
                    status_rows.append(
                        {
                            "run_id": RUN_ID,
                            "run_date": run_date,
                            "hour": hour,
                            "status": "ok",
                            "source_rows": source_rows,
                            "message": _jsonish(table_counts),
                        }
                    )
                except Exception as exc:  # noqa: BLE001
                    status_rows.append(
                        {
                            "run_id": RUN_ID,
                            "run_date": run_date,
                            "hour": hour,
                            "status": "failed",
                            "source_rows": source_rows,
                            "message": f"{type(exc).__name__}: {exc}",
                        }
                    )

        pd.DataFrame(daily_rows).to_sql("daily_files", conn, if_exists="replace", index=False)
        pd.DataFrame(file_rows).to_sql("hour_files", conn, if_exists="replace", index=False)
        pd.DataFrame(status_rows).to_sql("hourly_run_status", conn, if_exists="replace", index=False)

    print(f"Wrote all-day derived outputs: {OUTPUT_DIR}")
    print(f"Wrote SQLite results: {SQLITE_PATH}")


if __name__ == "__main__":
    main()
