from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import dask.dataframe as dd
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "atspm" / "src"))

from atspm import SignalDataProcessor  # noqa: E402

MAPPING_CSV = ROOT / "MAPPINGS_DET_INFO_OCT_2022.csv"
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
RAW_FILE_PATTERN = re.compile(r"atspm-(\d{4})-(\d{1,2})-(\d{1,2})_filtered\.csv$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run hourly ATSPM aggregation for one signal across all available days.")
    parser.add_argument("--signal-id", type=int, default=1470, help="Signal ID to process.")
    parser.add_argument(
        "--pattern",
        default="atspm-2024-10-*_filtered.csv",
        help="Glob pattern for the raw daily ATSPM CSV files.",
    )
    return parser.parse_args()


def _raw_files(pattern: str) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for path in sorted(ROOT.glob(pattern)):
        stem = path.stem.replace("atspm-", "").replace("_filtered", "")
        year, month, day = stem.split("-")
        parsed.append((f"{year}-{int(month):02d}-{int(day):02d}", path))
    return parsed


def _source_file_date(path_value: object) -> str:
    path_text = str(path_value)
    match = RAW_FILE_PATTERN.search(Path(path_text).name)
    if not match:
        return ""
    year, month, day = match.groups()
    return f"{year}-{int(month):02d}-{int(day):02d}"


def _output_paths(signal_id: int) -> dict[str, Path]:
    output_dir = ROOT / "derived" / f"dask_historical_{signal_id}_all_days"
    return {
        "output_dir": output_dir,
        "days_dir": output_dir / "days",
        "sqlite_path": output_dir / "atspm_hourly_results.sqlite",
    }


def _prepare_output_dir(paths: dict[str, Path]) -> None:
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    paths["days_dir"].mkdir(parents=True, exist_ok=True)
    if paths["sqlite_path"].exists():
        paths["sqlite_path"].unlink()


def _build_detector_config(signal_id: int) -> pd.DataFrame:
    mapping = pd.read_csv(MAPPING_CSV)
    mapping["DeviceId"] = pd.to_numeric(mapping["SIIA_ID"], errors="coerce")
    mapping = mapping[mapping["DeviceId"] == signal_id].copy()
    mapping["Phase"] = pd.to_numeric(mapping["phase"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    mapping["Parameter"] = pd.to_numeric(mapping["channel"], errors="coerce")
    mapping["distanceToStopbar"] = pd.to_numeric(mapping["distanceToStopbar"], errors="coerce").fillna(0)
    mapping["Function"] = mapping["distanceToStopbar"].apply(lambda value: "Advance" if value > 0 else "Presence")
    mapping = mapping.dropna(subset=["DeviceId", "Phase", "Parameter"])
    mapping["DeviceId"] = mapping["DeviceId"].astype(int)
    mapping["Phase"] = mapping["Phase"].astype(int)
    mapping["Parameter"] = mapping["Parameter"].astype(int)
    return mapping[["DeviceId", "Phase", "Parameter", "Function"]].drop_duplicates().reset_index(drop=True)


def _load_signal_rows_dask(files: list[tuple[str, Path]], signal_id: int) -> pd.DataFrame:
    file_paths = [str(path) for _, path in files]
    frame = dd.read_csv(
        file_paths,
        usecols=RAW_COLUMNS,
        dtype={
            "signalID": "float64",
            "timeStamp": "string",
            "eventCode": "float64",
            "eventParam": "float64",
        },
        blocksize="64MB",
        include_path_column="source_path",
        assume_missing=True,
    )
    frame = frame[frame["signalID"] == signal_id]
    frame["parsed_timestamp"] = dd.to_datetime(frame["timeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
    frame = frame.dropna(subset=["parsed_timestamp"])
    frame["run_date"] = frame["parsed_timestamp"].dt.strftime("%Y-%m-%d")
    frame["source_file_date"] = frame["source_path"].map(
        _source_file_date,
        meta=("source_file_date", "string"),
    )
    frame = frame[frame["run_date"] == frame["source_file_date"]]
    frame["hour"] = frame["parsed_timestamp"].dt.hour.astype("int64")
    materialized = frame.compute()
    if materialized.empty:
        return pd.DataFrame(
            columns=[*RAW_COLUMNS, "source_path", "parsed_timestamp", "run_date", "source_file_date", "hour"]
        )
    materialized["signalID"] = materialized["signalID"].astype(int)
    materialized["eventCode"] = materialized["eventCode"].astype(int)
    materialized["eventParam"] = materialized["eventParam"].astype(int)
    return materialized.sort_values("parsed_timestamp").reset_index(drop=True)


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
    run_id: str,
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
    next_frame.insert(0, "run_id", run_id)
    next_frame.to_sql(table_name, conn, if_exists="append", index=False)
    return len(next_frame)


def _jsonish(payload: dict[str, int]) -> str:
    return ", ".join(f"{key}={value}" for key, value in payload.items())


def _write_metadata(
    conn: sqlite3.Connection,
    run_id: str,
    signal_id: int,
    paths: dict[str, Path],
    detector_rows: int,
    total_source_rows: int,
    available_days: int,
) -> None:
    metadata = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "signal_id": signal_id,
                "days_dir": str(paths["days_dir"]),
                "sqlite_path": str(paths["sqlite_path"]),
                "detector_config_rows": detector_rows,
                "total_source_rows": total_source_rows,
                "available_days": available_days,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "note": "Dask was used for raw CSV loading, signal filtering, timestamp parsing, and day/hour preparation. Original source CSVs were not modified.",
            }
        ]
    )
    metadata.to_sql("run_metadata", conn, if_exists="replace", index=False)


def run_signal_history(signal_id: int, pattern: str = "atspm-2024-10-*_filtered.csv") -> dict[str, Path]:
    run_id = f"signal-{signal_id}-october-hourly-dask"
    files = _raw_files(pattern)
    paths = _output_paths(signal_id)
    _prepare_output_dir(paths)

    detector_config = _build_detector_config(signal_id)
    signal_rows = _load_signal_rows_dask(files, signal_id)

    with sqlite3.connect(paths["sqlite_path"]) as conn:
        _write_metadata(conn, run_id, signal_id, paths, len(detector_config), len(signal_rows), len(files))
        daily_rows: list[dict[str, object]] = []
        file_rows: list[dict[str, object]] = []
        status_rows: list[dict[str, object]] = []

        for run_date, source_csv in files:
            signal_day = signal_rows[signal_rows["run_date"] == run_date].copy()
            day_dir = paths["days_dir"] / run_date
            hours_dir = day_dir / "hours"
            day_dir.mkdir(parents=True, exist_ok=True)
            hours_dir.mkdir(parents=True, exist_ok=True)
            filtered_csv = day_dir / f"signal_{signal_id}_{run_date}_filtered.csv"
            signal_day[RAW_COLUMNS].to_csv(filtered_csv, index=False)

            daily_rows.append(
                {
                    "run_id": run_id,
                    "run_date": run_date,
                    "source_csv": str(source_csv),
                    "filtered_csv": str(filtered_csv),
                    "source_rows": len(signal_day),
                }
            )

            for hour in range(24):
                hour_frame = signal_day[signal_day["hour"] == hour].copy()
                hour_file = hours_dir / f"hour_{hour:02d}.csv"
                hour_frame[RAW_COLUMNS].to_csv(hour_file, index=False)
                source_rows = len(hour_frame)
                file_rows.append(
                    {
                        "run_id": run_id,
                        "run_date": run_date,
                        "hour": hour,
                        "hour_file": str(hour_file),
                        "source_rows": source_rows,
                    }
                )

                if source_rows == 0:
                    status_rows.append(
                        {
                            "run_id": run_id,
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
                        table_name: _write_frame(conn, table_name, table_frame, run_id, run_date, hour, source_rows)
                        for table_name, table_frame in tables.items()
                    }
                    status_rows.append(
                        {
                            "run_id": run_id,
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
                            "run_id": run_id,
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

    return paths


def main() -> None:
    args = _parse_args()
    paths = run_signal_history(args.signal_id, args.pattern)
    print(f"Wrote Dask-derived outputs: {paths['output_dir']}")
    print(f"Wrote SQLite results: {paths['sqlite_path']}")


if __name__ == "__main__":
    main()
