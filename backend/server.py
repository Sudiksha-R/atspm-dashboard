from __future__ import annotations

import json
import os
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd
from sklearn.cluster import DBSCAN, HDBSCAN

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "atspm" / "src"))

from atspm import SignalDataProcessor  # noqa: E402
from historical_hourly_run_dask import _output_paths as historical_output_paths  # noqa: E402
from historical_hourly_run_dask import run_signal_history  # noqa: E402
from mapping_store import load_effective_mapping  # noqa: E402

RAW_PATTERN = re.compile(r"atspm-(\d{4})-(\d{1,2})-(\d{1,2})_filtered\.csv$")
DEFAULT_RAW_GLOB = "atspm-*_filtered.csv"
DEFAULT_BEE_RIDGE_DIR_CANDIDATES = (
    ROOT / "derived" / "bee_ridge_atspm_feb_to_may",
    ROOT / "derived" / "bee_ridge_atspm_mar_to_may",
    ROOT / "derived" / "bee_ridge_atspm_v3",
)
DEFAULT_HISTORICAL_SIGNAL_ID = "1240"
DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
TIME_OF_DAY_PRESETS = [
    {"id": "am-peak", "label": "Weekday AM Peak", "start": "06:00", "end": "09:00"},
    {"id": "midday", "label": "Midday", "start": "10:00", "end": "14:00"},
    {"id": "pm-peak", "label": "Weekday PM Peak", "start": "15:00", "end": "19:00"},
    {"id": "weekend-daytime", "label": "Weekend Daytime", "start": "09:00", "end": "20:00"},
    {"id": "custom", "label": "Custom Period", "start": "06:00", "end": "18:00"},
]

PHASE_TABLE_COLUMNS = [
    "DeviceId",
    "Phase",
    "aog",
    "total_actuations",
    "green_time",
    "green_occupancy",
    "red_occupancy",
    "split_failures",
    "avg_wait",
    "max_wait",
    "skips",
    "GapOut",
    "MaxOut",
    "ForceOff",
    "cycle_length",
    "red_time",
    "gor_proxy",
    "sur_proxy",
]
TREND_TABLE_COLUMNS = [
    "DeviceId",
    "Bucket",
    "label",
    "aog",
    "total_actuations",
    "green_time",
    "green_occupancy",
    "red_occupancy",
    "split_failures",
    "avg_wait",
    "skips",
    "GapOut",
    "MaxOut",
    "ForceOff",
    "cycle_length",
    "red_time",
    "gor_proxy",
    "sur_proxy",
]

BOTTLENECK_MOE_DEFINITIONS = {
    "aogPct": "Arrival on Green percentage.",
    "aorPct": "Arrival on Red percentage, computed as 100 minus AOG.",
    "volume": "Average hourly detector actuation volume for the phase.",
    "avgWait": "Average phase wait in seconds.",
    "greenOccupancyPct": "Average green occupancy percentage.",
    "redOccupancyPct": "Average red occupancy percentage.",
    "splitFailures": "Average hourly split-failure count.",
    "maxOuts": "Average hourly Max-Out count.",
    "forceOffs": "Average hourly Force-Off count.",
}

BOTTLENECK_MOE_LABELS = {
    "aogPct": "AOG %",
    "aorPct": "AOR %",
    "volume": "Volume",
    "avgWait": "Avg Wait",
    "greenOccupancyPct": "Green Occ %",
    "redOccupancyPct": "Red Occ %",
    "splitFailures": "Split Failures",
    "maxOuts": "Max-Outs",
    "forceOffs": "Force-Offs",
}

CHANGE_DETECTION_MOE_KEYS = [
    "aogPct",
    "aorPct",
    "avgWait",
    "greenOccupancyPct",
    "redOccupancyPct",
    "splitFailures",
    "maxOuts",
    "forceOffs",
]


def _empty_phase_table() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="float64") for column in PHASE_TABLE_COLUMNS})


def _empty_trend_table() -> pd.DataFrame:
    return pd.DataFrame({column: pd.Series(dtype="float64") for column in TREND_TABLE_COLUMNS})


@dataclass(frozen=True)
class IntersectionMeta:
    id: str
    name: str
    route: str
    lat: float
    lon: float
    region: str


def _parse_file_date(path: Path) -> date | None:
    match = RAW_PATTERN.match(path.name)
    if not match:
        return None
    year, month, day = match.groups()
    return date(int(year), int(month), int(day))


def _raw_data_dir() -> Path:
    if raw_dir := os.environ.get("ATSPM_RAW_DIR"):
        return Path(raw_dir)
    for candidate in DEFAULT_BEE_RIDGE_DIR_CANDIDATES:
        if candidate.exists() and any(candidate.glob(DEFAULT_RAW_GLOB)):
            return candidate
    return ROOT


def _raw_data_glob() -> str:
    return os.environ.get("ATSPM_RAW_GLOB", DEFAULT_RAW_GLOB)


@lru_cache(maxsize=1)
def _raw_files() -> dict[date, Path]:
    files: dict[date, Path] = {}
    for path in _raw_data_dir().glob(_raw_data_glob()):
        parsed = _parse_file_date(path)
        if parsed:
            files[parsed] = path
    return dict(sorted(files.items()))


def _default_analysis_date() -> date:
    available = list(_raw_files().items())
    if not available:
        raise RuntimeError("No raw ATSPM files are available.")
    if len(available) == 1:
        return available[0][0]

    latest_date, latest_path = available[-1]
    previous_date, previous_path = available[-2]
    latest_size = latest_path.stat().st_size
    previous_size = previous_path.stat().st_size

    # Prefer the previous day when the newest file is clearly partial.
    if previous_size > 0 and latest_size < previous_size * 0.75:
        return previous_date
    return latest_date


@lru_cache(maxsize=1)
def _mapping_frame() -> pd.DataFrame:
    mapping = load_effective_mapping()
    mapping["DeviceId"] = pd.to_numeric(mapping["DeviceId"], errors="coerce")
    mapping["Phase"] = pd.to_numeric(mapping["Phase"], errors="coerce")
    mapping["Parameter"] = pd.to_numeric(mapping["Parameter"], errors="coerce")
    mapping["latitude"] = pd.to_numeric(mapping["latitude"], errors="coerce")
    mapping["longitude"] = pd.to_numeric(mapping["longitude"], errors="coerce")
    mapping["distanceToStopbar"] = pd.to_numeric(mapping["distanceToStopbar"], errors="coerce").fillna(0)
    mapping["route"] = (
        mapping.get("route", pd.Series(index=mapping.index, dtype="object"))
        .fillna(mapping.get("roadway_name", pd.Series(index=mapping.index, dtype="object")))
        .fillna("")
    )
    mapping["county"] = mapping.get("county", pd.Series(index=mapping.index, dtype="object")).fillna("Florida")
    return mapping


@lru_cache(maxsize=1)
def _available_signal_ids() -> tuple[int, ...]:
    ids: set[int] = set()
    for path in _raw_files().values():
        for chunk in pd.read_csv(path, usecols=["signalID"], chunksize=750_000):
            numeric_ids = pd.to_numeric(chunk["signalID"], errors="coerce").dropna().astype(int)
            ids.update(numeric_ids.unique().tolist())
    return tuple(sorted(ids))


@lru_cache(maxsize=1)
def _intersection_inventory() -> list[IntersectionMeta]:
    mapping = _mapping_frame()
    subset = mapping[mapping["DeviceId"].isin(_available_signal_ids())].copy()
    grouped = (
        subset.dropna(subset=["DeviceId", "latitude", "longitude"])
        .sort_values(["DeviceId", "name"])
        .groupby("DeviceId", as_index=False)
        .agg(
            name=("name", "first"),
            route=("route", "first"),
            lat=("latitude", "first"),
            lon=("longitude", "first"),
            region=("county", "first"),
        )
        .sort_values(["region", "name"])
    )

    return [
        IntersectionMeta(
            id=str(int(row.DeviceId)),
            name=str(row.name),
            route=str(row.route),
            lat=float(row.lat),
            lon=float(row.lon),
            region=str(row.region).title(),
        )
        for row in grouped.itertuples()
    ]


INTERSECTION_LOOKUP = {meta.id: meta for meta in _intersection_inventory()}
ORDERED_SIGNAL_IDS = [meta.id for meta in sorted(_intersection_inventory(), key=lambda meta: int(meta.id))]
DEFAULT_SELECTED_IDS = ORDERED_SIGNAL_IDS[:3]
LAST_THREE_IDS = ORDERED_SIGNAL_IDS[-3:]
MISSING_MAPPING_IDS = sorted(set(_available_signal_ids()) - {int(key) for key in INTERSECTION_LOOKUP})
HISTORICAL_BUILD_LOCK = Lock()


def _metric_definitions() -> dict[str, str]:
    return {
        "arrivals_on_green": "Arrival on Green. The share of detected arrivals that occur during green.",
        "average_wait": "Average phase wait in seconds before service.",
        "green_occupancy": "Share of green time that detectors are occupied.",
        "red_occupancy": "Share of red time that detectors are occupied, which can indicate queue pressure.",
        "cycle_length": "Average cycle length over the selected window.",
        "actuation_volume": "Total detector actuations counted in the selected window.",
        "split_failures": "Count of cycles that met split-failure thresholds in the selected window.",
        "max_outs": "Terminations where a phase consumed its maximum available green.",
        "gap_outs": "Terminations where demand ended before max green was reached.",
        "force_offs": "Terminations forced by coordination or controller timing constraints.",
        "skips": "Phase calls that were not served within the expected cycle window.",
        "phase_green_time": "Average green time served for the selected phase grouping.",
    }


def _parse_selected_ids(raw_ids: str | None) -> list[str]:
    valid_ids = list(INTERSECTION_LOOKUP.keys())
    if not raw_ids:
        return DEFAULT_SELECTED_IDS
    selected = [part.strip() for part in raw_ids.split(",") if part.strip() in valid_ids]
    return selected or DEFAULT_SELECTED_IDS


def _parse_days(raw_days: str | None, fallback_date_from: date, fallback_date_to: date) -> list[str]:
    if raw_days:
        selected = [part.strip() for part in raw_days.split(",") if part.strip() in DAY_NAMES]
        if selected:
            return selected
    if fallback_date_from == fallback_date_to:
        return [fallback_date_from.strftime("%A")]
    return DAY_NAMES


def _parse_preset(preset_id: str | None) -> dict[str, str]:
    lookup = {preset["id"]: preset for preset in TIME_OF_DAY_PRESETS}
    return lookup.get(preset_id or "custom", lookup["custom"])


def _parse_hours(preset_id: str | None, raw_from: str | None, raw_to: str | None) -> tuple[str, str, str]:
    preset = _parse_preset(preset_id)
    if preset["id"] != "custom":
        return preset["id"], preset["start"], preset["end"]
    return "custom", raw_from or preset["start"], raw_to or preset["end"]


def _safe_date(raw_value: str | None, fallback: date) -> date:
    if not raw_value:
        return fallback
    try:
        return date.fromisoformat(raw_value)
    except ValueError:
        return fallback


def _hour_from_string(value: str) -> int:
    return int(value.split(":")[0])


def _minutes_from_string(value: str) -> int:
    hour_text, minute_text = (value.split(":") + ["00"])[:2]
    return int(hour_text) * 60 + int(minute_text)


def _load_raw_slice(
    selected_ids: list[str],
    date_from: date,
    date_to: date,
    days_of_week: list[str],
    hour_from: str,
    hour_to: str,
) -> pd.DataFrame:
    selected_device_ids = {int(item) for item in selected_ids}
    selected_days = set(days_of_week)
    start_minute = _minutes_from_string(hour_from)
    end_minute = _minutes_from_string(hour_to)

    frames: list[pd.DataFrame] = []
    for file_date, path in _raw_files().items():
        if not (date_from <= file_date <= date_to):
            continue

        for chunk in pd.read_csv(
            path,
            usecols=["signalID", "timeStamp", "eventCode", "eventParam"],
            chunksize=500_000,
        ):
            chunk = chunk[chunk["signalID"].isin(selected_device_ids)].copy()
            if chunk.empty:
                continue

            chunk["TimeStamp"] = pd.to_datetime(chunk["timeStamp"], format="%m-%d-%Y %H:%M:%S.%f", errors="coerce")
            chunk = chunk.dropna(subset=["TimeStamp"])
            if chunk.empty:
                continue

            chunk = chunk[
                (chunk["TimeStamp"].dt.date >= date_from)
                & (chunk["TimeStamp"].dt.date <= date_to)
                & (chunk["TimeStamp"].dt.day_name().isin(selected_days))
            ]
            if chunk.empty:
                continue

            minute_series = chunk["TimeStamp"].dt.hour * 60 + chunk["TimeStamp"].dt.minute
            if start_minute <= end_minute:
                chunk = chunk[(minute_series >= start_minute) & (minute_series <= end_minute)]
            else:
                chunk = chunk[(minute_series >= start_minute) | (minute_series <= end_minute)]
            if chunk.empty:
                continue

            chunk = chunk.rename(
                columns={
                    "signalID": "DeviceId",
                    "eventCode": "EventId",
                    "eventParam": "Parameter",
                }
            )
            frames.append(chunk[["TimeStamp", "DeviceId", "EventId", "Parameter"]])

    if not frames:
        return pd.DataFrame(columns=["TimeStamp", "DeviceId", "EventId", "Parameter"])
    return pd.concat(frames, ignore_index=True).sort_values("TimeStamp").reset_index(drop=True)


def _build_detector_config(selected_ids: list[str]) -> pd.DataFrame:
    config = _mapping_frame()
    config = config[config["DeviceId"].isin({int(item) for item in selected_ids})].copy()
    config = config.dropna(subset=["DeviceId", "Phase", "Parameter"])
    config["DeviceId"] = config["DeviceId"].astype(int)
    config["Phase"] = config["Phase"].astype(int)
    config["Parameter"] = config["Parameter"].astype(int)
    return config[["DeviceId", "Phase", "Parameter", "Function"]].drop_duplicates().reset_index(drop=True)


def _empty_tables() -> dict[str, pd.DataFrame]:
    return {
        "arrival_on_green": pd.DataFrame(),
        "split_failures": pd.DataFrame(),
        "terminations": pd.DataFrame(),
        "phase_wait": pd.DataFrame(),
        "coordination_agg": pd.DataFrame(),
    }


def _run_processor(raw_data: pd.DataFrame, detector_config: pd.DataFrame) -> dict[str, pd.DataFrame]:
    if raw_data.empty or detector_config.empty:
        return _empty_tables()

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
            {
                "name": "timeline",
                "params": {"maxtime": True, "min_duration": 1, "cushion_time": 1, "live": False},
            },
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
        tables = {
            "arrival_on_green": processor.conn.query("SELECT * FROM arrival_on_green").df(),
            "split_failures": processor.conn.query("SELECT * FROM split_failures").df(),
            "terminations": processor.conn.query("SELECT * FROM terminations").df(),
            "phase_wait": processor.conn.query("SELECT * FROM phase_wait").df(),
            "coordination_agg": processor.conn.query("SELECT * FROM coordination_agg").df(),
        }

    for frame in tables.values():
        if not frame.empty and "TimeStamp" in frame.columns:
            frame["TimeStamp"] = pd.to_datetime(frame["TimeStamp"])
    return tables


def _bucket_frequency(date_from: date, date_to: date) -> str:
    if date_from == date_to:
        return "h"
    return "D"


def _bucket_label(ts: pd.Timestamp, freq: str) -> str:
    if freq == "H":
        return ts.strftime("%H:%M")
    return ts.strftime("%m/%d")


def _phase_table(tables: dict[str, pd.DataFrame], selected_phase: str) -> pd.DataFrame:
    aog = tables["arrival_on_green"]
    split_failures = tables["split_failures"]
    phase_wait = tables["phase_wait"]
    terminations = tables["terminations"]
    coordination = tables["coordination_agg"]

    if aog.empty and split_failures.empty and phase_wait.empty:
        return _empty_phase_table()

    phase_table = (
        aog.groupby(["DeviceId", "Phase"])
        .agg(aog=("Percent_AOG", "mean"), total_actuations=("Total_Actuations", "sum"))
        .reset_index()
        .merge(
            split_failures.groupby(["DeviceId", "Phase"])
            .agg(
                green_time=("Green_Time", "mean"),
                green_occupancy=("Green_Occupancy", "mean"),
                red_occupancy=("Red_Occupancy", "mean"),
                split_failures=("Split_Failure", "sum"),
            )
            .reset_index(),
            on=["DeviceId", "Phase"],
            how="outer",
        )
        .merge(
            phase_wait.groupby(["DeviceId", "Phase"])
            .agg(avg_wait=("AvgPhaseWait", "mean"), max_wait=("MaxPhaseWait", "max"), skips=("TotalSkips", "sum"))
            .reset_index(),
            on=["DeviceId", "Phase"],
            how="outer",
        )
    )

    if not terminations.empty:
        term_pivot = (
            terminations.pivot_table(
                index=["DeviceId", "Phase"],
                columns="PerformanceMeasure",
                values="Total",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        phase_table = phase_table.merge(term_pivot, on=["DeviceId", "Phase"], how="left")

    if not coordination.empty:
        coordination = coordination.copy()
        coordination["cycle_value"] = coordination["ActualCycleLength"].where(
            coordination["ActualCycleLength"] > 0, coordination["CycleLength"]
        )
        cycle_table = (
            coordination.groupby("DeviceId")
            .agg(cycle_length=("cycle_value", "mean"))
            .reset_index()
        )
        phase_table = phase_table.merge(cycle_table, on="DeviceId", how="left")

    phase_table = phase_table.fillna(0)
    for column in ["GapOut", "MaxOut", "ForceOff"]:
        if column not in phase_table.columns:
            phase_table[column] = 0

    if selected_phase != "all":
        phase_table = phase_table[phase_table["Phase"] == int(selected_phase)]
        if phase_table.empty:
            return _empty_phase_table()

    phase_table["red_time"] = (phase_table["cycle_length"] - phase_table["green_time"]).clip(lower=0)
    phase_table["gor_proxy"] = phase_table["green_time"] * phase_table["green_occupancy"]
    phase_table["sur_proxy"] = phase_table["red_time"] * phase_table["red_occupancy"]
    return phase_table.sort_values(["DeviceId", "Phase"]).reset_index(drop=True)


def _trend_table(
    tables: dict[str, pd.DataFrame],
    selected_phase: str,
    date_from: date,
    date_to: date,
) -> pd.DataFrame:
    freq = _bucket_frequency(date_from, date_to)
    buckets: list[pd.DataFrame] = []

    def with_bucket(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            next_frame = frame.copy()
            next_frame["Bucket"] = pd.Series(dtype="datetime64[ns]")
            return next_frame
        next_frame = frame.copy()
        next_frame["Bucket"] = next_frame["TimeStamp"].dt.floor(freq)
        return next_frame

    aog = with_bucket(tables["arrival_on_green"])
    split_failures = with_bucket(tables["split_failures"])
    phase_wait = with_bucket(tables["phase_wait"])
    terminations = with_bucket(tables["terminations"])
    coordination = with_bucket(tables["coordination_agg"])

    if selected_phase != "all":
        phase_value = int(selected_phase)
        if not aog.empty and "Phase" in aog.columns:
            aog = aog[aog["Phase"] == phase_value]
        if not split_failures.empty and "Phase" in split_failures.columns:
            split_failures = split_failures[split_failures["Phase"] == phase_value]
        if not phase_wait.empty and "Phase" in phase_wait.columns:
            phase_wait = phase_wait[phase_wait["Phase"] == phase_value]
        if not terminations.empty and "Phase" in terminations.columns:
            terminations = terminations[terminations["Phase"] == phase_value]

    if aog.empty and split_failures.empty and phase_wait.empty:
        return _empty_trend_table()

    trend = (
        aog.groupby(["DeviceId", "Bucket"])
        .agg(aog=("Percent_AOG", "mean"), total_actuations=("Total_Actuations", "sum"))
        .reset_index()
        .merge(
            split_failures.groupby(["DeviceId", "Bucket"])
            .agg(
                green_time=("Green_Time", "mean"),
                green_occupancy=("Green_Occupancy", "mean"),
                red_occupancy=("Red_Occupancy", "mean"),
                split_failures=("Split_Failure", "sum"),
            )
            .reset_index(),
            on=["DeviceId", "Bucket"],
            how="outer",
        )
        .merge(
            phase_wait.groupby(["DeviceId", "Bucket"])
            .agg(avg_wait=("AvgPhaseWait", "mean"), skips=("TotalSkips", "sum"))
            .reset_index(),
            on=["DeviceId", "Bucket"],
            how="outer",
        )
        .fillna(0)
    )

    if not terminations.empty:
        term_pivot = (
            terminations.pivot_table(
                index=["DeviceId", "Bucket"],
                columns="PerformanceMeasure",
                values="Total",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        trend = trend.merge(term_pivot, on=["DeviceId", "Bucket"], how="left")

    if not coordination.empty:
        coordination = coordination.copy()
        coordination["cycle_value"] = coordination["ActualCycleLength"].where(
            coordination["ActualCycleLength"] > 0, coordination["CycleLength"]
        )
        cycle_table = coordination.groupby(["DeviceId", "Bucket"]).agg(cycle_length=("cycle_value", "mean")).reset_index()
        trend = trend.merge(cycle_table, on=["DeviceId", "Bucket"], how="left")

    trend = trend.fillna(0)
    for column in ["GapOut", "MaxOut", "ForceOff"]:
        if column not in trend.columns:
            trend[column] = 0

    trend["label"] = trend["Bucket"].apply(lambda ts: _bucket_label(pd.Timestamp(ts), freq))
    trend["red_time"] = (trend["cycle_length"] - trend["green_time"]).clip(lower=0)
    trend["gor_proxy"] = trend["green_time"] * trend["green_occupancy"]
    trend["sur_proxy"] = trend["red_time"] * trend["red_occupancy"]
    return trend.sort_values(["DeviceId", "Bucket"]).reset_index(drop=True)


def _safe_mean(series: pd.Series) -> float:
    return 0.0 if series.empty else float(series.mean())


def _safe_sum(series: pd.Series) -> float:
    return 0.0 if series.empty else float(series.sum())


def _format_value(value: float, kind: str = "number") -> str:
    if kind == "percent":
        return f"{value:.1f}%"
    if kind == "seconds":
        return f"{value:.1f} s"
    if kind == "count":
        return f"{int(round(value))}"
    return f"{value:.1f}"


def _build_metric_cards(phase_table: pd.DataFrame) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    definitions = _metric_definitions()
    overview = [
        {
            "id": "arrivals_on_green",
            "label": "Arrivals on Green",
            "value": _format_value(_safe_mean(phase_table["aog"]) * 100, "percent"),
            "description": definitions["arrivals_on_green"],
        },
        {
            "id": "average_wait",
            "label": "Average Wait",
            "value": _format_value(_safe_mean(phase_table["avg_wait"]), "seconds"),
            "description": definitions["average_wait"],
        },
        {
            "id": "green_occupancy",
            "label": "Green Occ %",
            "value": _format_value(_safe_mean(phase_table["green_occupancy"]) * 100, "percent"),
            "description": definitions["green_occupancy"],
        },
        {
            "id": "red_occupancy",
            "label": "Red Occ %",
            "value": _format_value(_safe_mean(phase_table["red_occupancy"]) * 100, "percent"),
            "description": definitions["red_occupancy"],
        },
        {
            "id": "cycle_length",
            "label": "Cycle Length",
            "value": _format_value(_safe_mean(phase_table["cycle_length"]), "seconds"),
            "description": definitions["cycle_length"],
        },
        {
            "id": "actuation_volume",
            "label": "Actuation Volume",
            "value": _format_value(_safe_sum(phase_table["total_actuations"]), "count"),
            "description": definitions["actuation_volume"],
        },
    ]

    phase_ops = [
        {
            "id": "split_failures",
            "label": "Split Failures",
            "value": _format_value(_safe_sum(phase_table["split_failures"]), "count"),
            "description": definitions["split_failures"],
        },
        {
            "id": "max_outs",
            "label": "Max-Outs",
            "value": _format_value(_safe_sum(phase_table["MaxOut"]), "count"),
            "description": definitions["max_outs"],
        },
        {
            "id": "gap_outs",
            "label": "Gap-Outs",
            "value": _format_value(_safe_sum(phase_table["GapOut"]), "count"),
            "description": definitions["gap_outs"],
        },
        {
            "id": "force_offs",
            "label": "Force-Offs",
            "value": _format_value(_safe_sum(phase_table["ForceOff"]), "count"),
            "description": definitions["force_offs"],
        },
        {
            "id": "skips",
            "label": "Skips",
            "value": _format_value(_safe_sum(phase_table["skips"]), "count"),
            "description": definitions["skips"],
        },
        {
            "id": "phase_green_time",
            "label": "Green Time",
            "value": _format_value(_safe_mean(phase_table["green_time"]), "seconds"),
            "description": definitions["phase_green_time"],
        },
    ]
    return overview, phase_ops


def _intersection_payload(meta: IntersectionMeta, phase_table: pd.DataFrame, trend: pd.DataFrame) -> dict[str, Any]:
    device_id = int(meta.id)
    phase_rows = phase_table[phase_table["DeviceId"] == device_id].copy() if "DeviceId" in phase_table else _empty_phase_table()
    trend_rows = trend[trend["DeviceId"] == device_id].copy() if "DeviceId" in trend else _empty_trend_table()

    if phase_rows.empty:
        insights = ["No aggregated ATSPM data is available for this intersection in the selected window."]
    else:
        worst_phase = phase_rows.sort_values("avg_wait", ascending=False).iloc[0]
        insights = [
            f"Phase {int(worst_phase['Phase'])} has the highest average wait at {worst_phase['avg_wait']:.1f} s.",
            f"Arrivals on green average {phase_rows['aog'].mean() * 100:.1f}% across the selected window.",
            f"Split failures total {int(round(phase_rows['split_failures'].sum()))} for the current selection.",
        ]

    summary = {
        "meanAog": round(_safe_mean(phase_rows["aog"]) * 100, 1),
        "meanDelay": round(_safe_mean(phase_rows["avg_wait"]), 1),
        "meanGreenOccupancy": round(_safe_mean(phase_rows["green_occupancy"]) * 100, 1),
        "meanRedOccupancy": round(_safe_mean(phase_rows["red_occupancy"]) * 100, 1),
        "cycleLength": round(_safe_mean(phase_rows["cycle_length"]), 1),
        "splitFailures": int(round(_safe_sum(phase_rows["split_failures"]))),
        "maxOutCount": int(round(_safe_sum(phase_rows["MaxOut"]))),
        "gapOutCount": int(round(_safe_sum(phase_rows["GapOut"]))),
        "forceOffCount": int(round(_safe_sum(phase_rows["ForceOff"]))),
    }

    return {
        "id": meta.id,
        "name": meta.name,
        "route": meta.route,
        "lat": meta.lat,
        "lon": meta.lon,
        "region": meta.region,
        "summary": summary,
        "insights": insights,
        "phases": [
            {
                "phase": int(row["Phase"]),
                "aogPct": round(float(row["aog"] * 100), 1),
                "avgWait": round(float(row["avg_wait"]), 1),
                "maxWait": round(float(row["max_wait"]), 1),
                "greenOccupancyPct": round(float(row["green_occupancy"] * 100), 1),
                "redOccupancyPct": round(float(row["red_occupancy"] * 100), 1),
                "greenTime": round(float(row["green_time"]), 1),
                "totalActuations": int(round(float(row["total_actuations"]))),
                "splitFailures": int(round(float(row["split_failures"]))),
                "maxOuts": int(round(float(row["MaxOut"]))),
                "gapOuts": int(round(float(row["GapOut"]))),
                "forceOffs": int(round(float(row["ForceOff"]))),
                "skips": int(round(float(row["skips"]))),
            }
            for _, row in phase_rows.iterrows()
        ],
        "trend": [
            {
                "label": row["label"],
                "aogPct": round(float(row["aog"] * 100), 1),
                "avgWait": round(float(row["avg_wait"]), 1),
                "greenOccupancyPct": round(float(row["green_occupancy"] * 100), 1),
                "redOccupancyPct": round(float(row["red_occupancy"] * 100), 1),
                "splitFailures": int(round(float(row["split_failures"]))),
                "maxOuts": int(round(float(row["MaxOut"]))),
                "gapOuts": int(round(float(row["GapOut"]))),
                "forceOffs": int(round(float(row["ForceOff"]))),
                "cycleLength": round(float(row["cycle_length"]), 1),
            }
            for _, row in trend_rows.iterrows()
        ],
    }


def _corridor_summary(phase_table: pd.DataFrame) -> dict[str, float | int]:
    return {
        "meanAog": round(_safe_mean(phase_table["aog"]) * 100, 1),
        "meanDelay": round(_safe_mean(phase_table["avg_wait"]), 1),
        "meanGreenOccupancy": round(_safe_mean(phase_table["green_occupancy"]) * 100, 1),
        "meanRedOccupancy": round(_safe_mean(phase_table["red_occupancy"]) * 100, 1),
        "cycleLength": round(_safe_mean(phase_table["cycle_length"]), 1),
        "splitFailures": int(round(_safe_sum(phase_table["split_failures"]))),
        "maxOutCount": int(round(_safe_sum(phase_table["MaxOut"]))),
        "gapOutCount": int(round(_safe_sum(phase_table["GapOut"]))),
        "forceOffCount": int(round(_safe_sum(phase_table["ForceOff"]))),
    }


def _dashboard_metric_seed() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "aog": pd.Series(dtype=float),
            "avg_wait": pd.Series(dtype=float),
            "green_occupancy": pd.Series(dtype=float),
            "red_occupancy": pd.Series(dtype=float),
            "cycle_length": pd.Series(dtype=float),
            "total_actuations": pd.Series(dtype=float),
            "split_failures": pd.Series(dtype=float),
            "MaxOut": pd.Series(dtype=float),
            "GapOut": pd.Series(dtype=float),
            "ForceOff": pd.Series(dtype=float),
            "skips": pd.Series(dtype=float),
            "green_time": pd.Series(dtype=float),
        }
    )


def _historical_dashboard_tables(
    selected_ids: list[str],
    date_from: date,
    date_to: date,
    days_of_week: list[str],
    hour_from: str,
    hour_to: str,
    selected_phase: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    hour_start = int(hour_from.split(":")[0])
    hour_end = int(hour_to.split(":")[0])
    sql_params = (date_from.isoformat(), date_to.isoformat(), hour_start, hour_end)

    aog_frames: list[pd.DataFrame] = []
    wait_frames: list[pd.DataFrame] = []
    split_frames: list[pd.DataFrame] = []
    term_frames: list[pd.DataFrame] = []
    coord_frames: list[pd.DataFrame] = []

    for intersection_id in selected_ids:
        sqlite_path = historical_output_paths(int(intersection_id))["sqlite_path"]
        if not sqlite_path.exists():
            continue

        with sqlite3.connect(sqlite_path) as conn:
            aog = _sqlite_frame(
                conn,
                "arrival_on_green",
                """
                SELECT run_date, hour, DeviceId, Phase, Total_Actuations, Percent_AOG
                FROM arrival_on_green
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            waits = _sqlite_frame(
                conn,
                "phase_wait",
                """
                SELECT run_date, hour, DeviceId, Phase, AvgPhaseWait, MaxPhaseWait, TotalSkips
                FROM phase_wait
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            split = _sqlite_frame(
                conn,
                "split_failures",
                """
                SELECT run_date, hour, DeviceId, Phase, Green_Time, Green_Occupancy, Red_Occupancy, Split_Failure
                FROM split_failures
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            terms = _sqlite_frame(
                conn,
                "terminations",
                """
                SELECT run_date, hour, DeviceId, Phase, PerformanceMeasure, Total
                FROM terminations
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            coordination = _sqlite_frame(
                conn,
                "coordination_agg",
                """
                SELECT run_date, hour, DeviceId, CycleLength, ActualCycleLength
                FROM coordination_agg
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )

        aog = _filter_rank_frame(aog, days_of_week)
        waits = _filter_rank_frame(waits, days_of_week)
        split = _filter_rank_frame(split, days_of_week)
        terms = _filter_rank_frame(terms, days_of_week)
        coordination = _filter_rank_frame(coordination, days_of_week)

        if selected_phase != "all":
            try:
                phase_value = int(selected_phase)
            except ValueError:
                phase_value = None
            if phase_value is not None:
                if not aog.empty:
                    aog = aog[aog["Phase"] == phase_value]
                if not waits.empty:
                    waits = waits[waits["Phase"] == phase_value]
                if not split.empty:
                    split = split[split["Phase"] == phase_value]
                if not terms.empty:
                    terms = terms[terms["Phase"] == phase_value]

        if not aog.empty:
            aog_frames.append(aog)
        if not waits.empty:
            wait_frames.append(waits)
        if not split.empty:
            split_frames.append(split)
        if not terms.empty:
            term_frames.append(terms)
        if not coordination.empty:
            coord_frames.append(coordination)

    aog = pd.concat(aog_frames, ignore_index=True) if aog_frames else pd.DataFrame()
    waits = pd.concat(wait_frames, ignore_index=True) if wait_frames else pd.DataFrame()
    split = pd.concat(split_frames, ignore_index=True) if split_frames else pd.DataFrame()
    terms = pd.concat(term_frames, ignore_index=True) if term_frames else pd.DataFrame()
    coordination = pd.concat(coord_frames, ignore_index=True) if coord_frames else pd.DataFrame()

    phase_table = pd.DataFrame(columns=["DeviceId", "Phase"])
    if not aog.empty:
        phase_table = (
            aog.groupby(["DeviceId", "Phase"])
            .agg(total_actuations=("Total_Actuations", "sum"), aog=("Percent_AOG", "mean"))
            .reset_index()
        )
    if not split.empty:
        split_table = (
            split.groupby(["DeviceId", "Phase"])
            .agg(
                green_time=("Green_Time", "mean"),
                green_occupancy=("Green_Occupancy", "mean"),
                red_occupancy=("Red_Occupancy", "mean"),
                split_failures=("Split_Failure", "sum"),
            )
            .reset_index()
        )
        phase_table = split_table if phase_table.empty else phase_table.merge(split_table, on=["DeviceId", "Phase"], how="outer")
    if not waits.empty:
        wait_table = (
            waits.groupby(["DeviceId", "Phase"])
            .agg(avg_wait=("AvgPhaseWait", "mean"), max_wait=("MaxPhaseWait", "max"), skips=("TotalSkips", "sum"))
            .reset_index()
        )
        phase_table = wait_table if phase_table.empty else phase_table.merge(wait_table, on=["DeviceId", "Phase"], how="outer")

    if not terms.empty:
        term_pivot = (
            terms.pivot_table(
                index=["DeviceId", "Phase"],
                columns="PerformanceMeasure",
                values="Total",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        phase_table = term_pivot if phase_table.empty else phase_table.merge(term_pivot, on=["DeviceId", "Phase"], how="outer")

    if not coordination.empty:
        coordination = coordination.copy()
        coordination["cycle_value"] = coordination["ActualCycleLength"].where(
            coordination["ActualCycleLength"] > 0, coordination["CycleLength"]
        )
        cycle_table = coordination.groupby("DeviceId").agg(cycle_length=("cycle_value", "mean")).reset_index()
        phase_table = cycle_table if phase_table.empty else phase_table.merge(cycle_table, on=["DeviceId"], how="left")

    if phase_table.empty:
        phase_table = _dashboard_metric_seed()
    else:
        phase_table = phase_table.fillna(0)
        for column in ["GapOut", "MaxOut", "ForceOff", "cycle_length", "green_time", "skips", "avg_wait", "max_wait", "aog", "green_occupancy", "red_occupancy", "split_failures", "total_actuations"]:
            if column not in phase_table.columns:
                phase_table[column] = 0

    trend = pd.DataFrame(columns=["DeviceId", "hour"])
    if not aog.empty:
        trend = (
            aog.groupby(["DeviceId", "hour"])
            .agg(total_actuations=("Total_Actuations", "sum"), aog=("Percent_AOG", "mean"))
            .reset_index()
        )
    if not split.empty:
        split_trend = (
            split.groupby(["DeviceId", "hour"])
            .agg(
                green_time=("Green_Time", "mean"),
                green_occupancy=("Green_Occupancy", "mean"),
                red_occupancy=("Red_Occupancy", "mean"),
                split_failures=("Split_Failure", "sum"),
            )
            .reset_index()
        )
        trend = split_trend if trend.empty else trend.merge(split_trend, on=["DeviceId", "hour"], how="outer")
    if not waits.empty:
        wait_trend = (
            waits.groupby(["DeviceId", "hour"])
            .agg(avg_wait=("AvgPhaseWait", "mean"), skips=("TotalSkips", "sum"))
            .reset_index()
        )
        trend = wait_trend if trend.empty else trend.merge(wait_trend, on=["DeviceId", "hour"], how="outer")
    if not terms.empty:
        term_trend = (
            terms.pivot_table(
                index=["DeviceId", "hour"],
                columns="PerformanceMeasure",
                values="Total",
                aggfunc="sum",
                fill_value=0,
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        trend = term_trend if trend.empty else trend.merge(term_trend, on=["DeviceId", "hour"], how="outer")
    if not coordination.empty:
        coordination = coordination.copy()
        coordination["cycle_value"] = coordination["ActualCycleLength"].where(
            coordination["ActualCycleLength"] > 0, coordination["CycleLength"]
        )
        cycle_trend = coordination.groupby(["DeviceId", "hour"]).agg(cycle_length=("cycle_value", "mean")).reset_index()
        trend = cycle_trend if trend.empty else trend.merge(cycle_trend, on=["DeviceId", "hour"], how="outer")

    if trend.empty:
        trend = _empty_trend_table()
    else:
        trend = trend.fillna(0)
        for column in ["GapOut", "MaxOut", "ForceOff", "cycle_length", "green_time", "skips", "avg_wait", "aog", "green_occupancy", "red_occupancy", "split_failures", "total_actuations"]:
            if column not in trend.columns:
                trend[column] = 0
        trend["label"] = trend["hour"].apply(lambda hour: f"{int(hour):02d}:00")

    return phase_table, trend


def build_dashboard_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    default_date = _default_analysis_date()

    date_from = _safe_date(query.get("dateFrom", [None])[0], default_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], date_from)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    selected_ids = _parse_selected_ids(query.get("intersectionIds", [None])[0])
    days_of_week = _parse_days(query.get("daysOfWeek", [None])[0], date_from, date_to)
    preset_id, hour_from, hour_to = _parse_hours(
        query.get("timeOfDayPreset", [None])[0],
        query.get("hourFrom", [None])[0],
        query.get("hourTo", [None])[0],
    )
    selected_phase = query.get("phase", ["all"])[0]

    raw_data = _load_raw_slice(selected_ids, date_from, date_to, days_of_week, hour_from, hour_to)
    detector_config = _build_detector_config(selected_ids)
    tables = _run_processor(raw_data, detector_config)
    phase_table = _phase_table(tables, selected_phase)
    trend_table = _trend_table(tables, selected_phase, date_from, date_to)

    selected_intersections = [
        _intersection_payload(INTERSECTION_LOOKUP[intersection_id], phase_table, trend_table)
        for intersection_id in selected_ids
        if intersection_id in INTERSECTION_LOOKUP
    ]
    all_intersections = [
        _intersection_payload(meta, phase_table, trend_table)
        for meta in _intersection_inventory()
    ]

    if trend_table.empty:
        combined_trend: list[dict[str, Any]] = []
    else:
        combined = (
            trend_table.groupby("label")
            .agg(
                aog=("aog", "mean"),
                avg_wait=("avg_wait", "mean"),
                green_occupancy=("green_occupancy", "mean"),
                red_occupancy=("red_occupancy", "mean"),
                split_failures=("split_failures", "sum"),
                max_outs=("MaxOut", "sum"),
                gap_outs=("GapOut", "sum"),
                force_offs=("ForceOff", "sum"),
                cycle_length=("cycle_length", "mean"),
            )
            .reset_index()
        )
        combined_trend = [
            {
                "label": row["label"],
                "aogPct": round(float(row["aog"] * 100), 1),
                "avgWait": round(float(row["avg_wait"]), 1),
                "greenOccupancyPct": round(float(row["green_occupancy"] * 100), 1),
                "redOccupancyPct": round(float(row["red_occupancy"] * 100), 1),
                "splitFailures": int(round(float(row["split_failures"]))),
                "maxOuts": int(round(float(row["max_outs"]))),
                "gapOuts": int(round(float(row["gap_outs"]))),
                "forceOffs": int(round(float(row["force_offs"]))),
                "cycleLength": round(float(row["cycle_length"]), 1),
            }
            for _, row in combined.iterrows()
        ]

    overview_metrics, phase_operation_metrics = _build_metric_cards(phase_table if not phase_table.empty else pd.DataFrame({
        "aog": pd.Series(dtype=float),
        "avg_wait": pd.Series(dtype=float),
        "green_occupancy": pd.Series(dtype=float),
        "red_occupancy": pd.Series(dtype=float),
        "cycle_length": pd.Series(dtype=float),
        "total_actuations": pd.Series(dtype=float),
        "split_failures": pd.Series(dtype=float),
        "MaxOut": pd.Series(dtype=float),
        "GapOut": pd.Series(dtype=float),
        "ForceOff": pd.Series(dtype=float),
        "skips": pd.Series(dtype=float),
        "green_time": pd.Series(dtype=float),
    }))

    available_phases = sorted({int(phase) for phase in phase_table["Phase"].unique().tolist()}) if not phase_table.empty else []

    return {
        "meta": {
            "dataSource": f"ATSPM event CSV files from {_raw_data_dir()}",
            "note": (
                f"{len(MISSING_MAPPING_IDS)} signal IDs are present in the raw events but missing from the mapping CSV."
                if MISSING_MAPPING_IDS
                else ""
            ),
            "availableDates": [item.isoformat() for item in available_dates],
            "defaultDateFrom": default_date.isoformat(),
            "defaultDateTo": default_date.isoformat(),
            "availableDays": DAY_NAMES,
            "timeOfDayPresets": TIME_OF_DAY_PRESETS,
            "selectionPresets": [
                {"id": "all", "label": "All signals", "intersectionIds": ORDERED_SIGNAL_IDS},
                {"id": "first-three", "label": "First three", "intersectionIds": DEFAULT_SELECTED_IDS},
                {
                    "id": "last-three",
                    "label": "Last three",
                    "intersectionIds": LAST_THREE_IDS,
                },
            ],
            "metricDefinitions": _metric_definitions(),
        },
        "filters": {
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": hour_from,
            "hourTo": hour_to,
            "timeOfDayPreset": preset_id,
            "selectedIntersectionIds": selected_ids,
            "selectedPhase": selected_phase,
        },
        "corridor": {
            "id": "florida-atspm-october",
            "name": "Florida ATSPM Operations",
            "region": "Florida",
            "summary": _corridor_summary(phase_table) if not phase_table.empty else _corridor_summary(pd.DataFrame({
                "aog": pd.Series(dtype=float),
                "avg_wait": pd.Series(dtype=float),
                "green_occupancy": pd.Series(dtype=float),
                "red_occupancy": pd.Series(dtype=float),
                "cycle_length": pd.Series(dtype=float),
                "split_failures": pd.Series(dtype=float),
                "MaxOut": pd.Series(dtype=float),
                "GapOut": pd.Series(dtype=float),
                "ForceOff": pd.Series(dtype=float),
            })),
            "overviewMetrics": overview_metrics,
            "phaseOperationMetrics": phase_operation_metrics,
            "availablePhases": available_phases,
            "selectedPhase": selected_phase,
            "trend": combined_trend,
            "intersections": selected_intersections,
            "allIntersections": all_intersections,
        },
    }


def build_historical_dashboard_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    default_date = _default_analysis_date()

    date_from = _safe_date(query.get("dateFrom", [None])[0], default_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], date_from)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    selected_ids = _parse_selected_ids(query.get("intersectionIds", [None])[0])
    days_of_week = _parse_days(query.get("daysOfWeek", [None])[0], date_from, date_to)
    preset_id, hour_from, hour_to = _parse_hours(
        query.get("timeOfDayPreset", [None])[0],
        query.get("hourFrom", [None])[0],
        query.get("hourTo", [None])[0],
    )
    selected_phase = query.get("phase", ["all"])[0]

    all_ids = [meta.id for meta in _intersection_inventory()]
    selected_phase_table, selected_trend_table = _historical_dashboard_tables(
        selected_ids, date_from, date_to, days_of_week, hour_from, hour_to, selected_phase
    )
    all_phase_table, all_trend_table = _historical_dashboard_tables(
        all_ids, date_from, date_to, days_of_week, hour_from, hour_to, selected_phase
    )

    selected_intersections = [
        _intersection_payload(INTERSECTION_LOOKUP[intersection_id], all_phase_table, all_trend_table)
        for intersection_id in selected_ids
        if intersection_id in INTERSECTION_LOOKUP
    ]
    all_intersections = [
        _intersection_payload(meta, all_phase_table, all_trend_table)
        for meta in _intersection_inventory()
    ]

    if selected_trend_table.empty:
        combined_trend: list[dict[str, Any]] = []
    else:
        combined = (
            selected_trend_table.groupby("label")
            .agg(
                aog=("aog", "mean"),
                avg_wait=("avg_wait", "mean"),
                green_occupancy=("green_occupancy", "mean"),
                red_occupancy=("red_occupancy", "mean"),
                split_failures=("split_failures", "sum"),
                max_outs=("MaxOut", "sum"),
                gap_outs=("GapOut", "sum"),
                force_offs=("ForceOff", "sum"),
                cycle_length=("cycle_length", "mean"),
            )
            .reset_index()
        )
        combined_trend = [
            {
                "label": row["label"],
                "aogPct": round(float(row["aog"] * 100), 1),
                "avgWait": round(float(row["avg_wait"]), 1),
                "greenOccupancyPct": round(float(row["green_occupancy"] * 100), 1),
                "redOccupancyPct": round(float(row["red_occupancy"] * 100), 1),
                "splitFailures": int(round(float(row["split_failures"]))),
                "maxOuts": int(round(float(row["max_outs"]))),
                "gapOuts": int(round(float(row["gap_outs"]))),
                "forceOffs": int(round(float(row["force_offs"]))),
                "cycleLength": round(float(row["cycle_length"]), 1),
            }
            for _, row in combined.iterrows()
        ]

    overview_metrics, phase_operation_metrics = _build_metric_cards(
        selected_phase_table if not selected_phase_table.empty else _dashboard_metric_seed()
    )
    available_phases = (
        sorted({int(phase) for phase in all_phase_table["Phase"].unique().tolist()})
        if not all_phase_table.empty and "Phase" in all_phase_table.columns
        else []
    )

    return {
        "meta": {
            "dataSource": "Historical dashboard built from precomputed ATSPM MOE outputs",
            "note": "",
            "availableDates": [item.isoformat() for item in available_dates],
            "defaultDateFrom": default_date.isoformat(),
            "defaultDateTo": default_date.isoformat(),
            "availableDays": DAY_NAMES,
            "timeOfDayPresets": TIME_OF_DAY_PRESETS,
            "selectionPresets": [
                {"id": "all", "label": "All signals", "intersectionIds": ORDERED_SIGNAL_IDS},
                {"id": "first-three", "label": "First three", "intersectionIds": DEFAULT_SELECTED_IDS},
                {
                    "id": "last-three",
                    "label": "Last three",
                    "intersectionIds": LAST_THREE_IDS,
                },
            ],
            "metricDefinitions": _metric_definitions(),
        },
        "filters": {
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": hour_from,
            "hourTo": hour_to,
            "timeOfDayPreset": preset_id,
            "selectedIntersectionIds": selected_ids,
            "selectedPhase": selected_phase,
        },
        "corridor": {
            "id": "historical-dashboard",
            "name": "Dashboard",
            "region": "Precomputed MOE summary",
            "summary": _corridor_summary(selected_phase_table if not selected_phase_table.empty else _dashboard_metric_seed()),
            "overviewMetrics": overview_metrics,
            "phaseOperationMetrics": phase_operation_metrics,
            "availablePhases": available_phases,
            "selectedPhase": selected_phase,
            "trend": combined_trend,
            "intersections": selected_intersections,
            "allIntersections": all_intersections,
        },
    }


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", (table_name,))
    return cursor.fetchone() is not None


def _sqlite_table_count(conn: sqlite3.Connection, table_name: str) -> int:
    if not _sqlite_table_exists(conn, table_name):
        return 0
    return int(conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0])


def _historical_hour_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    status = pd.read_sql_query(
        "SELECT run_date, hour, status, source_rows, message FROM hourly_run_status ORDER BY run_date, hour",
        conn,
    )
    rows = [
        {
            "runDate": str(item.run_date),
            "hour": int(item.hour),
            "label": f"{str(item.run_date)[5:]} {int(item.hour):02d}:00",
            "status": str(item.status),
            "sourceRows": int(item.source_rows),
            "message": str(item.message),
            "aogPct": 0.0,
            "avgWait": 0.0,
            "splitFailures": 0,
            "maxOuts": 0,
            "gapOuts": 0,
            "forceOffs": 0,
        }
        for item in status.itertuples()
    ]
    by_hour = {(row["runDate"], row["hour"]): row for row in rows}

    if _sqlite_table_exists(conn, "arrival_on_green"):
        aog = pd.read_sql_query(
            """
            SELECT run_date, hour, AVG(Percent_AOG) * 100 AS aog_pct
            FROM arrival_on_green
            GROUP BY run_date, hour
            """,
            conn,
        )
        for item in aog.itertuples():
            by_hour[(str(item.run_date), int(item.hour))]["aogPct"] = round(float(item.aog_pct or 0), 1)

    if _sqlite_table_exists(conn, "phase_wait"):
        waits = pd.read_sql_query(
            """
            SELECT run_date, hour, AVG(AvgPhaseWait) AS avg_wait
            FROM phase_wait
            GROUP BY run_date, hour
            """,
            conn,
        )
        for item in waits.itertuples():
            by_hour[(str(item.run_date), int(item.hour))]["avgWait"] = round(float(item.avg_wait or 0), 1)

    if _sqlite_table_exists(conn, "split_failures"):
        split = pd.read_sql_query(
            """
            SELECT run_date, hour, SUM(Split_Failure) AS split_failures
            FROM split_failures
            GROUP BY run_date, hour
            """,
            conn,
        )
        for item in split.itertuples():
            by_hour[(str(item.run_date), int(item.hour))]["splitFailures"] = int(round(float(item.split_failures or 0)))

    if _sqlite_table_exists(conn, "terminations"):
        terms = pd.read_sql_query(
            """
            SELECT run_date, hour, PerformanceMeasure, SUM(Total) AS total
            FROM terminations
            GROUP BY run_date, hour, PerformanceMeasure
            """,
            conn,
        )
        metric_lookup = {"MaxOut": "maxOuts", "GapOut": "gapOuts", "ForceOff": "forceOffs"}
        for item in terms.itertuples():
            metric_key = metric_lookup.get(str(item.PerformanceMeasure))
            if metric_key:
                by_hour[(str(item.run_date), int(item.hour))][metric_key] = int(round(float(item.total or 0)))

    return rows


def _historical_day_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    status = pd.read_sql_query(
        """
        SELECT
            run_date,
            SUM(source_rows) AS source_rows,
            SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_hours,
            SUM(CASE WHEN status = 'empty' THEN 1 ELSE 0 END) AS empty_hours,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_hours
        FROM hourly_run_status
        GROUP BY run_date
        ORDER BY run_date
        """,
        conn,
    )
    rows = [
        {
            "runDate": str(item.run_date),
            "label": str(item.run_date)[5:],
            "sourceRows": int(item.source_rows),
            "okHours": int(item.ok_hours),
            "emptyHours": int(item.empty_hours),
            "failedHours": int(item.failed_hours),
            "aogPct": 0.0,
            "avgWait": 0.0,
            "splitFailures": 0,
            "maxOuts": 0,
            "gapOuts": 0,
            "forceOffs": 0,
        }
        for item in status.itertuples()
    ]
    by_date = {row["runDate"]: row for row in rows}

    if _sqlite_table_exists(conn, "arrival_on_green"):
        aog = pd.read_sql_query(
            """
            SELECT run_date, AVG(Percent_AOG) * 100 AS aog_pct
            FROM arrival_on_green
            GROUP BY run_date
            """,
            conn,
        )
        for item in aog.itertuples():
            by_date[str(item.run_date)]["aogPct"] = round(float(item.aog_pct or 0), 1)

    if _sqlite_table_exists(conn, "phase_wait"):
        waits = pd.read_sql_query(
            """
            SELECT run_date, AVG(AvgPhaseWait) AS avg_wait
            FROM phase_wait
            GROUP BY run_date
            """,
            conn,
        )
        for item in waits.itertuples():
            by_date[str(item.run_date)]["avgWait"] = round(float(item.avg_wait or 0), 1)

    if _sqlite_table_exists(conn, "split_failures"):
        split = pd.read_sql_query(
            """
            SELECT run_date, SUM(Split_Failure) AS split_failures
            FROM split_failures
            GROUP BY run_date
            """,
            conn,
        )
        for item in split.itertuples():
            by_date[str(item.run_date)]["splitFailures"] = int(round(float(item.split_failures or 0)))

    if _sqlite_table_exists(conn, "terminations"):
        terms = pd.read_sql_query(
            """
            SELECT run_date, PerformanceMeasure, SUM(Total) AS total
            FROM terminations
            GROUP BY run_date, PerformanceMeasure
            """,
            conn,
        )
        metric_lookup = {"MaxOut": "maxOuts", "GapOut": "gapOuts", "ForceOff": "forceOffs"}
        for item in terms.itertuples():
            metric_key = metric_lookup.get(str(item.PerformanceMeasure))
            if metric_key:
                by_date[str(item.run_date)][metric_key] = int(round(float(item.total or 0)))

    return rows


def _historical_signal_id(raw_signal_id: str | None) -> str:
    if raw_signal_id and raw_signal_id in INTERSECTION_LOOKUP:
        return raw_signal_id
    if DEFAULT_HISTORICAL_SIGNAL_ID in INTERSECTION_LOOKUP:
        return DEFAULT_HISTORICAL_SIGNAL_ID
    return next(iter(INTERSECTION_LOOKUP.keys()))


def _historical_available_signals() -> list[dict[str, str | float]]:
    return [
        {
            "id": meta.id,
            "name": meta.name,
            "route": meta.route,
            "region": meta.region,
            "lat": meta.lat,
            "lon": meta.lon,
        }
        for meta in _intersection_inventory()
    ]


def _signal_selection_presets() -> list[dict[str, Any]]:
    return [
        {"id": "all", "label": "All signals", "intersectionIds": ORDERED_SIGNAL_IDS},
        {"id": "first-three", "label": "First three", "intersectionIds": DEFAULT_SELECTED_IDS},
        {"id": "last-three", "label": "Last three", "intersectionIds": ORDERED_SIGNAL_IDS[-3:]},
    ]


def _ensure_historical_db(signal_id: str) -> dict[str, Path]:
    paths = historical_output_paths(int(signal_id))
    if paths["sqlite_path"].exists():
        return paths
    with HISTORICAL_BUILD_LOCK:
        if paths["sqlite_path"].exists():
            return paths
        run_signal_history(int(signal_id))
    return paths


def build_historical_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    signal_id = _historical_signal_id(query.get("signalId", [None])[0])
    signal_meta = INTERSECTION_LOOKUP[signal_id]
    paths = _ensure_historical_db(signal_id)

    if not paths["sqlite_path"].exists():
        return {
            "exists": False,
            "title": f"{signal_meta.name} historical ATSPM run",
            "message": "Run the precomputed MOE build to generate the saved summary data.",
            "signalId": int(signal_id),
            "selectedSignalId": signal_id,
            "availableSignals": _historical_available_signals(),
            "dbPath": str(paths["sqlite_path"]),
            "outputDir": str(paths["output_dir"]),
            "filteredCsvPath": str(paths["days_dir"]),
            "days": [],
            "hours": [],
            "tableCounts": [],
        }

    with sqlite3.connect(paths["sqlite_path"]) as conn:
        metadata = (
            pd.read_sql_query("SELECT * FROM run_metadata LIMIT 1", conn).iloc[0].to_dict()
            if _sqlite_table_exists(conn, "run_metadata")
            else {}
        )
        table_counts = [
            {"name": name, "rows": _sqlite_table_count(conn, name)}
            for name in [
                "hourly_run_status",
                "hour_files",
                "has_data",
                "arrival_on_green",
                "split_failures",
                "terminations",
                "timeline",
                "phase_wait",
                "coordination_agg",
            ]
        ]
        days = _historical_day_rows(conn) if _sqlite_table_exists(conn, "hourly_run_status") else []
        hours = _historical_hour_rows(conn) if _sqlite_table_exists(conn, "hourly_run_status") else []

    ok_hours = sum(1 for hour in hours if hour["status"] == "ok")
    empty_hours = sum(1 for hour in hours if hour["status"] == "empty")
    failed_hours = sum(1 for hour in hours if hour["status"] == "failed")
    return {
        "exists": True,
        "title": f"{signal_meta.name} historical ATSPM run",
        "signalId": int(metadata.get("signal_id", int(signal_id))),
        "selectedSignalId": signal_id,
        "availableSignals": _historical_available_signals(),
        "runDate": "All available October days",
        "sourceCsv": "Multiple October ATSPM CSV files",
        "dbPath": str(paths["sqlite_path"]),
        "outputDir": str(paths["output_dir"]),
        "filteredCsvPath": str(paths["days_dir"]),
        "filteredRows": int(metadata.get("total_source_rows", 0) or 0),
        "detectorConfigRows": int(metadata.get("detector_config_rows", 0) or 0),
        "createdAt": str(metadata.get("created_at", "")),
        "days": days,
        "summary": {
            "okHours": ok_hours,
            "emptyHours": empty_hours,
            "failedHours": failed_hours,
            "totalSourceRows": sum(hour["sourceRows"] for hour in hours),
        },
        "hours": hours,
        "tableCounts": table_counts,
    }


def _sqlite_frame(
    conn: sqlite3.Connection,
    table_name: str,
    select_sql: str,
    params: tuple[Any, ...],
) -> pd.DataFrame:
    if not _sqlite_table_exists(conn, table_name):
        return pd.DataFrame()
    return pd.read_sql_query(select_sql, conn, params=params)


def _filter_rank_frame(frame: pd.DataFrame, days_of_week: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    filtered = frame.copy()
    filtered["run_date"] = pd.to_datetime(filtered["run_date"], errors="coerce")
    filtered = filtered.dropna(subset=["run_date"])
    filtered["day_name"] = filtered["run_date"].dt.day_name()
    filtered = filtered[filtered["day_name"].isin(days_of_week)].copy()
    filtered["run_date"] = filtered["run_date"].dt.strftime("%Y-%m-%d")
    return filtered


def _parse_bottleneck_moes(raw_moes: str | None) -> list[str]:
    allowed = list(BOTTLENECK_MOE_DEFINITIONS.keys())
    if not raw_moes:
        return ["aogPct", "aorPct", "volume", "avgWait", "splitFailures"]
    selected = [part.strip() for part in raw_moes.split(",") if part.strip() in allowed]
    return selected or ["aogPct", "aorPct", "volume", "avgWait", "splitFailures"]


def _parse_signal_id_list(raw_signal_ids: str | None) -> list[str]:
    if not raw_signal_ids:
        return DEFAULT_SELECTED_IDS
    selected = [part.strip() for part in raw_signal_ids.split(",") if part.strip() in INTERSECTION_LOOKUP]
    ordered = [signal_id for signal_id in ORDERED_SIGNAL_IDS if signal_id in selected]
    return ordered or DEFAULT_SELECTED_IDS


def _parse_phase_id_list(raw_phase_ids: str | None, available_phases: list[int] | None = None) -> list[int]:
    if available_phases is None:
        available_phases = list(range(1, 9))
    available_set = {int(value) for value in available_phases}
    if not raw_phase_ids:
        return [available_phases[0]] if available_phases else [1]
    parsed = []
    for part in raw_phase_ids.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            phase = int(part)
        except ValueError:
            continue
        if phase in available_set and phase not in parsed:
            parsed.append(phase)
    return parsed or ([available_phases[0]] if available_phases else [1])


def _parse_float_range(raw_value: str | None, fallback: float, minimum: float, maximum: float) -> float:
    try:
        parsed = float(raw_value) if raw_value is not None else fallback
    except (TypeError, ValueError):
        parsed = fallback
    return max(minimum, min(maximum, parsed))


def _parse_int_range(raw_value: str | None, fallback: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(raw_value) if raw_value is not None else fallback
    except (TypeError, ValueError):
        parsed = fallback
    return max(minimum, min(maximum, parsed))


def _parse_change_detection_moe(raw_moe: str | None) -> str:
    if raw_moe in CHANGE_DETECTION_MOE_KEYS:
        return raw_moe
    return "avgWait"


def _parse_change_detection_granularity(raw_granularity: str | None) -> str:
    if raw_granularity in {"day", "hour"}:
        return raw_granularity
    return "hour"


def _parse_hour_label(raw_hour: str | None, fallback: str = "06:00") -> str:
    if not raw_hour:
        return fallback
    try:
        hour = int(str(raw_hour).split(":")[0])
    except (TypeError, ValueError):
        return fallback
    hour = max(0, min(23, hour))
    return f"{hour:02d}:00"


def _available_phase_list(conn: sqlite3.Connection) -> list[int]:
    phase_sets: list[set[int]] = []
    for table_name in ["arrival_on_green", "phase_wait", "split_failures", "terminations"]:
        if not _sqlite_table_exists(conn, table_name):
            continue
        rows = conn.execute(f"SELECT DISTINCT Phase FROM {table_name} WHERE Phase IS NOT NULL").fetchall()
        phase_sets.append({int(row[0]) for row in rows if row[0] is not None})
    combined = set().union(*phase_sets) if phase_sets else set()
    return sorted(combined)


def _build_bottleneck_hourly_vectors(
    conn: sqlite3.Connection,
    signal_id: str,
    phase: int,
    date_from: date,
    date_to: date,
    hour_start: int,
    hour_end: int,
) -> pd.DataFrame:
    sql_params = (date_from.isoformat(), date_to.isoformat(), hour_start, hour_end, phase)

    aog = _sqlite_frame(
        conn,
        "arrival_on_green",
        """
        SELECT
            run_date,
            hour,
            Phase,
            AVG(Percent_AOG) * 100.0 AS aogPct,
            AVG((1.0 - Percent_AOG)) * 100.0 AS aorPct,
            SUM(Total_Actuations) AS volume
        FROM arrival_on_green
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ? AND Phase = ?
        GROUP BY run_date, hour, Phase
        """,
        sql_params,
    )

    waits = _sqlite_frame(
        conn,
        "phase_wait",
        """
        SELECT
            run_date,
            hour,
            Phase,
            AVG(AvgPhaseWait) AS avgWait
        FROM phase_wait
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ? AND Phase = ?
        GROUP BY run_date, hour, Phase
        """,
        sql_params,
    )

    split = _sqlite_frame(
        conn,
        "split_failures",
        """
        SELECT
            run_date,
            hour,
            Phase,
            AVG(Green_Occupancy) * 100.0 AS greenOccupancyPct,
            AVG(Red_Occupancy) * 100.0 AS redOccupancyPct,
            SUM(Split_Failure) AS splitFailures
        FROM split_failures
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ? AND Phase = ?
        GROUP BY run_date, hour, Phase
        """,
        sql_params,
    )

    terms = _sqlite_frame(
        conn,
        "terminations",
        """
        SELECT
            run_date,
            hour,
            Phase,
            PerformanceMeasure,
            SUM(Total) AS total
        FROM terminations
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ? AND Phase = ?
        GROUP BY run_date, hour, Phase, PerformanceMeasure
        """,
        sql_params,
    )

    frames = [frame for frame in [aog, waits, split] if not frame.empty]
    merged = pd.DataFrame(columns=["run_date", "hour", "Phase"])
    if frames:
        merged = frames[0]
        for frame in frames[1:]:
            merged = merged.merge(frame, on=["run_date", "hour", "Phase"], how="outer")

    if not terms.empty:
        pivot = (
            terms.pivot_table(
                index=["run_date", "hour", "Phase"],
                columns="PerformanceMeasure",
                values="total",
                aggfunc="sum",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )
        pivot = pivot.rename(columns={"MaxOut": "maxOuts", "ForceOff": "forceOffs"})
        for column in ["maxOuts", "forceOffs"]:
            if column not in pivot.columns:
                pivot[column] = 0.0
        merged = pivot if merged.empty else merged.merge(
            pivot[["run_date", "hour", "Phase", "maxOuts", "forceOffs"]],
            on=["run_date", "hour", "Phase"],
            how="outer",
        )

    if merged.empty:
        return merged

    merged["run_date"] = pd.to_datetime(merged["run_date"], errors="coerce")
    merged = merged.dropna(subset=["run_date"]).copy()
    merged["run_date"] = merged["run_date"].dt.strftime("%Y-%m-%d")
    merged["dayName"] = pd.to_datetime(merged["run_date"], errors="coerce").dt.day_name()
    merged["label"] = merged["hour"].map(lambda value: f"{int(value):02d}:00")
    merged["slotKey"] = merged["dayName"] + "-" + merged["label"]
    numeric_columns = [key for key in BOTTLENECK_MOE_DEFINITIONS.keys() if key in merged.columns]
    for column in numeric_columns:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    return merged


def _build_multi_bottleneck_hourly_vectors(
    signal_ids: list[str],
    selected_phases: list[int],
    date_from: date,
    date_to: date,
    hour_start: int,
    hour_end: int,
) -> tuple[pd.DataFrame, dict[str, list[int]], list[int]]:
    frames: list[pd.DataFrame] = []
    available_phase_map: dict[str, list[int]] = {}
    all_available_phases: set[int] = set()

    for signal_id in signal_ids:
        paths = _ensure_historical_db(signal_id)
        if not paths["sqlite_path"].exists():
            available_phase_map[signal_id] = []
            continue
        with sqlite3.connect(paths["sqlite_path"]) as conn:
            phases_for_signal = _available_phase_list(conn)
            available_phase_map[signal_id] = phases_for_signal
            all_available_phases.update(phases_for_signal)
            phases_to_use = [phase for phase in selected_phases if phase in set(phases_for_signal)]
            for phase in phases_to_use:
                frame = _build_bottleneck_hourly_vectors(
                    conn,
                    signal_id,
                    phase,
                    date_from,
                    date_to,
                    hour_start,
                    hour_end,
                )
                if frame.empty:
                    continue
                frame = frame.copy()
                frame["signalId"] = signal_id
                frame["selectedPhase"] = phase
                frames.append(frame)

    if not frames:
        return pd.DataFrame(), available_phase_map, sorted(all_available_phases)
    return pd.concat(frames, ignore_index=True), available_phase_map, sorted(all_available_phases)


def _aggregate_bottleneck_slots(hourly_vectors: pd.DataFrame, days_of_week: list[str], hour_start: int, hour_end: int) -> pd.DataFrame:
    if hourly_vectors.empty:
        base_rows = []
        for day_name in days_of_week:
            for hour in range(hour_start, hour_end + 1):
                base_rows.append({"dayName": day_name, "hour": hour, "label": f"{hour:02d}:00", "sampleCount": 0})
        return pd.DataFrame(base_rows)

    filtered = hourly_vectors[hourly_vectors["dayName"].isin(days_of_week)].copy()
    if filtered.empty:
        return _aggregate_bottleneck_slots(pd.DataFrame(), days_of_week, hour_start, hour_end)

    metric_columns = [key for key in BOTTLENECK_MOE_DEFINITIONS.keys() if key in filtered.columns]
    aggregations: dict[str, tuple[str, str]] = {column: (column, "mean") for column in metric_columns}
    aggregations["sampleCount"] = ("run_date", "size")
    grouped = (
        filtered.groupby(["dayName", "hour", "label"], as_index=False)
        .agg(**aggregations)
    )

    base_rows = []
    for day_name in days_of_week:
        for hour in range(hour_start, hour_end + 1):
            base_rows.append({"dayName": day_name, "hour": hour, "label": f"{hour:02d}:00"})
    base = pd.DataFrame(base_rows)
    merged = base.merge(grouped, on=["dayName", "hour", "label"], how="left")
    merged["sampleCount"] = merged["sampleCount"].fillna(0).astype(int)
    return merged


def _percentile_scale_frame(frame: pd.DataFrame, columns: list[str]) -> tuple[pd.DataFrame, dict[str, dict[str, float]]]:
    scaled = frame.copy()
    normalization: dict[str, dict[str, float]] = {}
    for column in columns:
        series = pd.to_numeric(scaled[column], errors="coerce")
        valid = series.dropna()
        if valid.empty:
            continue
        lower = float(valid.quantile(0.05))
        upper = float(valid.quantile(0.95))
        value_range = upper - lower
        if value_range <= 0:
            value_range = 1.0
        scaled[column] = (series - lower) / value_range
        normalization[column] = {
            "p05": round(lower, 4),
            "p95": round(upper, 4),
            "range": round(value_range, 4),
        }
    return scaled, normalization


def _run_dbscan_on_slots(
    slot_vectors: pd.DataFrame,
    selected_moes: list[str],
    eps: float,
    min_samples: int,
) -> tuple[pd.DataFrame, list[str], int, dict[str, dict[str, float]]]:
    usable = [column for column in selected_moes if column in slot_vectors.columns]
    if not usable:
        slot_vectors["clusterId"] = pd.Series([None] * len(slot_vectors), dtype="object")
        slot_vectors["clusterLabel"] = pd.Series(["No data"] * len(slot_vectors), dtype="object")
        slot_vectors["isOutlier"] = False
        return slot_vectors, [], 0, {}

    working = slot_vectors.copy()
    valid_mask = working["sampleCount"] > 0
    valid = working.loc[valid_mask, usable].copy()
    usable_after_data = [column for column in usable if valid[column].notna().any()]
    if not usable_after_data or len(valid) < 2:
        working["clusterId"] = pd.Series([None] * len(working), dtype="object")
        working["clusterLabel"] = working["sampleCount"].map(lambda count: "No data" if count == 0 else "Unclustered")
        working["isOutlier"] = False
        return working, usable_after_data, 0, {}

    valid = valid[usable_after_data].copy()
    valid = valid.fillna(valid.mean(numeric_only=True)).fillna(0.0)
    scaled_frame, normalization = _percentile_scale_frame(valid, usable_after_data)
    labels = DBSCAN(eps=eps, min_samples=min_samples).fit_predict(scaled_frame[usable_after_data])

    working["clusterId"] = pd.Series([None] * len(working), dtype="object")
    working["clusterLabel"] = pd.Series(["No data"] * len(working), dtype="object")
    working["isOutlier"] = False

    label_series = pd.Series(labels, index=working.index[valid_mask])
    cluster_count = len({int(label) for label in labels if int(label) >= 0})

    for index, label in label_series.items():
        if int(label) == -1:
            working.at[index, "clusterId"] = -1
            working.at[index, "clusterLabel"] = "Outlier"
            working.at[index, "isOutlier"] = True
        else:
            working.at[index, "clusterId"] = int(label)
            working.at[index, "clusterLabel"] = f"Cluster {int(label) + 1}"

    working.loc[(working["sampleCount"] > 0) & (working["clusterLabel"] == "No data"), "clusterLabel"] = "Unclustered"
    return working, usable_after_data, cluster_count, normalization


def _run_hdbscan_on_slots(
    slot_vectors: pd.DataFrame,
    selected_moes: list[str],
    min_cluster_size: int,
    min_samples: int,
    cluster_selection_epsilon: float,
) -> tuple[pd.DataFrame, list[str], int, dict[str, dict[str, float]]]:
    usable = [column for column in selected_moes if column in slot_vectors.columns]
    if not usable:
        slot_vectors["clusterId"] = pd.Series([None] * len(slot_vectors), dtype="object")
        slot_vectors["clusterLabel"] = pd.Series(["No data"] * len(slot_vectors), dtype="object")
        slot_vectors["isOutlier"] = False
        return slot_vectors, [], 0, {}

    working = slot_vectors.copy()
    valid_mask = working["sampleCount"] > 0
    valid = working.loc[valid_mask, usable].copy()
    usable_after_data = [column for column in usable if valid[column].notna().any()]
    if not usable_after_data or len(valid) < 2:
        working["clusterId"] = pd.Series([None] * len(working), dtype="object")
        working["clusterLabel"] = working["sampleCount"].map(lambda count: "No data" if count == 0 else "Unclustered")
        working["isOutlier"] = False
        return working, usable_after_data, 0, {}

    valid = valid[usable_after_data].copy()
    valid = valid.fillna(valid.mean(numeric_only=True)).fillna(0.0)
    scaled_frame, normalization = _percentile_scale_frame(valid, usable_after_data)
    labels = HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=min_samples,
        cluster_selection_epsilon=cluster_selection_epsilon,
        allow_single_cluster=True,
    ).fit_predict(scaled_frame[usable_after_data])

    working["clusterId"] = pd.Series([None] * len(working), dtype="object")
    working["clusterLabel"] = pd.Series(["No data"] * len(working), dtype="object")
    working["isOutlier"] = False

    label_series = pd.Series(labels, index=working.index[valid_mask])
    cluster_count = len({int(label) for label in labels if int(label) >= 0})

    for index, label in label_series.items():
        if int(label) == -1:
            working.at[index, "clusterId"] = -1
            working.at[index, "clusterLabel"] = "Outlier"
            working.at[index, "isOutlier"] = True
        else:
            working.at[index, "clusterId"] = int(label)
            working.at[index, "clusterLabel"] = f"Cluster {int(label) + 1}"

    working.loc[(working["sampleCount"] > 0) & (working["clusterLabel"] == "No data"), "clusterLabel"] = "Unclustered"
    return working, usable_after_data, cluster_count, normalization


def _cluster_time_band(hours: list[int]) -> str:
    if not hours:
        return "Mixed timing"
    mean_hour = sum(hours) / len(hours)
    if mean_hour < 10:
        return "AM pattern"
    if mean_hour < 14:
        return "Midday pattern"
    if mean_hour < 18:
        return "PM pattern"
    return "Evening pattern"


def _cluster_day_pattern(day_names: list[str]) -> str:
    if not day_names:
        return "Mixed days"
    weekday_names = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}
    weekend_names = {"Saturday", "Sunday"}
    unique_days = set(day_names)
    if unique_days.issubset(weekday_names):
        return "Weekday"
    if unique_days.issubset(weekend_names):
        return "Weekend"
    day_counts = Counter(day_names)
    top_days = [name for name, _count in day_counts.most_common(2)]
    return " / ".join(top_days)


def _cluster_feature_phrase(key: str, z_score: float) -> str:
    label = BOTTLENECK_MOE_LABELS.get(key, key)
    direction = "higher" if z_score >= 0 else "lower"
    return f"{direction} {label.lower()}"


def _cluster_performance_tag(z_scores: dict[str, float]) -> str:
    wait_z = z_scores.get("avgWait", 0.0)
    split_z = z_scores.get("splitFailures", 0.0)
    aog_z = z_scores.get("aogPct", 0.0)
    aor_z = z_scores.get("aorPct", 0.0)
    max_out_z = z_scores.get("maxOuts", 0.0)
    force_off_z = z_scores.get("forceOffs", 0.0)
    if wait_z >= 0.8 or split_z >= 0.8 or aor_z >= 0.8 or max_out_z >= 0.8 or force_off_z >= 0.8 or aog_z <= -0.8:
        return "Potential bottleneck"
    if wait_z <= -0.5 and split_z <= -0.4 and (aog_z >= 0.3 or aor_z <= -0.3):
        return "Smoother pattern"
    return "Mixed pattern"


def _build_bottleneck_change_detections(slot_vectors: pd.DataFrame, used_moes: list[str]) -> list[dict[str, Any]]:
    working = slot_vectors[slot_vectors["sampleCount"] > 0].copy()
    if working.empty or len(working) < 2 or not used_moes:
        return []

    baseline_stats: dict[str, dict[str, float]] = {}
    for key in used_moes:
        if key not in working.columns or not working[key].notna().any():
            continue
        mean_value = float(working[key].mean())
        std_value = float(working[key].std(ddof=0))
        baseline_stats[key] = {
            "mean": mean_value,
            "std": std_value if std_value > 0 else 1.0,
        }

    change_rows: list[dict[str, Any]] = []
    for day_name in DAY_NAMES:
        day_frame = working[working["dayName"] == day_name].sort_values("hour").reset_index(drop=True)
        if len(day_frame) < 2:
            continue
        for idx in range(1, len(day_frame)):
            previous = day_frame.iloc[idx - 1]
            current = day_frame.iloc[idx]
            if int(current["hour"]) - int(previous["hour"]) != 1:
                continue

            change_parts: list[tuple[float, str, float]] = []
            distance_terms: list[float] = []
            for key in used_moes:
                if key not in baseline_stats:
                    continue
                previous_value = previous.get(key)
                current_value = current.get(key)
                if pd.isna(previous_value) or pd.isna(current_value):
                    continue
                delta = float(current_value) - float(previous_value)
                std_value = baseline_stats[key]["std"]
                z_delta = delta / std_value if std_value else 0.0
                distance_terms.append(z_delta * z_delta)
                if abs(delta) > 0.01:
                    label = BOTTLENECK_MOE_LABELS.get(key, key)
                    sign = "+" if delta >= 0 else ""
                    change_parts.append((abs(z_delta), f"{label} {sign}{delta:.1f}", delta))

            if not distance_terms:
                continue

            base_distance = (sum(distance_terms) / len(distance_terms)) ** 0.5
            cluster_changed = previous.get("clusterId") != current.get("clusterId")
            outlier_edge = bool(previous.get("isOutlier")) or bool(current.get("isOutlier"))
            severity = base_distance + (0.45 if cluster_changed else 0.0) + (0.25 if outlier_edge else 0.0)

            change_parts.sort(key=lambda item: item[0], reverse=True)
            top_changes = [text for _weight, text, _delta in change_parts[:3]]
            if outlier_edge:
                transition_type = "Outlier edge"
            elif cluster_changed:
                transition_type = "Cluster change"
            else:
                transition_type = "Metric shift"

            if severity >= 2.0:
                severity_label = "High"
            elif severity >= 1.2:
                severity_label = "Moderate"
            else:
                severity_label = "Low"

            summary_sentence = (
                f"{day_name} {previous['label']} to {current['label']}: "
                + (", ".join(top_changes) if top_changes else "noticeable MOE change")
                + "."
            )
            change_rows.append(
                {
                    "dayName": day_name,
                    "fromHour": int(previous["hour"]),
                    "fromLabel": str(previous["label"]),
                    "toHour": int(current["hour"]),
                    "toLabel": str(current["label"]),
                    "fromClusterLabel": str(previous.get("clusterLabel", "Unknown")),
                    "toClusterLabel": str(current.get("clusterLabel", "Unknown")),
                    "transitionType": transition_type,
                    "severityScore": round(float(severity), 2),
                    "severityLabel": severity_label,
                    "summarySentence": summary_sentence,
                    "topChanges": top_changes,
                }
            )

    change_rows.sort(key=lambda item: item["severityScore"], reverse=True)
    return change_rows[:10]


def build_ranking_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    default_date = _default_analysis_date()

    date_from = _safe_date(query.get("dateFrom", [None])[0], default_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], date_from)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    days_of_week = _parse_days(query.get("daysOfWeek", [None])[0], date_from, date_to)
    preset_id, hour_from, hour_to = _parse_hours(
        query.get("timeOfDayPreset", [None])[0],
        query.get("hourFrom", [None])[0],
        query.get("hourTo", [None])[0],
    )
    hour_start = int(hour_from.split(":")[0])
    hour_end = int(hour_to.split(":")[0])

    rows: list[dict[str, Any]] = []
    sql_params = (date_from.isoformat(), date_to.isoformat(), hour_start, hour_end)
    all_ids = [meta.id for meta in _intersection_inventory()]
    raw_phase_table = _empty_phase_table()

    raw_data = _load_raw_slice(all_ids, date_from, date_to, days_of_week, hour_from, hour_to)
    if not raw_data.empty:
        detector_config = _build_detector_config(all_ids)
        tables = _run_processor(raw_data, detector_config)
        raw_phase_table = _phase_table(tables, "all")

    for meta in _intersection_inventory():
        sqlite_path = historical_output_paths(int(meta.id))["sqlite_path"]
        if not sqlite_path.exists():
            fallback = raw_phase_table[raw_phase_table["DeviceId"] == int(meta.id)].copy()
            if fallback.empty:
                rows.append(
                    {
                        "id": meta.id,
                        "name": meta.name,
                        "route": meta.route,
                        "region": meta.region,
                        "hasHistoricalDb": False,
                        "hasData": False,
                        "aogPct": None,
                        "aorPct": None,
                        "avgWait": None,
                        "greenOccupancyPct": None,
                        "redOccupancyPct": None,
                        "splitFailures": None,
                        "maxOuts": None,
                        "forceOffs": None,
                    }
                )
                continue

            aog_pct = round(float(fallback["aog"].mean() * 100), 1)
            rows.append(
                {
                    "id": meta.id,
                    "name": meta.name,
                    "route": meta.route,
                    "region": meta.region,
                    "hasHistoricalDb": False,
                    "hasData": True,
                    "aogPct": aog_pct,
                    "aorPct": round(max(0.0, 100.0 - aog_pct), 1),
                    "avgWait": round(float(fallback["avg_wait"].mean()), 1),
                    "greenOccupancyPct": round(float(fallback["green_occupancy"].mean() * 100), 1),
                    "redOccupancyPct": round(float(fallback["red_occupancy"].mean() * 100), 1),
                    "splitFailures": int(round(float(fallback["split_failures"].sum()))),
                    "maxOuts": int(round(float(fallback["MaxOut"].sum()))),
                    "forceOffs": int(round(float(fallback["ForceOff"].sum()))),
                }
            )
            continue

        with sqlite3.connect(sqlite_path) as conn:
            aog = _sqlite_frame(
                conn,
                "arrival_on_green",
                """
                SELECT run_date, hour, Percent_AOG
                FROM arrival_on_green
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            waits = _sqlite_frame(
                conn,
                "phase_wait",
                """
                SELECT run_date, hour, AvgPhaseWait
                FROM phase_wait
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            split = _sqlite_frame(
                conn,
                "split_failures",
                """
                SELECT run_date, hour, Green_Occupancy, Red_Occupancy, Split_Failure
                FROM split_failures
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )
            terms = _sqlite_frame(
                conn,
                "terminations",
                """
                SELECT run_date, hour, PerformanceMeasure, Total
                FROM terminations
                WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
                """,
                sql_params,
            )

        aog = _filter_rank_frame(aog, days_of_week)
        waits = _filter_rank_frame(waits, days_of_week)
        split = _filter_rank_frame(split, days_of_week)
        terms = _filter_rank_frame(terms, days_of_week)

        has_data = not aog.empty or not waits.empty or not split.empty or not terms.empty
        max_outs = None
        force_offs = None
        if not terms.empty:
            max_outs = int(round(float(terms.loc[terms["PerformanceMeasure"] == "MaxOut", "Total"].sum())))
            force_offs = int(round(float(terms.loc[terms["PerformanceMeasure"] == "ForceOff", "Total"].sum())))

        aog_pct = round(float(aog["Percent_AOG"].mean() * 100), 1) if not aog.empty else None
        rows.append(
            {
                "id": meta.id,
                "name": meta.name,
                "route": meta.route,
                "region": meta.region,
                "hasHistoricalDb": True,
                "hasData": has_data,
                "aogPct": aog_pct,
                "aorPct": round(max(0.0, 100.0 - aog_pct), 1) if aog_pct is not None else None,
                "avgWait": round(float(waits["AvgPhaseWait"].mean()), 1) if not waits.empty else None,
                "greenOccupancyPct": round(float(split["Green_Occupancy"].mean() * 100), 1) if not split.empty else None,
                "redOccupancyPct": round(float(split["Red_Occupancy"].mean() * 100), 1) if not split.empty else None,
                "splitFailures": int(round(float(split["Split_Failure"].sum()))) if not split.empty else None,
                "maxOuts": max_outs,
                "forceOffs": force_offs,
            }
        )

    built_count = sum(1 for row in rows if row["hasHistoricalDb"])
    data_count = sum(1 for row in rows if row["hasData"])
    return {
        "meta": {
            "dataSource": "Precomputed ATSPM MOE results with direct event fallback for signals that are not built yet",
            "builtSignals": built_count,
            "signalsWithData": data_count,
            "availableSignals": len(rows),
            "metricDefinitions": {
                "aogPct": "Arrival on Green percentage from the saved MOE results.",
                "aorPct": "Arrival on Red percentage derived here as 100 minus AOG for the selected filtered window.",
                "avgWait": "Average phase wait in seconds from the saved MOE results.",
                "greenOccupancyPct": "Average green occupancy percentage from the saved MOE results.",
                "redOccupancyPct": "Average red occupancy percentage from the saved MOE results.",
                "splitFailures": "Total split failures from the saved MOE results.",
                "maxOuts": "Total Max-Out terminations from the saved MOE results.",
                "forceOffs": "Total Force-Off terminations from the saved MOE results.",
                "compositeScore": "Composite score built from normalized AOG, AOR, wait, split failures, max-outs, and force-offs for the current filtered window.",
            },
        },
        "filters": {
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": hour_from,
            "hourTo": hour_to,
            "timeOfDayPreset": preset_id,
        },
        "rows": rows,
    }


def build_day_hour_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    default_date = _default_analysis_date()

    signal_id = _historical_signal_id(query.get("signalId", [None])[0])
    metric = str(query.get("metric", ["avgWait"])[0] or "avgWait")
    allowed_metrics = {
        "aogPct",
        "aorPct",
        "avgWait",
        "greenOccupancyPct",
        "redOccupancyPct",
        "splitFailures",
        "maxOuts",
        "forceOffs",
    }
    if metric not in allowed_metrics:
        metric = "avgWait"

    date_from = _safe_date(query.get("dateFrom", [None])[0], default_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], date_from)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    days_of_week = _parse_days(query.get("daysOfWeek", [None])[0], date_from, date_to)
    preset_id, hour_from, hour_to = _parse_hours(
        query.get("timeOfDayPreset", [None])[0],
        query.get("hourFrom", [None])[0],
        query.get("hourTo", [None])[0],
    )
    hour_start = int(hour_from.split(":")[0])
    hour_end = int(hour_to.split(":")[0])
    sql_params = (date_from.isoformat(), date_to.isoformat(), hour_start, hour_end)

    signal_meta = INTERSECTION_LOOKUP[signal_id]
    sqlite_path = historical_output_paths(int(signal_id))["sqlite_path"]
    if not sqlite_path.exists():
        return {
            "meta": {
                "dataSource": "Precomputed ATSPM MOE results",
                "metricDefinitions": {},
                "availableSignals": _historical_available_signals(),
            },
            "filters": {
                "signalId": signal_id,
                "metric": metric,
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
                "daysOfWeek": days_of_week,
                "hourFrom": hour_from,
                "hourTo": hour_to,
                "timeOfDayPreset": preset_id,
            },
            "signal": {
                "id": signal_meta.id,
                "name": signal_meta.name,
                "route": signal_meta.route,
                "region": signal_meta.region,
                "lat": signal_meta.lat,
                "lon": signal_meta.lon,
            },
            "hours": [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)],
            "days": days_of_week,
            "cells": [],
        }

    with sqlite3.connect(sqlite_path) as conn:
        aog = _sqlite_frame(
            conn,
            "arrival_on_green",
            """
            SELECT run_date, hour, Percent_AOG
            FROM arrival_on_green
            WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
            """,
            sql_params,
        )
        waits = _sqlite_frame(
            conn,
            "phase_wait",
            """
            SELECT run_date, hour, AvgPhaseWait
            FROM phase_wait
            WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
            """,
            sql_params,
        )
        split = _sqlite_frame(
            conn,
            "split_failures",
            """
            SELECT run_date, hour, Green_Occupancy, Red_Occupancy, Split_Failure
            FROM split_failures
            WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
            """,
            sql_params,
        )
        terms = _sqlite_frame(
            conn,
            "terminations",
            """
            SELECT run_date, hour, PerformanceMeasure, Total
            FROM terminations
            WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
            """,
            sql_params,
        )

    aog = _filter_rank_frame(aog, days_of_week)
    waits = _filter_rank_frame(waits, days_of_week)
    split = _filter_rank_frame(split, days_of_week)
    terms = _filter_rank_frame(terms, days_of_week)

    def _group_metric(frame: pd.DataFrame, value_key: str, transform=None, agg: str = "mean") -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["day_name", "hour", "value", "sampleCount"])
        grouped = frame.copy()
        grouped["run_date"] = pd.to_datetime(grouped["run_date"], errors="coerce")
        grouped = grouped.dropna(subset=["run_date"])
        grouped["day_name"] = grouped["run_date"].dt.day_name()
        if transform is not None:
            grouped[value_key] = grouped[value_key].map(transform)
        if agg == "sum":
            out = (
                grouped.groupby(["day_name", "hour"], as_index=False)
                .agg(value=(value_key, "sum"), sampleCount=(value_key, "size"))
            )
        else:
            out = (
                grouped.groupby(["day_name", "hour"], as_index=False)
                .agg(value=(value_key, "mean"), sampleCount=(value_key, "size"))
            )
        return out

    metric_frame = pd.DataFrame(columns=["day_name", "hour", "value", "sampleCount"])
    if metric == "aogPct":
        metric_frame = _group_metric(aog, "Percent_AOG", lambda value: float(value) * 100, "mean")
    elif metric == "aorPct":
        metric_frame = _group_metric(aog, "Percent_AOG", lambda value: 100.0 - float(value) * 100, "mean")
    elif metric == "avgWait":
        metric_frame = _group_metric(waits, "AvgPhaseWait", float, "mean")
    elif metric == "greenOccupancyPct":
        metric_frame = _group_metric(split, "Green_Occupancy", lambda value: float(value) * 100, "mean")
    elif metric == "redOccupancyPct":
        metric_frame = _group_metric(split, "Red_Occupancy", lambda value: float(value) * 100, "mean")
    elif metric == "splitFailures":
        metric_frame = _group_metric(split, "Split_Failure", float, "mean")
    elif metric in {"maxOuts", "forceOffs"} and not terms.empty:
        measure_name = "MaxOut" if metric == "maxOuts" else "ForceOff"
        metric_frame = _group_metric(terms.loc[terms["PerformanceMeasure"] == measure_name].copy(), "Total", float, "mean")

    metric_lookup: dict[tuple[str, int], dict[str, Any]] = {}
    if not metric_frame.empty:
        for item in metric_frame.itertuples():
            metric_lookup[(str(item.day_name), int(item.hour))] = {
                "value": round(float(item.value), 1),
                "sampleCount": int(item.sampleCount),
            }

    hours = [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)]
    cells: list[dict[str, Any]] = []
    for day_name in days_of_week:
        for hour in range(hour_start, hour_end + 1):
            point = metric_lookup.get((day_name, hour))
            cells.append(
                {
                    "dayName": day_name,
                    "hour": hour,
                    "label": f"{hour:02d}:00",
                    "value": point["value"] if point else None,
                    "sampleCount": point["sampleCount"] if point else 0,
                }
            )

    return {
        "meta": {
            "dataSource": "Precomputed ATSPM MOE results",
            "metricDefinitions": {
                "aogPct": "Average arrival-on-green percentage for matching day-hour buckets across the selected date window.",
                "aorPct": "Average arrival-on-red percentage derived as 100 minus arrival-on-green for matching day-hour buckets.",
                "avgWait": "Average phase wait in seconds for matching day-hour buckets across the selected date window.",
                "greenOccupancyPct": "Average green occupancy percentage for matching day-hour buckets.",
                "redOccupancyPct": "Average red occupancy percentage for matching day-hour buckets.",
                "splitFailures": "Average split-failure count for matching day-hour buckets.",
                "maxOuts": "Average Max-Out termination count for matching day-hour buckets.",
                "forceOffs": "Average Force-Off termination count for matching day-hour buckets.",
            },
            "availableSignals": _historical_available_signals(),
        },
        "filters": {
            "signalId": signal_id,
            "metric": metric,
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": hour_from,
            "hourTo": hour_to,
            "timeOfDayPreset": preset_id,
        },
        "signal": {
            "id": signal_meta.id,
            "name": signal_meta.name,
            "route": signal_meta.route,
            "region": signal_meta.region,
            "lat": signal_meta.lat,
            "lon": signal_meta.lon,
        },
        "hours": hours,
        "days": days_of_week,
        "cells": cells,
    }


def _change_detection_hourly_metric_points(
    conn: sqlite3.Connection,
    metric: str,
    date_from: date,
    date_to: date,
    hour_start: int,
    hour_end: int,
) -> pd.DataFrame:
    sql_params = (date_from.isoformat(), date_to.isoformat(), hour_start, hour_end)
    aog = _sqlite_frame(
        conn,
        "arrival_on_green",
        """
        SELECT run_date, hour, Percent_AOG
        FROM arrival_on_green
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
        """,
        sql_params,
    )
    waits = _sqlite_frame(
        conn,
        "phase_wait",
        """
        SELECT run_date, hour, AvgPhaseWait
        FROM phase_wait
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
        """,
        sql_params,
    )
    split = _sqlite_frame(
        conn,
        "split_failures",
        """
        SELECT run_date, hour, Green_Occupancy, Red_Occupancy, Split_Failure
        FROM split_failures
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
        """,
        sql_params,
    )
    terms = _sqlite_frame(
        conn,
        "terminations",
        """
        SELECT run_date, hour, PerformanceMeasure, Total
        FROM terminations
        WHERE run_date >= ? AND run_date <= ? AND hour >= ? AND hour <= ?
        """,
        sql_params,
    )

    def _group_metric(frame: pd.DataFrame, value_key: str, transform=None) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["run_date", "hour", "value"])
        grouped = frame.copy()
        grouped["run_date"] = pd.to_datetime(grouped["run_date"], errors="coerce")
        grouped = grouped.dropna(subset=["run_date"])
        if transform is not None:
            grouped[value_key] = grouped[value_key].map(transform)
        out = (
            grouped.groupby(["run_date", "hour"], as_index=False)
            .agg(value=(value_key, "mean"))
        )
        out["run_date"] = out["run_date"].dt.strftime("%Y-%m-%d")
        return out

    if metric == "aogPct":
        return _group_metric(aog, "Percent_AOG", lambda value: float(value) * 100)
    if metric == "aorPct":
        return _group_metric(aog, "Percent_AOG", lambda value: 100.0 - float(value) * 100)
    if metric == "avgWait":
        return _group_metric(waits, "AvgPhaseWait", float)
    if metric == "greenOccupancyPct":
        return _group_metric(split, "Green_Occupancy", lambda value: float(value) * 100)
    if metric == "redOccupancyPct":
        return _group_metric(split, "Red_Occupancy", lambda value: float(value) * 100)
    if metric == "splitFailures":
        return _group_metric(split, "Split_Failure", float)
    if metric in {"maxOuts", "forceOffs"} and not terms.empty:
        measure_name = "MaxOut" if metric == "maxOuts" else "ForceOff"
        return _group_metric(terms.loc[terms["PerformanceMeasure"] == measure_name].copy(), "Total", float)
    return pd.DataFrame(columns=["run_date", "hour", "value"])


def _build_intersection_change_rows(
    signal_id: str,
    metric: str,
    granularity: str,
    date_from: date,
    date_to: date,
    hour_start: int,
    hour_end: int,
    selected_date: date,
    selected_hour_label: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    paths = _ensure_historical_db(signal_id)
    if not paths["sqlite_path"].exists():
        return [], {
            "signalId": signal_id,
            "signalName": INTERSECTION_LOOKUP[signal_id].name,
            "currentValue": None,
            "historicalMean": None,
            "deltaPct": None,
            "status": "No data",
            "tone": "calm",
            "historyCount": 0,
        }

    with sqlite3.connect(paths["sqlite_path"]) as conn:
        hourly = _change_detection_hourly_metric_points(conn, metric, date_from, date_to, hour_start, hour_end)

    if hourly.empty:
        return [], {
            "signalId": signal_id,
            "signalName": INTERSECTION_LOOKUP[signal_id].name,
            "currentValue": None,
            "historicalMean": None,
            "deltaPct": None,
            "status": "No data",
            "tone": "calm",
            "historyCount": 0,
        }

    hourly["run_date"] = pd.to_datetime(hourly["run_date"], errors="coerce")
    hourly = hourly.dropna(subset=["run_date", "value"]).copy()
    hourly["dayName"] = hourly["run_date"].dt.day_name()
    hourly = hourly.sort_values(["run_date", "hour"]).reset_index(drop=True)
    if hourly.empty:
        return [], {
            "signalId": signal_id,
            "signalName": INTERSECTION_LOOKUP[signal_id].name,
            "currentValue": None,
            "historicalMean": None,
            "deltaPct": None,
            "status": "No data",
            "tone": "calm",
            "historyCount": 0,
        }

    if granularity == "day":
        daily = (
            hourly.groupby(["run_date", "dayName"], as_index=False)
            .agg(value=("value", "mean"))
        )
        current = daily.loc[daily["run_date"].dt.date == selected_date].copy()
        prior = daily.loc[daily["run_date"].dt.date < selected_date].copy()
        label = selected_date.isoformat()
        hour_value = None
    else:
        selected_hour = int(selected_hour_label.split(":")[0])
        current = hourly.loc[(hourly["run_date"].dt.date == selected_date) & (hourly["hour"] == selected_hour)].copy()
        prior = hourly.loc[(hourly["run_date"].dt.date < selected_date) & (hourly["hour"] == selected_hour)].copy()
        label = f"{selected_date.isoformat()} {selected_hour:02d}:00"
        hour_value = selected_hour

    current_value = None if current.empty else float(current["value"].mean())
    historical_mean = None if prior.empty else float(prior["value"].mean())
    history_count = int(len(prior))
    delta_pct = None
    status = "No data"
    tone = "calm"
    direction = "none"

    if current_value is not None and historical_mean is None:
        status = "Baseline pending"
    elif current_value is not None and historical_mean is not None:
        if historical_mean == 0:
            delta_pct = 0.0 if current_value == 0 else 100.0
        else:
            delta_pct = ((current_value - historical_mean) / abs(historical_mean)) * 100.0
        if delta_pct <= -5.0:
            status = "Lower than history"
            tone = "watch"
            direction = "below"
        elif delta_pct >= 5.0:
            status = "Higher than history"
            tone = "alert"
            direction = "above"
        else:
            status = "Near history"
            tone = "calm"
            direction = "within"

    row = {
        "signalId": signal_id,
        "signalName": INTERSECTION_LOOKUP[signal_id].name,
        "date": selected_date.isoformat(),
        "dayName": selected_date.strftime("%A"),
        "hour": hour_value,
        "label": label,
        "value": None if current_value is None else round(current_value, 2),
        "historicalMean": None if historical_mean is None else round(historical_mean, 2),
        "historyCount": history_count,
        "deltaPct": None if delta_pct is None else round(delta_pct, 2),
        "status": status,
        "tone": tone,
        "direction": direction,
    }
    return [row], {
        "signalId": signal_id,
        "signalName": INTERSECTION_LOOKUP[signal_id].name,
        "currentValue": row["value"],
        "historicalMean": row["historicalMean"],
        "deltaPct": row["deltaPct"],
        "status": status,
        "tone": tone,
        "historyCount": history_count,
    }


def build_change_detection_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    if not available_dates:
        raise RuntimeError("No raw ATSPM files are available.")

    min_date = min(available_dates)
    max_date = max(available_dates)
    date_from = min_date
    date_to = max_date

    hour_start = 6
    hour_end = 21
    selected_signal_ids = _parse_signal_id_list(query.get("signalIds", [None])[0])
    selected_moe = _parse_change_detection_moe(query.get("moe", [None])[0])
    granularity = _parse_change_detection_granularity(query.get("granularity", [None])[0])
    selected_date = _safe_date(query.get("selectedDate", [None])[0], max_date)
    selected_hour = _parse_hour_label(query.get("selectedHour", [None])[0], "06:00")

    rows: list[dict[str, Any]] = []
    intersection_summaries: list[dict[str, Any]] = []

    for signal_id in selected_signal_ids:
        signal_rows, signal_summary = _build_intersection_change_rows(
            signal_id,
            selected_moe,
            granularity,
            date_from,
            date_to,
            hour_start,
            hour_end,
            selected_date,
            selected_hour,
        )
        rows.extend(signal_rows)
        intersection_summaries.append(signal_summary)

    total_points = len(rows)
    baseline_ready = sum(1 for row in rows if row["historicalMean"] is not None)
    total_change_count = sum(1 for row in rows if row["tone"] in {"watch", "alert"})
    stable_count = sum(1 for row in rows if row["tone"] == "calm" and row["historicalMean"] is not None)
    pending_count = sum(1 for row in rows if row["historicalMean"] is None)
    available_hours = [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)]
    if selected_hour not in available_hours:
        selected_hour = available_hours[0]

    map_statuses = []
    for summary in intersection_summaries:
        meta = INTERSECTION_LOOKUP[summary["signalId"]]
        map_statuses.append(
            {
                "signalId": summary["signalId"],
                "signalName": meta.name,
                "lat": meta.lat,
                "lon": meta.lon,
                "deltaPct": summary["deltaPct"],
                "tone": summary["tone"],
            }
        )

    top_changes = sorted(
        [row for row in rows if row["deltaPct"] is not None],
        key=lambda row: abs(float(row["deltaPct"])),
        reverse=True,
    )

    return {
        "meta": {
            "dataSource": "Precomputed ATSPM MOE results",
            "availableSignals": _historical_available_signals(),
            "selectionPresets": _signal_selection_presets(),
            "moeOptions": [
                {"key": key, "label": BOTTLENECK_MOE_LABELS.get(key, key), "description": BOTTLENECK_MOE_DEFINITIONS[key]}
                for key in CHANGE_DETECTION_MOE_KEYS
            ],
            "granularityOptions": [
                {"key": "hour", "label": "Hour-wise"},
                {"key": "day", "label": "Day-wise"},
            ],
        },
        "filters": {
            "signalIds": selected_signal_ids,
            "moe": selected_moe,
            "granularity": granularity,
            "dateFrom": min_date.isoformat(),
            "dateTo": max_date.isoformat(),
            "hourFrom": f"{hour_start:02d}:00",
            "hourTo": f"{hour_end:02d}:00",
            "selectedDate": selected_date.isoformat(),
            "selectedHour": selected_hour,
        },
        "slotSelection": {
            "date": selected_date.isoformat(),
            "dayName": selected_date.strftime("%A"),
            "hour": selected_hour,
        },
        "summary": {
            "selectedSignalCount": len(selected_signal_ids),
            "pointCount": total_points,
            "baselineReady": baseline_ready,
            "changeCount": total_change_count,
            "stableCount": stable_count,
            "pendingCount": pending_count,
            "slotPointCount": total_points,
            "slotChangeCount": total_change_count,
        },
        "intersectionSummaries": intersection_summaries,
        "mapStatuses": map_statuses,
        "availableDates": [value.isoformat() for value in available_dates],
        "availableHours": available_hours,
        "rows": rows,
        "slotRows": rows,
        "topChanges": top_changes[:20],
    }


def build_bottleneck_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    if not available_dates:
        raise RuntimeError("No raw ATSPM files are available.")

    signal_id = _historical_signal_id(query.get("signalId", [None])[0])
    paths = _ensure_historical_db(signal_id)
    signal_meta = INTERSECTION_LOOKUP[signal_id]

    min_date = min(available_dates)
    max_date = max(available_dates)
    date_from = _safe_date(query.get("dateFrom", [None])[0], min_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], max_date)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    days_of_week = DAY_NAMES
    hour_start = 6
    hour_end = 21
    selected_moes = _parse_bottleneck_moes(query.get("moes", [None])[0])
    eps = _parse_float_range(query.get("eps", [None])[0], 0.25, 0.1, 5.0)
    min_samples = _parse_int_range(query.get("minSamples", [None])[0], 2, 1, 12)

    if not paths["sqlite_path"].exists():
        return {
            "meta": {
                "dataSource": "Precomputed ATSPM MOE results",
                "availableSignals": _historical_available_signals(),
                "moeOptions": [
                    {"key": key, "label": BOTTLENECK_MOE_LABELS.get(key, key), "description": description}
                    for key, description in BOTTLENECK_MOE_DEFINITIONS.items()
                ],
            },
            "filters": {
                "signalId": signal_id,
                "phase": 1,
                "dateFrom": date_from.isoformat(),
                "dateTo": date_to.isoformat(),
                "daysOfWeek": days_of_week,
                "hourFrom": f"{hour_start:02d}:00",
                "hourTo": f"{hour_end:02d}:00",
                "selectedMoes": selected_moes,
                "eps": eps,
                "minSamples": min_samples,
            },
            "signal": {
                "id": signal_meta.id,
                "name": signal_meta.name,
                "route": signal_meta.route,
                "region": signal_meta.region,
                "lat": signal_meta.lat,
                "lon": signal_meta.lon,
            },
            "availablePhases": [],
            "hours": [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)],
            "days": days_of_week,
            "slots": [],
            "clusterSummaries": [],
            "changeDetections": [],
        }

    with sqlite3.connect(paths["sqlite_path"]) as conn:
        available_phases = _available_phase_list(conn)
        phase = pd.to_numeric(pd.Series([query.get("phase", [None])[0]]), errors="coerce").iloc[0]
        selected_phase = int(phase) if pd.notna(phase) and int(phase) in available_phases else (available_phases[0] if available_phases else 1)
        hourly_vectors = _build_bottleneck_hourly_vectors(
            conn,
            signal_id,
            selected_phase,
            date_from,
            date_to,
            hour_start,
            hour_end,
        )

    slot_vectors = _aggregate_bottleneck_slots(hourly_vectors, days_of_week, hour_start, hour_end)
    slot_vectors, used_moes, cluster_count, normalization = _run_dbscan_on_slots(slot_vectors, selected_moes, eps, min_samples)

    slot_rows: list[dict[str, Any]] = []
    for item in slot_vectors.itertuples():
        row = {
            "dayName": str(item.dayName),
            "hour": int(item.hour),
            "label": str(item.label),
            "sampleCount": int(item.sampleCount),
            "clusterId": None if pd.isna(item.clusterId) else int(item.clusterId),
            "clusterLabel": str(item.clusterLabel),
            "isOutlier": bool(item.isOutlier),
        }
        for key in BOTTLENECK_MOE_DEFINITIONS:
            value = getattr(item, key, None) if hasattr(item, key) else None
            row[key] = None if pd.isna(value) else round(float(value), 2)
        slot_rows.append(row)

    cluster_summaries: list[dict[str, Any]] = []
    clustered = slot_vectors[(slot_vectors["sampleCount"] > 0) & (slot_vectors["clusterId"].notna())].copy()
    clustered = clustered[clustered["clusterId"] >= 0]
    if not clustered.empty:
        baseline_stats: dict[str, dict[str, float]] = {}
        for key in used_moes:
            if key not in clustered.columns or not clustered[key].notna().any():
                continue
            mean_value = float(clustered[key].mean())
            std_value = float(clustered[key].std(ddof=0))
            baseline_stats[key] = {
                "mean": mean_value,
                "std": std_value if std_value > 0 else 0.0,
            }

        for cluster_id, frame in clustered.groupby("clusterId"):
            ordered_slots = [f"{row.dayName} {row.label}" for row in frame.itertuples()]
            hours = [int(row.hour) for row in frame.itertuples()]
            days = [str(row.dayName) for row in frame.itertuples()]
            z_scores: dict[str, float] = {}
            signature_parts: list[tuple[float, str]] = []
            summary = {
                "clusterId": int(cluster_id),
                "clusterLabel": f"Cluster {int(cluster_id) + 1}",
                "slotCount": int(len(frame)),
                "slots": ordered_slots,
                "slotsPreview": ordered_slots[:8],
                "timeBand": _cluster_time_band(hours),
                "dayPattern": _cluster_day_pattern(days),
            }
            for key in used_moes:
                if key not in frame.columns or not frame[key].notna().any():
                    summary[key] = None
                    continue
                cluster_mean = float(frame[key].mean())
                summary[key] = round(cluster_mean, 2)
                baseline = baseline_stats.get(key)
                if baseline is None:
                    continue
                baseline_std = baseline["std"]
                z_score = 0.0 if baseline_std == 0 else (cluster_mean - baseline["mean"]) / baseline_std
                z_scores[key] = z_score
                if abs(z_score) >= 0.7:
                    signature_parts.append((abs(z_score), _cluster_feature_phrase(key, z_score)))
            signature_parts.sort(key=lambda item: item[0], reverse=True)
            top_phrases = [phrase for _weight, phrase in signature_parts[:3]]
            if not top_phrases:
                relaxed_parts: list[tuple[float, str]] = []
                for key, z_score in z_scores.items():
                    if abs(z_score) >= 0.25:
                        relaxed_parts.append((abs(z_score), _cluster_feature_phrase(key, z_score)))
                relaxed_parts.sort(key=lambda item: item[0], reverse=True)
                top_phrases = [phrase for _weight, phrase in relaxed_parts[:2]]
            summary["performanceTag"] = _cluster_performance_tag(z_scores)
            summary["signature"] = ", ".join(top_phrases) if top_phrases else "Near the overall average profile"
            summary["summarySentence"] = (
                f'{summary["dayPattern"]} {summary["timeBand"].lower()} with {summary["signature"].lower()}.'
            )
            cluster_summaries.append(summary)
        cluster_summaries.sort(key=lambda item: item["clusterId"])

    change_detections = _build_bottleneck_change_detections(slot_vectors, used_moes)

    return {
        "meta": {
            "dataSource": "Precomputed ATSPM MOE results",
            "availableSignals": _historical_available_signals(),
            "moeOptions": [
                {"key": key, "label": BOTTLENECK_MOE_LABELS.get(key, key), "description": description}
                for key, description in BOTTLENECK_MOE_DEFINITIONS.items()
            ],
            "algorithm": {
                "name": "DBSCAN",
                "eps": eps,
                "minSamples": min_samples,
                "normalization": "Percentile min-max scaling using the 5th and 95th percentile of each MOE",
                "normalizationStats": normalization,
            },
        },
        "filters": {
            "signalId": signal_id,
            "phase": selected_phase,
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": f"{hour_start:02d}:00",
            "hourTo": f"{hour_end:02d}:00",
            "selectedMoes": selected_moes,
            "usedMoes": used_moes,
            "eps": eps,
            "minSamples": min_samples,
        },
        "signal": {
            "id": signal_meta.id,
            "name": signal_meta.name,
            "route": signal_meta.route,
            "region": signal_meta.region,
            "lat": signal_meta.lat,
            "lon": signal_meta.lon,
        },
        "availablePhases": available_phases,
        "hours": [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)],
        "days": days_of_week,
        "summary": {
            "slotCount": len(slot_rows),
            "slotsWithData": int(sum(1 for row in slot_rows if row["sampleCount"] > 0)),
            "clusterCount": cluster_count,
            "outlierCount": int(sum(1 for row in slot_rows if row["clusterId"] == -1)),
            "changeCount": len(change_detections),
        },
        "slots": slot_rows,
        "clusterSummaries": cluster_summaries,
        "changeDetections": change_detections,
    }


def build_hdbscan_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    if not available_dates:
        raise RuntimeError("No raw ATSPM files are available.")

    min_date = min(available_dates)
    max_date = max(available_dates)
    date_from = _safe_date(query.get("dateFrom", [None])[0], min_date)
    date_to = _safe_date(query.get("dateTo", [None])[0], max_date)
    if date_to < date_from:
        date_from, date_to = date_to, date_from

    days_of_week = DAY_NAMES
    hour_start = 6
    hour_end = 21
    selected_signal_ids = _parse_signal_id_list(query.get("signalIds", [None])[0])
    all_available_phases = list(range(1, 9))
    selected_phases = _parse_phase_id_list(query.get("phases", [None])[0], all_available_phases)
    selected_moes = _parse_bottleneck_moes(query.get("moes", [None])[0])
    min_cluster_size = _parse_int_range(query.get("minClusterSize", [None])[0], 6, 2, 40)
    min_samples = _parse_int_range(query.get("minSamples", [None])[0], 3, 1, 20)
    cluster_selection_epsilon = _parse_float_range(query.get("clusterSelectionEpsilon", [None])[0], 0.05, 0.0, 5.0)

    hourly_vectors, phase_map, available_phases = _build_multi_bottleneck_hourly_vectors(
        selected_signal_ids,
        selected_phases,
        date_from,
        date_to,
        hour_start,
        hour_end,
    )
    effective_phases = _parse_phase_id_list(
        ",".join(str(phase) for phase in selected_phases),
        available_phases or all_available_phases,
    )
    slot_vectors = _aggregate_bottleneck_slots(hourly_vectors, days_of_week, hour_start, hour_end)
    slot_vectors, used_moes, cluster_count, normalization = _run_hdbscan_on_slots(
        slot_vectors,
        selected_moes,
        min_cluster_size,
        min_samples,
        cluster_selection_epsilon,
    )

    slot_rows: list[dict[str, Any]] = []
    for item in slot_vectors.itertuples():
        row = {
            "dayName": str(item.dayName),
            "hour": int(item.hour),
            "label": str(item.label),
            "sampleCount": int(item.sampleCount),
            "clusterId": None if pd.isna(item.clusterId) else int(item.clusterId),
            "clusterLabel": str(item.clusterLabel),
            "isOutlier": bool(item.isOutlier),
        }
        for key in BOTTLENECK_MOE_DEFINITIONS:
            value = getattr(item, key, None) if hasattr(item, key) else None
            row[key] = None if pd.isna(value) else round(float(value), 2)
        slot_rows.append(row)

    cluster_summaries: list[dict[str, Any]] = []
    clustered = slot_vectors[(slot_vectors["sampleCount"] > 0) & (slot_vectors["clusterId"].notna())].copy()
    clustered = clustered[clustered["clusterId"] >= 0]
    if not clustered.empty:
        baseline_stats: dict[str, dict[str, float]] = {}
        for key in used_moes:
            if key not in clustered.columns or not clustered[key].notna().any():
                continue
            mean_value = float(clustered[key].mean())
            std_value = float(clustered[key].std(ddof=0))
            baseline_stats[key] = {
                "mean": mean_value,
                "std": std_value if std_value > 0 else 0.0,
            }

        for cluster_id, frame in clustered.groupby("clusterId"):
            ordered_slots = [f"{row.dayName} {row.label}" for row in frame.itertuples()]
            hours = [int(row.hour) for row in frame.itertuples()]
            days = [str(row.dayName) for row in frame.itertuples()]
            z_scores: dict[str, float] = {}
            signature_parts: list[tuple[float, str]] = []
            summary = {
                "clusterId": int(cluster_id),
                "clusterLabel": f"Cluster {int(cluster_id) + 1}",
                "slotCount": int(len(frame)),
                "slots": ordered_slots,
                "slotsPreview": ordered_slots[:8],
                "timeBand": _cluster_time_band(hours),
                "dayPattern": _cluster_day_pattern(days),
            }
            for key in used_moes:
                if key not in frame.columns or not frame[key].notna().any():
                    summary[key] = None
                    continue
                cluster_mean = float(frame[key].mean())
                summary[key] = round(cluster_mean, 2)
                baseline = baseline_stats.get(key)
                if baseline is None:
                    continue
                baseline_std = baseline["std"]
                z_score = 0.0 if baseline_std == 0 else (cluster_mean - baseline["mean"]) / baseline_std
                z_scores[key] = z_score
                if abs(z_score) >= 0.7:
                    signature_parts.append((abs(z_score), _cluster_feature_phrase(key, z_score)))
            signature_parts.sort(key=lambda item: item[0], reverse=True)
            top_phrases = [phrase for _weight, phrase in signature_parts[:3]]
            if not top_phrases:
                relaxed_parts: list[tuple[float, str]] = []
                for key, z_score in z_scores.items():
                    if abs(z_score) >= 0.25:
                        relaxed_parts.append((abs(z_score), _cluster_feature_phrase(key, z_score)))
                relaxed_parts.sort(key=lambda item: item[0], reverse=True)
                top_phrases = [phrase for _weight, phrase in relaxed_parts[:2]]
            summary["performanceTag"] = _cluster_performance_tag(z_scores)
            summary["signature"] = ", ".join(top_phrases) if top_phrases else "Near the overall average profile"
            summary["summarySentence"] = (
                f'{summary["dayPattern"]} {summary["timeBand"].lower()} with {summary["signature"].lower()}.'
            )
            cluster_summaries.append(summary)
        cluster_summaries.sort(key=lambda item: item["clusterId"])

    change_detections = _build_bottleneck_change_detections(slot_vectors, used_moes)
    selected_signals_meta = [INTERSECTION_LOOKUP[signal_id] for signal_id in selected_signal_ids if signal_id in INTERSECTION_LOOKUP]

    return {
        "meta": {
            "dataSource": "Precomputed ATSPM MOE results",
            "availableSignals": _historical_available_signals(),
            "selectionPresets": _signal_selection_presets(),
            "moeOptions": [
                {"key": key, "label": BOTTLENECK_MOE_LABELS.get(key, key), "description": description}
                for key, description in BOTTLENECK_MOE_DEFINITIONS.items()
            ],
            "algorithm": {
                "name": "HDBSCAN",
                "minClusterSize": min_cluster_size,
                "minSamples": min_samples,
                "clusterSelectionEpsilon": cluster_selection_epsilon,
                "normalization": "Percentile min-max scaling using the 5th and 95th percentile of each MOE",
                "normalizationStats": normalization,
            },
        },
        "filters": {
            "signalIds": selected_signal_ids,
            "phases": effective_phases,
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
            "daysOfWeek": days_of_week,
            "hourFrom": f"{hour_start:02d}:00",
            "hourTo": f"{hour_end:02d}:00",
            "selectedMoes": selected_moes,
            "usedMoes": used_moes,
            "minClusterSize": min_cluster_size,
            "minSamples": min_samples,
            "clusterSelectionEpsilon": cluster_selection_epsilon,
        },
        "selectedSignals": [
            {
                "id": meta.id,
                "name": meta.name,
                "route": meta.route,
                "region": meta.region,
                "lat": meta.lat,
                "lon": meta.lon,
            }
            for meta in selected_signals_meta
        ],
        "availablePhases": available_phases or all_available_phases,
        "phaseAvailability": phase_map,
        "hours": [f"{hour:02d}:00" for hour in range(hour_start, hour_end + 1)],
        "days": days_of_week,
        "summary": {
            "selectedSignalCount": len(selected_signals_meta),
            "selectedPhaseCount": len(effective_phases),
            "slotCount": len(slot_rows),
            "slotsWithData": int(sum(1 for row in slot_rows if row["sampleCount"] > 0)),
            "clusterCount": cluster_count,
            "outlierCount": int(sum(1 for row in slot_rows if row["clusterId"] == -1)),
            "changeCount": len(change_detections),
        },
        "slots": slot_rows,
        "clusterSummaries": cluster_summaries,
        "changeDetections": change_detections,
    }


class ApiHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # The React client aborts older requests when filters change quickly.
            pass

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({}, 204)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self._send_json({"ok": True})
            return
        if parsed.path == "/api/dashboard":
            self._send_json(build_dashboard_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/historical-dashboard":
            self._send_json(build_historical_dashboard_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/ranking":
            self._send_json(build_ranking_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/day-hour":
            self._send_json(build_day_hour_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/bottleneck-clusters":
            self._send_json(build_bottleneck_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/hdbscan-clusters":
            self._send_json(build_hdbscan_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/change-detection":
            self._send_json(build_change_detection_payload(parse_qs(parsed.query)))
            return
        if parsed.path == "/api/historical-run":
            self._send_json(build_historical_payload(parse_qs(parsed.query)))
            return
        self._send_json({"error": "Not found"}, 404)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8000), ApiHandler)
    print("ATSPM backend listening on http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
