from __future__ import annotations

import json
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "atspm" / "src"))

from atspm import SignalDataProcessor  # noqa: E402

RAW_PATTERN = re.compile(r"atspm-(\d{4})-(\d{1,2})-(\d{1,2})_filtered\.csv$")
MAPPING_PATH = ROOT / "MAPPINGS_DET_INFO_OCT_2022.csv"
HISTORICAL_OUTPUT_DIR = ROOT / "derived" / "dask_historical_1470_all_days"
HISTORICAL_DB_PATH = HISTORICAL_OUTPUT_DIR / "atspm_hourly_results.sqlite"
HISTORICAL_FILTERED_CSV = HISTORICAL_OUTPUT_DIR / "days"
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


@lru_cache(maxsize=1)
def _raw_files() -> dict[date, Path]:
    files: dict[date, Path] = {}
    for path in ROOT.glob("atspm-2024-10-*_filtered.csv"):
        parsed = _parse_file_date(path)
        if parsed:
            files[parsed] = path
    return dict(sorted(files.items()))


@lru_cache(maxsize=1)
def _mapping_frame() -> pd.DataFrame:
    mapping = pd.read_csv(MAPPING_PATH)
    mapping["DeviceId"] = pd.to_numeric(mapping["SIIA_ID"], errors="coerce")
    mapping["Phase"] = pd.to_numeric(mapping["phase"].astype(str).str.extract(r"(\d+)")[0], errors="coerce")
    mapping["Parameter"] = pd.to_numeric(mapping["channel"], errors="coerce")
    mapping["latitude"] = pd.to_numeric(mapping["latitude"], errors="coerce")
    mapping["longitude"] = pd.to_numeric(mapping["longitude"], errors="coerce")
    mapping["distanceToStopbar"] = pd.to_numeric(mapping["distanceToStopbar"], errors="coerce").fillna(0)
    mapping["route"] = (
        mapping.get("roadway_name", pd.Series(index=mapping.index, dtype="object"))
        .fillna(mapping.get("roadwayName", pd.Series(index=mapping.index, dtype="object")))
        .fillna("")
    )
    mapping["county"] = mapping["county"].fillna("Florida")
    mapping["Function"] = mapping["distanceToStopbar"].apply(lambda value: "Advance" if value > 0 else "Presence")
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
DEFAULT_SELECTED_IDS = [meta.id for meta in _intersection_inventory()[:3]]
MISSING_MAPPING_IDS = sorted(set(_available_signal_ids()) - {int(key) for key in INTERSECTION_LOOKUP})


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
    start_hour = _hour_from_string(hour_from)
    end_hour = _hour_from_string(hour_to)

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

            hour_series = chunk["TimeStamp"].dt.hour
            if start_hour <= end_hour:
                chunk = chunk[(hour_series >= start_hour) & (hour_series <= end_hour)]
            else:
                chunk = chunk[(hour_series >= start_hour) | (hour_series <= end_hour)]
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


def build_dashboard_payload(query: dict[str, list[str]]) -> dict[str, Any]:
    available_dates = list(_raw_files().keys())
    default_date = available_dates[-1]

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
            "dataSource": "October 2024 ATSPM event CSV files",
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
                {"id": "all", "label": "All signals", "intersectionIds": [meta.id for meta in _intersection_inventory()]},
                {"id": "first-three", "label": "First three", "intersectionIds": DEFAULT_SELECTED_IDS},
                {
                    "id": "volusia-sample",
                    "label": "Volusia sample",
                    "intersectionIds": [meta.id for meta in _intersection_inventory() if meta.region == "Volusia"][:3],
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


def build_historical_payload() -> dict[str, Any]:
    if not HISTORICAL_DB_PATH.exists():
        return {
            "exists": False,
            "title": "Signal 1470 historical ATSPM run",
            "message": "Run backend/historical_hourly_run_dask.py to generate the derived SQLite database.",
            "dbPath": str(HISTORICAL_DB_PATH),
            "outputDir": str(HISTORICAL_OUTPUT_DIR),
            "filteredCsvPath": str(HISTORICAL_FILTERED_CSV),
            "days": [],
            "hours": [],
            "tableCounts": [],
        }

    with sqlite3.connect(HISTORICAL_DB_PATH) as conn:
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
        "title": "Signal 1470 historical ATSPM run",
        "signalId": int(metadata.get("signal_id", 1470)),
        "runDate": "All available October days",
        "sourceCsv": "Multiple October ATSPM CSV files",
        "dbPath": str(HISTORICAL_DB_PATH),
        "outputDir": str(HISTORICAL_OUTPUT_DIR),
        "filteredCsvPath": str(HISTORICAL_FILTERED_CSV),
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
        if parsed.path == "/api/historical-run":
            self._send_json(build_historical_payload())
            return
        self._send_json({"error": "Not found"}, 404)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 8000), ApiHandler)
    print("ATSPM backend listening on http://127.0.0.1:8000")
    server.serve_forever()


if __name__ == "__main__":
    main()
