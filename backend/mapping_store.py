from __future__ import annotations

import csv
import re
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
BEE_RIDGE_INTERSECTIONS_CSV = ROOT / "atspm" / "BeeRidgeIntersections.csv"
MAPPING_DB_PATH = ROOT / "derived" / "reference_data.sqlite"
NORMALIZED_COLUMNS = [
    "ATSPM_ID",
    "name",
    "latitude",
    "longitude",
    "roadway_name",
    "route",
    "county",
    "SIIA_ID",
    "DeviceId",
    "Phase",
    "Parameter",
    "Function",
    "laneType",
    "detectorID",
    "distanceToStopbar",
    "det_type",
    "source",
    "source_detail",
    "is_candidate_only",
]
DETECTOR_PHASE_PATTERN = re.compile(r"RDZ(\d+)", re.IGNORECASE)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _parse_location(value: str) -> tuple[float | None, float | None]:
    parts = [part.strip() for part in str(value).split(",")]
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def _load_intersections() -> pd.DataFrame:
    frame = pd.read_csv(BEE_RIDGE_INTERSECTIONS_CSV)
    frame["ATMS_ID"] = pd.to_numeric(frame["ATMS_ID"], errors="coerce")
    frame = frame.dropna(subset=["ATMS_ID"]).copy()
    frame["ATMS_ID"] = frame["ATMS_ID"].astype(int)
    frame["SIG_ID"] = pd.to_numeric(frame["SIG_ID"], errors="coerce")

    locations = frame["Location"].apply(_parse_location)
    frame["latitude"] = locations.apply(lambda item: item[0])
    frame["longitude"] = locations.apply(lambda item: item[1])
    frame["name"] = frame["MAJOR_STREET"].fillna("").str.strip() + " & " + frame["MINOR_STREET"].fillna("").str.strip()
    frame["roadway_name"] = frame["MAJOR_STREET"].fillna("").str.strip() + ", " + frame["MINOR_STREET"].fillna("").str.strip()
    frame["route"] = frame["roadway_name"]
    frame["county"] = "Sarasota"
    frame["ATSPM_ID"] = frame["ATMS_ID"].map(lambda value: f"SRQ-{value}")
    frame["mapping_path"] = frame["File"].map(lambda value: str(ROOT / "atspm" / str(value).strip()))
    return frame


def _intersection_reference(intersections: pd.DataFrame) -> pd.DataFrame:
    return intersections[
        [
            "SIG_ID",
            "ATMS_ID",
            "ATSPM_ID",
            "name",
            "MAJOR_STREET",
            "MINOR_STREET",
            "latitude",
            "longitude",
            "roadway_name",
            "route",
            "county",
            "Folder",
            "File",
            "mapping_path",
        ]
    ].rename(columns={"ATMS_ID": "DeviceId"}).copy()


def _lane_type_label(value: object) -> str | None:
    lookup = {
        "L": "Left",
        "T": "Through",
        "TR": "Through Right",
        "R": "Right",
    }
    text = str(value).strip().upper()
    if not text:
        return None
    return lookup.get(text, str(value).strip())


def _infer_phase(row: pd.Series) -> int | None:
    call_phase = pd.to_numeric(pd.Series([row.get("Call Phase")]), errors="coerce").iloc[0]
    if pd.notna(call_phase):
        return int(call_phase)

    detector_name = str(row.get("Detector", "")).strip()
    match = DETECTOR_PHASE_PATTERN.search(detector_name)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _function_label(row: pd.Series) -> str:
    distance = pd.to_numeric(pd.Series([row.get("Distance to Stop Bar (Ft)")]), errors="coerce").iloc[0]
    call_extend = str(row.get("Call/Extend", "")).strip().lower()
    detector_type = str(row.get("Detector Type", "")).strip().upper()
    if pd.notna(distance) and float(distance) > 0:
        return "Advance"
    if detector_type in {"DZ", "ACS/AS/DZ"} or call_extend == "extend":
        return "Advance"
    return "Presence"


def _candidate_mapping(intersections: pd.DataFrame) -> pd.DataFrame:
    metadata_by_id = {int(row.ATMS_ID): row for row in intersections.itertuples()}
    rows: list[dict[str, object]] = []

    for metadata in metadata_by_id.values():
        mapping_path = Path(metadata.mapping_path)
        if not mapping_path.exists():
            raise FileNotFoundError(f"Missing detector mapping CSV for signal {metadata.ATMS_ID}: {mapping_path}")

        with mapping_path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            for raw_row in csv.DictReader(handle):
                detector_id = pd.to_numeric(pd.Series([raw_row.get("Detector ID")]), errors="coerce").iloc[0]
                if pd.isna(detector_id):
                    continue
                detector_name = str(raw_row.get("Detector", "")).strip()
                if not detector_name:
                    continue

                distance = pd.to_numeric(
                    pd.Series([raw_row.get("Distance to Stop Bar (Ft)")]),
                    errors="coerce",
                ).iloc[0]
                phase = _infer_phase(pd.Series(raw_row))
                rows.append(
                    {
                        "ATSPM_ID": metadata.ATSPM_ID,
                        "name": metadata.name,
                        "latitude": metadata.latitude,
                        "longitude": metadata.longitude,
                        "roadway_name": metadata.roadway_name,
                        "route": metadata.route,
                        "county": metadata.county,
                        "SIIA_ID": metadata.ATMS_ID,
                        "DeviceId": metadata.ATMS_ID,
                        "Phase": phase,
                        "Parameter": int(detector_id),
                        "Function": _function_label(pd.Series(raw_row)),
                        "laneType": _lane_type_label(raw_row.get("Movement Type")),
                        "detectorID": detector_name,
                        "distanceToStopbar": None if pd.isna(distance) else float(distance),
                        "det_type": (
                            str(raw_row.get("Detector System", "")).strip()
                            + " "
                            + str(raw_row.get("Detector Type", "")).strip()
                        ).strip(),
                        "source": "bee_ridge_detector_csv",
                        "source_detail": mapping_path.name,
                        "is_candidate_only": 0 if phase is not None else 1,
                    }
                )

    return pd.DataFrame(rows, columns=NORMALIZED_COLUMNS)


def _effective_mapping(candidate: pd.DataFrame) -> pd.DataFrame:
    effective = candidate[candidate["Phase"].notna()].copy()
    if effective.empty:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)

    effective["DeviceId"] = pd.to_numeric(effective["DeviceId"], errors="coerce")
    effective["Phase"] = pd.to_numeric(effective["Phase"], errors="coerce")
    effective["Parameter"] = pd.to_numeric(effective["Parameter"], errors="coerce")
    effective = effective.dropna(subset=["DeviceId", "Phase", "Parameter"]).copy()
    effective["DeviceId"] = effective["DeviceId"].astype(int)
    effective["Phase"] = effective["Phase"].astype(int)
    effective["Parameter"] = effective["Parameter"].astype(int)
    effective["is_candidate_only"] = 0
    return effective[NORMALIZED_COLUMNS].drop_duplicates().reset_index(drop=True)


def _unmapped_rows(candidate: pd.DataFrame) -> pd.DataFrame:
    unmapped = candidate[candidate["Phase"].isna()].copy()
    if unmapped.empty:
        return pd.DataFrame(columns=NORMALIZED_COLUMNS)
    return unmapped[NORMALIZED_COLUMNS].reset_index(drop=True)


def _source_paths(intersections: pd.DataFrame) -> list[Path]:
    paths = [BEE_RIDGE_INTERSECTIONS_CSV]
    for mapping_path in intersections["mapping_path"].tolist():
        paths.append(Path(mapping_path))
    return paths


def _source_mtime(path: Path) -> float:
    return path.stat().st_mtime if path.exists() else 0.0


def ensure_mapping_store() -> Path:
    intersections = _load_intersections()
    source_paths = _source_paths(intersections)

    if MAPPING_DB_PATH.exists():
        db_mtime = MAPPING_DB_PATH.stat().st_mtime
        if MAPPING_DB_PATH.stat().st_size > 0 and db_mtime >= max(_source_mtime(path) for path in source_paths):
            try:
                with sqlite3.connect(MAPPING_DB_PATH) as conn:
                    existing_tables = {
                        row[0]
                        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
                    }
                if {
                    "bee_ridge_intersections",
                    "mapping_bee_ridge_candidate",
                    "mapping_bee_ridge_unmapped",
                    "mapping_effective",
                } <= existing_tables:
                    return MAPPING_DB_PATH
            except sqlite3.DatabaseError:
                pass
        MAPPING_DB_PATH.unlink(missing_ok=True)

    MAPPING_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    intersection_reference = _intersection_reference(intersections)
    candidate = _candidate_mapping(intersections)
    effective = _effective_mapping(candidate)
    unmapped = _unmapped_rows(candidate)

    with sqlite3.connect(MAPPING_DB_PATH) as conn:
        intersection_reference.to_sql("bee_ridge_intersections", conn, if_exists="replace", index=False)
        candidate.to_sql("mapping_bee_ridge_candidate", conn, if_exists="replace", index=False)
        unmapped.to_sql("mapping_bee_ridge_unmapped", conn, if_exists="replace", index=False)
        effective.to_sql("mapping_effective", conn, if_exists="replace", index=False)

    return MAPPING_DB_PATH


def load_effective_mapping() -> pd.DataFrame:
    db_path = ensure_mapping_store()
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query("SELECT * FROM mapping_effective", conn)
