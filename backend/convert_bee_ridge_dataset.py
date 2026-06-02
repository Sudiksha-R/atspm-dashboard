from __future__ import annotations

import csv
import os
import re
import shutil
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
INTERSECTIONS_CSV = ROOT / "atspm" / "BeeRidgeIntersections.csv"
DEFAULT_OUTPUT_DIR = ROOT / "derived" / "bee_ridge_atspm_full"
RAW_HEADERS = ["signalID", "timeStamp", "eventCode", "eventParam"]
MEMBER_DAY_PATTERN = re.compile(r"TRAF_\d+_(\d{4})_(\d{2})_(\d{2})_\d{4}\.csv$", re.IGNORECASE)


def _output_dir() -> Path:
    raw = os.environ.get("BEE_RIDGE_OUTPUT_DIR")
    return Path(raw) if raw else DEFAULT_OUTPUT_DIR


def _work_dir(output_dir: Path) -> Path:
    return output_dir.parent / f"{output_dir.name}_work"


def _source_dirs() -> list[Path]:
    raw = os.environ.get("BEE_RIDGE_SOURCE_DIRS", "").strip()
    if not raw:
        return []

    parts = [Path(part.strip()) for part in raw.split(os.pathsep) if part.strip()]
    return parts


def _output_name(day_key: str) -> str:
    year, month, day = day_key.split("-")
    return f"atspm-{int(year)}-{int(month)}-{int(day)}_filtered.csv"


def _intersection_rows() -> list[dict[str, str]]:
    with INTERSECTIONS_CSV.open(newline="", encoding="utf-8", errors="ignore") as handle:
        return list(csv.DictReader(handle))


def _format_timestamp(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None
    try:
        date_part, time_part = text.split(" ", 1)
        month, day, year = [piece.strip() for piece in date_part.split("/")]
        base_time, dot, fraction = time_part.partition(".")
        micros = (fraction.strip() + "000000")[:6] if dot else "000000"
        return f"{int(month):02d}-{int(day):02d}-{year.strip()} {base_time.strip()}.{micros}"
    except ValueError:
        return None


def _parse_event_row(row: list[str]) -> tuple[str, int, int] | None:
    if len(row) < 3:
        return None

    timestamp_text = row[0].strip()
    event_code_text = row[1].strip()
    event_param_text = row[2].strip()
    if not timestamp_text or not event_code_text or not event_param_text:
        return None

    formatted_timestamp = _format_timestamp(timestamp_text)
    if formatted_timestamp is None:
        return None

    try:
        event_code = int(event_code_text)
        event_param = int(event_param_text)
    except ValueError:
        return None

    return formatted_timestamp, event_code, event_param


def _iter_csv_rows_from_zip(zip_file: Path):
    with ZipFile(zip_file) as archive:
        member_names = sorted(name for name in archive.namelist() if name.lower().endswith(".csv"))
        for member_name in member_names:
            match = MEMBER_DAY_PATTERN.search(Path(member_name).name)
            if not match:
                continue
            day_key = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
            with archive.open(member_name) as member:
                wrapper = TextIOWrapper(member, encoding="utf-8", errors="ignore", newline="")
                try:
                    for raw_line in wrapper:
                        yield day_key, raw_line.strip("\r\n").split(",")
                finally:
                    wrapper.detach()


def _iter_csv_rows_from_dir(source_dir: Path):
    csv_paths = sorted(path for path in source_dir.rglob("*.csv") if path.is_file())
    for csv_path in csv_paths:
        match = MEMBER_DAY_PATTERN.search(csv_path.name)
        if not match:
            continue
        day_key = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        with csv_path.open(encoding="utf-8", errors="ignore", newline="") as handle:
            for raw_line in handle:
                yield day_key, raw_line.strip("\r\n").split(",")


def _iter_source_rows(source_path: Path):
    if source_path.is_file() and source_path.suffix.lower() == ".zip":
        yield from _iter_csv_rows_from_zip(source_path)
        return

    if source_path.is_dir():
        yield from _iter_csv_rows_from_dir(source_path)
        return

    raise FileNotFoundError(f"Unsupported Bee Ridge source path: {source_path}")


def _convert_signal(signal_id: int, source_path: str, work_dir: str) -> tuple[int, dict[str, int]]:
    source = Path(source_path)
    signal_dir = Path(work_dir) / f"signal_{signal_id}"
    signal_dir.mkdir(parents=True, exist_ok=True)

    handles: dict[str, object] = {}
    writers: dict[str, csv.writer] = {}
    day_counts: dict[str, int] = defaultdict(int)

    try:
        for day_key, row in _iter_source_rows(source):
            parsed = _parse_event_row(row)
            if parsed is None:
                continue

            timestamp_text, event_code, event_param = parsed
            if day_key not in writers:
                output_path = signal_dir / _output_name(day_key)
                handle = output_path.open("w", newline="", encoding="utf-8")
                writer = csv.writer(handle)
                writer.writerow(RAW_HEADERS)
                handles[day_key] = handle
                writers[day_key] = writer

            writers[day_key].writerow([signal_id, timestamp_text, event_code, event_param])
            day_counts[day_key] += 1
    finally:
        for handle in handles.values():
            handle.close()

    return signal_id, dict(day_counts)


def _convert_signal_job(args: tuple[int, str, str]) -> tuple[int, dict[str, int]]:
    return _convert_signal(*args)


def _merge_signal_outputs(output_dir: Path, work_dir: Path) -> dict[str, int]:
    output_dir.mkdir(parents=True, exist_ok=True)
    for existing_path in output_dir.glob("atspm-*_filtered.csv"):
        existing_path.unlink()

    day_counts: dict[str, int] = defaultdict(int)
    signal_dirs = sorted(path for path in work_dir.iterdir() if path.is_dir())
    all_day_files = sorted({path.name for signal_dir in signal_dirs for path in signal_dir.glob("atspm-*_filtered.csv")})

    for day_name in all_day_files:
        final_path = output_dir / day_name
        with final_path.open("w", newline="", encoding="utf-8") as out_handle:
            writer = csv.writer(out_handle)
            writer.writerow(RAW_HEADERS)
            for signal_dir in signal_dirs:
                signal_path = signal_dir / day_name
                if not signal_path.exists():
                    continue
                with signal_path.open(newline="", encoding="utf-8", errors="ignore") as in_handle:
                    reader = csv.reader(in_handle)
                    next(reader, None)
                    for row in reader:
                        writer.writerow(row)
                        day_counts[day_name] += 1
    return dict(day_counts)


def main() -> None:
    if not INTERSECTIONS_CSV.exists():
        raise FileNotFoundError(f"Missing intersection metadata CSV: {INTERSECTIONS_CSV}")

    output_dir = _output_dir()
    work_dir = _work_dir(output_dir)
    shutil.rmtree(work_dir, ignore_errors=True)
    work_dir.mkdir(parents=True, exist_ok=True)

    intersections = _intersection_rows()
    source_dirs = _source_dirs()
    jobs: list[tuple[int, str, str]] = []
    for intersection in intersections:
        signal_id = int(intersection["ATMS_ID"])
        default_zip_path = ROOT / "atspm" / f"Ctrl0{signal_id}.zip"

        if default_zip_path.exists():
            jobs.append((signal_id, str(default_zip_path), str(work_dir)))
            continue

        matched_source_dir = None
        for source_dir in source_dirs:
            candidate = source_dir / f"Ctrl0{signal_id}"
            if candidate.exists():
                matched_source_dir = candidate
                break

        if matched_source_dir is None:
            searched = [str(default_zip_path)] + [str(source_dir / f"Ctrl0{signal_id}") for source_dir in source_dirs]
            raise FileNotFoundError(f"Missing Bee Ridge source for signal {signal_id}. Checked: {searched}")

        jobs.append((signal_id, str(matched_source_dir), str(work_dir)))

    max_workers = len(jobs) or 1
    per_signal_counts: dict[int, dict[str, int]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for signal_id, day_counts in pool.map(_convert_signal_job, jobs):
            per_signal_counts[signal_id] = day_counts

    merged_day_counts = _merge_signal_outputs(output_dir, work_dir)

    print(f"Wrote {len(merged_day_counts)} ATSPM-style day files to {output_dir}")
    for day_name in sorted(merged_day_counts):
        print(f"{day_name}: {merged_day_counts[day_name]} rows")
    print("Per-signal row counts:")
    for signal_id in sorted(per_signal_counts):
        print(f"{signal_id}: {sum(per_signal_counts[signal_id].values())} rows")


if __name__ == "__main__":
    main()
