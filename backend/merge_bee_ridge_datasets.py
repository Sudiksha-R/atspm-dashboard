from __future__ import annotations

import argparse
import csv
import shutil
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASE_DIR = ROOT / "derived" / "bee_ridge_atspm_v3"
DEFAULT_APPEND_DIR = ROOT / "derived" / "bee_ridge_atspm_april_may"
DEFAULT_OUTPUT_DIR = ROOT / "derived" / "bee_ridge_atspm_feb_to_may"
RAW_HEADERS = ["signalID", "timeStamp", "eventCode", "eventParam"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a new Bee Ridge dataset directory by merging an existing ATSPM-style dataset with new day files."
    )
    parser.add_argument("--base-dir", type=Path, default=DEFAULT_BASE_DIR, help="Existing dataset directory to copy from.")
    parser.add_argument(
        "--append-dir",
        type=Path,
        default=DEFAULT_APPEND_DIR,
        help="New dataset directory whose day files should be appended/overlaid.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Brand-new dataset directory to write.",
    )
    return parser.parse_args()


def _day_files(directory: Path) -> dict[str, Path]:
    return {path.name: path for path in sorted(directory.glob("atspm-*_filtered.csv")) if path.is_file()}


def _ensure_header(path: Path) -> None:
    with path.open(newline="", encoding="utf-8", errors="ignore") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
    if header != RAW_HEADERS:
        raise ValueError(f"Unexpected header in {path}: {header}")


def _copy_base(base_dir: Path, output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    shutil.copytree(base_dir, output_dir)


def _overlay_days(output_dir: Path, append_files: dict[str, Path]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for name, source_path in append_files.items():
        _ensure_header(source_path)
        target_path = output_dir / name
        shutil.copy2(source_path, target_path)
        with target_path.open(newline="", encoding="utf-8", errors="ignore") as handle:
            reader = csv.reader(handle)
            next(reader, None)
            for _ in reader:
                counts[name] += 1
    return dict(counts)


def main() -> None:
    args = _parse_args()
    base_dir = args.base_dir.resolve()
    append_dir = args.append_dir.resolve()
    output_dir = args.output_dir.resolve()

    if not base_dir.exists():
        raise FileNotFoundError(f"Base dataset directory not found: {base_dir}")
    if not append_dir.exists():
        raise FileNotFoundError(f"Append dataset directory not found: {append_dir}")

    base_files = _day_files(base_dir)
    append_files = _day_files(append_dir)

    if not base_files:
        raise FileNotFoundError(f"No ATSPM-style day files found in base dataset directory: {base_dir}")
    if not append_files:
        raise FileNotFoundError(f"No ATSPM-style day files found in append dataset directory: {append_dir}")

    _copy_base(base_dir, output_dir)
    appended_counts = _overlay_days(output_dir, append_files)

    merged_files = _day_files(output_dir)
    print(f"Created merged dataset at {output_dir}")
    print(f"Base files copied: {len(base_files)}")
    print(f"Files overlaid/appended from new dataset: {len(append_files)}")
    print(f"Total files in merged dataset: {len(merged_files)}")
    for name in sorted(appended_counts):
        print(f"{name}: {appended_counts[name]} rows")


if __name__ == "__main__":
    main()
