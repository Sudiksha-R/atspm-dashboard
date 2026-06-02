from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
CTRL01240_OVERLAY_PATH = ROOT / "derived" / "ctrl01240_atspm" / "detector_config_1240.csv"


def append_detector_config_overlays(mapping: pd.DataFrame, signal_ids: list[int] | tuple[int, ...] | set[int]) -> pd.DataFrame:
    frames = [mapping.copy()]

    if 1240 in signal_ids and CTRL01240_OVERLAY_PATH.exists():
        overlay = pd.read_csv(CTRL01240_OVERLAY_PATH)
        overlay = overlay[["DeviceId", "Phase", "Parameter", "Function"]].copy()
        overlay["DeviceId"] = pd.to_numeric(overlay["DeviceId"], errors="coerce").astype("Int64")
        overlay["Phase"] = pd.to_numeric(overlay["Phase"], errors="coerce").astype("Int64")
        overlay["Parameter"] = pd.to_numeric(overlay["Parameter"], errors="coerce").astype("Int64")
        overlay["Function"] = overlay["Function"].astype(str)
        overlay = overlay.dropna(subset=["DeviceId", "Phase", "Parameter", "Function"]).astype(
            {"DeviceId": int, "Phase": int, "Parameter": int}
        )
        frames.append(overlay)

    return pd.concat(frames, ignore_index=True).drop_duplicates().reset_index(drop=True)
