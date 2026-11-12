from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/tmp/anomaly-ppt-deps")
sys.path.insert(0, str(Path(__file__).resolve().parent))

from pptx import Presentation
from pptx.enum.text import PP_ALIGN

from expand_editable_anomaly_deck import (
    BORDER, GREEN, INK, MINT, MUTED, NAVY, ORANGE, PALE, RED, WHITE,
    box, footer, header, rich_lines, tag, text,
)


SOURCE = Path("/home/rajavaram.s/codex-recovery/Task5_Anomaly_Dashboard_Update_EDITABLE_DETAILED_FINAL.pptx")
OUTPUT = Path("/tmp/Task5_Anomaly_Dashboard_COMPLETE_UPDATE_FINAL.pptx")


def dashboard_snapshot(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "02", "Complete Dashboard Update", "The full Bee Ridge anomaly workspace is implemented as five connected views.",
           "Two distinct detectors and one shared MOE explorer are served from separate read-optimized stores.")

    cards = [
        ("5", "DASHBOARD TABS", "Corridor, anomaly timeline, ranking, interruptions, and MOE Explorer"),
        ("2", "DETECTION CODES", "Shawn traffic-anomaly + adapted Yash historic EOI"),
        ("3", "DUCKDB STORES", "Anomaly, interruption-event, and all-MOE analytical stores"),
        ("14", "MAPPED SIGNALS", "Bee Ridge intersections available to shared dashboard filters"),
    ]
    for i, (value, title_, body) in enumerate(cards):
        x = .62 + i * 3.08
        box(slide, x, 1.72, 2.82, 1.36, WHITE, True, BORDER)
        text(slide, value, x+.2, 1.89, .7, .42, 22, GREEN if i != 1 else ORANGE, True)
        text(slide, title_, x+.95, 1.87, 1.6, .25, 8, MUTED, True)
        text(slide, body, x+.2, 2.35, 2.4, .5, 9, INK)

    box(slide, .62, 3.38, 5.86, 3.15, NAVY, True)
    text(slide, "WHAT USERS CAN DO", .94, 3.66, 4.9, .28, 11, "8FD2BC", True)
    rich_lines(slide, [
        "Filter the shared analysis window by date, direction, GEH threshold, and selected intersections.",
        "Move from corridor-level severity to an intersection timeline and event detail.",
        "Compare observed traffic with modeled or historical expected traffic.",
        "Rank anomalies and sustained reductions to prioritize investigation.",
        "Explore 15-minute or hourly operational MOEs without rerunning either detector.",
    ], .94, 4.08, 4.95, 2.05, 10.5, WHITE, True, 10)

    box(slide, 6.84, 3.38, 5.88, 3.15, WHITE, True, BORDER)
    text(slide, "CURRENT IMPLEMENTATION", 7.16, 3.66, 4.9, .28, 11, GREEN, True)
    rich_lines(slide, [
        "The browser reads bounded FastAPI endpoints; detector models run offline during store builds.",
        "Global and tab-specific loading bands show when data requests are in progress.",
        "Code 1 uses hourly Phase 2/6 volume; Code 2 uses 15-minute Phase 2/6 volume.",
        "One mapped signal currently has no arrival_on_green history, so 13 signals are modeled by Code 1.",
        "The dashboard provides decision support; automated engineer paging is not implemented.",
    ], 7.16, 4.08, 4.95, 2.05, 10.5, INK, True, 10)
    footer(slide)
    return slide


def workflow(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "09", "End-to-End Dashboard Workflow", "How the five tabs work together during an investigation.",
           "Each step narrows the question while preserving the same selected date, direction, and intersections.")
    stages = [
        ("1", "CORRIDOR", "Scan the Bee Ridge map, KPI totals, severity colors, and highest departures.", GREEN),
        ("2", "RANKING", "Identify intersections with the most anomaly activity and inspect recent GEH events.", GREEN),
        ("3", "ANOMALY + CHANGEPOINTS", "Validate an event against expected volume and distinguish a spike from a persistent shift.", GREEN),
        ("4", "INTERRUPTIONS / EOI", "Check whether the same period contains a sustained low-volume event under Code 2.", ORANGE),
        ("5", "MOE EXPLORER", "Inspect raw operational context—volume, waits, occupancy, AOG/AOR, split failures, and timing.", GREEN),
    ]
    for i, (number, title_, body, color) in enumerate(stages):
        y = 1.68 + i * 1.03
        box(slide, .62, y, 12.1, .83, WHITE, True, BORDER)
        box(slide, .86, y+.18, .46, .46, color, True)
        text(slide, number, .86, y+.18, .46, .46, 10, WHITE, True, PP_ALIGN.CENTER)
        text(slide, title_, 1.56, y+.09, 2.3, .28, 10, color, True)
        text(slide, body, 3.95, y+.06, 7.98, .66, 10.5, INK)
    box(slide, 8.2, 6.82, 4.1, .25, MINT, True)
    text(slide, "Strongest evidence: a period supported by both detectors + MOE context", 8.32, 6.82, 3.86, .25, 8, GREEN, True, PP_ALIGN.CENTER)
    footer(slide)
    return slide


def yash_usage(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "08", "How Yash’s Code Was Used", "The historic EOI concept was adapted—not dropped into the dashboard unchanged.",
           "The Bee Ridge implementation preserves the historical-comparison idea and rebuilds it around this project’s data and UI.")
    box(slide, .62, 1.72, 5.86, 4.88, NAVY, True)
    tag(slide, "RETAINED CONCEPT", .92, 2.0, 1.72, GREEN)
    text(slide, "From yash-5595/interruptions-historic-EOI", .92, 2.45, 4.95, .38, 17, WHITE, True)
    rich_lines(slide, [
        "Treat a meaningful traffic interruption as a reduction relative to historical traffic.",
        "Compare the current interval with comparable prior periods instead of using one fixed corridor-wide threshold.",
        "Represent the result as an Event of Interest that an analyst can investigate.",
    ], .92, 3.08, 4.95, 2.2, 11, WHITE, True, 14)
    text(slide, "This is the second detection approach—not part of Shawn’s GEH model.", .92, 5.72, 4.95, .42, 10, "A9CFC2", True)

    box(slide, 6.84, 1.72, 5.88, 4.88, WHITE, True, BORDER)
    tag(slide, "ADAPTED FOR BEE RIDGE", 7.14, 2.0, 2.05, ORANGE)
    text(slide, "What was implemented in this project", 7.14, 2.45, 5.0, .38, 17, NAVY, True)
    rich_lines(slide, [
        "Use the project’s MOE DuckDB store as the source instead of the repository’s original data path.",
        "Analyze 15-minute Phase 2 eastbound and Phase 6 westbound volume for every mapped signal.",
        "Smooth two intervals; compare the same clock time 1, 2, and 3 weeks earlier; require at least two valid values.",
        "Group consecutive drops into events and calculate duration, expected/observed volume, reduction, and severity.",
        "Materialize interruption_events.duckdb, expose FastAPI endpoints, and build the separate Interruptions / EOI tab.",
    ], 7.14, 3.02, 5.02, 2.85, 10.5, INK, True, 10)
    footer(slide)
    return slide


def renumber(prs):
    for index, slide in enumerate(prs.slides, 1):
        for shape in slide.shapes:
            if shape.has_text_frame and shape.top < 500000 and shape.text.strip().isdigit() and len(shape.text.strip()) == 2:
                shape.text = f"{index:02}"


def main():
    prs = Presentation(SOURCE)
    if len(prs.slides) != 10:
        raise RuntimeError("Expected the 10-slide detailed source deck")
    original_picture_count = sum(1 for s in prs.slides for x in s.shapes if x.shape_type == 13)
    dashboard_snapshot(prs)
    workflow(prs)
    yash_usage(prs)
    original = list(prs.slides._sldIdLst[:10])
    added = list(prs.slides._sldIdLst[10:])
    # Executive update after title; Yash attribution after its algorithm; workflow before tab guide.
    order = [original[0], added[0], *original[1:6], added[2], original[6], added[1], *original[7:]]
    for item in list(prs.slides._sldIdLst): prs.slides._sldIdLst.remove(item)
    for item in order: prs.slides._sldIdLst.append(item)
    renumber(prs)
    prs.core_properties.title = "Task 5 — Complete Bee Ridge Anomaly Dashboard Update"
    prs.save(OUTPUT)
    checked = Presentation(OUTPUT)
    assert len(checked.slides) == 13
    assert sum(1 for s in checked.slides for x in s.shapes if x.shape_type == 13) == original_picture_count
    checked.save("/tmp/Task5_Anomaly_Dashboard_COMPLETE_UPDATE_roundtrip.pptx")
    print(f"Created {len(checked.slides)} slides; preserved {original_picture_count} pictures")


if __name__ == "__main__": main()
