from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, "/tmp/anomaly-ppt-deps")

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt


SOURCE = Path("Task5_Anomaly_Dashboard_Update_EDITABLE_FINAL.pptx")
OUTPUT = Path("/tmp/Task5_Anomaly_Dashboard_Update_EDITABLE_DETAILED.pptx")

NAVY, INK, GREEN = "102A2E", "163136", "176B57"
MINT, PALE, ORANGE = "DDF2EA", "F4F8F6", "E99745"
RED, MUTED, WHITE, BORDER = "C95A55", "607477", "FFFFFF", "D7E3DF"


def rgb(v): return RGBColor.from_string(v)


def box(slide, x, y, w, h, fill, rounded=True, line=None):
    shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE if rounded else MSO_SHAPE.RECTANGLE,
                                   Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid(); shape.fill.fore_color.rgb = rgb(fill)
    shape.line.color.rgb = rgb(line or fill)
    if rounded: shape.adjustments[0] = .1
    return shape


def text(slide, value, x, y, w, h, size=16, color=INK, bold=False,
         align=PP_ALIGN.LEFT, valign=MSO_ANCHOR.MIDDLE, margin=.02):
    shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = shape.text_frame; tf.clear(); tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(margin); tf.margin_top = tf.margin_bottom = 0
    tf.vertical_anchor = valign
    p = tf.paragraphs[0]; p.text = value; p.alignment = align
    p.font.name = "Aptos"; p.font.size = Pt(size); p.font.bold = bold; p.font.color.rgb = rgb(color)
    return shape


def rich_lines(slide, items, x, y, w, h, size=12, color=INK, bullet=True, spacing=8):
    shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    tf = shape.text_frame; tf.clear(); tf.word_wrap = True
    tf.margin_left = tf.margin_right = Inches(.03); tf.margin_top = tf.margin_bottom = 0
    for i, item in enumerate(items):
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = item; p.font.name = "Aptos"; p.font.size = Pt(size); p.font.color.rgb = rgb(color)
        p.space_after = Pt(spacing); p.level = 0
        if bullet: p.text = "•  " + item
    return shape


def header(slide, number, kicker, title, subtitle):
    box(slide, 0, 0, 13.333, 7.5, PALE, False)
    box(slide, 0, 0, .15, 7.5, GREEN, False)
    text(slide, kicker.upper(), .62, .28, 10.6, .28, 10, GREEN, True)
    text(slide, number, 12.25, .28, .47, .28, 13, GREEN, True, PP_ALIGN.RIGHT)
    text(slide, title, .62, .64, 12.05, .55, 25, NAVY, True)
    text(slide, subtitle, .63, 1.17, 12.0, .32, 11, MUTED)


def footer(slide):
    box(slide, .62, 7.08, 12.1, .012, BORDER, False)
    text(slide, "UF TRANSPORTATION INSTITUTE", .62, 7.13, 4, .18, 8, MUTED, True)
    text(slide, "TASK 5 • TECHNICAL UPDATE", 9.2, 7.13, 3.52, .18, 8, MUTED, True, PP_ALIGN.RIGHT)


def tag(slide, value, x, y, w, fill=GREEN):
    box(slide, x, y, w, .32, fill)
    text(slide, value, x+.08, y, w-.16, .32, 8, WHITE, True, PP_ALIGN.CENTER)


def numbered_item(slide, n, title, body, x, y, w, color=GREEN):
    box(slide, x, y, .42, .42, color)
    text(slide, str(n), x, y, .42, .42, 10, WHITE, True, PP_ALIGN.CENTER)
    text(slide, title, x+.58, y-.03, w-.58, .24, 10, color, True)
    text(slide, body, x+.58, y+.22, w-.58, .48, 10, INK)


def add_comparison(prs):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    header(s, "02", "Code Provenance", "Two distinct detectors, intentionally kept separate.",
           "They share ATSPM traffic data, but differ in time grain, baseline, scoring, and output.")
    box(s, .62, 1.72, 5.86, 4.85, NAVY)
    tag(s, "CODE 1", .92, 2.0, 1.1)
    text(s, "Shawn Strasser · traffic-anomaly 2.5.4", .92, 2.42, 4.95, .42, 18, WHITE, True)
    text(s, "Implemented in build_anomaly_store.py", .92, 2.84, 4.95, .26, 10, "A9CFC2")
    rich_lines(s, [
        "Input: hourly Phase 2 and Phase 6 volume",
        "Entity: one intersection-direction pair",
        "Expected value: 42-day rolling time-of-day model",
        "Event score: GEH; default threshold = 3.5",
        "Separate changepoint pass: 14-day window",
        "Feeds Corridor, Anomaly/Changepoints, and Ranking tabs",
    ], .92, 3.3, 5.0, 2.55, 11, WHITE, True, 10)
    box(s, 6.84, 1.72, 5.88, 4.85, WHITE, True, BORDER)
    tag(s, "CODE 2", 7.14, 2.0, 1.1, ORANGE)
    text(s, "Adapted from Yash · historic EOI", 7.14, 2.42, 5.0, .42, 18, NAVY, True)
    text(s, "Implemented in build_interruption_store.py", 7.14, 2.84, 5.0, .26, 10, MUTED)
    rich_lines(s, [
        "Input: 15-minute Phase 2 and Phase 6 volume",
        "Observed value: two-interval / 30-minute smoothing",
        "Expected value: same clock time 1, 2, and 3 weeks prior",
        "Requires ≥2 valid history values and expected volume ≥10",
        "Consecutive below-baseline points become one sustained event",
        "Feeds only the Interruptions / EOI tab",
    ], 7.14, 3.3, 5.0, 2.55, 11, INK, True, 10)
    footer(s)
    return s


def add_shawn(prs):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    header(s, "03", "Code 1 • Shawn Strasser traffic-anomaly", "How the hourly anomaly and changepoint pipeline works.",
           "The model is run offline; the dashboard reads the materialized DuckDB results.")
    steps = [
        ("EXTRACT", "Read arrival_on_green and sum Total_Actuations by date, hour, signal, and Phase 2/6."),
        ("DECOMPOSE", "Create a rolling expected volume using 42 days, 60-minute frequency, ≥168 samples, and ≥5 time-of-day samples."),
        ("SCORE", "Compare observed and predicted volume with GEH. Store signed score, absolute score, deviation, and deviation percent."),
        ("CLASSIFY", "Normal <3.5; Watch 3.5–<5; High 5–<7; Critical ≥7. The UI threshold is adjustable."),
        ("CHANGEPOINT", "Run a separate 14-day structural-shift model with score threshold 1.0, ≥3-day separation, and ≥30 samples."),
        ("MATERIALIZE", "Write observations, anomalies, changepoints, intersections, metadata, and time/entity indexes to anomaly.duckdb."),
    ]
    for i, (title_, body) in enumerate(steps):
        col, row = i % 2, i // 2
        x, y = .62 + col*6.15, 1.72 + row*1.52
        box(s, x, y, 5.84, 1.27, WHITE, True, BORDER)
        box(s, x+.18, y+.19, .5, .5, GREEN)
        text(s, f"{i+1:02}", x+.18, y+.19, .5, .5, 10, WHITE, True, PP_ALIGN.CENTER)
        text(s, title_, x+.85, y+.13, 4.65, .26, 9, GREEN, True)
        text(s, body, x+.85, y+.42, 4.66, .68, 10, INK)
    footer(s)
    return s


def add_yash(prs):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    header(s, "06", "Code 2 • Yash historic EOI adaptation", "How sustained interruptions are detected.",
           "This logic is implemented separately from traffic-anomaly and produces its own event store.")
    flow = [
        ("1", "15-MIN INPUT", "Phase 2/6 volume from moe_analytics.duckdb"),
        ("2", "SMOOTH", "2 consecutive intervals → 30-minute observed volume"),
        ("3", "BASELINE", "Average same time 1, 2, and 3 weeks earlier"),
        ("4", "QUALIFY POINT", "Observed < expected; ≥2 history days; expected ≥10"),
        ("5", "GROUP EVENT", "Join consecutive 15-minute drops for the same signal + phase"),
        ("6", "SUMMARIZE", "Duration, observed, expected, reduction %, and severity"),
    ]
    for i, (n, title_, body) in enumerate(flow):
        x = .62 + (i % 3)*4.08; y = 1.72 + (i // 3)*1.45
        box(s, x, y, 3.8, 1.2, WHITE, True, BORDER)
        box(s, x+.16, y+.2, .42, .42, ORANGE)
        text(s, n, x+.16, y+.2, .42, .42, 10, WHITE, True, PP_ALIGN.CENTER)
        text(s, title_, x+.75, y+.12, 2.75, .25, 9, ORANGE, True)
        text(s, body, x+.75, y+.4, 2.75, .63, 9.5, INK)
    box(s, .62, 4.82, 7.35, 1.72, NAVY)
    text(s, "Reduction calculation", .92, 5.06, 2.4, .28, 12, "8FD2BC", True)
    text(s, "100 × (expected − observed) ÷ expected", .92, 5.42, 6.3, .43, 20, WHITE, True)
    text(s, "Default UI qualification: duration ≥30 minutes and average reduction ≥30%", .92, 5.92, 6.3, .3, 10, "CBE3DB")
    box(s, 8.28, 4.82, 4.44, 1.72, WHITE, True, BORDER)
    text(s, "Stored severity", 8.58, 5.04, 2.0, .28, 12, GREEN, True)
    text(s, "Low  <30%", 8.58, 5.43, 1.7, .25, 10, MUTED, True)
    text(s, "Watch  30–<50%", 10.42, 5.43, 1.85, .25, 10, ORANGE, True)
    text(s, "High  50–<70%", 8.58, 5.85, 1.7, .25, 10, RED, True)
    text(s, "Critical  ≥70%", 10.42, 5.85, 1.85, .25, 10, RED, True)
    footer(s)
    return s


def add_tabs(prs):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    header(s, "08", "Dashboard Guide", "What each tab contains and which code powers it.",
           "All global filters update the relevant API queries; detector-specific controls stay within their tab.")
    rows = [
        ("CORRIDOR", "CODE 1", "Map + KPIs + top departures", "Colors intersections by severity; shows modeled intersections, anomaly count, maximum GEH, and top events."),
        ("ANOMALY & CHANGEPOINTS", "CODE 1", "Observed vs expected timeline", "Select a directional entity; inspect GEH anomalies, magnitude, event table, and persistent before/after shifts."),
        ("INTERSECTIONS RANKING", "CODE 1", "Corridor investigation queue", "Sorts intersections by anomaly count and lists the ten most recent directional GEH events; click through to timeline."),
        ("INTERRUPTIONS / EOI", "CODE 2", "Sustained low-volume events", "Filters phase, duration, and reduction; shows KPIs, selected-event chart, and events ranked by average reduction."),
        ("MOE EXPLORER", "SHARED DATA", "15-minute or hourly operational data", "Plots and tabulates volume, AOG/AOR, waits, occupancy, split failures, timing metrics, and other supported MOEs."),
    ]
    for i, (tab_, owner, purpose, detail) in enumerate(rows):
        y = 1.68 + i*1.03
        box(s, .62, y, 12.1, .83, WHITE, True, BORDER)
        text(s, tab_, .85, y+.08, 2.55, .23, 9, GREEN, True)
        tag(s, owner, 3.43, y+.1, 1.35, ORANGE if owner == "CODE 2" else GREEN)
        text(s, purpose, 5.02, y+.04, 2.55, .3, 11, NAVY, True)
        text(s, detail, 7.63, y+.04, 4.73, .68, 9.5, INK)
    footer(s)
    return s


def add_architecture(prs):
    s = prs.slides.add_slide(prs.slide_layouts[6])
    header(s, "10", "Implementation Architecture", "Precompute once; serve bounded reads to the browser.",
           "The detector models and large historical scans do not run inside the React client.")
    boxes = [
        (.62, 1.82, 2.35, 1.12, "SOURCE SQLITE", "Historical ATSPM outputs\nper intersection"),
        (3.42, 1.82, 2.35, 1.12, "OFFLINE PIPELINES", "Hourly anomaly + 15-min\nMOE/interruption builds"),
        (6.22, 1.82, 2.35, 1.12, "DUCKDB STORES", "anomaly · MOE ·\ninterruption events"),
        (9.02, 1.82, 2.35, 1.12, "FASTAPI", "Bounded, filtered\nread endpoints"),
    ]
    for i, (x,y,w,h,title_,body) in enumerate(boxes):
        box(s, x,y,w,h, NAVY if i in (1,2) else WHITE, True, BORDER)
        text(s,title_,x+.18,y+.14,w-.36,.23,9,"8FD2BC" if i in (1,2) else GREEN,True)
        text(s,body,x+.18,y+.43,w-.36,.53,10,WHITE if i in (1,2) else INK,True)
        if i < len(boxes)-1:
            text(s,"→",x+w+.12,y+.35,.32,.36,20,ORANGE,True,PP_ALIGN.CENTER)
    box(s, 1.08, 3.48, 11.15, 2.55, WHITE, True, BORDER)
    text(s, "Store and endpoint separation", 1.4, 3.76, 3.1, .32, 15, NAVY, True)
    matrix = [
        ("anomaly.duckdb", "/api/overview · /api/anomalies · /api/timeline · /api/changepoints", "Code 1 views"),
        ("interruption_events.duckdb", "/api/interruptions/meta · events · timeline/{event_id}", "Code 2 view"),
        ("moe_analytics.duckdb", "/api/moe/meta · observations · daily-summary", "MOE Explorer + Code 2 source"),
    ]
    for i,(store,api,use) in enumerate(matrix):
        y=4.3+i*.5
        text(s,store,1.4,y,2.6,.27,10,GREEN,True)
        text(s,api,4.1,y,5.55,.27,9,INK)
        text(s,use,10.0,y,1.78,.27,9,MUTED,True,PP_ALIGN.RIGHT)
    footer(s)
    return s


def renumber(prs):
    for index, slide in enumerate(prs.slides, 1):
        for shape in slide.shapes:
            if shape.has_text_frame and shape.top < Inches(.55):
                value = shape.text.strip()
                if re.fullmatch(r"\d{2}", value):
                    shape.text_frame.paragraphs[0].text = f"{index:02}"
                    p = shape.text_frame.paragraphs[0]
                    p.alignment = PP_ALIGN.RIGHT; p.font.name="Aptos"; p.font.size=Pt(13); p.font.bold=True; p.font.color.rgb=rgb(GREEN)


def main():
    prs = Presentation(SOURCE)
    if len(prs.slides) != 5:
        raise RuntimeError("Expected the user's five-slide EDITABLE_FINAL deck")
    if sum(1 for s in prs.slides for x in s.shapes if x.shape_type == 13) < 7:
        raise RuntimeError("Protected dashboard images are missing; refusing to continue")
    comparison = add_comparison(prs)
    shawn = add_shawn(prs)
    yash = add_yash(prs)
    tabs = add_tabs(prs)
    architecture = add_architecture(prs)
    original = list(prs.slides._sldIdLst[:5])
    added = list(prs.slides._sldIdLst[5:])
    order = [original[0], added[0], added[1], original[1], original[2], added[2], original[3], added[3], original[4], added[4]]
    for item in list(prs.slides._sldIdLst): prs.slides._sldIdLst.remove(item)
    for item in order: prs.slides._sldIdLst.append(item)
    renumber(prs)
    prs.core_properties.title = "Task 5 — Bee Ridge Anomaly Dashboard Technical Update"
    prs.core_properties.subject = "Editable deck with detector provenance, algorithms, and dashboard tab guide"
    prs.save(OUTPUT)
    checked = Presentation(OUTPUT)
    assert len(checked.slides) == 10
    assert sum(1 for s in checked.slides for x in s.shapes if x.shape_type == 13) >= 7
    assert all(sum(1 for x in s.shapes if x.has_text_frame) >= 8 for s in checked.slides)
    checked.save("/tmp/Task5_Anomaly_Dashboard_Update_EDITABLE_DETAILED_FINAL.pptx")
    print("Created 10-slide detailed editable deck; protected pictures retained")


if __name__ == "__main__": main()
