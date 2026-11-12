from __future__ import annotations

import sys
import subprocess
import zipfile
from pathlib import Path

sys.path.insert(0, "/tmp/anomaly-ppt-deps")

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.util import Inches, Pt


SOURCE = Path("/tmp/Task5_Anomaly_Dashboard_Update.pptx")
ASSETS = Path("/tmp/anomaly_editable_assets")
OUTPUT = Path("/tmp/Task5_Anomaly_Dashboard_Update_EDITABLE.pptx")

NAVY = "102A2E"
INK = "163136"
GREEN = "176B57"
MINT = "DDF2EA"
PALE = "F4F8F6"
ORANGE = "E99745"
RED = "C95A55"
MUTED = "607477"
WHITE = "FFFFFF"
BORDER = "D7E3DF"


def rgb(value: str) -> RGBColor:
    return RGBColor.from_string(value)


def box(slide, x, y, w, h, fill, radius=True, line=None, line_width=1):
    kind = MSO_SHAPE.ROUNDED_RECTANGLE if radius else MSO_SHAPE.RECTANGLE
    shape = slide.shapes.add_shape(kind, Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid()
    shape.fill.fore_color.rgb = rgb(fill)
    shape.line.color.rgb = rgb(line or fill)
    shape.line.width = Pt(line_width)
    if radius:
        shape.adjustments[0] = 0.12
    return shape


def label(slide, value, x, y, w, h, size=18, color=INK, bold=False,
          align=PP_ALIGN.LEFT, font="Aptos", margin=0, valign=MSO_ANCHOR.MIDDLE):
    shape = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = shape.text_frame
    frame.clear()
    frame.margin_left = frame.margin_right = Inches(margin)
    frame.margin_top = frame.margin_bottom = 0
    frame.vertical_anchor = valign
    paragraph = frame.paragraphs[0]
    paragraph.text = value
    paragraph.alignment = align
    paragraph.font.name = font
    paragraph.font.size = Pt(size)
    paragraph.font.bold = bold
    paragraph.font.color.rgb = rgb(color)
    return shape


def header(slide, number, kicker, title, subtitle=""):
    box(slide, 0, 0, 13.333, 7.5, PALE, False)
    box(slide, 0, 0, 0.15, 7.5, GREEN, False)
    label(slide, kicker.upper(), 0.62, 0.28, 9.8, 0.28, 10, GREEN, True)
    label(slide, number, 12.3, 0.28, 0.42, 0.28, 13, GREEN, True, PP_ALIGN.RIGHT)
    label(slide, title, 0.62, 0.64, 12.05, 0.55, 25, NAVY, True, font="Aptos Display")
    if subtitle:
        label(slide, subtitle, 0.63, 1.17, 12.0, 0.32, 11, MUTED)


def footer(slide):
    line = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, Inches(0.62), Inches(7.08), Inches(12.1), Inches(0.012))
    line.fill.solid(); line.fill.fore_color.rgb = rgb(BORDER); line.line.fill.background()
    label(slide, "UF TRANSPORTATION INSTITUTE", 0.62, 7.13, 4.0, 0.18, 8, MUTED, True)
    label(slide, "TASK 5 • PROGRESS UPDATE", 9.2, 7.13, 3.52, 0.18, 8, MUTED, True, PP_ALIGN.RIGHT)


def picture_card(slide, image, x, y, w, h):
    box(slide, x, y, w, h, WHITE, True, BORDER)
    slide.shapes.add_picture(str(image), Inches(x + 0.08), Inches(y + 0.08),
                             width=Inches(w - 0.16), height=Inches(h - 0.16))


def pill(slide, value, x, y, w, fill=GREEN):
    box(slide, x, y, w, 0.32, fill, True)
    label(slide, value, x + 0.08, y, w - 0.16, 0.32, 8, WHITE, True, PP_ALIGN.CENTER)


def extract_assets():
    ASSETS.mkdir(exist_ok=True)
    with zipfile.ZipFile(SOURCE) as archive:
        for i in range(1, 6):
            data = archive.read(f"ppt/media/image{i}.jpg")
            image = ASSETS / f"image{i}.jpg"
            image.write_bytes(data)
            subprocess.run([
                "convert", str(image), "-resize", "90%", "-strip", "-interlace", "Plane",
                "-sampling-factor", "4:2:0", "-quality", "84", str(image)
            ], check=True)


def add_slide_1(prs, images):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "01", "Task 5 • Bee Ridge Anomaly Detection", "Same traffic data. Two implemented detection codes.",
           "Each code has its own result tabs and a distinct anomaly definition.")
    box(slide, 0.62, 1.77, 5.0, 4.55, NAVY, True)
    label(slide, "DETECTION STRATEGY", 0.94, 2.05, 4.2, 0.25, 9, "8FD2BC", True)
    label(slide, "Code 1 · traffic-anomaly", 0.94, 2.45, 4.2, 0.38, 18, WHITE, True)
    label(slide, "Hourly GEH anomalies + changepoints", 0.94, 2.83, 4.2, 0.3, 11, "CBE3DB")
    label(slide, "Tabs: Corridor • Anomaly and Changepoints\n• Intersections Ranking", 0.94, 3.2, 4.15, 0.65, 11, WHITE, True)
    label(slide, "Code 2 · Yash historic EOI", 0.94, 4.18, 4.2, 0.38, 18, WHITE, True)
    label(slide, "Sustained 15-minute volume reductions", 0.94, 4.56, 4.2, 0.3, 11, "CBE3DB")
    label(slide, "Tab: Interruptions / EOI", 0.94, 4.94, 4.15, 0.35, 11, WHITE, True)
    box(slide, 0.94, 5.48, 4.05, 0.52, "193F42", True)
    label(slide, "MOE Explorer = shared data view, not a detector", 1.08, 5.48, 3.77, 0.52, 10, WHITE, True)
    picture_card(slide, images[0], 5.95, 1.77, 6.77, 4.55)
    footer(slide)


def add_slide_2(prs, images):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "02", "Detector 1 • Hourly GEH", "Find unusual hours and persistent structural change.",
           "A 42-day time-of-day baseline makes different-volume signals comparable.")
    picture_card(slide, images[1], 0.62, 1.72, 12.1, 3.75)
    pill(slide, "CODE 1 RESULT · ANOMALY & CHANGEPOINTS", 9.0, 1.9, 3.35)
    steps = [
        ("01", "AGGREGATE", "Phase 2 + 6 volume", "Hourly direction series"),
        ("02", "MODEL", "Expected volume", "Rolling 42-day baseline"),
        ("03", "SCORE", "Observed vs expected", "GEH statistic"),
        ("04", "FLAG", "Hourly anomalies", "14-day structural shifts"),
    ]
    for i, (num, tag, top, bottom) in enumerate(steps):
        x = 0.62 + i * 3.08
        box(slide, x, 5.72, 2.82, 1.02, WHITE, True, BORDER)
        box(slide, x + 0.15, 5.89, 0.42, 0.42, GREEN, True)
        label(slide, num, x + 0.15, 5.89, 0.42, 0.42, 9, WHITE, True, PP_ALIGN.CENTER)
        label(slide, tag, x + 0.7, 5.83, 1.8, 0.2, 8, GREEN, True)
        label(slide, top, x + 0.7, 6.06, 1.95, 0.23, 10, NAVY, True)
        label(slide, bottom, x + 0.7, 6.31, 1.95, 0.2, 8, MUTED)
    footer(slide)


def add_slide_3(prs, images):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "03", "Detector 1 • Corridor Prioritization", "Turn anomaly volume into an investigation queue.",
           "Rank by absolute GEH severity, then use direction, time, and persistence for context.")
    picture_card(slide, images[2], 0.62, 1.72, 9.05, 4.98)
    pill(slide, "CODE 1 RESULT · RANKING", 0.82, 1.92, 2.75)
    box(slide, 9.98, 1.72, 2.74, 4.98, NAVY, True)
    label(slide, "READ THE OUTPUT", 10.27, 2.03, 2.15, 0.24, 9, "8FD2BC", True)
    items = [("COMPARE", "GEH normalizes", "across volumes"), ("RANK", "Absolute severity", "surfaces priorities"),
             ("INVESTIGATE", "Direction + time", "provide context")]
    for i, (tag, top, bottom) in enumerate(items):
        y = 2.62 + i * 1.08
        box(slide, 10.27, y, 0.32, 0.32, ORANGE, True)
        label(slide, tag, 10.78, y - 0.03, 1.58, 0.22, 8, "F1B875", True)
        label(slide, top, 10.78, y + 0.22, 1.62, 0.25, 11, WHITE, True)
        label(slide, bottom, 10.78, y + 0.49, 1.62, 0.22, 9, "CBE3DB")
    box(slide, 10.27, 5.7, 2.15, 0.6, "193F42", True)
    label(slide, "Decision support—not paging\non a single event", 10.38, 5.7, 1.93, 0.6, 9, WHITE, True, PP_ALIGN.CENTER)
    footer(slide)


def add_slide_4(prs, images):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "04", "Detector 2 • Sustained Interruptions", "Detect traffic loss that persists—not one noisy interval.",
           "Two-interval smoothing is compared with the same clock time 1, 2, and 3 weeks earlier.")
    picture_card(slide, images[3], 0.62, 1.72, 12.1, 3.86)
    pill(slide, "CODE 2 RESULT · INTERRUPTIONS / EOI", 9.03, 1.91, 3.32, ORANGE)
    # Editable event summary matching the added dashboard evidence.
    columns = [0.62, 4.55, 7.55, 8.55, 9.55, 10.55, 11.56]
    widths = [3.93, 3.0, 1.0, 1.0, 1.0, 1.01, 1.16]
    headers = ["INTERSECTION", "DIRECTION", "START", "DURATION", "OBSERVED", "EXPECTED", "REDUCTION"]
    for x, w, value in zip(columns, widths, headers):
        box(slide, x, 5.77, w, 0.28, MINT, False, MINT)
        label(slide, value, x + 0.06, 5.77, w - 0.12, 0.28, 6.5, MUTED, True)
    rows = [
        ["1262 · Cattlemen Rd", "Westbound", "May 15, 9 AM", "2h", "20.1", "48.6", "−58.3%"],
        ["1254 · McIntosh Rd", "Eastbound", "May 17, 1 PM", "1h 15m", "37.6", "67", "−43.4%"],
        ["1264 · I-75 SB Ramps", "Westbound", "May 17, 2 AM", "30 min", "8.6", "15", "−43.1%"],
    ]
    for r, row in enumerate(rows):
        y = 6.07 + r * 0.25
        for c, (x, w, value) in enumerate(zip(columns, widths, row)):
            label(slide, value, x + 0.06, y, w - 0.12, 0.22, 6.5, RED if c == 6 else INK, c in (0, 6))
    footer(slide)


def add_slide_5(prs, images):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    header(slide, "05", "Shared Underlying Data • MOE Explorer", "Inspect the same measurements used by both implemented codes.",
           "This is a data exploration view—not a third anomaly detector.")
    picture_card(slide, images[4], 0.62, 1.72, 12.1, 4.72)
    pill(slide, "SHARED SOURCE VIEW · MOE EXPLORER", 0.84, 1.92, 2.94)
    box(slide, 8.38, 5.76, 3.94, 0.46, MINT, True)
    label(slide, "Code 1 + Code 2 use the same underlying traffic data", 8.55, 5.76, 3.6, 0.46, 9, GREEN, True, PP_ALIGN.CENTER)
    footer(slide)


def main():
    extract_assets()
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)
    images = [ASSETS / f"image{i}.jpg" for i in range(1, 6)]
    add_slide_1(prs, images)
    add_slide_2(prs, images)
    add_slide_3(prs, images)
    add_slide_4(prs, images)
    add_slide_5(prs, images)
    prs.core_properties.title = "Task 5 — Bee Ridge Anomaly Dashboard Update"
    prs.core_properties.subject = "Editable native PowerPoint"
    prs.save(OUTPUT)
    check = Presentation(OUTPUT)
    assert len(check.slides) == 5
    assert all(len(slide.shapes) > 8 for slide in check.slides)
    check.save(Path("/tmp/Task5_Anomaly_Dashboard_Update_EDITABLE_roundtrip.pptx"))
    print(f"Created {OUTPUT} with {len(check.slides)} editable slides")


if __name__ == "__main__":
    main()
