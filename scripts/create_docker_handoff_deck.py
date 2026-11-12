from __future__ import annotations

from pathlib import Path

from pptx import Presentation
from pptx.dml.color import RGBColor
from pptx.enum.shapes import MSO_SHAPE
from pptx.enum.text import MSO_ANCHOR, PP_ALIGN
from pptx.util import Inches, Pt


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "Bee_Ridge_Docker_Handoff_5_Slides.pptx"

WIDE_W = Inches(13.333)
WIDE_H = Inches(7.5)

INK = "172923"
MUTED = "60716B"
GREEN = "1D6654"
TEAL = "2C8B76"
NAVY = "27465A"
AMBER = "D9952B"
RED = "B54A47"
PALE_GREEN = "EAF4F0"
PALE_BLUE = "EAF1F5"
PALE_AMBER = "FBF2E3"
PALE_RED = "F8ECEB"
PAPER = "FFFFFF"
CANVAS = "F4F7F5"
RULE = "D7E0DC"


def rgb(value: str) -> RGBColor:
    return RGBColor.from_string(value)


def add_rect(slide, x, y, w, h, fill=PAPER, line=RULE, radius=False):
    shape_type = MSO_SHAPE.ROUNDED_RECTANGLE if radius else MSO_SHAPE.RECTANGLE
    shape = slide.shapes.add_shape(shape_type, Inches(x), Inches(y), Inches(w), Inches(h))
    shape.fill.solid()
    shape.fill.fore_color.rgb = rgb(fill)
    shape.line.color.rgb = rgb(line)
    shape.line.width = Pt(1)
    if radius:
        shape.adjustments[0] = 0.08
    return shape


def add_text(
    slide,
    text,
    x,
    y,
    w,
    h,
    size=18,
    color=INK,
    bold=False,
    align=PP_ALIGN.LEFT,
    valign=MSO_ANCHOR.TOP,
    font="Arial",
    margin=0,
):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = box.text_frame
    frame.clear()
    frame.margin_left = Inches(margin)
    frame.margin_right = Inches(margin)
    frame.margin_top = Inches(margin)
    frame.margin_bottom = Inches(margin)
    frame.vertical_anchor = valign
    paragraph = frame.paragraphs[0]
    paragraph.text = text
    paragraph.alignment = align
    paragraph.font.name = font
    paragraph.font.size = Pt(size)
    paragraph.font.bold = bold
    paragraph.font.color.rgb = rgb(color)
    return box


def add_rich_text(slide, runs, x, y, w, h, size=14, color=INK, margin=0.12):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = box.text_frame
    frame.clear()
    frame.word_wrap = True
    frame.margin_left = Inches(margin)
    frame.margin_right = Inches(margin)
    frame.margin_top = Inches(margin)
    frame.margin_bottom = Inches(margin)
    paragraph = frame.paragraphs[0]
    paragraph.space_after = Pt(0)
    for item in runs:
        run = paragraph.add_run()
        run.text = item[0]
        run.font.name = "Arial"
        run.font.size = Pt(item[2] if len(item) > 2 else size)
        run.font.bold = item[1]
        run.font.color.rgb = rgb(item[3] if len(item) > 3 else color)
    return box


def add_bullets(slide, items, x, y, w, h, size=15, color=INK, accent=GREEN, gap=8):
    box = slide.shapes.add_textbox(Inches(x), Inches(y), Inches(w), Inches(h))
    frame = box.text_frame
    frame.clear()
    frame.word_wrap = True
    frame.margin_left = Inches(0.03)
    frame.margin_right = Inches(0.03)
    frame.margin_top = 0
    frame.margin_bottom = 0
    for index, item in enumerate(items):
        paragraph = frame.paragraphs[0] if index == 0 else frame.add_paragraph()
        paragraph.text = item
        paragraph.level = 0
        paragraph.font.name = "Arial"
        paragraph.font.size = Pt(size)
        paragraph.font.color.rgb = rgb(color)
        paragraph.space_after = Pt(gap)
        paragraph.bullet = True
    marker = add_rect(slide, x - 0.17, y + 0.04, 0.05, min(h - 0.08, 0.28 * len(items)), fill=accent, line=accent)
    marker.line.fill.background()
    return box


def add_pill(slide, text, x, y, w, fill=PALE_GREEN, color=GREEN):
    shape = add_rect(slide, x, y, w, 0.34, fill=fill, line=fill, radius=True)
    add_text(slide, text, x, y + 0.01, w, 0.3, size=9, color=color, bold=True, align=PP_ALIGN.CENTER, valign=MSO_ANCHOR.MIDDLE)
    return shape


def add_title(slide, number, eyebrow, title, subtitle=None):
    add_rect(slide, 0, 0, 13.333, 0.12, fill=GREEN, line=GREEN)
    add_text(slide, f"0{number}", 0.48, 0.37, 0.55, 0.28, size=11, color=TEAL, bold=True)
    add_text(slide, eyebrow.upper(), 1.0, 0.37, 5.0, 0.28, size=11, color=MUTED, bold=True)
    add_text(slide, title, 0.48, 0.76, 12.15, 0.62, size=27, color=INK, bold=True)
    if subtitle:
        add_text(slide, subtitle, 0.5, 1.36, 12.0, 0.42, size=12.5, color=MUTED)


def add_footer(slide, number):
    add_rect(slide, 0.48, 7.18, 12.36, 0.012, fill=RULE, line=RULE)
    add_text(slide, "BEE RIDGE ATSPM DASHBOARD  /  DOCKER HANDOFF", 0.5, 7.23, 5.8, 0.2, size=8, color=MUTED, bold=True)
    add_text(slide, str(number), 12.2, 7.22, 0.6, 0.2, size=8, color=MUTED, bold=True, align=PP_ALIGN.RIGHT)


def add_arrow(slide, x, y, w=0.42, h=0.42, color=TEAL, rotation=0):
    arrow = slide.shapes.add_shape(MSO_SHAPE.CHEVRON, Inches(x), Inches(y), Inches(w), Inches(h))
    arrow.fill.solid()
    arrow.fill.fore_color.rgb = rgb(color)
    arrow.line.fill.background()
    arrow.rotation = rotation
    return arrow


def new_deck() -> Presentation:
    prs = Presentation()
    prs.slide_width = WIDE_W
    prs.slide_height = WIDE_H
    prs.core_properties.title = "Bee Ridge ATSPM Dashboard Docker Handoff"
    prs.core_properties.subject = "Docker architecture, containers, and recipient requirements"
    prs.core_properties.author = "Bee Ridge ATSPM Dashboard Team"
    prs.core_properties.keywords = "Docker, ATSPM, Bee Ridge, deployment, handoff"
    return prs


def slide_one(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = rgb(CANVAS)
    add_title(
        slide,
        1,
        "Purpose and approach",
        "Dockerizing the Bee Ridge ATSPM Dashboard",
        "A portable, repeatable deployment for the dashboard code while keeping large traffic datasets outside the images.",
    )

    add_rect(slide, 0.5, 2.02, 5.0, 4.72, fill=PAPER, line=RULE, radius=True)
    add_text(slide, "What Docker provides", 0.82, 2.31, 4.25, 0.35, size=20, bold=True)
    add_bullets(
        slide,
        [
            "Packages application code, runtime libraries, and configuration into versioned images.",
            "Starts isolated containers that behave consistently across laptops, servers, and cloud hosts.",
            "Uses Docker Compose to launch the frontend and backend together as one application.",
            "Separates replaceable application images from persistent Bee Ridge traffic data.",
        ],
        0.96,
        2.88,
        4.15,
        2.72,
        size=14,
        gap=13,
    )
    add_pill(slide, "CONSISTENT", 0.93, 6.02, 1.15)
    add_pill(slide, "PORTABLE", 2.18, 6.02, 1.0, fill=PALE_BLUE, color=NAVY)
    add_pill(slide, "REPEATABLE", 3.28, 6.02, 1.25, fill=PALE_AMBER, color=AMBER)

    add_text(slide, "How we use it", 6.02, 2.08, 2.5, 0.32, size=14, color=MUTED, bold=True)
    steps = [
        ("01", "Source", "React UI + Python API", PALE_BLUE, NAVY),
        ("02", "Images", "Dependencies + runtime", PALE_GREEN, GREEN),
        ("03", "Containers", "Two running services", PALE_AMBER, AMBER),
    ]
    y = 2.62
    for index, (num, heading, body, fill, tone) in enumerate(steps):
        add_rect(slide, 6.0, y, 5.98, 1.03, fill=fill, line=fill, radius=True)
        add_text(slide, num, 6.25, y + 0.27, 0.5, 0.3, size=11, color=tone, bold=True)
        add_text(slide, heading, 6.88, y + 0.18, 1.25, 0.28, size=15, color=INK, bold=True)
        add_text(slide, body, 8.35, y + 0.2, 3.15, 0.3, size=13, color=MUTED)
        if index < len(steps) - 1:
            add_arrow(slide, 8.6, y + 1.08, 0.42, 0.32, color=tone, rotation=90)
        y += 1.35
    add_rich_text(
        slide,
        [("Result: ", True, 13, GREEN), ("one command starts the complete dashboard environment.", False, 13, INK)],
        6.05,
        6.5,
        6.0,
        0.37,
        margin=0,
    )
    add_footer(slide, 1)


def slide_two(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = rgb(PAPER)
    add_title(
        slide,
        2,
        "System architecture",
        "Overall process flow",
        "Docker Compose creates a private network, starts the API first, and exposes only the nginx frontend to the user.",
    )
    add_pill(slide, "DOCKER COMPOSE ORCHESTRATION", 4.85, 1.86, 3.65, fill=PALE_GREEN, color=GREEN)

    y = 2.55
    add_rect(slide, 0.5, y, 2.1, 2.0, fill=PALE_BLUE, line="BED2DE", radius=True)
    add_text(slide, "USER", 0.82, y + 0.25, 1.45, 0.26, size=10, color=NAVY, bold=True)
    add_text(slide, "Web browser", 0.82, y + 0.63, 1.5, 0.35, size=19, color=INK, bold=True)
    add_text(slide, "http://host:4174", 0.82, y + 1.15, 1.55, 0.25, size=11, color=MUTED)
    add_text(slide, "HTTPS/domain optional", 0.82, y + 1.49, 1.55, 0.22, size=9.5, color=MUTED)

    add_arrow(slide, 2.83, y + 0.76, 0.62, 0.55, color=NAVY)
    add_text(slide, "HTTP", 2.77, y + 1.37, 0.72, 0.2, size=8, color=MUTED, bold=True, align=PP_ALIGN.CENTER)

    add_rect(slide, 3.68, y, 2.6, 2.0, fill=PALE_GREEN, line="BBD7CD", radius=True)
    add_text(slide, "FRONTEND CONTAINER", 3.98, y + 0.25, 1.95, 0.26, size=10, color=GREEN, bold=True)
    add_text(slide, "nginx + React", 3.98, y + 0.63, 2.0, 0.35, size=19, color=INK, bold=True)
    add_text(slide, "Host 4174 → container 80", 3.98, y + 1.15, 2.0, 0.25, size=10.5, color=MUTED)
    add_text(slide, "Serves UI and proxies /api/*", 3.98, y + 1.49, 2.0, 0.22, size=9.5, color=MUTED)

    add_arrow(slide, 6.52, y + 0.76, 0.62, 0.55, color=TEAL)
    add_text(slide, "/api/*", 6.47, y + 1.37, 0.72, 0.2, size=8, color=MUTED, bold=True, align=PP_ALIGN.CENTER)

    add_rect(slide, 7.37, y, 2.6, 2.0, fill=PALE_AMBER, line="E9C98F", radius=True)
    add_text(slide, "BACKEND CONTAINER", 7.67, y + 0.25, 1.95, 0.26, size=10, color=AMBER, bold=True)
    add_text(slide, "Python API", 7.67, y + 0.63, 2.0, 0.35, size=19, color=INK, bold=True)
    add_text(slide, "Internal port 8000", 7.67, y + 1.15, 2.0, 0.25, size=10.5, color=MUTED)
    add_text(slide, "Calculations + cached responses", 7.67, y + 1.49, 2.0, 0.22, size=9.5, color=MUTED)

    add_arrow(slide, 10.21, y + 0.76, 0.62, 0.55, color=AMBER)
    add_text(slide, "READ", 10.16, y + 1.37, 0.72, 0.2, size=8, color=MUTED, bold=True, align=PP_ALIGN.CENTER)

    add_rect(slide, 11.05, y, 1.78, 2.0, fill="F2F3F2", line=RULE, radius=True)
    add_text(slide, "HOST DATA", 11.29, y + 0.25, 1.3, 0.26, size=10, color=MUTED, bold=True)
    add_text(slide, "derived/", 11.29, y + 0.63, 1.3, 0.35, size=18, color=INK, bold=True)
    add_text(slide, "CSV + SQLite\nreference + cache", 11.29, y + 1.12, 1.3, 0.54, size=10, color=MUTED)

    add_rect(slide, 0.5, 5.0, 12.33, 1.54, fill=CANVAS, line=RULE, radius=True)
    add_text(slide, "STARTUP SEQUENCE", 0.78, 5.28, 1.65, 0.22, size=10, color=MUTED, bold=True)
    sequence = [
        ("1", "Build images"),
        ("2", "Mount ./derived"),
        ("3", "Start backend"),
        ("4", "Pass /api/health"),
        ("5", "Start frontend"),
    ]
    x = 2.55
    for idx, (num, label) in enumerate(sequence):
        add_rect(slide, x, 5.2, 1.68, 0.78, fill=PAPER, line=RULE, radius=True)
        add_text(slide, num, x + 0.12, 5.39, 0.3, 0.22, size=10, color=GREEN, bold=True, align=PP_ALIGN.CENTER)
        add_text(slide, label, x + 0.48, 5.36, 1.05, 0.28, size=10.5, color=INK, bold=True)
        if idx < len(sequence) - 1:
            add_arrow(slide, x + 1.79, 5.38, 0.28, 0.28, color=TEAL)
        x += 2.03
    add_text(slide, "External dependency: browser access to OpenStreetMap tiles unless map tiles are hosted internally.", 0.78, 6.15, 11.2, 0.24, size=10, color=MUTED)
    add_footer(slide, 2)


def slide_three(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = rgb(CANVAS)
    add_title(
        slide,
        3,
        "Container 1 of 2",
        "Backend image: Python API and traffic analytics",
        "Built from python:3.11-slim; reads mounted data and returns calculated dashboard payloads on internal port 8000.",
    )

    add_text(slide, "IMAGE LAYERS", 0.5, 1.98, 2.0, 0.25, size=10, color=MUTED, bold=True)
    layers = [
        ("Base runtime", "Python 3.11 slim + build tools", PALE_BLUE, NAVY),
        ("Python dependencies", "Pandas, NumPy, SciPy, Dask, PyArrow, DuckDB, scikit-learn", PALE_GREEN, GREEN),
        ("ATSPM package", "Local Shawn Strasser atspm source, SQL queries, and Bee Ridge mappings", PALE_AMBER, AMBER),
        ("Analysis modules", "Grouping, synchrony, bottlenecks, correlation, clustering, change detection", "F0EEF6", "64507C"),
        ("API application", "backend/server.py and historical processing helpers", PAPER, INK),
    ]
    y = 2.35
    for heading, body, fill, tone in layers:
        add_rect(slide, 0.5, y, 6.55, 0.72, fill=fill, line=fill, radius=True)
        add_text(slide, heading, 0.78, y + 0.17, 1.62, 0.24, size=12, color=tone, bold=True)
        add_text(slide, body, 2.5, y + 0.14, 4.15, 0.32, size=10.5, color=INK)
        y += 0.82

    add_rect(slide, 7.42, 2.0, 5.4, 2.17, fill=PAPER, line=RULE, radius=True)
    add_text(slide, "Runtime responsibilities", 7.75, 2.3, 4.75, 0.3, size=18, color=INK, bold=True)
    add_bullets(
        slide,
        [
            "Load CSV and SQLite data from /app/derived.",
            "Run and cache analytical calculations requested by each tab.",
            "Serve JSON endpoints such as /api/dashboard and /api/grouping-preview.",
            "Report readiness through /api/health before nginx starts.",
        ],
        7.92,
        2.82,
        4.4,
        1.12,
        size=10.5,
        gap=4,
        accent=AMBER,
    )

    add_rect(slide, 7.42, 4.43, 5.4, 2.03, fill=PALE_RED, line="E7C4C1", radius=True)
    add_text(slide, "Not stored in the image", 7.75, 4.73, 4.75, 0.3, size=18, color=RED, bold=True)
    add_bullets(
        slide,
        [
            "Raw Bee Ridge ATSPM CSV files",
            "Historical dask_historical_* SQLite outputs",
            "reference_data.sqlite and generated runtime caches",
        ],
        7.92,
        5.25,
        4.35,
        0.92,
        size=11.5,
        gap=7,
        accent=RED,
    )
    add_rich_text(
        slide,
        [("Why: ", True, 10.5, RED), ("data can be updated independently and does not inflate each image rebuild.", False, 10.5, MUTED)],
        7.75,
        6.17,
        4.6,
        0.26,
        margin=0,
    )
    add_footer(slide, 3)


def slide_four(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = rgb(PAPER)
    add_title(
        slide,
        4,
        "Container 2 of 2",
        "Frontend image: React build served by nginx",
        "A multi-stage image compiles the UI with Node, then ships only optimized static files and nginx in the final runtime.",
    )

    add_pill(slide, "BUILD STAGE", 0.62, 2.0, 1.25, fill=PALE_BLUE, color=NAVY)
    add_rect(slide, 0.5, 2.47, 5.28, 3.47, fill=PALE_BLUE, line="BED2DE", radius=True)
    add_text(slide, "node:20-alpine", 0.86, 2.8, 4.4, 0.38, size=21, color=NAVY, bold=True)
    add_text(slide, "What enters this stage", 0.86, 3.38, 4.3, 0.25, size=11, color=MUTED, bold=True)
    add_bullets(
        slide,
        [
            "package.json and locked npm dependencies",
            "React + TypeScript source and CSS",
            "Leaflet, Recharts, Plotly, and Vite tooling",
            "npm ci followed by npm run build",
        ],
        1.03,
        3.76,
        4.2,
        1.35,
        size=12,
        gap=7,
        accent=NAVY,
    )
    add_rect(slide, 0.86, 5.33, 4.5, 0.39, fill=PAPER, line="BED2DE", radius=True)
    add_text(slide, "OUTPUT  →  /app/dist (HTML, JS, CSS, assets)", 1.03, 5.42, 4.15, 0.18, size=9.5, color=NAVY, bold=True)

    add_arrow(slide, 6.15, 3.76, 0.7, 0.72, color=TEAL)
    add_text(slide, "COPY dist", 6.04, 4.58, 0.92, 0.2, size=8, color=MUTED, bold=True, align=PP_ALIGN.CENTER)

    add_pill(slide, "RUNTIME STAGE", 7.2, 2.0, 1.48, fill=PALE_GREEN, color=GREEN)
    add_rect(slide, 7.08, 2.47, 5.74, 3.47, fill=PALE_GREEN, line="BBD7CD", radius=True)
    add_text(slide, "nginx:1.27-alpine", 7.44, 2.8, 4.8, 0.38, size=21, color=GREEN, bold=True)
    add_text(slide, "What remains in the final image", 7.44, 3.38, 4.7, 0.25, size=11, color=MUTED, bold=True)
    add_bullets(
        slide,
        [
            "Compiled dashboard files only",
            "nginx web server and custom routing configuration",
            "SPA fallback to index.html for browser navigation",
            "Reverse proxy from /api/* to backend:8000",
        ],
        7.61,
        3.76,
        4.55,
        1.35,
        size=12,
        gap=7,
        accent=GREEN,
    )
    add_rect(slide, 7.44, 5.33, 4.96, 0.39, fill=PAPER, line="BBD7CD", radius=True)
    add_text(slide, "EXPOSED  →  host 4174 maps to nginx port 80", 7.62, 5.42, 4.58, 0.18, size=9.5, color=GREEN, bold=True)

    add_rect(slide, 0.5, 6.22, 12.32, 0.58, fill=PALE_AMBER, line="E9C98F", radius=True)
    add_rich_text(
        slide,
        [
            ("Pre-handoff correction: ", True, 11, AMBER),
            ("align the Docker build variable with the React key ", False, 11, INK),
            ("VITE_API_BASE_URL", True, 11, NAVY),
            (" so browser API calls reliably use nginx.", False, 11, INK),
        ],
        0.78,
        6.34,
        11.72,
        0.31,
        margin=0,
    )
    add_footer(slide, 4)


def checklist_column(slide, x, y, w, title, tone, fill, items):
    add_rect(slide, x, y, w, 2.0, fill=fill, line=fill, radius=True)
    add_text(slide, title, x + 0.27, y + 0.24, w - 0.54, 0.28, size=15, color=tone, bold=True)
    add_bullets(slide, items, x + 0.42, y + 0.68, w - 0.75, 1.08, size=10.4, gap=4, accent=tone)


def slide_five(prs):
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = rgb(CANVAS)
    add_title(
        slide,
        5,
        "Recipient discovery checklist",
        "What we need before deployment on their side",
        "These answers determine the image format, data package, network configuration, and operational handoff.",
    )

    checklist_column(
        slide,
        0.5,
        1.93,
        5.98,
        "1. Platform and capacity",
        NAVY,
        PALE_BLUE,
        [
            "Target OS and CPU architecture: x86_64 or ARM64",
            "Docker Engine/Desktop and Docker Compose v2 availability",
            "Available CPU, RAM, and disk; current derived/ tree is 35 GB",
            "Internet-connected build or air-gapped prebuilt image delivery",
        ],
    )
    checklist_column(
        slide,
        6.85,
        1.93,
        5.98,
        "2. Network and access",
        GREEN,
        PALE_GREEN,
        [
            "Deployment host, URL/domain, and whether port 4174 is allowed",
            "Reverse proxy, HTTPS certificate, firewall, or VPN requirements",
            "Browser access to OpenStreetMap tiles or an internal tile server",
            "Expected users, concurrent usage, and authentication requirements",
        ],
    )
    checklist_column(
        slide,
        0.5,
        4.17,
        5.98,
        "3. Data and security",
        AMBER,
        PALE_AMBER,
        [
            "Exact date range and intersections they need",
            "Raw CSVs, reference_data.sqlite, and historical SQLite outputs required",
            "Approved transfer method, persistent storage path, and permissions",
            "Data sensitivity, retention, backup, and deletion policies",
        ],
    )
    checklist_column(
        slide,
        6.85,
        4.17,
        5.98,
        "4. Operations and ownership",
        "72568C",
        "F0EEF6",
        [
            "Source bundle versus prebuilt image tar/registry delivery",
            "Who owns deployment, upgrades, logs, backups, and incident support",
            "How application and data updates will be delivered and versioned",
            "Whether the separate anomaly dashboard is in or out of scope",
        ],
    )

    add_rect(slide, 0.5, 6.43, 12.33, 0.48, fill=INK, line=INK, radius=True)
    add_rich_text(
        slide,
        [
            ("OUR FINAL READINESS TASKS  ", True, 9.5, "A9D6C8"),
            ("Fix API variable  •  define minimum data manifest  •  rebuild current bundle  •  run recipient-style acceptance test", False, 10.5, PAPER),
        ],
        0.78,
        6.52,
        11.8,
        0.25,
        margin=0,
    )
    add_footer(slide, 5)


def main():
    prs = new_deck()
    slide_one(prs)
    slide_two(prs)
    slide_three(prs)
    slide_four(prs)
    slide_five(prs)
    prs.save(OUTPUT)
    print(OUTPUT)


if __name__ == "__main__":
    main()
