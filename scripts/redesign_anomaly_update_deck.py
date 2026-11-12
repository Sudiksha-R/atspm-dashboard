from __future__ import annotations

import base64
import html
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "Task5_Anomaly_Dashboard_Update.pptx"
OUTPUT = Path("/tmp/Task5_Anomaly_Dashboard_Update_Redesigned.pptx")
PREVIEW_DIR = Path("/tmp/Task5_Anomaly_Dashboard_Update_Redesigned_preview")
W, H = 1600, 900

NAVY = "#102A2E"
INK = "#163136"
GREEN = "#176B57"
MINT = "#DDF2EA"
PALE = "#F4F8F6"
ORANGE = "#E99745"
RED = "#C95A55"
BLUE = "#3978A8"
MUTED = "#607477"
WHITE = "#FFFFFF"


def image_uri(path: Path) -> str:
    encoded = base64.b64encode(path.read_bytes()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"


def text(x: int, y: int, value: str, size: int, color: str = INK, weight: int = 400,
         anchor: str = "start", tracking: int = 0) -> str:
    return (
        f'<text x="{x}" y="{y}" fill="{color}" font-family="Liberation Sans, Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" letter-spacing="{tracking}px">'
        f'{html.escape(value)}</text>'
    )


def multiline(x: int, y: int, lines: list[str], size: int, color: str = INK,
              weight: int = 400, line_height: int | None = None) -> str:
    line_height = line_height or int(size * 1.3)
    spans = "".join(
        f'<tspan x="{x}" dy="{0 if i == 0 else line_height}">{html.escape(line)}</tspan>'
        for i, line in enumerate(lines)
    )
    return (
        f'<text x="{x}" y="{y}" fill="{color}" font-family="Liberation Sans, Arial, sans-serif" '
        f'font-size="{size}" font-weight="{weight}">{spans}</text>'
    )


def rounded_rect(x: int, y: int, w: int, h: int, fill: str, radius: int = 24,
                 stroke: str = "none", stroke_width: int = 0) -> str:
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}"/>'


def header(number: str, kicker: str, title: str, subtitle: str = "") -> str:
    parts = [
        rounded_rect(0, 0, W, H, PALE, 0),
        f'<rect x="0" y="0" width="18" height="900" fill="{GREEN}"/>',
        text(74, 64, kicker.upper(), 17, GREEN, 700, tracking=2),
        text(1518, 64, number, 22, GREEN, 700, "end"),
        text(74, 130, title, 44, NAVY, 700),
    ]
    if subtitle:
        parts.append(text(76, 172, subtitle, 20, MUTED, 400))
    return "".join(parts)


def footer() -> str:
    return (
        f'<line x1="74" y1="852" x2="1526" y2="852" stroke="#D7E3DF" stroke-width="2"/>'
        + text(74, 880, "UF TRANSPORTATION INSTITUTE", 14, MUTED, 700, tracking=1)
        + text(1526, 880, "TASK 5 • PROGRESS UPDATE", 14, MUTED, 700, "end", tracking=1)
    )


def screenshot_card(uri: str, x: int, y: int, w: int, h: int, crop: str = "xMidYMid meet") -> str:
    return (
        f'<filter id="shadow"><feDropShadow dx="0" dy="10" stdDeviation="12" flood-color="#102A2E" flood-opacity="0.16"/></filter>'
        + rounded_rect(x, y, w, h, WHITE, 26, "#D7E3DF", 2).replace('/>', ' filter="url(#shadow)"/>')
        + f'<clipPath id="shotclip"><rect x="{x+12}" y="{y+12}" width="{w-24}" height="{h-24}" rx="16"/></clipPath>'
        + f'<image href="{uri}" x="{x+12}" y="{y+12}" width="{w-24}" height="{h-24}" preserveAspectRatio="{crop}" clip-path="url(#shotclip)"/>'
    )


def slide_1(images: list[str]) -> str:
    body = header("01", "Task 5 • Bee Ridge Anomaly Detection", "Same traffic data. Two implemented detection codes.",
                  "Each code has its own dashboard result tabs and a distinct anomaly definition.")
    body += screenshot_card(images[0], 720, 220, 806, 548)
    body += rounded_rect(74, 224, 594, 542, NAVY, 30)
    body += text(112, 276, "DETECTION STRATEGY", 16, "#8FD2BC", 700, tracking=2)
    body += text(112, 332, "Code 1 · traffic-anomaly", 29, WHITE, 700)
    body += text(112, 368, "Hourly GEH anomalies + changepoints", 18, "#CBE3DB", 400)
    body += f'<line x1="112" y1="405" x2="624" y2="405" stroke="#315A5E" stroke-width="2"/>'
    body += text(112, 460, "Code 2 · Yash historic EOI", 29, WHITE, 700)
    body += text(112, 496, "15-minute sustained volume reductions", 18, "#CBE3DB", 400)
    body += rounded_rect(112, 550, 512, 164, "#193F42", 22)
    body += text(140, 592, "IMPLEMENTED AS SEPARATE VIEWS", 14, ORANGE, 700, tracking=1)
    body += multiline(140, 635, ["Code 1 results and Code 2 results are", "shown in different dashboard tabs."], 22, WHITE, 700, 31)
    body += footer()
    return body


def slide_2(images: list[str]) -> str:
    body = header("02", "Detector 1 • Hourly GEH", "Find unusual hours and persistent structural change.",
                  "A 42-day time-of-day baseline makes different-volume signals comparable.")
    body += screenshot_card(images[1], 74, 215, 1452, 445)
    body += rounded_rect(1080, 236, 408, 42, GREEN, 12)
    body += text(1284, 263, "CODE 1 RESULT · ANOMALY & CHANGEPOINTS", 14, WHITE, 700, "middle", tracking=1)
    steps = [
        ("01", "AGGREGATE", "Phase 2 + 6 volume", "into hourly direction series"),
        ("02", "MODEL", "Expected volume", "from a rolling 42-day baseline"),
        ("03", "SCORE", "Observed vs expected", "with the GEH statistic"),
        ("04", "FLAG", "Hourly anomalies", "and 14-day structural shifts"),
    ]
    for i, (num, label, top, bottom) in enumerate(steps):
        x = 74 + i * 369
        body += rounded_rect(x, 690, 340, 126, WHITE, 20, "#D7E3DF", 2)
        body += rounded_rect(x+18, 710, 48, 48, GREEN, 14)
        body += text(x+42, 742, num, 16, WHITE, 700, "middle")
        body += text(x+84, 726, label, 13, GREEN, 700, tracking=1)
        body += text(x+84, 754, top, 18, NAVY, 700)
        body += text(x+84, 782, bottom, 15, MUTED, 400)
    body += footer()
    return body


def slide_3(images: list[str]) -> str:
    body = header("03", "Detector 1 • Corridor Prioritization", "Turn anomaly volume into an investigation queue.",
                  "Rank by absolute GEH severity—then use direction, time, and persistence for context.")
    body += screenshot_card(images[2], 74, 215, 1090, 592)
    body += rounded_rect(96, 236, 350, 42, GREEN, 12)
    body += text(271, 263, "CODE 1 RESULT · RANKING", 14, WHITE, 700, "middle", tracking=1)
    body += rounded_rect(1200, 215, 326, 592, NAVY, 26)
    body += text(1234, 262, "READ THE OUTPUT", 15, "#8FD2BC", 700, tracking=2)
    callouts = [
        ("COMPARE", "GEH normalizes", "across different volumes"),
        ("RANK", "Absolute severity", "surfaces priority signals"),
        ("INVESTIGATE", "Direction + timestamp", "provide operational context"),
    ]
    for i, (tag, top, bottom) in enumerate(callouts):
        y = 310 + i * 132
        body += f'<circle cx="1248" cy="{y}" r="18" fill="{ORANGE}"/>'
        body += text(1282, y-5, tag, 13, "#F1B875", 700, tracking=1)
        body += text(1282, y+27, top, 19, WHITE, 700)
        body += text(1282, y+52, bottom, 16, "#CBE3DB", 400)
    body += rounded_rect(1234, 704, 258, 66, "#193F42", 16)
    body += text(1363, 732, "Decision support—not paging", 15, WHITE, 700, "middle")
    body += text(1363, 754, "on a single event", 14, "#CBE3DB", 400, "middle")
    body += footer()
    return body


def slide_4(images: list[str]) -> str:
    body = header("04", "Detector 2 • Sustained Interruptions", "Detect traffic loss that persists—not a single noisy interval.",
                  "Two-interval smoothing is compared with the same clock time 1, 2, and 3 weeks earlier.")
    body += screenshot_card(images[3], 74, 215, 1452, 490)
    body += rounded_rect(1084, 236, 404, 42, ORANGE, 12)
    body += text(1286, 263, "CODE 2 RESULT · INTERRUPTIONS / EOI", 14, WHITE, 700, "middle", tracking=1)
    stats = [("21", "EVENTS"), ("11", "INTERSECTIONS"), ("3 h 30 m", "LONGEST EVENT"), ("58.3%", "LARGEST REDUCTION")]
    for i, (value, label) in enumerate(stats):
        x = 74 + i * 369
        body += rounded_rect(x, 730, 340, 86, WHITE, 18, "#D7E3DF", 2)
        body += text(x+24, 770, value, 27, GREEN if i < 2 else RED, 700)
        body += text(x+24, 798, label, 13, MUTED, 700, tracking=1)
    body += footer()
    return body


def slide_5(images: list[str]) -> str:
    body = header("05", "Implemented Dashboard Structure", "Which result belongs to which code?",
                  "The dashboard separates the two detection methods while keeping the source data consistent.")
    body += rounded_rect(74, 220, 700, 500, NAVY, 28)
    body += text(112, 270, "CODE 1", 16, "#8FD2BC", 700, tracking=2)
    body += text(112, 316, "Shawn Strasser traffic-anomaly", 28, WHITE, 700)
    body += text(112, 357, "Detects hourly GEH departures and changepoints.", 19, "#CBE3DB", 400)
    for i, label in enumerate(["Corridor", "Anomaly and Changepoints", "Intersections Ranking"]):
        y = 430 + i * 72
        body += rounded_rect(112, y-30, 42, 42, GREEN, 12)
        body += text(133, y-2, str(i+1), 15, WHITE, 700, "middle")
        body += text(176, y, label, 21, WHITE, 700)
    body += rounded_rect(826, 220, 700, 500, WHITE, 28, "#D7E3DF", 2)
    body += text(864, 270, "CODE 2", 16, ORANGE, 700, tracking=2)
    body += text(864, 316, "Adapted from Yash's historic EOI repo", 28, NAVY, 700)
    body += multiline(864, 357, ["Detects sustained 15-minute volume reductions", "against the same time in the prior 1–3 weeks."], 19, MUTED, 400, 29)
    body += rounded_rect(864, 450, 624, 82, "#FFF1E5", 18, "#F0BE8B", 2)
    body += text(896, 484, "SEPARATE RESULT TAB", 13, ORANGE, 700, tracking=1)
    body += text(896, 516, "Interruptions / EOI", 23, NAVY, 700)
    body += rounded_rect(864, 568, 624, 104, MINT, 18)
    body += text(896, 602, "SHARED SOURCE", 13, GREEN, 700, tracking=1)
    body += multiline(896, 635, ["MOE Explorer displays the underlying data.", "It is not a third detection code."], 18, NAVY, 700, 27)
    body += footer()
    return body


def wrap_svg(body: str) -> str:
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}">{body}</svg>'''


def slide_xml(original: str) -> str:
    # Preserve the complete, known-good PowerPoint slide structure and place
    # the redesigned artwork last in the shape tree so it covers the old design.
    overlay = '''<p:pic><p:nvPicPr><p:cNvPr id="99" name="Redesigned slide artwork"/><p:cNvPicPr><a:picLocks noChangeAspect="1"/></p:cNvPicPr><p:nvPr/></p:nvPicPr><p:blipFill><a:blip r:embed="rId2"/><a:stretch><a:fillRect/></a:stretch></p:blipFill><p:spPr><a:xfrm><a:off x="0" y="0"/><a:ext cx="12192000" cy="6858000"/></a:xfrm><a:prstGeom prst="rect"><a:avLst/></a:prstGeom></p:spPr></p:pic>'''
    return original.replace("</p:spTree>", overlay + "</p:spTree>", 1)


def main() -> None:
    if not SOURCE.exists():
        raise SystemExit(f"Missing source deck: {SOURCE}")
    PREVIEW_DIR.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="anomaly-deck-") as temp_name:
        temp = Path(temp_name)
        extracted = temp / "pptx"
        with zipfile.ZipFile(SOURCE) as archive:
            archive.extractall(extracted)
        media = extracted / "ppt" / "media"
        images = [image_uri(media / f"image{i}.jpg") for i in range(1, 6)]
        builders = [slide_1, slide_2, slide_3, slide_4, slide_5]
        rendered: list[Path] = []
        for index, builder in enumerate(builders, start=1):
            svg_path = temp / f"slide{index}.svg"
            png_path = temp / f"slide{index}.png"
            svg_path.write_text(wrap_svg(builder(images)), encoding="utf-8")
            subprocess.run(["rsvg-convert", "-w", str(W), "-h", str(H), "-o", str(png_path), str(svg_path)], check=True)
            preview_path = PREVIEW_DIR / f"slide{index}.jpg"
            subprocess.run(["convert", str(png_path), "-quality", "91", str(preview_path)], check=True)
            rendered.append(preview_path)
            shutil.copy2(preview_path, media / f"image{index}.jpg")
            slide_path = extracted / "ppt" / "slides" / f"slide{index}.xml"
            original_slide = slide_path.read_text(encoding="utf-8")
            slide_path.write_text(slide_xml(original_slide), encoding="utf-8")
        with zipfile.ZipFile(OUTPUT, "w", zipfile.ZIP_DEFLATED) as archive:
            for path in extracted.rglob("*"):
                if path.is_file():
                    archive.write(path, path.relative_to(extracted))
    print(f"Wrote {OUTPUT}")
    print(f"Wrote previews to {PREVIEW_DIR}")


if __name__ == "__main__":
    main()
