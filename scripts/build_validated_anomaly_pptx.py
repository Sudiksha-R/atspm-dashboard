from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, "/tmp/anomaly-ppt-deps")

from pptx import Presentation
from pptx.util import Inches


PREVIEW_DIR = Path("/tmp/Task5_Anomaly_Dashboard_Update_Redesigned_preview")
FIRST_SAVE = Path("/tmp/Task5_Anomaly_Dashboard_Update_Redesigned_native_first_save.pptx")
FINAL = Path("/tmp/Task5_Anomaly_Dashboard_Update_Redesigned_FINAL.pptx")


def main() -> None:
    slide_images = [PREVIEW_DIR / f"slide{number}.jpg" for number in range(1, 6)]
    missing = [path for path in slide_images if not path.exists()]
    if missing:
        raise SystemExit(f"Missing rendered slides: {missing}")

    presentation = Presentation()
    presentation.slide_width = Inches(13.333333)
    presentation.slide_height = Inches(7.5)

    # Remove the default slide if a template implementation ever adds one.
    for slide_id in list(presentation.slides._sldIdLst):
        presentation.part.drop_rel(slide_id.rId)
        presentation.slides._sldIdLst.remove(slide_id)

    blank_layout = presentation.slide_layouts[6]
    for slide_image in slide_images:
        slide = presentation.slides.add_slide(blank_layout)
        slide.shapes.add_picture(
            str(slide_image),
            0,
            0,
            width=presentation.slide_width,
            height=presentation.slide_height,
        )

    presentation.save(FIRST_SAVE)

    # A full python-pptx load/save round trip exercises relationships, media,
    # content types, slide masters, and package serialization before handoff.
    reopened = Presentation(FIRST_SAVE)
    if len(reopened.slides) != 5:
        raise RuntimeError(f"Expected 5 slides, found {len(reopened.slides)}")
    for index, slide in enumerate(reopened.slides, start=1):
        pictures = [shape for shape in slide.shapes if shape.shape_type == 13]
        if len(pictures) != 1:
            raise RuntimeError(f"Slide {index} should contain one picture; found {len(pictures)}")
    reopened.save(FINAL)

    final_check = Presentation(FINAL)
    if len(final_check.slides) != 5:
        raise RuntimeError("Final validation reload failed")
    print(f"PowerPoint-native round-trip validation passed: {FINAL}")


if __name__ == "__main__":
    main()
