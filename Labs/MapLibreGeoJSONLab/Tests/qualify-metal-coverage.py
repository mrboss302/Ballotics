"""Compare unclipped geographic fill coverage and antialiasing in the thematic Census screenshots."""
import argparse
import json
import runpy
from pathlib import Path

import numpy as np
from PIL import Image


def viewport(path, expected_size):
    image = np.asarray(Image.open(path).convert("RGB"))
    # The clear color is shared by both backends. The rounded SwiftUI clip leaves all
    # four extreme coordinates present, although the four corner pixels are absent.
    y, x = np.where(np.all(image == (237, 242, 245), axis=2))
    rect = image[y.min():y.max() + 1, x.min():x.max() + 1]
    assert (rect.shape[1], rect.shape[0]) == expected_size, (path, rect.shape, expected_size)
    return rect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("screens", type=Path)
    parser.add_argument("frames", type=Path)
    parser.add_argument("--detail", nargs=2, metavar=("CG", "METAL"))
    args = parser.parse_args()
    reference = runpy.run_path(str(Path(__file__).with_name("compare-metal-coverage.py")))
    frames = json.loads(args.frames.read_text())
    events = [json.loads(line) for line in (args.screens / "counties-metal-4x.jsonl").read_text().splitlines()]
    submit = next(row for row in events if row["event"] == "metal_first_submit")
    width, height = submit["drawable_width"], submit["drawable_height"]
    fit = frames["fit"]
    # iPhone 17 Pro portrait capture at three backing pixels per point.
    scale = min((width - 72) / fit[2], (height - 72) / fit[3])
    y, x = np.ogrid[:height, :width]
    masks = {"all": np.ones((height, width), dtype=bool)}
    for region in ("alaska", "hawaii"):
        left, top, w, h = frames[region]
        xmin = width / 2 + (left - fit[0] - fit[2] / 2) * scale
        ymin = height / 2 + (top - fit[1] - fit[3] / 2) * scale
        masks[region] = ((x >= xmin - 2) & (x <= xmin + w * scale + 2)
                         & (y >= ymin - 2) & (y <= ymin + h * scale + 2))
    masks["mainland"] = ~(masks["alaska"] | masks["hawaii"])
    images = {path.stem: viewport(path, (width, height)) for path in args.screens.glob("counties-*.png")}
    cg = reference["fill_mask"](images["counties-cg"])
    full = images["counties-metal-4x"].astype(np.int16)
    out = {"drawable": [width, height], "samples": {}}
    for name, image in sorted(images.items()):
        land = reference["fill_mask"](image)
        cg_far = cg & ~reference["grow"](land)
        metal_far = land & ~reference["grow"](cg)
        diff = np.max(abs(image.astype(np.int16) - full), axis=2)
        regions = {}
        for region, area in masks.items():
            cg_area, metal_area = (area & cg).sum(), (area & land).sum()
            assert cg_area and metal_area, (name, region)
            regions[region] = {
                "cg_land_pixels": int(cg_area), "land_pixels": int(metal_area),
                "iou": float((area & cg & land).sum() / (area & (cg | land)).sum()),
                "cg_far_pct": float(100 * (area & cg_far).sum() / cg_area),
                "other_far_pct": float(100 * (area & metal_far).sum() / metal_area),
                "pixels_differing_from_4x_by_more_than_8": int((area & (diff > 8)).sum()),
            }
        out["samples"][name] = regions
    if args.detail:
        out["selection_detail"] = reference["detail_comparison"](*args.detail)
    print(json.dumps(out, indent=2))
    if any(max(row["cg_far_pct"], row["other_far_pct"]) > 0.5
           for row in out["samples"]["counties-metal-4x"].values()):
        raise SystemExit("Full-quality geographic coverage exceeds the 0.5% per-direction tolerance.")


if __name__ == "__main__":
    main()
