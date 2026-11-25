"""Compare default portrait Census county screenshots by region using Pillow."""

import argparse
import json

import numpy as np
from PIL import Image, ImageFilter


REGIONS = {
    "all": (0.04, 0.265, 0.96, 0.861),
    "mainland": (0.04, 0.265, 0.96, 0.586),
    "alaska": (0.04, 0.59, 0.782, 0.861),
    "hawaii": (0.782, 0.59, 0.96, 0.666),
}


def crop(image, region):
    return image.crop(tuple(round(value * extent) for value, extent in zip(
        region, (image.width, image.height, image.width, image.height)
    )))


def fill_mask(image):
    red, green, blue = np.moveaxis(np.asarray(image, dtype=np.int16), -1, 0)
    return (red >= 55) & (blue <= 235) & (blue - red >= 35) & (blue - green >= 12)


def grow(mask):
    return np.asarray(Image.fromarray(mask).convert("L").filter(ImageFilter.MaxFilter(7))) > 0


def compare(first, second):
    a, b = fill_mask(first), fill_mask(second)
    return {
        "cg_fill_pixels": int(a.sum()),
        "metal_fill_pixels": int(b.sum()),
        "intersection_over_union": float((a & b).sum() / (a | b).sum()),
        "cg_beyond_3px_percent": float(100 * (a & ~grow(b)).sum() / a.sum()),
        "metal_beyond_3px_percent": float(100 * (b & ~grow(a)).sum() / b.sum()),
    }


def detail_comparison(first, second):
    a, b = (np.asarray(Image.open(path).convert("RGB"), dtype=np.int16) for path in (first, second))
    if a.shape != b.shape:
        raise SystemExit("Use map-detail screenshots at equal zoom, crop and resolution.")

    def selected(image):
        red, green, blue = np.moveaxis(image, -1, 0)
        return (red > 205) & (red - blue > 90) & (green > 125)

    m, n = selected(a), selected(b)
    delta = np.abs(a - b).max(axis=2)
    return {
        "width": int(a.shape[1]),
        "height": int(a.shape[0]),
        "selected_cg_pixels": int(m.sum()),
        "selected_metal_pixels": int(n.sum()),
        "selected_mask_iou": float((m & n).sum() / (m | n).sum()),
        "exact_rgb_fraction": float((delta == 0).mean()),
        "rgb_difference_at_most_8_fraction": float((delta <= 8).mean()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("core_graphics")
    parser.add_argument("metal_4x")
    parser.add_argument("metal_1x")
    parser.add_argument("--detail", nargs=2, metavar=("CG", "METAL"))
    paths = parser.parse_args()
    cg, metal, single = (Image.open(path).convert("RGB") for path in (
        paths.core_graphics, paths.metal_4x, paths.metal_1x
    ))
    if cg.size != metal.size or cg.size != single.size or cg.width >= cg.height:
        parser.error("Use three portrait screenshots captured at the same simulator resolution.")
    results = {name: compare(crop(cg, region), crop(metal, region)) for name, region in REGIONS.items()}
    results["mainland_unique_colors"] = {
        "cg": len(crop(cg, REGIONS["mainland"]).getcolors(cg.width * cg.height)),
        "metal_1x": len(crop(single, REGIONS["mainland"]).getcolors(cg.width * cg.height)),
        "metal_4x": len(crop(metal, REGIONS["mainland"]).getcolors(cg.width * cg.height)),
    }
    if paths.detail:
        results["detail"] = detail_comparison(*paths.detail)
    print(json.dumps(results, indent=2))
    # Allow thin border color differences and require the land masks to stay aligned.
    if any(max(row["cg_beyond_3px_percent"], row["metal_beyond_3px_percent"]) > 0.5
           for name, row in results.items() if name in REGIONS):
        raise SystemExit("Census coverage exceeded the 0.5% distant mismatch threshold.")


if __name__ == "__main__":
    main()
