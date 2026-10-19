"""Regenerate brand/source/laminardb-wordmark.svg as outlined lettering.

The wordmark is "laminardb" set in Liberation Sans Regular 2.1.5 (SIL Open Font
License 1.1, https://github.com/liberationfonts/liberation-fonts). The font file
is not distributed with this repository; download it and pass its path.

Size, tracking and position were measured from the raster reference
(references/wordmark-check.png in the design handoff): 147 px type,
-0.04 em tracking, glyph origin 211 mark units right of the mark's left edge and
baseline 108.4 units below its top. Coordinates are in mark units, so the path
drops into a lockup next to brand/source/laminardb-mark.svg without a transform.

    pip install fonttools
    python brand/tools/outline_wordmark.py path/to/LiberationSans-Regular.ttf
"""

import sys
from pathlib import Path

from fontTools.pens.boundsPen import BoundsPen
from fontTools.pens.svgPathPen import SVGPathPen
from fontTools.pens.transformPen import TransformPen
from fontTools.ttLib import TTFont

TEXT = "laminardb"
PX_PER_UNIT = 1.125
FONT_SIZE = 147 / PX_PER_UNIT
TRACKING_EM = -0.04
ORIGIN_X = 211.0
BASELINE_Y = 108.4
OUTPUT = Path(__file__).resolve().parent.parent / "source" / "laminardb-wordmark.svg"


def outline_text(
    font_path: str, text: str, size: float, tracking_em: float, x: float, baseline: float
) -> tuple[str, tuple[float, float, float, float], float]:
    """Return SVG path data, ink bounds and the end x of `text` set without kerning."""
    font = TTFont(font_path)
    glyphs = font.getGlyphSet()
    cmap = font.getBestCmap()
    scale = size / font["head"].unitsPerEm
    path_pen = SVGPathPen(glyphs, ntos=lambda v: f"{v:.2f}".rstrip("0").rstrip("."))
    bounds_pen = BoundsPen(glyphs)
    for char in text:
        name = cmap[ord(char)]
        transform = (scale, 0, 0, -scale, x, baseline)
        glyphs[name].draw(TransformPen(path_pen, transform))
        glyphs[name].draw(TransformPen(bounds_pen, transform))
        x += font["hmtx"][name][0] * scale + tracking_em * size
    return path_pen.getCommands(), bounds_pen.bounds, x


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    d, (x0, y0, x1, y1), _ = outline_text(
        sys.argv[1], TEXT, FONT_SIZE, TRACKING_EM, ORIGIN_X, BASELINE_Y
    )
    box = f"{x0:.2f} {y0:.2f} {x1 - x0:.2f} {y1 - y0:.2f}"
    OUTPUT.write_text(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{box}">\n'
        "  <title>laminardb</title>\n"
        "  <!-- Outlined from Liberation Sans Regular 2.1.5 (SIL OFL 1.1) by brand/tools/outline_wordmark.py -->\n"
        f'  <path id="wordmark" fill="#162C29" d="{d}"/>\n'
        "</svg>\n",
        encoding="utf-8",
    )
    print(f"wrote {OUTPUT} (viewBox {box})")


if __name__ == "__main__":
    main()
