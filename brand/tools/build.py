"""Derive every LaminarDB logo, icon and social image from the canonical sources.

Sources: brand/source/laminardb-mark.svg (channel mark), brand/source/laminardb-wordmark.svg
(outlined lettering) and brand/source/conceptual-flow.svg (site illustration). Exports are
generated; edit the sources, then rerun this script.

Requirements: Python 3.11+, Pillow, fontTools, and resvg on PATH (`cargo install resvg`).
Social images set their text in Liberation Sans and Liberation Mono 2.1.5 (SIL OFL 1.1);
pass the directory that holds the .ttf files. `--console` also refreshes the Console's copies.

    python brand/tools/build.py --fonts path/to/liberation-fonts-ttf-2.1.5 \
        [--console path/to/laminardb-console-ui]
"""

import argparse
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image

from outline_wordmark import outline_text

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "brand" / "source"
EXPORTS = ROOT / "brand" / "exports"
SITE = ROOT / "site"

INK, ACCENT, SURFACE, MUTED = "#162C29", "#2D695E", "#F7F8F5", "#52665F"
ON_DARK_INK, ON_DARK_ACCENT = "#F7F8F5", "#74B09D"
COLOURWAYS = {
    "": (INK, ACCENT),
    "-on-dark": (ON_DARK_INK, ON_DARK_ACCENT),
    "-ink": (INK, INK),
    "-white": ("#FFFFFF", "#FFFFFF"),
}
MARK_W, MARK_H = 135, 127
# Distance from the mark's box centre to its farthest corners (0,0) and (135,127).
MARK_RADIUS = (MARK_W**2 + MARK_H**2) ** 0.5 / 2

# Small-size variant: stroke 20 instead of 14 on the same outer edges and outer radii.
FAVICON_MARK = {
    "outer": "M0 0H20V71A36 36 0 0 0 56 107H135V127H56A56 56 0 0 1 0 71Z",
    "middle": "M36 0H56V63A8 8 0 0 0 64 71H135V91H64A28 28 0 0 1 36 63Z",
    "inner": "M72 0H92V35H135V55H86A14 14 0 0 1 72 41Z",
}

SOCIAL_EYEBROW = "OPEN-SOURCE STREAMING SQL"
SOCIAL_HEADLINE = ("Process streaming", "data with ", "SQL.")
SOCIAL_BODY = (
    "Continuous SQL over event streams, written in Rust.",
    "Embed it in an application or run it as a server.",
)
SOCIAL_FOOTER = "laminardb.io"
SOCIAL_CAPTION = "CONCEPTUAL DATA FLOW"


def load_sources() -> tuple[dict[str, str], str, str]:
    mark = (SOURCE / "laminardb-mark.svg").read_text(encoding="utf-8")
    paths = dict(re.findall(r'<path id="(\w+)" fill="[^"]+" d="([^"]+)"', mark))
    word = (SOURCE / "laminardb-wordmark.svg").read_text(encoding="utf-8")
    word_d = re.search(r'<path id="wordmark" fill="[^"]+" d="([^"]+)"', word).group(1)
    word_box = re.search(r'viewBox="([^"]+)"', word).group(1)
    return paths, word_d, word_box


def logo_width(word_box: str) -> float:
    x, _, w, _ = (float(v) for v in word_box.split())
    return round(x + w, 2)


def mark_body(paths: dict[str, str], ink: str, accent: str) -> str:
    return (
        f'<path fill="{ink}" d="{paths["outer"]}"/>'
        f'<path fill="{accent}" d="{paths["middle"]}"/>'
        f'<path fill="{ink}" d="{paths["inner"]}"/>'
    )


def svg(view_box: str, body: str, title: str) -> str:
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{view_box}">'
        f"<title>{title}</title>{body}</svg>\n"
    )


def tile(paths: dict[str, str], size: float, fill: float, radius: float = 0) -> str:
    """A square tile with the on-dark mark centred; `fill` is the share of the
    inscribed circle the mark's corners reach, which keeps a circular crop clear."""
    scale = fill * size / 2 / MARK_RADIUS
    dx, dy = (size - MARK_W * scale) / 2, (size - MARK_H * scale) / 2
    return (
        f'<rect width="{size}" height="{size}" rx="{radius}" fill="{INK}"/>'
        f'<g transform="translate({dx:.3f} {dy:.3f}) scale({scale:.5f})">'
        f"{mark_body(paths, ON_DARK_INK, ON_DARK_ACCENT)}</g>"
    )


def rasterise(svg_path: Path, png_path: Path, width: int, height: int | None = None) -> None:
    png_path.parent.mkdir(parents=True, exist_ok=True)
    size = ["-w", str(width)] + (["-h", str(height)] if height else [])
    subprocess.run(["resvg", *size, str(svg_path), str(png_path)], check=True)


def write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")
    return path


def build_logos(paths: dict[str, str], word_d: str, word_box: str) -> None:
    logo_w = logo_width(word_box)
    for suffix, (ink, accent) in COLOURWAYS.items():
        mark = mark_body(paths, ink, accent)
        logo = write(
            EXPORTS / "svg" / f"laminardb-logo{suffix}.svg",
            svg(f"0 0 {logo_w} {MARK_H}", mark + f'<path fill="{ink}" d="{word_d}"/>', "LaminarDB"),
        )
        symbol = write(
            EXPORTS / "svg" / f"laminardb-mark{suffix}.svg",
            svg(f"0 0 {MARK_W} {MARK_H}", mark, "LaminarDB"),
        )
        if suffix in ("", "-on-dark"):
            for width in (600, 1200, 2400):
                rasterise(logo, EXPORTS / "png" / f"laminardb-logo{suffix}-{width}.png", width)
            for width in (64, 128, 256, 512, 1024):
                rasterise(symbol, EXPORTS / "png" / f"laminardb-mark{suffix}-{width}.png", width)
            write(
                EXPORTS / "svg" / f"laminardb-wordmark{suffix}.svg",
                svg(word_box, f'<path fill="{ink}" d="{word_d}"/>', "laminardb"),
            )


def build_icons(paths: dict[str, str]) -> None:
    app = write(EXPORTS / "svg" / "laminardb-app-icon.svg", svg("0 0 1024 1024", tile(paths, 1024, 0.8), "LaminarDB"))
    for name, size in (("apple-touch-icon", 180), ("icon-192", 192), ("icon-512", 512)):
        rasterise(app, EXPORTS / "png" / f"{name}.png", size)
    for size in (1024, 512):
        rasterise(app, EXPORTS / "github" / f"avatar-{size}.png", size)

    favicon = write(
        EXPORTS / "svg" / "laminardb-favicon.svg",
        svg("0 0 64 64", tile(FAVICON_MARK, 64, 0.9, radius=12), "LaminarDB"),
    )
    frames = []
    for size in (16, 32, 48):
        png = EXPORTS / "png" / f"favicon-{size}.png"
        rasterise(favicon, png, size)
        frames.append(Image.open(png).convert("RGBA"))
    frames[-1].save(EXPORTS / "favicon.ico", format="ICO", sizes=[(16, 16), (32, 32), (48, 48)], append_images=frames[:-1])


def text_path(fonts: Path, face: str, text: str, size: float, tracking: float, x: float, y: float, fill: str):
    d, _, end = outline_text(str(fonts / f"Liberation{face}.ttf"), text, size, tracking, x, y)
    return f'<path fill="{fill}" d="{d}"/>', end


def social_card(paths: dict[str, str], word_d: str, fonts: Path, width: int, height: int) -> str:
    k = height / 630
    left = 80 * k
    logo_scale = 40 * k / MARK_H
    body = [f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>']
    body.append(
        f'<g transform="translate({left:.2f} {64 * k:.2f}) scale({logo_scale:.5f})">'
        f'{mark_body(paths, INK, ACCENT)}<path fill="{INK}" d="{word_d}"/></g>'
    )
    eyebrow, _ = text_path(fonts, "Mono-Regular", SOCIAL_EYEBROW, 15 * k, 0.18, left, 196 * k, MUTED)
    first, _ = text_path(fonts, "Sans-Bold", SOCIAL_HEADLINE[0], 70 * k, -0.035, left, 276 * k, INK)
    second, end = text_path(fonts, "Sans-Bold", SOCIAL_HEADLINE[1], 70 * k, -0.035, left, 352 * k, INK)
    sql, _ = text_path(fonts, "Sans-Bold", SOCIAL_HEADLINE[2], 70 * k, -0.035, end, 352 * k, ACCENT)
    body += [eyebrow, first, second, sql]
    for i, line in enumerate(SOCIAL_BODY):
        p, _ = text_path(fonts, "Sans-Regular", line, 24 * k, 0, left, (418 + i * 36) * k, MUTED)
        body.append(p)
    body.append(f'<rect x="{left:.2f}" y="{528 * k:.2f}" width="{width - 2 * left:.2f}" height="{max(1, round(k)):.0f}" fill="#D4DDD7"/>')
    footer, _ = text_path(fonts, "Mono-Regular", SOCIAL_FOOTER, 17 * k, 0.04, left, 572 * k, INK)
    caption, _ = text_path(fonts, "Mono-Regular", SOCIAL_CAPTION, 12 * k, 0.18, 712 * k, 396 * k, MUTED)
    body += [footer, caption]
    flow = (SOURCE / "conceptual-flow.svg").read_text(encoding="utf-8")
    inner = re.search(r"<svg[^>]*>(.*)</svg>", flow, re.S).group(1)
    flow_scale = 440 * k / 595
    body.append(f'<g class="ldb-flow" transform="translate({700 * k:.2f} {150 * k:.2f}) scale({flow_scale:.5f})">{inner}</g>')
    return svg(f"0 0 {width} {height}", "".join(body), "LaminarDB")


def build_social(paths: dict[str, str], word_d: str, fonts: Path) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        for folder, name, (w, h) in (
            ("social", "social-preview-1200x630", (1200, 630)),
            ("github", "repository-preview-1280x640", (1280, 640)),
        ):
            source = Path(tmp) / f"{name}.svg"
            source.write_text(social_card(paths, word_d, fonts, w, h), encoding="utf-8")
            rasterise(source, EXPORTS / folder / f"{name}.png", w, h)


def replace_between(path: Path, marker: str, content: str) -> None:
    text = path.read_text(encoding="utf-8")
    pattern = re.compile(rf"(<!-- {marker} -->).*?(<!-- /{marker} -->)", re.S)
    if not pattern.search(text):
        raise SystemExit(f"{path}: missing <!-- {marker} --> markers")
    path.write_text(pattern.sub(lambda m: m.group(1) + content + m.group(2), text), encoding="utf-8", newline="\n")


def sync_site(paths: dict[str, str], word_d: str, word_box: str) -> None:
    logo_w = logo_width(word_box)
    for name in ("apple-touch-icon.png", "icon-192.png", "icon-512.png"):
        shutil.copyfile(EXPORTS / "png" / name, SITE / name)
    shutil.copyfile(EXPORTS / "svg" / "laminardb-favicon.svg", SITE / "favicon.svg")
    shutil.copyfile(EXPORTS / "favicon.ico", SITE / "favicon.ico")
    (SITE / "img").mkdir(exist_ok=True)
    shutil.copyfile(EXPORTS / "social" / "social-preview-1200x630.png", SITE / "img" / "social-preview.png")
    lockup = (
        f'<svg class="brand-lockup" viewBox="0 0 {logo_w} {MARK_H}" role="img" aria-label="LaminarDB">'
        f'<path style="fill:var(--logo-ink)" d="{paths["outer"]}"/>'
        f'<path style="fill:var(--logo-accent)" d="{paths["middle"]}"/>'
        f'<path style="fill:var(--logo-ink)" d="{paths["inner"]}"/>'
        f'<path style="fill:var(--logo-ink)" d="{word_d}"/></svg>'
    )
    flow = (SOURCE / "conceptual-flow.svg").read_text(encoding="utf-8")
    flow = re.sub(r"\s*<!--.*?-->", "", flow, flags=re.S).strip()
    for page in SITE.rglob("*.html"):
        if "<!-- brand:lockup -->" in page.read_text(encoding="utf-8"):
            replace_between(page, "brand:lockup", lockup)
    replace_between(SITE / "index.html", "brand:flow", flow)


def sync_console(console: Path) -> None:
    shutil.copyfile(EXPORTS / "png" / "apple-touch-icon.png", console / "public" / "apple-touch-icon.png")
    shutil.copyfile(EXPORTS / "svg" / "laminardb-favicon.svg", console / "public" / "favicon.svg")
    shutil.copyfile(EXPORTS / "favicon.ico", console / "public" / "favicon.ico")
    assets = console / "src" / "assets" / "brand"
    assets.mkdir(parents=True, exist_ok=True)
    for name in ("laminardb-logo.svg", "laminardb-logo-on-dark.svg", "laminardb-mark.svg"):
        shutil.copyfile(EXPORTS / "svg" / name, assets / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fonts", type=Path, required=True, help="directory with LiberationSans-*.ttf and LiberationMono-Regular.ttf")
    parser.add_argument("--console", type=Path, help="laminardb-console-ui checkout to refresh")
    args = parser.parse_args()
    paths, word_d, word_box = load_sources()
    build_logos(paths, word_d, word_box)
    build_icons(paths)
    build_social(paths, word_d, args.fonts)
    sync_site(paths, word_d, word_box)
    if args.console:
        sync_console(args.console)


if __name__ == "__main__":
    main()
