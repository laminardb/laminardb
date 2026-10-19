# LaminarDB brand

The LaminarDB identity is a channel mark (three nested, turning channels) and the lowercase
wordmark `laminardb`, on a warm light canvas with dark green-black type and one green accent. Use
`LaminarDB` in prose and the exact names in code, packages, URLs and commands.

## Files

| Path | Use |
| --- | --- |
| `source/laminardb-mark.svg` | Canonical mark geometry. Edit this to change the mark. |
| `source/laminardb-wordmark.svg` | Canonical lettering, already outlined. Regenerate with `tools/outline_wordmark.py`. |
| `source/conceptual-flow.svg` | Site illustration ("Conceptual data flow"). Inlined into `site/index.html` by the build. |
| `exports/svg/laminardb-logo*.svg` | Horizontal logo: colour for light surfaces, `-on-dark`, and one-colour `-ink` and `-white`. |
| `exports/svg/laminardb-mark*.svg` | Mark alone, same four colourways. |
| `exports/svg/laminardb-wordmark*.svg` | Lettering alone, for light and dark surfaces. |
| `exports/svg/laminardb-app-icon.svg` | Square app icon and avatar source (mark on ink). |
| `exports/svg/laminardb-favicon.svg` | Small-size favicon variant (see below). |
| `exports/png/` | Logo PNGs at 600, 1200 and 2400 px wide; mark PNGs from 64 to 1024 px; favicon, touch and manifest icons. |
| `exports/favicon.ico` | 16, 32 and 48 px frames, each rendered separately. |
| `exports/github/avatar-{512,1024}.png` | GitHub organisation avatar. |
| `exports/github/repository-preview-1280x640.png` | GitHub repository social preview. |
| `exports/social/social-preview-1200x630.png` | Open Graph image used by the website. |

Exports are generated. Do not edit them by hand.

## Colour

| Token | Light | Dark | Role |
| --- | --- | --- | --- |
| surface | `#F7F8F5` | `#0F1E1B` | Page background |
| surface-subtle | `#E9EEE8` | `#162C29` | Footer, quiet bands |
| ink | `#162C29` | `#F7F8F5` | Text, primary buttons, mark outer and inner channels |
| accent | `#2D695E` | `#74B09D` | Emphasis, links on hover, mark middle channel |
| muted | `#52665F` | `#A3B4AD` | Secondary text, labels |
| border | `#D4DDD7` | `#30443F` | Rules and outlines |

The light values were sampled from the reference renders. The dark values were chosen for
contrast. Text contrast on the surface (WCAG 2.x): ink 13.8:1, accent 6.0:1, muted 5.8:1 in light;
ink 16.1:1, accent 6.9:1, muted 7.9:1 in dark. Borders are decorative (1.3:1) and are never the only
way to identify a control. In the mark, ink and accent differ by 2.3:1 in both colourways, which
keeps the middle channel distinct.

The accent is not a status colour. Keep success, warning and error colours separate from it.

## Typography

The site uses Arimo (headings and text) and Cousine (labels and code), both SIL Open Font
License 1.1, self-hosted from `site/fonts/` with their licences. They are metric-compatible with
Liberation Sans and Liberation Mono, which the reference renders used. The wordmark is Liberation
Sans Regular 2.1.5 at -0.04 em tracking, converted to outlines, so no font is needed to display it.

## Spacing and size

- Clear space around the logo or mark: at least the width of one channel stroke doubled (28 units
  in the mark's 135 × 127 grid, about 21% of the mark's height).
- Logo: at least 96 px wide on screen. Below that, use the mark alone.
- Mark: at least 16 px. Below 32 px, prefer the favicon variant.
- Do not recolour channels individually, add effects, outline, rotate or stretch the mark, or set
  the wordmark in another typeface or weight.

## Small sizes

`laminardb-favicon.svg` is an optical adjustment for 16 to 48 px: each channel is 20 units wide
instead of 14, keeping the same outer edges and outer radii, and the mark sits on a rounded ink tile
in the on-dark colours so it stays visible on light and dark browser chrome. Use the canonical mark
at 64 px and above.

The app icon and avatar put the canonical mark on an ink square. The mark's corners stay inside a
circle of 80% of the square's width, so circular crops do not clip it.

## Provenance

No original vector master existed. The mark, lettering placement and illustration are vector
reconstructions of the raster references in the design handoff (`desktop-1440.png`,
`mobile-390.png`, `wordmark-check.png`, `channel-mark-detail.png`, `hero-flow-detail.png`):

- Mark: edges and arc radii were measured from `wordmark-check.png` (1 unit = 1.125 px). Stroke 14,
  gap 22, outer radii 14, 28 and 56 on concentric arcs. Rendered at the reference scale, 46 of
  8,610 ink pixels differ by more than half coverage.
- Lettering: the reference matches Liberation Sans Regular at 147 px with -0.04 em tracking (mean
  alpha difference 5.9%; Arial 21.5%, Inter 35.4%). The page renders show heavier header lettering;
  the wordmark reference governs letterforms, so the logo uses the regular weight.
- Illustration: paths, markers and grid were measured from `hero-flow-detail.png`. It is a
  conceptual drawing, not a Console screen or a real pipeline.

The colours, geometry and lettering carry no claim of originality, exclusivity or legal clearance.

## Regenerate

```bash
cargo install resvg                 # SVG rasteriser
pip install fonttools pillow
# Liberation fonts 2.1.5: https://github.com/liberationfonts/liberation-fonts/releases
python brand/tools/outline_wordmark.py path/to/LiberationSans-Regular.ttf   # only if the lettering changes
python brand/tools/build.py --fonts path/to/liberation-fonts-ttf-2.1.5 \
    --console ../laminardb-console-ui
```

`build.py` writes `exports/`, copies icons and the social image into `site/`, replaces the logo
and illustration between the `<!-- brand:lockup -->` and `<!-- brand:flow -->` markers in the site
pages, and, with `--console`, refreshes the Console's favicon, touch icon and logo files.

Uploading the avatar or repository preview to GitHub is a settings change made by an organisation
owner; committing the files does not change them.
