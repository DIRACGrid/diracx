#!/usr/bin/env python3
"""Render an ADR from docs/adr/ as a printable A4 PDF.

Mermaid fences have no LaTeX equivalent, so each one is drawn by headless Chrome
and cropped to a small vector PDF before pandoc runs; the result stays selectable
text rather than a screenshot. Rendered diagrams are cached by content hash, so
re-running after a prose edit only costs the pandoc pass.

Usage: python scripts/generate_adr_pdf.py 5
       python scripts/generate_adr_pdf.py 2 3 4 5 6 7 --combined
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

REPO = pathlib.Path(__file__).resolve().parent.parent
ADR_DIR = REPO / "docs" / "adr"
ASSETS = pathlib.Path(__file__).resolve().parent / "adr_pdf"

MERMAID_VERSION = "11"
MERMAID_URL = (
    f"https://cdn.jsdelivr.net/npm/mermaid@{MERMAID_VERSION}/dist/mermaid.min.js"
)

MERMAID_FENCE = re.compile(r"^```mermaid\n(.*?)^```\n", re.M | re.S)

# Text area of an A4 page at the 2.2 cm margins set below, in TeX points.
TEXT_W, TEXT_H = 470.0, 660.0
# A diagram shrunk below this fraction of its natural size is too cramped to sit
# in the text flow, and gets a full page of its own instead.
OWN_PAGE_BELOW = 0.55

CHROME_CANDIDATES = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
]
CHROME_ON_PATH = [
    "google-chrome",
    "google-chrome-stable",
    "chromium",
    "chromium-browser",
]


def find_chrome() -> str:
    """Locate a Chrome-family browser to draw the mermaid diagrams with."""
    override = os.environ.get("CHROME") or os.environ.get("CHROME_PATH")
    if override:
        if not pathlib.Path(override).is_file():
            sys.exit(f"CHROME is set to {override}, which is not a file")
        return override
    for name in CHROME_ON_PATH:
        found = shutil.which(name)
        if found:
            return found
    for candidate in CHROME_CANDIDATES:
        if pathlib.Path(candidate).is_file():
            return candidate
    sys.exit(
        "No Chrome-family browser found to render the mermaid diagrams. Install "
        "Google Chrome or Chromium, or point the CHROME environment variable at one."
    )


def mermaid_js(cache: pathlib.Path) -> pathlib.Path:
    """Return the cached mermaid bundle, downloading it the first time."""
    js = cache / f"mermaid-{MERMAID_VERSION}.min.js"
    if not js.exists():
        print(f"  downloading mermaid {MERMAID_VERSION}", flush=True)
        cache.mkdir(parents=True, exist_ok=True)
        with urllib.request.urlopen(MERMAID_URL, timeout=120) as response:  # noqa: S310
            js.write_bytes(response.read())
    return js


def render_diagram(code: str, out_pdf: pathlib.Path, cache: pathlib.Path) -> None:
    """Draw one mermaid diagram to a PDF cropped to the drawing."""
    template = (ASSETS / "diagram.html").read_text()
    escaped = code.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    page = template.replace("__MERMAID_JS__", mermaid_js(cache).as_uri())
    page = page.replace("__CODE__", escaped)

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        tmp = pathlib.Path(td)
        html = tmp / "diagram.html"
        html.write_text(page)
        proc = subprocess.Popen(  # noqa: S603
            [
                find_chrome(),
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--allow-file-access-from-files",
                "--hide-scrollbars",
                "--virtual-time-budget=20000",
                "--no-pdf-header-footer",
                f"--user-data-dir={tmp / 'profile'}",
                f"--print-to-pdf={out_pdf}",
                html.as_uri(),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # Chrome regularly lingers after writing the PDF, so wait for the file to
        # stop growing rather than for the process to exit.
        deadline, previous, settled = time.time() + 120, -1, 0
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.25)
            size = out_pdf.stat().st_size if out_pdf.exists() else -1
            settled = settled + 1 if size > 500 and size == previous else 0
            previous = size
            if settled >= 3:
                break
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    if not out_pdf.exists() or out_pdf.stat().st_size < 500:
        out_pdf.unlink(missing_ok=True)
        sys.exit(
            "Chrome did not produce a diagram; run with CHROME set to a working browser"
        )


def page_size(pdf: pathlib.Path) -> tuple[float, float]:
    box = re.search(rb"/MediaBox\s*\[([^\]]*)\]", pdf.read_bytes())
    if box is None:
        sys.exit(f"{pdf} has no page size; delete it from the cache and try again")
    x0, y0, x1, y1 = (float(value) for value in box.group(1).split())
    return x1 - x0, y1 - y0


def figure(pdf: pathlib.Path) -> str:
    """Place a diagram inline, on its own page, or sideways, whichever shows it largest."""
    width, height = page_size(pdf)
    portrait = min(TEXT_W / width, TEXT_H / height)
    landscape = min(TEXT_H / width, TEXT_W / height)
    if portrait >= OWN_PAGE_BELOW:
        return f"![]({pdf})\n"
    # Both oversized cases are full-page floats, so the prose keeps flowing and no
    # heading is left stranded above a forced page break.
    if landscape > 1.15 * portrait:
        env, opts, box = "sidewaysfigure", "", r"width=\textheight,height=\textwidth"
    else:
        env, opts, box = "figure", "[p]", r"width=\textwidth,height=0.92\textheight"
    return (
        "```{=latex}\n"
        f"\\begin{{{env}}}{opts}\n"
        "\\centering\n"
        f"\\includegraphics[{box},keepaspectratio]{{{pdf}}}\n"
        f"\\end{{{env}}}\n"
        "```\n"
    )


def adr_key(source: pathlib.Path) -> str:
    """Return the DX-ADR-0NN part of an ADR filename."""
    return source.name[:10]


def prepare(
    source: pathlib.Path, cache: pathlib.Path, *, keep_heading: bool
) -> tuple[str, str]:
    """Return the ADR's title and a body with its diagrams rendered and included."""
    text = source.read_text()

    def replace(match: re.Match[str]) -> str:
        code = match.group(1)
        pdf = (
            cache / "figures" / f"{hashlib.sha256(code.encode()).hexdigest()[:16]}.pdf"
        )
        if not pdf.exists():
            print(f"  rendering diagram {pdf.stem}", flush=True)
            render_diagram(code, pdf, cache)
        return figure(pdf)

    text = MERMAID_FENCE.sub(replace, text)
    heading = re.match(r"# (.*?)\n", text)
    if not heading:
        sys.exit(f"{source.name} does not start with a level-one heading")
    title, rest = heading.group(1), text[heading.end() :]

    if not keep_heading:
        # The heading becomes the PDF title, so the rest is promoted a level by pandoc.
        return title, rest

    # In a combined document the heading stays put as a chapter. It is tagged so
    # cross-references have an anchor, and the chapter counter is forced to the
    # ADR's own number so that section 5.3 is DX-ADR-005 section 3.
    key = adr_key(source)
    number = int(key.rsplit("-", 1)[-1])
    return title, (
        "```{=latex}\n"
        f"\\setcounter{{chapter}}{{{number - 1}}}\n"
        "```\n\n"
        f"# {title} {{#{key.lower()}}}\n{rest}"
    )


def adr_sources() -> dict[str, pathlib.Path]:
    return {
        adr_key(path): path
        for path in sorted(ADR_DIR.glob("DX-ADR-[0-9][0-9][0-9]_*.md"))
    }


def resolve(number: str, sources: dict[str, pathlib.Path]) -> pathlib.Path:
    if not number.isdigit():
        sys.exit(f"'{number}' is not an ADR number")
    key = f"DX-ADR-{int(number):03d}"
    if key not in sources:
        available = ", ".join(sorted(k.rsplit("-", 1)[-1] for k in sources))
        sys.exit(f"No source for {key} in docs/adr (available: {available})")
    return sources[key]


def numbers_of(adrs: list[pathlib.Path]) -> list[int]:
    return sorted(int(adr_key(a).rsplit("-", 1)[-1]) for a in adrs)


def combined_subtitle(adrs: list[pathlib.Path]) -> str:
    """Name the ADRs a combined document gathers, as a range where possible."""
    numbers = numbers_of(adrs)
    if len(numbers) > 1 and numbers == list(range(numbers[0], numbers[-1] + 1)):
        return f"DX-ADR-{numbers[0]:03d} to DX-ADR-{numbers[-1]:03d}"
    return ", ".join(f"DX-ADR-{n:03d}" for n in numbers)


def combined_stem(adrs: list[pathlib.Path]) -> str:
    numbers = numbers_of(adrs)
    if numbers == list(range(numbers[0], numbers[-1] + 1)):
        return f"DX-ADR-{numbers[0]:03d}-{numbers[-1]:03d}"
    return "DX-ADR-" + "+".join(f"{n:03d}" for n in numbers)


def build(
    adrs: list[pathlib.Path],
    out_pdf: pathlib.Path,
    cache: pathlib.Path,
    sources: dict[str, pathlib.Path],
    toc: bool | None,
    *,
    combined: bool,
    title: str = "",
) -> pathlib.Path:
    parts = []
    for source in adrs:
        print(f"{source.name}", flush=True)
        adr_title, part = prepare(source, cache, keep_heading=combined)
        if not combined:
            title = adr_title
        parts.append(part)
    # A chapter already starts a page of its own, so the parts just run together.
    document = "\n\n".join(parts)

    if toc is None:
        # Short ADRs are easier to read without a page of contents in front, but a
        # combined document is never short.
        toc = combined or sum(len(a.read_text().splitlines()) for a in adrs) > 250

    # Only a combined document can resolve a cross-reference internally.
    internal = ",".join(adr_key(a) for a in adrs) if combined else ""

    out_pdf.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as td:
        markdown = pathlib.Path(td) / "adr.md"
        markdown.write_text(document)
        command = [
            "pandoc",
            str(markdown),
            "-f",
            "markdown",
            "-o",
            str(out_pdf),
            "--standalone",
            "--pdf-engine=tectonic",
            "--lua-filter",
            str(ASSETS / "links.lua"),
            "-H",
            str(ASSETS / "preamble.tex"),
            "--syntax-highlighting=tango",
            "--number-sections",
            "--resource-path",
            f"{ADR_DIR}:{td}",
            "-M",
            f"title={title}",
            "-M",
            "siblings=" + ",".join(p.name for p in sources.values()),
            "-M",
            f"internal={internal}",
            "-V",
            "papersize=a4",
            "-V",
            "geometry:a4paper",
            "-V",
            "geometry:margin=2.2cm",
            "-V",
            "fontsize=10pt",
            "-V",
            "colorlinks=true",
            "-V",
            "linkcolor=[HTML]{1A4F8C}",
            "-V",
            "urlcolor=[HTML]{1A4F8C}",
        ]
        if combined:
            command += [
                "--top-level-division=chapter",
                "-M",
                f"subtitle={combined_subtitle(adrs)}",
                "-M",
                f"date={datetime.datetime.now(tz=datetime.timezone.utc).date()}",
                "-V",
                "documentclass=report",
            ]
        else:
            command += [
                "--shift-heading-level-by=-1",
                "-V",
                "documentclass=article",
            ]
        if toc:
            command += ["--toc", "--toc-depth=3"]
        subprocess.run(command, check=True)  # noqa: S603
    return out_pdf


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("number", nargs="+", help="ADR number, for example 5 or 005")
    parser.add_argument(
        "--combined",
        action="store_true",
        help="gather every ADR given into one PDF instead of one PDF each",
    )
    parser.add_argument(
        "--title",
        default="DiracX Architecture Decision Records",
        help="cover title for --combined",
    )
    parser.add_argument(
        "--output",
        type=pathlib.Path,
        default=None,
        help="write to this exact path instead of a name derived from the ADRs",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=REPO / "adr-pdfs",
        help="where to write the PDFs (default: adr-pdfs/)",
    )
    parser.add_argument(
        "--cache-dir",
        type=pathlib.Path,
        default=None,
        help="where to cache mermaid and the rendered diagrams",
    )
    toc = parser.add_mutually_exclusive_group()
    toc.add_argument(
        "--toc",
        dest="toc",
        action="store_true",
        default=None,
        help="always include a table of contents",
    )
    toc.add_argument(
        "--no-toc",
        dest="toc",
        action="store_false",
        help="never include a table of contents",
    )
    args = parser.parse_args()

    cache = (
        args.cache_dir
        or pathlib.Path(
            os.environ.get("XDG_CACHE_HOME", pathlib.Path.home() / ".cache")
        )
        / "diracx-adr-pdf"
    )

    sources = adr_sources()
    selected = [resolve(number, sources) for number in args.number]

    def report(pdf: pathlib.Path) -> None:
        shown = pdf.relative_to(REPO) if pdf.is_relative_to(REPO) else pdf
        print(f"  -> {shown}", flush=True)

    if args.combined:
        out_pdf = args.output or args.output_dir / f"{combined_stem(selected)}.pdf"
        report(
            build(
                selected,
                out_pdf,
                cache,
                sources,
                args.toc,
                combined=True,
                title=args.title,
            )
        )
        return

    if args.output and len(selected) > 1:
        sys.exit("--output names one file; pass --combined or use --output-dir")
    for source in selected:
        out_pdf = args.output or args.output_dir / f"{source.stem}.pdf"
        report(build([source], out_pdf, cache, sources, args.toc, combined=False))


if __name__ == "__main__":
    main()
