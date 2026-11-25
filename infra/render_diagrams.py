"""Render every Mermaid diagram to HTML and PNG in docs/diagrams with one shared theme.

Run: uv run python infra/render_diagrams.py (needs `playwright install chromium` and internet for mermaid).
The lineage graph is copied from governance/lineage.mmd, which governance/lineage.py generates.
"""

from __future__ import annotations

import html
import shutil
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "docs" / "diagrams"
MERMAID = "https://cdn.jsdelivr.net/npm/mermaid@12.3.0/dist/mermaid.min.js"
THEME = (
    "{theme: 'base', themeVariables: {fontFamily: 'Helvetica, Arial, sans-serif', fontSize: '15px', "
    "primaryColor: '#e8f1fb', primaryBorderColor: '#2b6cb0', primaryTextColor: '#1a202c', "
    "lineColor: '#4a5568', clusterBkg: '#f7fafc', clusterBorder: '#a0aec0'}}"
)
PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>{title}</title>
<style>body {{ margin: 24px; background: #fff; }}</style></head>
<body><pre class="mermaid">{source}</pre>
<script src="{mermaid}"></script>
<script>mermaid.initialize(Object.assign({{startOnLoad: true}}, {theme}));</script>
</body></html>
"""


def main() -> None:
    shutil.copyfile(ROOT / "governance" / "lineage.mmd", OUT / "lineage.mmd")
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1600, "height": 900}, device_scale_factor=2)
        for src in sorted(OUT.glob("*.mmd")):
            target = src.with_suffix(".html")
            target.write_text(PAGE.format(title=src.stem, source=html.escape(src.read_text()),
                                          mermaid=MERMAID, theme=THEME))
            page.goto(target.as_uri())
            page.wait_for_selector("pre.mermaid svg")
            page.locator("pre.mermaid svg").screenshot(path=str(src.with_suffix(".png")))
            print(f"rendered {src.stem}")
        browser.close()


if __name__ == "__main__":
    main()
