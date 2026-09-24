"""Copy elkjs into the built site.

The simulator's layout engine is 1.6 MB and only eleven pages carry a model, so it is not in
``extra_javascript``: the renderer fetches it itself the first time a model is scrolled to,
from ``assets/js/vendor/elk.bundled.js`` beside its own script. That URL has to exist in the
built site, and the bundle used to exist only as a file somebody had copied into the source
tree by hand -- untracked, unignored, and absent from a fresh clone, which built a widget that
could not lay anything out. It comes from the environment now, and lands in ``site_dir`` on
``on_post_build``, so the source tree carries none of it and ``mkdocs serve`` gets it too.

The licences travel with it. conda-forge declares elkjs as ``EPL-2.0 AND EPL-1.0 AND Apache-2.0
AND Apache-1.1 AND MIT`` and ships eight files for it, because the ``lib/`` bundles carry ELK's
Java dependencies through GWT and code from its JavaScript bundler, where the npm tarball
carries ELK's own licence alone. Those eight are in the package's ``info/licenses``, which is
in the package cache rather than in the prefix, so the environment's own conda-meta record is
what points at them; failing that we ship the one licence the prefix does hold and say so,
rather than serving the bundle with less than it came with.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path

log = logging.getLogger("mkdocs.hooks.elkjs")

#: Where the recipe's ``npm install --global`` puts the package inside the prefix.
PACKAGE = Path("lib/node_modules/elkjs")
BUNDLE = PACKAGE / "lib/elk.bundled.js"
#: Where the renderer looks for it, relative to its own script.
VENDOR = Path("assets/js/vendor")


def _prefix() -> Path | None:
    """Find the environment holding elkjs: the active one, or the workspace's mkdocs environment.

    ``mkdocs build`` is normally run through pixi, which sets ``CONDA_PREFIX``; running it
    from an activated shell or straight out of the environment's own ``bin`` is common enough
    to be worth the second guess.
    """
    candidates = []
    if os.environ.get("CONDA_PREFIX"):
        candidates.append(Path(os.environ["CONDA_PREFIX"]))
    candidates.append(Path(__file__).resolve().parent.parent / ".pixi/envs/mkdocs")
    for prefix in candidates:
        if (prefix / BUNDLE).is_file():
            return prefix
    return None


def _licences(prefix: Path) -> list[Path]:
    """Collect every licence file the package was built with, or the one the prefix holds."""
    meta = sorted((prefix / "conda-meta").glob("elkjs-*.json"))
    for record in meta:
        try:
            cached = json.loads(record.read_text()).get("extracted_package_dir")
        except (OSError, ValueError):
            continue
        if not cached:
            continue
        licences = Path(cached) / "info/licenses"
        if licences.is_dir():
            return sorted(p for p in licences.iterdir() if p.is_file())
    own = prefix / PACKAGE / "LICENSE.md"
    if own.is_file():
        log.warning(
            "elkjs: shipping %s alone. The package's other licence files are in its "
            "info/licenses, which is in the package cache rather than the environment, and "
            "this build could not reach it.",
            own.name,
        )
        return [own]
    return []


def on_post_build(config, **kwargs) -> None:
    """Copy the bundle and its licence files into the built site."""
    prefix = _prefix()
    if prefix is None:
        log.warning(
            "elkjs is not in this environment, so the built site carries no layout engine and "
            "every model will sit at its loading bar. `pixi install -e mkdocs` puts it there."
        )
        return
    out = Path(config["site_dir"]) / VENDOR
    out.mkdir(parents=True, exist_ok=True)
    shutil.copy2(prefix / BUNDLE, out / BUNDLE.name)
    for licence in _licences(prefix):
        shutil.copy2(licence, out / licence.name)
    log.info("elkjs: copied %s into %s", BUNDLE.name, VENDOR)
