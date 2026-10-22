"""Load a corpus of submitted CWL workgraphs and summarise a year of them.

The corpus layout is what ``generate/`` produces from the AnalysisProductions
tags and the mc-requests merges:

    <corpus>/ap/<tag>/summary.json, <tag>/<production>/<request>.cwl.json
    <corpus>/mc/<commit>/summary.json, <commit>/<request>.cwl.json

Each ``summary.json`` carries the submission ``date`` and ``kind``. Each
``.cwl.json`` is one workgraph; the inline ``run`` bodies of its steps are the
transformations that would be stored.

Usage:
    python corpus.py <corpus> [--json report.json]
"""

from __future__ import annotations

import argparse
import json
import statistics
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from diracx.core.cwl import canonical_bytes, decompose, pack


@dataclass(frozen=True)
class Submission:
    """One submitted workgraph.

    Attributes:
        date: Submission date, ``YYYY-MM-DD``.
        kind: ``ap`` or ``mc``.
        source: Tag or commit it came from.
        name: File name of the workgraph.
        size: Size of the workgraph as generated, in bytes.
        transformations: Run bodies of its steps.
    """

    date: str
    kind: str
    source: str
    name: str
    size: int
    transformations: tuple[dict[str, Any], ...]


def load(corpus: Path) -> Iterator[Submission]:
    """Yield every workgraph of a corpus, in submission order.

    Args:
        corpus: Root directory of the corpus.

    Yields:
        The submissions, sorted by date.
    """
    batches = []
    for summary_path in corpus.glob("*/*/summary.json"):
        summary = json.loads(summary_path.read_text())
        batches.append((summary["date"], summary["kind"], summary_path.parent))
    for date, kind, directory in sorted(batches):
        for path in sorted(directory.rglob("*.cwl.json")):
            text = path.read_text()
            workgraph = json.loads(text)
            runs = tuple(
                step["run"]
                for step in _steps(workgraph)
                if isinstance(step.get("run"), dict)
            )
            yield Submission(date, kind, directory.name, path.name, len(text), runs)


def _steps(workgraph: dict[str, Any]) -> list[dict[str, Any]]:
    steps = workgraph.get("steps") or []
    if isinstance(steps, dict):
        return [{"id": k, **v} for k, v in steps.items()]
    return steps


def compress(data: bytes) -> int:
    """Return the compressed size, zstd if available, else deflate."""
    try:
        import zstandard

        return len(zstandard.ZstdCompressor(level=3).compress(data))
    except ImportError:
        return len(zlib.compress(data, 6))


def report(corpus: Path) -> dict[str, Any]:
    """Summarise a corpus as a year of submissions.

    Args:
        corpus: Root directory of the corpus.

    Returns:
        Totals, per-month and per-kind breakdowns, and what each storage
        layout would hold.
    """
    months: dict[str, Counter] = defaultdict(Counter)
    kinds: dict[str, Counter] = defaultdict(Counter)
    flat: dict[bytes, bytes] = {}
    tree: dict[bytes, bytes] = {}
    fanout: Counter = Counter()
    sizes = []
    errors: Counter = Counter()
    for sub in load(corpus):
        month = sub.date[:7]
        months[month]["workgraphs"] += 1
        kinds[sub.kind]["workgraphs"] += 1
        kinds[sub.kind]["workgraph_bytes"] += sub.size
        for run in sub.transformations:
            try:
                root, nodes = decompose(run)
            except ValueError as e:
                errors[str(e)[:80]] += 1
                continue
            months[month]["transformations"] += 1
            kinds[sub.kind]["transformations"] += 1
            if root not in flat:
                body = canonical_bytes(pack(root, nodes))
                flat[root] = body
                sizes.append(len(body))
                months[month]["new_flat_rows"] += 1
                months[month]["new_flat_bytes"] += len(body)
                kinds[sub.kind]["flat_rows"] += 1
                kinds[sub.kind]["flat_bytes"] += len(body)
            for pid, node in nodes.items():
                fanout[pid] += 1
                tree.setdefault(pid, node.body)
    flat_bytes = sum(len(b) for b in flat.values())
    tree_bytes = sum(len(b) for b in tree.values())
    return {
        "totals": {
            "workgraphs": sum(m["workgraphs"] for m in months.values()),
            "transformations": sum(m["transformations"] for m in months.values()),
            "flat": {
                "rows": len(flat),
                "bytes": flat_bytes,
                "compressed": sum(compress(b) for b in flat.values()),
            },
            "tree": {
                "rows": len(tree),
                "bytes": tree_bytes,
                "compressed": sum(compress(b) for b in tree.values()),
            },
            "flat_row_bytes": {
                "median": statistics.median(sizes) if sizes else 0,
                "p99": sorted(sizes)[int(len(sizes) * 0.99)] if sizes else 0,
                "max": max(sizes, default=0),
            },
            "most_shared_nodes": [n for _, n in fanout.most_common(5)],
            "rejected": dict(errors),
        },
        "by_kind": {k: dict(v) for k, v in sorted(kinds.items())},
        "by_month": {k: dict(v) for k, v in sorted(months.items())},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--json", type=Path, help="Also write the report here")
    args = parser.parse_args()
    result = report(args.corpus)
    if args.json:
        args.json.write_text(json.dumps(result, indent=1))
    print(json.dumps(result["totals"], indent=1))
    print(json.dumps(result["by_kind"], indent=1))
    print(
        f"{'month':<9}{'workgraphs':>12}{'transf.':>10}{'new rows':>10}{'new KB':>10}"
    )
    for month, m in result["by_month"].items():
        print(
            f"{month:<9}{m['workgraphs']:>12}{m['transformations']:>10}"
            f"{m['new_flat_rows']:>10}{m['new_flat_bytes'] / 1024:>10.0f}"
        )


if __name__ == "__main__":
    main()
