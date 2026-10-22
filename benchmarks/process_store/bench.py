"""Benchmark flat vs tree storage of CWL processes against a real database.

Ingests a corpus (see ``corpus.py``) in submission order into two schemas:

* ``<db>_flat``: what DiracX does (``diracx.logic.transformation.processes``),
  one ``Processes`` row per transformation holding its packed subgraph;
* ``<db>_tree``: the DX-ADR-004 alternative, one ``Processes`` row per node
  plus ``ProcessReferences`` edges, materialised with a recursive CTE.

Then measures what a dispatcher pays to materialise a parcel's payload on a
cache miss, sequentially and with concurrent clients, and the on-disk size of
each schema.

Usage:
    python bench.py <corpus> --db-url mysql+aiomysql://root:pw@127.0.0.1:3306/bench
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import statistics
import time
from pathlib import Path
from typing import Any

from corpus import load
from sqlalchemy import BINARY, literal, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from diracx.core.cwl import (
    CanonicalProcess,
    canonical_bytes,
    check_policy,
    decompose,
    pack,
    parse_reference,
    unpack,
)
from diracx.db.sql import TransformationDB
from diracx.db.sql.transformation.db import _insert_ignore
from diracx.db.sql.transformation.schema import Processes, ProcessReferences
from diracx.logic.transformation import processes
from diracx.logic.transformation.processes import load_process, store_process

# ---------------------------------------------------------------------------
# The tree layout, on the DX-ADR-004 tables, for comparison only
# ---------------------------------------------------------------------------


async def store_tree(db: TransformationDB, process: dict[str, Any]) -> bytes:
    # The same acceptance rules as store_process, so both layouts do the same
    # work and store the same corpus
    root, nodes = decompose(process)
    check_policy(nodes)
    if await db.get_processes([root]):
        return root
    referenced = {child for node in nodes.values() for child in node.children}
    for process_id in sorted(referenced - set(nodes)):
        nodes.update(unpack(await load_tree(db, process_id))[1])
    processes.validate_packed(pack(root, nodes))
    await db.insert_processes({pid: json.loads(n.body) for pid, n in nodes.items()})
    edges = [
        {"parent_process_id": pid, "child_process_id": c}
        for pid, n in nodes.items()
        for c in n.children
    ]
    if edges:
        await db.conn.execute(_insert_ignore(ProcessReferences).values(edges))
    return root


async def load_tree(db: TransformationDB, process_id: bytes) -> dict[str, Any]:
    tree = select(literal(process_id, BINARY(32)).label("ProcessID")).cte(
        "tree", recursive=True
    )
    tree = tree.union(
        select(ProcessReferences.child_process_id).join(
            tree, ProcessReferences.parent_process_id == tree.c.ProcessID
        )
    )
    stmt = select(Processes.process_id, Processes.process).join(
        tree, Processes.process_id == tree.c.ProcessID
    )
    nodes = {}
    for pid, body in await db.conn.execute(stmt):
        children = {
            parse_reference(step["run"])
            for step in (body.get("steps") or {}).values()
            if isinstance(step.get("run"), str)
        }
        nodes[pid] = CanonicalProcess(
            pid, canonical_bytes(body), tuple(sorted(children))
        )
    return pack(process_id, nodes)


LAYOUTS = {"flat": (store_process, load_process), "tree": (store_tree, load_tree)}

# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


async def make_db(url: str, schema: str) -> TransformationDB:
    # URL.set ignores None, so connect to the system schema to create ours
    server = make_url(url).set(database="mysql")
    engine = create_async_engine(server)
    async with engine.begin() as conn:
        await conn.execute(text(f"DROP DATABASE IF EXISTS `{schema}`"))
        await conn.execute(text(f"CREATE DATABASE `{schema}`"))
    await engine.dispose()
    target = make_url(url).set(database=schema)
    return TransformationDB(target.render_as_string(hide_password=False))


async def ingest(db: TransformationDB, layout: str, corpus: Path, batch: int) -> dict:
    store, _ = LAYOUTS[layout]
    ids: list[bytes] = []
    timings: list[tuple[float, int]] = []
    rejected: list[str] = []
    pending: list[dict[str, Any]] = []
    submitted = 0
    for sub in load(corpus):
        pending.extend(sub.transformations)
        if len(pending) < batch:
            continue
        timings.append(await _ingest_batch(db, store, pending, ids, rejected))
        submitted += len(pending)
        pending = []
    if pending:
        timings.append(await _ingest_batch(db, store, pending, ids, rejected))
        submitted += len(pending)
    total = sum(t for t, _ in timings)
    return {
        "transformations": submitted,
        "seconds": round(total, 2),
        "per_transformation_ms": round(1e3 * total / max(submitted, 1), 3),
        "rejected": len(rejected),
        "ids": ids,
    }


async def _ingest_batch(db, store, runs, ids, rejected) -> tuple[float, int]:
    start = time.perf_counter()
    async with db as conn_db:  # one transaction per batch, like one submission
        for run in runs:
            try:
                ids.append(await store(conn_db, run))
            except ValueError as e:
                rejected.append(str(e))
    return time.perf_counter() - start, len(runs)


async def lookups(
    db: TransformationDB, layout: str, ids: list[bytes], n: int, clients: int
) -> dict:
    _, load_fn = LAYOUTS[layout]
    sample = [random.choice(ids) for _ in range(n)]
    latencies: list[float] = []

    async def client(keys):
        for key in keys:
            start = time.perf_counter()
            async with db as conn_db:
                await load_fn(conn_db, key)
            latencies.append(time.perf_counter() - start)

    for key in sample[:50]:  # warm the buffer pool and the connection pool
        async with db as conn_db:
            await load_fn(conn_db, key)
    start = time.perf_counter()
    await asyncio.gather(*(client(sample[i::clients]) for i in range(clients)))
    wall = time.perf_counter() - start
    latencies.sort()
    return {
        "clients": clients,
        "lookups": n,
        "p50_us": round(1e6 * statistics.median(latencies)),
        "p99_us": round(1e6 * latencies[int(0.99 * len(latencies))]),
        "per_second": round(n / wall),
    }


async def table_sizes(db: TransformationDB, schema: str) -> dict:
    async with db as conn_db:
        await conn_db.conn.execute(text("ANALYZE TABLE Processes, ProcessReferences"))
        rows = await conn_db.conn.execute(
            text(
                "SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH "
                "FROM information_schema.TABLES WHERE TABLE_SCHEMA = :s "
                "AND TABLE_NAME IN ('Processes', 'ProcessReferences')"
            ),
            {"s": schema},
        )
        return {
            name: {"rows": r, "data_bytes": d, "index_bytes": i}
            for name, r, d, i in rows
        }


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("corpus", type=Path)
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--layouts", default="flat,tree")
    parser.add_argument(
        "--batch", type=int, default=50, help="Transformations per transaction"
    )
    parser.add_argument("--lookups", type=int, default=5000)
    parser.add_argument("--clients", default="1,8")
    parser.add_argument("--json", type=Path)
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate new processes with cwltool, as DiracX does; off by default "
        "so that both layouts are timed on storage alone",
    )
    args = parser.parse_args()
    random.seed(1)
    if not args.validate:
        processes.validate_packed = lambda packed: None

    results = {}
    base = make_url(args.db_url).database or "bench"
    for layout in args.layouts.split(","):
        schema = f"{base}_{layout}"
        db = await make_db(args.db_url, schema)
        async with db.engine_context():
            async with db.engine.begin() as conn:
                await conn.run_sync(db.metadata.create_all)
            ingested = await ingest(db, layout, args.corpus, args.batch)
            ids = sorted(set(ingested.pop("ids")))
            result = {
                "ingest": ingested,
                "distinct_roots": len(ids),
                "lookups": [
                    await lookups(db, layout, ids, args.lookups, clients)
                    for clients in map(int, args.clients.split(","))
                ],
                "tables": await table_sizes(db, schema),
            }
        results[layout] = result
        print(layout, json.dumps(result, indent=1))
    if args.json:
        args.json.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
