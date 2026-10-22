# Process store benchmarks

How the content-addressed CWL process store (DX-ADR-004, DX-ADR-007) behaves
on a realistic volume of submissions, and how the flat layout DiracX uses
compares with the tree layout the ADR first described.

- **flat** (`diracx.logic.transformation.processes`): one `Processes` row per
    transformation, holding its whole subgraph as a packed `$graph` document.
    Materialising a parcel's payload is one primary-key read.
- **tree** (in `bench.py` only, on the same DX-ADR-004 tables): one
    `Processes` row per CWL process, child `run` bodies replaced by references,
    edges in `ProcessReferences`, materialised with a recursive CTE.

Both key a process by the Merkle root of its canonical tree
(`diracx.core.cwl`), so the identity of a transformation does not depend on
the layout.

## Corpus

A directory of submitted workgraphs as CWL JSON, grouped by submission:

```
<corpus>/<kind>/<submission>/summary.json   # {"date": "YYYY-MM-DD", "kind": "ap" | "mc", ...}
<corpus>/<kind>/<submission>/**/*.cwl.json  # one workgraph each, run bodies inline
```

Generating one is VO-specific. For LHCb, every AnalysisProductions tag and
every mc-requests merge of a year was converted with LbAPCommon's
`prod_request_to_cwl` (the converter `lb-ap test` uses).

## Running

```bash
# What a year of submissions looks like
pixi run python benchmarks/process_store/corpus.py <corpus> --json report.json

# Ingest and lookup benchmark against MySQL or MariaDB
docker run -d --name bench-mysql -e MYSQL_ROOT_PASSWORD=bench -p 33307:3306 mysql:8.4
pixi run python benchmarks/process_store/bench.py <corpus> \
    --db-url mysql+aiomysql://root:bench@127.0.0.1:33307/bench --json bench.json
```

`bench.py` drops and recreates the `<db>_flat` and `<db>_tree` schemas.
Lookups are timed from Python, so they include the driver round trip and JSON
decoding: they are what a dispatcher pays on a payload cache miss. Both
layouts are timed on storage alone; pass `--validate` to include the cwltool
validation DiracX runs on every process not already stored.

## Results

A year of LHCb submissions, 2025-10-19 to 2026-10-19: every
AnalysisProductions tag (2,567 tags, 5,545 productions, of which 267 no longer
render with today's LbAPCommon) and every mc-requests merge (728). MySQL 8.4
in Docker on a laptop, default 128 MiB buffer pool.

|                                        | flat             | tree                   |
| -------------------------------------- | ---------------- | ---------------------- |
| workgraphs / transformations submitted | 84,406 / 182,071 | same                   |
| ... of which analysis productions      | 71,851 / 144,474 | same                   |
| `Processes` rows                       | 89,779           | 169,872                |
| canonical bytes (zstd)                 | 319 MB (121 MB)  | 258 MB (124 MB)        |
| on disk (InnoDB data + index)          | 623 MiB          | 504 MiB + 32 MiB edges |
| ingest per transformation              | 2.15 ms          | 2.33 ms                |
| lookup p50 / p99, 1 client             | 2.1 / 6.6 ms     | 2.3 / 9.5 ms           |
| lookups per second, 1 / 8 clients      | 385 / 2,787      | 343 / 2,169            |

Flat storage needs half the rows and is faster to read, by about 10% for one
client and 28% under concurrency, for about 14% more disk, a difference
compression all but removes. The largest flat row of the year is 26 kB
(median 3 kB, p99 9 kB).

Validating a process with cwltool takes a median of 16 ms (analysis
productions) and 19 ms (simulation) once the CWL schema is loaded, and all of
a random sample of 301 distinct processes of the year validated. Since only
processes not already stored are validated, a year costs about 25 CPU-minutes.

Simulation alone (12,555 workgraphs, 37,597 transformations) fits in the
buffer pool and is faster on both layouts: flat 13,328 rows, 1.4 / 4.8 ms
p50 / p99 and 2,672 lookups per second with 8 clients; tree 27,547 rows,
2.4 / 5.7 ms and 1,514.
