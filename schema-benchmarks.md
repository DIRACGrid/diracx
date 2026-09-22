# Schema benchmarks to run before implementation

Working notes. Each entry is a decision already taken in the ADRs that rests on a
performance argument rather than a measurement. Measure, then either keep the decision
or reopen it.

## 1. Clustered primary keys on the bulk tables (DX-ADR-004)

**The decision.** `Inputs`, `ParcelInputs` and `EdgeOutputs` are keyed on
`(TransformationID, <UUIDv7>)` rather than the UUID alone, so one transformation's rows
are contiguous in the clustered index. `Parcels` keeps a bare `ParcelID`, because user
parcels have no transformation.

**The argument being tested.** InnoDB records insert direction per page, so each
transformation's right-hand edge should split at the insertion point and stay nearly
full, the same way a single append-only stream does. If that holds, the cost of the
composite key is buffer-pool residency for one hot page per active transformation, plus
the width of a `TransformationID` on every secondary index entry. The benefit is that
per-transformation scans and the cleaning delete become range operations instead of
touching pages across the whole table.

**What to measure.**

- Insert throughput and page fill factor with 1, 10, 100 and 1000 transformations
    inserting concurrently, composite key against bare UUIDv7. Read the fill factor from
    `information_schema` or `innodb_ruby`, not from the row count.
- Wall-clock and I/O for `DELETE FROM Inputs WHERE TransformationID = ?` on a table
    holding 10⁸ rows, for a transformation with 10⁶ of them, both key layouts.
- Total index size for both layouts at 10⁸ rows, since the composite key widens every
    secondary index.
- The same three on MariaDB as well as MySQL 8.0, since the split heuristics are the
    most likely place the two diverge.

**What would reopen the decision.** Insert throughput falling off as the number of
concurrent transformations grows, or page fill dropping towards half, would mean the
direction heuristic is not surviving the interleaving, and the per-transformation table
sketch comes back.

## 2. Counter journal insert rate (DX-ADR-009)

`JournalID` is a plain `AUTO_INCREMENT` on the assumption that the allocation mutex is
not a bottleneck and the right-most index page is the real limit. Measure the sustained
insert rate against the peak transition rate, on MariaDB's consecutive lock mode as
well as MySQL's interleaved mode. Hash partitioning is the fallback and should be
measured at the same time, since the journal has no foreign keys.

## 3. Edge output volume (DX-ADR-004)

`EdgeOutputs` holds one row per output file per consuming edge. Measure the row count
and index size for a realistic fan-out, an LHCb simulation workgraph feeding
reconstruction and a removal transformation, and compare against a producer-side table
carrying one cursor per consumer.

## 4. Feeder reconciliation cost (DX-ADR-006)

The feeder watermark is allowed to miss rows, and a reconciliation action at
`Finalizing` is what makes that safe. Measure a full re-evaluation of a realistic
bookkeeping query against a transformation holding 10⁶ inputs, since the reconciliation
is what decides whether the watermark can be cheap.
