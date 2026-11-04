"""Table declarations and operations for journalled counters (DX-ADR-009).

A journalled counter is declared by a *key*, the columns the counts are grouped
by, and one or more *measures*, the additive quantities it sums. From that
declaration this module generates the two tables of the counter:

* ``<Name>Counters``: one row per key, holding the folded measures.
* ``<Name>CounterJournal``: the signed deltas appended by writers in the same
  transaction as the change they describe, waiting to be folded.

plus the ``CounterLocks`` table shared by every counter of a database, whose
rows serialise the fold and rebuild operations of each counter.

Neither generated table has a foreign key to the table being counted, so that
appending a delta never takes a lock on a row it does not own.

Usage
-----

Declaring a counter (in a schema module)::

    MY_COUNTER = journalled_counter(
        Base.metadata,
        "File",
        key=[("DatasetID", BigInteger()), ("Status", String(32))],
        measures=["Count", "Bytes"],
    )

Writing deltas (inside the caller's transaction)::

    await MY_COUNTER.journal(
        conn,
        [
            {"DatasetID": 7, "Status": "New", "CountDelta": -2, "BytesDelta": -30},
            {"DatasetID": 7, "Status": "Copied", "CountDelta": +2, "BytesDelta": +30},
        ],
    )

Reading (inside the caller's transaction)::

    rows = await MY_COUNTER.read(conn)
    # Each row has the key columns, one column per measure, and UpdatedAt.

Folding (from a periodic task, opens its own transaction)::

    n = await MY_COUNTER.fold(engine, batch_size=5000)
    if n == batch_size:
        # Journal had more rows than the batch; schedule another fold.
        ...

Rebuilding (from a maintenance task, opens its own transaction)::

    counted = [
        CountedTable(
            Files,
            key_exprs={"DatasetID": Files.c.DatasetID, "Status": Files.c.Status},
            measure_exprs={"Count": func.count(), "Bytes": func.sum(Files.c.Size)},
        )
    ]
    await MY_COUNTER.rebuild(engine, counted)

Deleting a scope (after counted rows are already unwritable and deleted)::

    await MY_COUNTER.delete_scope(engine, {"DatasetID": 7})
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Column,
    Enum,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    cast,
    delete,
    func,
    insert,
    literal_column,
    or_,
    select,
    union_all,
)
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import FromClause
from sqlalchemy.types import TypeEngine

from .functions import utcnow_us
from .types import MicrosecondDateTime

logger = logging.getLogger(__name__)

COUNTER_LOCKS_TABLE = "CounterLocks"
COUNTERS_INFO_KEY = "journalled_counters"


class CounterLockOperation(StrEnum):
    """Operations serialised by the ``CounterLocks`` table."""

    FOLD = "fold"
    REBUILD = "rebuild"


# SQLite only auto-increments a column declared exactly as INTEGER PRIMARY KEY
BIGINT_AUTOINCREMENT = BigInteger().with_variant(Integer(), "sqlite")

# Session temporary table used by fold() to hold claimed journal IDs
_FOLD_CLAIM = Table(
    "FoldClaim",
    MetaData(),
    Column("JournalID", BIGINT_AUTOINCREMENT, primary_key=True),
    prefixes=["TEMPORARY"],
)


def _is_sqlite(conn_or_engine: AsyncConnection | AsyncEngine) -> bool:
    return conn_or_engine.dialect.name == "sqlite"


@dataclass(frozen=True)
class CountedTable:
    """Source table declaration for ``rebuild()``.

    Attributes:
        table: The SQLAlchemy ``FromClause`` being counted (a ``Table``,
            ``Join``, or subquery).
        key_exprs: Mapping from counter key name to the column expression
            that recounts it on this table.
        measure_exprs: Mapping from measure name to the aggregate expression
            that recounts it (e.g. ``func.count()``, ``func.sum(col)``).
    """

    table: FromClause
    key_exprs: dict[str, ColumnElement]
    measure_exprs: dict[str, ColumnElement]


@dataclass(frozen=True)
class JournalledCounter:
    """The tables generated for one journalled counter.

    Attributes:
        name: Name of the counter, as used in ``CounterLocks``.
        key: Names of the key columns, in primary-key order.
        measures: Names of the measures summed by the counter.
        counters_table: Table of folded counts, one row per key.
        journal_table: Table of signed deltas waiting to be folded.
    """

    name: str
    key: tuple[str, ...]
    measures: tuple[str, ...]
    counters_table: Table
    journal_table: Table

    @staticmethod
    def delta_column(measure: str) -> str:
        """Name of the journal column carrying the deltas of a measure.

        Args:
            measure: Name of the measure.

        Returns:
            The journal column name.
        """
        return f"{measure}Delta"

    def lock_rows(self) -> list[dict[str, str]]:
        """Rows of ``CounterLocks`` that must exist for this counter.

        Returns:
            One row per serialised operation of the counter.
        """
        return [
            {"CounterName": self.name, "Operation": op.value}
            for op in CounterLockOperation
        ]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_scope(self, scope: Mapping[str, Any]) -> None:
        scope_keys = list(scope.keys())
        prefix = list(self.key[: len(scope_keys)])
        if scope_keys != prefix:
            raise ValueError(
                f"Scope keys {scope_keys} must be a prefix of "
                f"the counter key {list(self.key)}"
            )

    def _scope_where(self, table: Table, scope: Mapping[str, Any]) -> ColumnElement:
        return and_(*(table.c[k] == v for k, v in scope.items()))

    @asynccontextmanager
    async def _locked_txn(
        self,
        engine: AsyncEngine,
        operations: Sequence[CounterLockOperation],
        isolation_level: str,
    ) -> AsyncIterator[AsyncConnection]:
        """Open a transaction, set isolation, and acquire counter locks.

        On SQLite the isolation level and locks are skipped (single-writer
        serialisation is sufficient for testing).
        """
        sqlite = _is_sqlite(engine)
        if not sqlite:
            engine = engine.execution_options(isolation_level=isolation_level)
        async with engine.begin() as conn:
            if not sqlite:
                locks = self.counters_table.metadata.tables[COUNTER_LOCKS_TABLE]
                for operation in operations:
                    await conn.execute(
                        select(literal_column("1"))
                        .select_from(locks)
                        .where(
                            locks.c.CounterName == self.name,
                            locks.c.Operation == operation,
                        )
                        .with_for_update()
                    )
            yield conn

    # ------------------------------------------------------------------
    # Operations (DX-ADR-009)
    # ------------------------------------------------------------------

    async def journal(self, conn: AsyncConnection, deltas: Sequence[dict]) -> None:
        """Append pre-aggregated delta rows to the journal.

        Must be called inside the same transaction that mutates the counted
        rows.  Each dict maps key column names to their values and delta
        column names (``<measure>Delta``) to signed integers.

        Args:
            conn: Connection with an active transaction.
            deltas: One row per key touched by the caller's change.
        """
        if not deltas:
            return
        await conn.execute(insert(self.journal_table), list(deltas))

    async def read(
        self,
        conn: AsyncConnection,
        scope: Mapping[str, Any] | None = None,
    ) -> Sequence:
        """Return exact counts by summing folded counters and the journal.

        The result is exact at any aggregation lag because it combines both
        halves in a single SQL statement.

        Callers should pass a ``scope`` that fixes a prefix of the key
        (e.g. a single ``TransformationID``).  An unscoped read scans
        both full tables, which is prohibitively expensive for counters
        spanning many scopes.

        Args:
            conn: Connection with an active transaction.
            scope: Optional key-prefix dict applied to both halves.

        Returns:
            Rows of ``(key…, measure…, UpdatedAt)``. A key with no counter
            row and no journal rows is absent; callers treat absence as zero.
        """
        if scope:
            self._validate_scope(scope)

        ct = self.counters_table
        jt = self.journal_table

        counter_q = select(
            *[ct.c[k] for k in self.key],
            *[ct.c[m].label(m) for m in self.measures],
            ct.c.UpdatedAt,
        )
        journal_q = select(
            *[jt.c[k] for k in self.key],
            *[jt.c[self.delta_column(m)].label(m) for m in self.measures],
            jt.c.InsertedAt.label("UpdatedAt"),
        )

        if scope:
            counter_q = counter_q.where(self._scope_where(ct, scope))
            journal_q = journal_q.where(self._scope_where(jt, scope))

        combined = union_all(counter_q, journal_q).subquery("t")

        key_refs = [combined.c[k] for k in self.key]
        aggs = [
            cast(func.sum(combined.c[m]), BigInteger).label(m) for m in self.measures
        ]
        aggs.append(func.max(combined.c.UpdatedAt).label("UpdatedAt"))

        stmt = select(*key_refs, *aggs).group_by(*key_refs)

        result = await conn.execute(stmt)
        return result.all()

    async def fold(self, engine: AsyncEngine, batch_size: int = 5000) -> int:
        """Claim committed journal rows, fold them into counters, and delete.

        Opens its own transaction at READ COMMITTED (record locks only on
        MySQL/MariaDB, no-op on SQLite).

        Claimed journal IDs are loaded into a ``FoldClaim`` session temporary
        table via ``INSERT … SELECT … FOR UPDATE SKIP LOCKED`` so that the
        aggregation and delete JOIN on it.

        Args:
            engine: Database engine.
            batch_size: Maximum journal rows to fold in one transaction.

        Returns:
            Number of journal rows folded. If equal to ``batch_size``, the
            caller should schedule another fold immediately.
        """
        jt = self.journal_table
        ct = self.counters_table

        async with self._locked_txn(
            engine, [CounterLockOperation.FOLD], "READ COMMITTED"
        ) as conn:
            sqlite = _is_sqlite(conn)

            # Create/clear the session temp table
            await conn.execute(CreateTable(_FOLD_CLAIM, if_not_exists=True))
            await conn.execute(delete(_FOLD_CLAIM))

            # Claim committed rows directly into FoldClaim
            claim_q = select(jt.c.JournalID).order_by(jt.c.JournalID).limit(batch_size)
            if not sqlite:
                claim_q = claim_q.with_for_update(skip_locked=True)
            await conn.execute(insert(_FOLD_CLAIM).from_select(["JournalID"], claim_q))

            n_claimed = (
                await conn.execute(select(func.count()).select_from(_FOLD_CLAIM))
            ).scalar()
            if not n_claimed:
                return 0

            # Upsert aggregated deltas into counters (JOIN FoldClaim)
            key_cols = [jt.c[k] for k in self.key]
            agg_measures = [
                func.sum(jt.c[self.delta_column(m)]).label(m) for m in self.measures
            ]
            agg_time = func.max(jt.c.InsertedAt).label("UpdatedAt")

            agg_q = (
                select(*key_cols, *agg_measures, agg_time)
                .select_from(
                    jt.join(
                        _FOLD_CLAIM,
                        jt.c.JournalID == _FOLD_CLAIM.c.JournalID,
                    )
                )
                .group_by(*key_cols)
            )

            col_names = list(self.key) + list(self.measures) + ["UpdatedAt"]

            if sqlite:
                ins = sqlite_insert(ct).from_select(col_names, agg_q)
                set_ = {m: ct.c[m] + ins.excluded[m] for m in self.measures}
                set_["UpdatedAt"] = func.max(ct.c.UpdatedAt, ins.excluded.UpdatedAt)
                await conn.execute(
                    ins.on_conflict_do_update(
                        index_elements=list(self.key),
                        set_=set_,
                    )
                )
            else:
                ins = mysql_insert(ct).from_select(col_names, agg_q)
                on_dup = {m: ct.c[m] + ins.inserted[m] for m in self.measures}
                on_dup["UpdatedAt"] = func.greatest(
                    ct.c.UpdatedAt, ins.inserted.UpdatedAt
                )
                await conn.execute(ins.on_duplicate_key_update(**on_dup))

            # Delete folded journal rows
            await conn.execute(
                delete(jt).where(jt.c.JournalID.in_(select(_FOLD_CLAIM.c.JournalID)))
            )

            return n_claimed

    async def rebuild(
        self,
        engine: AsyncEngine,
        counted: Sequence[CountedTable],
        scope: Mapping[str, Any] | None = None,
    ) -> int:
        """Recount from source tables and write correction deltas.

        Opens its own transaction at REPEATABLE READ (snapshot covers the
        lock and all three reads on MySQL; no-op on SQLite).

        .. warning::

           An unscoped rebuild (``scope=None``) holds a REPEATABLE READ
           snapshot for the duration of the full recount.  Every journal
           row a concurrent fold deletes stays as a delete-marked record
           until the snapshot closes, degrading reads system-wide.
           Prefer scoped rebuilds; use unscoped only as a maintenance
           operation on a quiet system.

        Args:
            engine: Database engine.
            counted: Source table declarations.
            scope: Optional key-prefix dict.

        Returns:
            Number of correction deltas written.
        """
        if scope:
            self._validate_scope(scope)
        else:
            logger.warning(
                "Unscoped rebuild of %s: holds a long read snapshot that "
                "degrades journal reads system-wide",
                self.name,
            )

        ct = self.counters_table
        jt = self.journal_table

        async with self._locked_txn(
            engine, [CounterLockOperation.REBUILD], "REPEATABLE READ"
        ) as conn:
            branches: list = []

            # Recount branches (one per counted table)
            for ct_decl in counted:
                unknown = set(ct_decl.key_exprs) - set(self.key)
                if unknown:
                    raise ValueError(
                        f"CountedTable key_exprs has unknown keys: {unknown}"
                    )
                key_selects = [ct_decl.key_exprs[k].label(k) for k in self.key]
                measure_selects = []
                for m in self.measures:
                    expr = ct_decl.measure_exprs.get(m)
                    if expr is not None:
                        measure_selects.append(expr.label(m))
                    else:
                        measure_selects.append(literal_column("0").label(m))

                branch = select(*key_selects, *measure_selects).select_from(
                    ct_decl.table
                )
                if scope:
                    branch = branch.where(
                        and_(*(ct_decl.key_exprs[k] == v for k, v in scope.items()))
                    )
                branch = branch.group_by(*[ct_decl.key_exprs[k] for k in self.key])
                branches.append(branch)

            # Negate counters
            counter_branch = select(
                *[ct.c[k] for k in self.key],
                *[(-ct.c[m]).label(m) for m in self.measures],
            )
            if scope:
                counter_branch = counter_branch.where(self._scope_where(ct, scope))
            branches.append(counter_branch)

            # Negate journal
            journal_branch = select(
                *[jt.c[k] for k in self.key],
                *[(-jt.c[self.delta_column(m)]).label(m) for m in self.measures],
            )
            if scope:
                journal_branch = journal_branch.where(self._scope_where(jt, scope))
            branches.append(journal_branch)

            combined = union_all(*branches).subquery("t")
            key_refs = [combined.c[k] for k in self.key]

            having_clause = or_(*(func.sum(combined.c[m]) != 0 for m in self.measures))

            corrections_q = (
                select(
                    *key_refs,
                    *[
                        cast(func.sum(combined.c[m]), BigInteger).label(
                            self.delta_column(m)
                        )
                        for m in self.measures
                    ],
                )
                .group_by(*key_refs)
                .having(having_clause)
            )

            # Write corrections directly into the journal
            col_names = list(self.key) + [self.delta_column(m) for m in self.measures]
            result = await conn.execute(
                insert(jt).from_select(col_names, corrections_q)
            )
            return result.rowcount

    async def delete_scope(
        self,
        engine: AsyncEngine,
        scope: Mapping[str, Any],
    ) -> None:
        """Remove all counter and journal rows matching a scope.

        The scope's counted rows must already be unwritable and deleted by the
        caller.  Opens its own short transaction at READ COMMITTED.

        Args:
            engine: Database engine.
            scope: Key-prefix dict (e.g. ``{"DatasetID": 7}``).
        """
        if not scope:
            raise ValueError("delete_scope requires a non-empty scope")
        self._validate_scope(scope)

        ct = self.counters_table
        jt = self.journal_table

        async with self._locked_txn(
            engine,
            [CounterLockOperation.REBUILD, CounterLockOperation.FOLD],
            "READ COMMITTED",
        ) as conn:
            await conn.execute(delete(ct).where(self._scope_where(ct, scope)))
            await conn.execute(delete(jt).where(self._scope_where(jt, scope)))


def counter_locks_table(metadata: MetaData) -> Table:
    """Return the ``CounterLocks`` table of a metadata, creating it if needed.

    Args:
        metadata: Metadata of the database holding the counters.

    Returns:
        The shared ``CounterLocks`` table.
    """
    if COUNTER_LOCKS_TABLE in metadata.tables:
        return metadata.tables[COUNTER_LOCKS_TABLE]
    return Table(
        COUNTER_LOCKS_TABLE,
        metadata,
        Column("CounterName", String(64), primary_key=True),
        Column(
            "Operation",
            Enum(CounterLockOperation, name="counter_lock_operation"),
            primary_key=True,
        ),
    )


def journalled_counter(
    metadata: MetaData,
    name: str,
    key: Sequence[tuple[str, TypeEngine]],
    measures: Sequence[str],
) -> JournalledCounter:
    """Declare a journalled counter and generate its tables.

    Args:
        metadata: Metadata of the database the counter belongs to.
        name: Name of the counter; the tables are ``<name>Counters`` and
            ``<name>CounterJournal``.
        key: ``(column name, column type)`` of each key column, in
            primary-key order.
        measures: Names of the additive measures. Each is a ``BIGINT`` in the
            counters table and a signed ``<measure>Delta`` in the journal.

    Returns:
        The declaration, holding the generated tables.

    Raises:
        ValueError: If the key or the measures are empty or overlap.
    """
    key_names = tuple(column_name for column_name, _ in key)
    if not key_names or not measures:
        raise ValueError("A counter needs at least one key column and one measure")
    if set(key_names) & set(measures):
        raise ValueError("Key columns and measures must have distinct names")

    counter_locks_table(metadata)

    counters = Table(
        f"{name}Counters",
        metadata,
        *(
            Column(column_name, type_, primary_key=True, nullable=False)
            for column_name, type_ in key
        ),
        *(Column(measure, BigInteger, nullable=False) for measure in measures),
        Column("UpdatedAt", MicrosecondDateTime(), nullable=False),
    )

    journal = Table(
        f"{name}CounterJournal",
        metadata,
        Column(
            "JournalID",
            BIGINT_AUTOINCREMENT,
            primary_key=True,
            autoincrement=True,
        ),
        *(Column(column_name, type_, nullable=False) for column_name, type_ in key),
        *(
            Column(
                JournalledCounter.delta_column(measure),
                BigInteger,
                nullable=False,
            )
            for measure in measures
        ),
        Column(
            "InsertedAt",
            MicrosecondDateTime(),
            nullable=False,
            server_default=utcnow_us(),
        ),
        Index(f"{name}CounterJournalKey", *key_names),
    )

    counter = JournalledCounter(
        name=name,
        key=key_names,
        measures=tuple(measures),
        counters_table=counters,
        journal_table=journal,
    )
    metadata.info.setdefault(COUNTERS_INFO_KEY, []).append(counter)
    return counter


def journalled_counters(metadata: MetaData) -> list[JournalledCounter]:
    """Return every journalled counter declared on a metadata.

    Args:
        metadata: Metadata of the database holding the counters.

    Returns:
        The counters, in declaration order.
    """
    return list(metadata.info.get(COUNTERS_INFO_KEY, []))
