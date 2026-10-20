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

    await MY_COUNTER.journal(conn, [
        {"DatasetID": 7, "Status": "New",    "CountDelta": -2, "BytesDelta": -30},
        {"DatasetID": 7, "Status": "Copied", "CountDelta": +2, "BytesDelta": +30},
    ])

Reading (inside the caller's transaction)::

    rows = await MY_COUNTER.read(conn)
    # Each row has the key columns, one column per measure, and UpdatedAt.

Folding (from a periodic task, opens its own transaction)::

    n = await MY_COUNTER.fold(engine, batch_size=5000)
    if n == batch_size:
        # Journal had more rows than the batch; schedule another fold.
        ...

Rebuilding (from a maintenance task, opens its own transaction)::

    counted = [CountedTable(
        Files,
        key_exprs=[Files.c.DatasetID, Files.c.Status],
        measure_exprs={"Count": func.count(), "Bytes": func.sum(Files.c.Size)},
    )]
    await MY_COUNTER.rebuild(engine, counted)

Deleting a scope (after counted rows are already unwritable and deleted)::

    await MY_COUNTER.delete_scope(
        engine, MY_COUNTER.counters_table.c.DatasetID == 7
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

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
    delete,
    func,
    insert,
    literal_column,
    select,
    union_all,
)
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.types import TypeEngine

from .functions import utcnow_us
from .types import MicrosecondDateTime

COUNTER_LOCKS_TABLE = "CounterLocks"
COUNTER_LOCK_OPERATIONS = ("rebuild", "fold")
# Key of MetaData.info under which journalled_counter() registers each counter
COUNTERS_INFO_KEY = "journalled_counters"

# SQLite only auto-increments a column declared exactly as INTEGER PRIMARY KEY
BIGINT_AUTOINCREMENT = BigInteger().with_variant(Integer(), "sqlite")


def _is_sqlite(conn: AsyncConnection) -> bool:
    return conn.dialect.name == "sqlite"


@dataclass(frozen=True)
class CountedTable:
    """Source table declaration for ``rebuild()``.

    Attributes:
        table: The SQLAlchemy ``Table`` being counted.
        key_exprs: One column expression per counter key column, in the same
            order as ``JournalledCounter.key``.
        measure_exprs: Mapping from measure name to the aggregate expression
            that recounts it (e.g. ``func.count()``, ``func.sum(col)``).
    """

    table: Table
    key_exprs: Sequence[ColumnElement]
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
            {"CounterName": self.name, "Operation": operation}
            for operation in COUNTER_LOCK_OPERATIONS
        ]

    # ------------------------------------------------------------------
    # Operations (DX-ADR-009)
    # ------------------------------------------------------------------

    async def journal(
        self, conn: AsyncConnection, deltas: Sequence[dict]
    ) -> None:
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
        where: ColumnElement | None = None,
    ) -> Sequence:
        """Return exact counts by summing folded counters and the journal.

        The result is exact at any aggregation lag because it combines both
        halves in a single SQL statement.

        Args:
            conn: Connection with an active transaction.
            where: Optional predicate applied to both halves. Column
                references must use the counter table's columns (the same
                names exist on the journal).

        Returns:
            Rows of ``(key…, measure…, UpdatedAt)``. A key with no counter
            row and no journal rows is absent; callers treat absence as zero.
        """
        ct = self.counters_table
        jt = self.journal_table

        key_cols_ct = [ct.c[k] for k in self.key]
        key_cols_jt = [jt.c[k] for k in self.key]

        m_label = "__m_{}"
        t_label = "__t"

        counter_selects = []
        journal_selects = []
        for m in self.measures:
            lbl = m_label.format(m)
            counter_selects.append(ct.c[m].label(lbl))
            journal_selects.append(jt.c[self.delta_column(m)].label(lbl))

        counter_selects.append(ct.c["UpdatedAt"].label(t_label))
        journal_selects.append(jt.c["InsertedAt"].label(t_label))

        counter_q = select(*key_cols_ct, *counter_selects)
        journal_q = select(*key_cols_jt, *journal_selects)

        if where is not None:
            journal_where = _translate_predicate(where, ct, jt)
            counter_q = counter_q.where(where)
            journal_q = journal_q.where(journal_where)

        combined = union_all(counter_q, journal_q).subquery("t")

        key_refs = [combined.c[k] for k in self.key]
        aggs = [
            func.sum(combined.c[m_label.format(m)]).label(m) for m in self.measures
        ]
        aggs.append(func.max(combined.c[t_label]).label("UpdatedAt"))

        stmt = select(*key_refs, *aggs).group_by(*key_refs)

        result = await conn.execute(stmt)
        return result.all()

    async def fold(
        self, engine: AsyncEngine, batch_size: int = 5000
    ) -> int:
        """Claim committed journal rows, fold them into counters, and delete.

        Opens its own transaction at READ COMMITTED (record locks only on
        MySQL/MariaDB, no-op on SQLite).

        Args:
            engine: Database engine.
            batch_size: Maximum journal rows to fold in one transaction.

        Returns:
            Number of journal rows folded. If equal to ``batch_size``, the
            caller should schedule another fold immediately.
        """
        jt = self.journal_table
        ct = self.counters_table
        locks = self.counters_table.metadata.tables[COUNTER_LOCKS_TABLE]

        async with engine.connect() as conn:
            sqlite = _is_sqlite(conn)
            if not sqlite:
                await conn.execution_options(isolation_level="READ COMMITTED")

            async with conn.begin():
                # 1. Serialise folds
                if not sqlite:
                    await conn.execute(
                        select(literal_column("1"))
                        .select_from(locks)
                        .where(
                            and_(
                                locks.c.CounterName == self.name,
                                locks.c.Operation == "fold",
                            )
                        )
                        .with_for_update()
                    )

                # 2. Claim committed rows (SKIP LOCKED on MySQL)
                claim_stmt = (
                    select(jt.c.JournalID)
                    .order_by(jt.c.JournalID)
                    .limit(batch_size)
                )
                if not sqlite:
                    claim_stmt = claim_stmt.with_for_update(skip_locked=True)

                claimed = (await conn.execute(claim_stmt)).scalars().all()
                if not claimed:
                    return 0

                # 3. Upsert aggregated deltas into counters
                key_cols = [jt.c[k] for k in self.key]
                agg_measures = [
                    func.sum(jt.c[self.delta_column(m)]).label(m)
                    for m in self.measures
                ]
                agg_time = func.max(jt.c.InsertedAt).label("UpdatedAt")

                agg_q = (
                    select(*key_cols, *agg_measures, agg_time)
                    .where(jt.c.JournalID.in_(claimed))
                    .group_by(*key_cols)
                )

                agg_rows = (await conn.execute(agg_q)).all()

                for row in agg_rows:
                    row_dict = row._mapping
                    key_vals = {k: row_dict[k] for k in self.key}
                    measure_vals = {m: row_dict[m] for m in self.measures}

                    if sqlite:
                        existing = await conn.execute(
                            select(ct.c[self.key[0]])
                            .where(
                                and_(*(ct.c[k] == key_vals[k] for k in self.key))
                            )
                            .limit(1)
                        )
                        if existing.first() is not None:
                            update_vals = {
                                m: ct.c[m] + measure_vals[m] for m in self.measures
                            }
                            update_vals["UpdatedAt"] = func.max(
                                ct.c.UpdatedAt, row_dict["UpdatedAt"]
                            )
                            from sqlalchemy import update

                            await conn.execute(
                                update(ct)
                                .where(
                                    and_(
                                        *(ct.c[k] == key_vals[k] for k in self.key)
                                    )
                                )
                                .values(**update_vals)
                            )
                        else:
                            await conn.execute(
                                insert(ct).values(
                                    **key_vals,
                                    **measure_vals,
                                    UpdatedAt=row_dict["UpdatedAt"],
                                )
                            )
                    else:
                        # MySQL ON DUPLICATE KEY UPDATE
                        ins = insert(ct).values(
                            **key_vals,
                            **measure_vals,
                            UpdatedAt=row_dict["UpdatedAt"],
                        )
                        on_dup = {
                            m: ct.c[m] + ins.inserted[m] for m in self.measures
                        }
                        on_dup["UpdatedAt"] = func.greatest(
                            ct.c.UpdatedAt, ins.inserted.UpdatedAt
                        )
                        from sqlalchemy.dialects.mysql import insert as mysql_insert

                        ins = mysql_insert(ct).values(
                            **key_vals,
                            **measure_vals,
                            UpdatedAt=row_dict["UpdatedAt"],
                        )
                        on_dup_mysql = {
                            m: ct.c[m] + ins.inserted[m] for m in self.measures
                        }
                        on_dup_mysql["UpdatedAt"] = func.greatest(
                            ct.c.UpdatedAt, ins.inserted.UpdatedAt
                        )
                        await conn.execute(
                            ins.on_duplicate_key_update(**on_dup_mysql)
                        )

                # 4. Delete folded journal rows
                await conn.execute(
                    delete(jt).where(jt.c.JournalID.in_(claimed))
                )

                return len(claimed)

    async def rebuild(
        self,
        engine: AsyncEngine,
        counted: Sequence[CountedTable],
        where: ColumnElement | None = None,
    ) -> int:
        """Recount from source tables and write correction deltas.

        Opens its own transaction at REPEATABLE READ (snapshot covers the
        lock and all three reads on MySQL; no-op on SQLite).

        Args:
            engine: Database engine.
            counted: Source table declarations.
            where: Optional scope predicate (prefix of the key).

        Returns:
            Number of correction deltas written.
        """
        ct = self.counters_table
        jt = self.journal_table
        locks = self.counters_table.metadata.tables[COUNTER_LOCKS_TABLE]

        async with engine.connect() as conn:
            sqlite = _is_sqlite(conn)
            if not sqlite:
                await conn.execution_options(isolation_level="REPEATABLE READ")

            async with conn.begin():
                # 1. Lock rebuild row
                if not sqlite:
                    await conn.execute(
                        select(literal_column("1"))
                        .select_from(locks)
                        .where(
                            and_(
                                locks.c.CounterName == self.name,
                                locks.c.Operation == "rebuild",
                            )
                        )
                        .with_for_update()
                    )

                # 2. Build the recount - counters - journal correction query
                branches = []

                # Recount branches (one per counted table)
                for ct_decl in counted:
                    key_exprs = [
                        expr.label(k) for expr, k in zip(ct_decl.key_exprs, self.key)
                    ]
                    measure_selects = []
                    for m in self.measures:
                        expr = ct_decl.measure_exprs.get(m)
                        if expr is not None:
                            measure_selects.append(expr.label(f"__m_{m}"))
                        else:
                            measure_selects.append(literal_column("0").label(f"__m_{m}"))

                    branch = select(*key_exprs, *measure_selects).select_from(
                        ct_decl.table
                    )
                    if where is not None:
                        recount_where = _translate_predicate_by_position(
                            where, ct, ct_decl.table, ct_decl.key_exprs, self.key
                        )
                        branch = branch.where(recount_where)
                    branch = branch.group_by(
                        *[expr for expr, _ in zip(ct_decl.key_exprs, self.key)]
                    )
                    branches.append(branch)

                # Negate counters
                counter_measures = [
                    (-ct.c[m]).label(f"__m_{m}") for m in self.measures
                ]
                counter_branch = select(
                    *[ct.c[k] for k in self.key], *counter_measures
                )
                if where is not None:
                    counter_branch = counter_branch.where(where)
                branches.append(counter_branch)

                # Negate journal
                journal_measures = [
                    (-jt.c[self.delta_column(m)]).label(f"__m_{m}")
                    for m in self.measures
                ]
                journal_branch = select(
                    *[jt.c[k] for k in self.key], *journal_measures
                )
                if where is not None:
                    journal_where = _translate_predicate(where, ct, jt)
                    journal_branch = journal_branch.where(journal_where)
                branches.append(journal_branch)

                combined = union_all(*branches).subquery("t")

                key_refs = [combined.c[k] for k in self.key]

                from sqlalchemy import or_

                having_clause = or_(*(
                    func.sum(combined.c[f"__m_{m}"]) != 0
                    for m in self.measures
                ))

                corrections_q = (
                    select(
                        *key_refs,
                        *[
                            func.sum(combined.c[f"__m_{m}"]).label(
                                self.delta_column(m)
                            )
                            for m in self.measures
                        ],
                    )
                    .group_by(*key_refs)
                    .having(having_clause)
                )

                correction_rows = (await conn.execute(corrections_q)).all()

                if not correction_rows:
                    return 0

                # 3. Write corrections via journal()
                deltas = []
                for row in correction_rows:
                    row_dict = row._mapping
                    delta = {k: row_dict[k] for k in self.key}
                    for m in self.measures:
                        delta[self.delta_column(m)] = row_dict[self.delta_column(m)]
                    deltas.append(delta)

                await self.journal(conn, deltas)
                return len(deltas)

    async def delete_scope(
        self,
        engine: AsyncEngine,
        where: ColumnElement,
    ) -> None:
        """Remove all counter and journal rows matching a scope.

        The scope's counted rows must already be unwritable and deleted by the
        caller.  Opens its own short transaction at READ COMMITTED.

        Args:
            engine: Database engine.
            where: Scope predicate (e.g. ``counters.c.DatasetID == 7``).
        """
        ct = self.counters_table
        jt = self.journal_table
        locks = self.counters_table.metadata.tables[COUNTER_LOCKS_TABLE]

        async with engine.connect() as conn:
            sqlite = _is_sqlite(conn)
            if not sqlite:
                await conn.execution_options(isolation_level="READ COMMITTED")

            async with conn.begin():
                # Lock rebuild then fold (order per ADR)
                if not sqlite:
                    await conn.execute(
                        select(literal_column("1"))
                        .select_from(locks)
                        .where(
                            and_(
                                locks.c.CounterName == self.name,
                                locks.c.Operation == "rebuild",
                            )
                        )
                        .with_for_update()
                    )
                    await conn.execute(
                        select(literal_column("1"))
                        .select_from(locks)
                        .where(
                            and_(
                                locks.c.CounterName == self.name,
                                locks.c.Operation == "fold",
                            )
                        )
                        .with_for_update()
                    )

                await conn.execute(delete(ct).where(where))
                journal_where = _translate_predicate(where, ct, jt)
                await conn.execute(delete(jt).where(journal_where))


def _translate_predicate(
    pred: ColumnElement, from_table: Table, to_table: Table
) -> ColumnElement:
    """Rewrite a predicate's column references from one table to another.

    Both tables must have columns with the same names for any column
    referenced in the predicate.
    """
    from sqlalchemy.sql.visitors import replacement_traverse

    def replace(element):
        if hasattr(element, "key") and hasattr(element, "table"):
            if element.table is from_table:
                return to_table.c[element.key]
        return None

    return replacement_traverse(pred, {}, replace)


def _translate_predicate_by_position(
    pred: ColumnElement,
    from_table: Table,
    to_table: Table,
    to_key_exprs: Sequence[ColumnElement],
    key_names: tuple[str, ...],
) -> ColumnElement:
    """Rewrite a scope predicate for a counted table whose columns differ."""
    key_map = {}
    for name, expr in zip(key_names, to_key_exprs):
        key_map[name] = expr

    from sqlalchemy.sql.visitors import replacement_traverse

    def replace(element):
        if hasattr(element, "key") and hasattr(element, "table"):
            if element.table is from_table and element.key in key_map:
                return key_map[element.key]
        return None

    return replacement_traverse(pred, {}, replace)


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
            Enum(*COUNTER_LOCK_OPERATIONS, name="counter_lock_operation"),
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
        # The newest InsertedAt folded into the row
        Column("UpdatedAt", MicrosecondDateTime(), nullable=False),
    )

    journal = Table(
        f"{name}CounterJournal",
        metadata,
        Column("JournalID", BIGINT_AUTOINCREMENT, primary_key=True, autoincrement=True),
        *(Column(column_name, type_, nullable=False) for column_name, type_ in key),
        *(
            Column(JournalledCounter.delta_column(measure), BigInteger, nullable=False)
            for measure in measures
        ),
        # Set by the database so that writers' clock skew cannot distort it
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
