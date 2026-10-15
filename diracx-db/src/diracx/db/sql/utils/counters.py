"""Table declarations for journalled counters (DX-ADR-009).

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
)
from sqlalchemy.types import TypeEngine

from .functions import utcnow_us
from .types import MicrosecondDateTime

COUNTER_LOCKS_TABLE = "CounterLocks"
COUNTER_LOCK_OPERATIONS = ("rebuild", "fold")

# SQLite only auto-increments a column declared exactly as INTEGER PRIMARY KEY
BIGINT_AUTOINCREMENT = BigInteger().with_variant(Integer(), "sqlite")


@dataclass(frozen=True)
class JournalledCounter:
    """The tables generated for one journalled counter.

    Attributes:
        name: Name of the counter, as used in ``CounterLocks``.
        key: Names of the key columns, in primary-key order.
        measures: Names of the measures summed by the counter.
        counters: Table of folded counts, one row per key.
        journal: Table of signed deltas waiting to be folded.
    """

    name: str
    key: tuple[str, ...]
    measures: tuple[str, ...]
    counters: Table
    journal: Table

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

    return JournalledCounter(
        name=name,
        key=key_names,
        measures=tuple(measures),
        counters=counters,
        journal=journal,
    )
