from __future__ import annotations

import pytest

from diracx.core.exceptions import InvalidQueryError
from diracx.core.models import (
    PilotFieldsMapping,
    PilotStatus,
    ScalarSearchOperator,
    ScalarSearchSpec,
    SortDirection,
    SortSpec,
    VectorSearchOperator,
    VectorSearchSpec,
)
from diracx.db.sql.pilots.db import PilotAgentsDB

MAIN_VO = "lhcb"
N = 100


@pytest.fixture
async def pilot_db(tmp_path):
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


PILOT_REASONS = [
    "I was sick",
    "I can't, I have a pony.",
    "I was shopping",
    "I was sleeping",
]

PILOT_STATUSES = list(PilotStatus)


@pytest.fixture
async def populated_pilot_db(pilot_db):
    async with pilot_db as pilot_db:
        # Add pilots
        refs = [f"ref_{i + 1}" for i in range(N)]
        stamps = [f"stamp_{i + 1}" for i in range(N)]
        pilot_references = dict(zip(stamps, refs))

        vo = MAIN_VO

        await pilot_db.add_pilots(
            stamps, vo, grid_type="DIRAC", pilot_references=pilot_references
        )

        await pilot_db.update_pilot_fields(
            [
                PilotFieldsMapping(
                    PilotStamp=pilot_stamp,
                    BenchMark=i**2,
                    StatusReason=PILOT_REASONS[i % len(PILOT_REASONS)],
                    AccountingSent=True,
                    Status=PILOT_STATUSES[i % len(PILOT_STATUSES)],
                    CurrentJobID=i,
                    Queue=f"queue_{i}",
                )
                for i, pilot_stamp in enumerate(stamps)
            ]
        )

    yield pilot_db


async def test_search_parameters(populated_pilot_db):
    """Test that we can search specific parameters for pilots in the database."""
    async with populated_pilot_db as pilot_db:
        # Search a specific parameter: PilotID
        total, result = await pilot_db.search_pilots(["PilotID"], [], [])
        assert total == N
        assert result
        for r in result:
            assert r.keys() == {"PilotID"}

        # Search a specific parameter: Status
        total, result = await pilot_db.search_pilots(["Status"], [], [])
        assert total == N
        assert result
        for r in result:
            assert r.keys() == {"Status"}

        # Search for multiple parameters: PilotID, Status
        total, result = await pilot_db.search_pilots(["PilotID", "Status"], [], [])
        assert total == N
        assert result
        for r in result:
            assert r.keys() == {"PilotID", "Status"}

        # Search for a specific parameter but use distinct: Status
        total, result = await pilot_db.search_pilots(["Status"], [], [], distinct=True)
        assert total == len(PILOT_STATUSES)
        assert result

        # Search for a non-existent parameter: Dummy
        with pytest.raises(InvalidQueryError):
            total, result = await pilot_db.search_pilots(["Dummy"], [], [])


async def test_search_conditions(populated_pilot_db):
    """Test that we can search for specific pilots in the database."""
    async with populated_pilot_db as pilot_db:
        # Search a specific scalar condition: PilotID eq 3
        condition = ScalarSearchSpec(
            parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=3
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 1
        assert result
        assert len(result) == 1
        assert result[0]["PilotID"] == 3

        # Search a specific scalar condition: PilotID lt 3
        condition = ScalarSearchSpec(
            parameter="PilotID", operator=ScalarSearchOperator.LESS_THAN, value=3
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 2
        assert result
        assert len(result) == 2
        assert result[0]["PilotID"] == 1
        assert result[1]["PilotID"] == 2

        # Search a specific scalar condition: PilotID neq 3
        condition = ScalarSearchSpec(
            parameter="PilotID", operator=ScalarSearchOperator.NOT_EQUAL, value=3
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 99
        assert result
        assert len(result) == 99
        assert all(r["PilotID"] != 3 for r in result)

        # Search a specific scalar condition: PilotID eq 5873 (does not exist)
        condition = ScalarSearchSpec(
            parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=5873
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert not result

        # Search a specific vector condition: PilotID in 1,2,3
        condition = VectorSearchSpec(
            parameter="PilotID", operator=VectorSearchOperator.IN, values=[1, 2, 3]
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 3
        assert result
        assert len(result) == 3
        assert all(r["PilotID"] in [1, 2, 3] for r in result)

        # Search a specific vector condition: PilotID in 1,2,5873 (one of them does not exist)
        condition = VectorSearchSpec(
            parameter="PilotID", operator=VectorSearchOperator.IN, values=[1, 2, 5873]
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 2
        assert result
        assert len(result) == 2
        assert all(r["PilotID"] in [1, 2] for r in result)

        # Search a specific vector condition: PilotID not in 1,2,3
        condition = VectorSearchSpec(
            parameter="PilotID", operator=VectorSearchOperator.NOT_IN, values=[1, 2, 3]
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 97
        assert result
        assert len(result) == 97
        assert all(r["PilotID"] not in [1, 2, 3] for r in result)

        # Search a specific vector condition: PilotID not in 1,2,5873 (one of them does not exist)
        condition = VectorSearchSpec(
            parameter="PilotID",
            operator=VectorSearchOperator.NOT_IN,
            values=[1, 2, 5873],
        )
        total, result = await pilot_db.search_pilots([], [condition], [])
        assert total == 98
        assert result
        assert len(result) == 98
        assert all(r["PilotID"] not in [1, 2] for r in result)

        # Search for multiple conditions based on different parameters: PilotID eq 70, PilotID in 4,5,6
        condition1 = ScalarSearchSpec(
            parameter="PilotStamp", operator=ScalarSearchOperator.EQUAL, value="stamp_5"
        )
        condition2 = VectorSearchSpec(
            parameter="PilotID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
        )
        total, result = await pilot_db.search_pilots([], [condition1, condition2], [])
        assert total == 1
        assert result
        assert len(result) == 1
        assert result[0]["PilotID"] == 5
        assert result[0]["PilotStamp"] == "stamp_5"

        # Search for multiple conditions based on the same parameter: PilotID eq 70, PilotID in 4,5,6
        condition1 = ScalarSearchSpec(
            parameter="PilotID", operator=ScalarSearchOperator.EQUAL, value=70
        )
        condition2 = VectorSearchSpec(
            parameter="PilotID", operator=VectorSearchOperator.IN, values=[4, 5, 6]
        )
        total, result = await pilot_db.search_pilots([], [condition1, condition2], [])
        assert total == 0
        assert not result


async def test_search_sorts(populated_pilot_db):
    """Test that we can search for pilots in the database and sort the results."""
    async with populated_pilot_db as pilot_db:
        # Search and sort by PilotID in ascending order
        sort = SortSpec(parameter="PilotID", direction=SortDirection.ASC)
        total, result = await pilot_db.search_pilots([], [], [sort])
        assert total == N
        assert result
        for i, r in enumerate(result):
            assert r["PilotID"] == i + 1

        # Search and sort by PilotID in descending order
        sort = SortSpec(parameter="PilotID", direction=SortDirection.DESC)
        total, result = await pilot_db.search_pilots([], [], [sort])
        assert total == N
        assert result
        for i, r in enumerate(result):
            assert r["PilotID"] == N - i

        # Search and sort by PilotStamp in ascending order
        sort = SortSpec(parameter="PilotStamp", direction=SortDirection.ASC)
        total, result = await pilot_db.search_pilots([], [], [sort])
        assert total == N
        assert result
        # Assert that stamp_10 is before stamp_2 because of the lexicographical order
        assert result[2]["PilotStamp"] == "stamp_100"
        assert result[12]["PilotStamp"] == "stamp_2"

        # Search and sort by PilotStamp in descending order
        sort = SortSpec(parameter="PilotStamp", direction=SortDirection.DESC)
        total, result = await pilot_db.search_pilots([], [], [sort])
        assert total == N
        assert result
        # Assert that stamp_10 is before stamp_2 because of the lexicographical order
        assert result[97]["PilotStamp"] == "stamp_100"
        assert result[87]["PilotStamp"] == "stamp_2"

        # Search and sort by PilotStamp in ascending order and PilotID in descending order
        sort1 = SortSpec(parameter="PilotStamp", direction=SortDirection.ASC)
        sort2 = SortSpec(parameter="PilotID", direction=SortDirection.DESC)
        total, result = await pilot_db.search_pilots([], [], [sort1, sort2])
        assert total == N
        assert result
        assert result[0]["PilotStamp"] == "stamp_1"
        assert result[0]["PilotID"] == 1
        assert result[99]["PilotStamp"] == "stamp_99"
        assert result[99]["PilotID"] == 99


@pytest.mark.parametrize(
    "per_page, page, expected_len, expected_first_id, expect_exception",
    [
        (10, 1, 10, 1, None),  # Page 1
        (10, 2, 10, 11, None),  # Page 2
        (10, 10, 10, 91, None),  # Page 10
        (50, 2, 50, 51, None),  # Page 2 with 50 per page
        (10, 11, 0, None, None),  # Page beyond range, should return empty
        (10, 0, None, None, InvalidQueryError),  # Invalid page
        (0, 1, None, None, InvalidQueryError),  # Invalid per_page
    ],
)
async def test_search_pagination(
    populated_pilot_db,
    per_page,
    page,
    expected_len,
    expected_first_id,
    expect_exception,
):
    """Test pagination logic in pilot search."""
    async with populated_pilot_db as pilot_db:
        if expect_exception:
            with pytest.raises(expect_exception):
                await pilot_db.search_pilots([], [], [], per_page=per_page, page=page)
        else:
            total, result = await pilot_db.search_pilots(
                [], [], [], per_page=per_page, page=page
            )
            assert total == N
            if expected_len == 0:
                assert not result
            else:
                assert result
                assert len(result) == expected_len
                assert result[0]["PilotID"] == expected_first_id
