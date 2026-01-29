from __future__ import annotations

import pytest
from pytest_lazy_fixtures import lf

from diracx.core.exceptions import InvalidQueryError
from diracx.testing.mock_osdb import MockOSDBMixin
from diracx.testing.osdb import DummyOSDB


@pytest.fixture(
    params=[
        pytest.param(lf("prefilled_dummy_opensearch_db"), id="dummy_opensearch_db"),
        pytest.param(lf("prefilled_sql_opensearch_db"), id="sql_opensearch_db"),
    ]
)
def prefilled_db(request):
    """Provide a prefilled database for testing."""
    return request.param


async def test_specified_parameters(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs

    results = await prefilled_db.search(None, [], [])
    assert len(results) == 3
    assert doc1 in results and doc2 in results and doc3 in results

    results = await prefilled_db.search([], [], [])
    assert len(results) == 3
    assert doc1 in results and doc2 in results and doc3 in results

    results = await prefilled_db.search(["IntField"], [], [])
    expected_results = []
    for doc in [doc1, doc2, doc3]:
        expected_doc = {key: doc[key] for key in {"IntField"}}
        # Ensure the document is not already in the list
        # If it is the all() check below no longer makes sense
        assert expected_doc not in expected_results
        expected_results.append(expected_doc)
    assert len(results) == len(expected_results)
    assert all(result in expected_results for result in results)

    results = await prefilled_db.search(["IntField", "UnknownField"], [], [])
    expected_results = [
        {"IntField": doc1["IntField"], "UnknownField": doc1["UnknownField"]},
        {"IntField": doc2["IntField"], "UnknownField": doc2["UnknownField"]},
        {"IntField": doc3["IntField"]},
    ]
    assert len(results) == len(expected_results)
    assert all(result in expected_results for result in results)


async def test_pagination_asc(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    sort = [{"parameter": "IntField", "direction": "asc"}]

    results = await prefilled_db.search(None, [], sort)
    assert results == [doc3, doc2, doc1]

    # Pagination has no effect if a specific page isn't requested
    results = await prefilled_db.search(None, [], sort, per_page=2)
    assert results == [doc3, doc2, doc1]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=1)
    assert results == [doc3, doc2]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=2)
    assert results == [doc1]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=3)
    assert results == []

    results = await prefilled_db.search(None, [], sort, per_page=1, page=1)
    assert results == [doc3]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=2)
    assert results == [doc2]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=3)
    assert results == [doc1]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=4)
    assert results == []


async def test_pagination_desc(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    sort = [{"parameter": "IntField", "direction": "desc"}]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=1)
    assert results == [doc1, doc2]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=2)
    assert results == [doc3]


async def test_eq_filter_long(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    part = {"parameter": "IntField", "operator": "eq"}

    # Search for an ID which doesn't exist
    results = await prefilled_db.search(None, [part | {"value": "78"}], [])
    assert results == []

    # Check the DB contains what we expect when not filtering
    results = await prefilled_db.search(None, [], [])
    assert len(results) == 3
    assert doc1 in results
    assert doc2 in results
    assert doc3 in results

    # Search separately for the two documents which do exist
    results = await prefilled_db.search(None, [part | {"value": "1234"}], [])
    assert results == [doc1]
    results = await prefilled_db.search(None, [part | {"value": "679"}], [])
    assert results == [doc2]
    results = await prefilled_db.search(None, [part | {"value": "42"}], [])
    assert results == [doc3]


async def test_operators_long(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    part = {"parameter": "IntField"}

    query = part | {"operator": "neq", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"], doc3["IntField"]}

    query = part | {"operator": "in", "values": ["1234", "42"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc3["IntField"]}

    query = part | {"operator": "not in", "values": ["1234", "42"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"]}

    query = part | {"operator": "lt", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"], doc3["IntField"]}

    query = part | {"operator": "lt", "value": "679"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc3["IntField"]}

    query = part | {"operator": "gt", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "lt", "value": "42"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()


async def test_operators_date(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    part = {"parameter": "DateField"}

    query = part | {"operator": "eq", "value": doc3["DateField"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc3["IntField"]}

    query = part | {"operator": "neq", "value": doc2["DateField"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc3["IntField"]}

    doc1_time = doc1["DateField"].strftime("%Y-%m-%dT%H:%M")
    doc2_time = doc2["DateField"].strftime("%Y-%m-%dT%H:%M")
    doc3_time = doc3["DateField"].strftime("%Y-%m-%dT%H:%M")

    query = part | {"operator": "in", "values": [doc1_time, doc2_time]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc2["IntField"]}

    query = part | {"operator": "not in", "values": [doc1_time, doc2_time]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc3["IntField"]}

    query = part | {"operator": "lt", "value": doc1_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"], doc3["IntField"]}

    query = part | {"operator": "lt", "value": doc3_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"]}

    query = part | {"operator": "lt", "value": doc2_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "gt", "value": doc1_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "gt", "value": doc3_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"]}

    query = part | {"operator": "gt", "value": doc2_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc3["IntField"]}


@pytest.mark.parametrize(
    "date_format",
    [
        "%Y-%m-%d",
        "%Y-%m-%dT%H",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
    ],
)
async def test_operators_date_partial_doc1(prefilled_db: DummyOSDB, date_format: str):
    """Search by datetime without specifying an exact match.

    The parameterized date_format argument should match doc1 but not doc2 or doc3.
    """
    doc1, doc2, doc3 = prefilled_db.test_docs
    formatted_date = doc1["DateField"].strftime(date_format)

    query = {"parameter": "DateField", "operator": "eq", "value": formatted_date}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"]}

    query = {"parameter": "DateField", "operator": "neq", "value": formatted_date}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"], doc3["IntField"]}


async def test_operators_keyword(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    part = {"parameter": "KeywordField1"}

    query = part | {"operator": "eq", "value": doc1["KeywordField1"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc2["IntField"]}

    query = part | {"operator": "neq", "value": doc1["KeywordField1"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc3["IntField"]}

    part = {"parameter": "KeywordField0"}

    query = part | {
        "operator": "in",
        "values": [doc1["KeywordField0"], doc3["KeywordField0"]],
    }
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc1["IntField"], doc3["IntField"]}

    query = part | {"operator": "in", "values": ["missing"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {
        "operator": "not in",
        "values": [doc1["KeywordField0"], doc3["KeywordField0"]],
    }
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {doc2["IntField"]}

    query = part | {"operator": "not in", "values": ["missing"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {
        doc1["IntField"],
        doc2["IntField"],
        doc3["IntField"],
    }

    # The MockOSDBMixin doesn't validate if types are indexed correctly
    if not isinstance(prefilled_db, MockOSDBMixin):
        with pytest.raises(InvalidQueryError):
            query = part | {"operator": "lt", "value": "a"}
            await prefilled_db.search(["IntField"], [query], [])

        with pytest.raises(InvalidQueryError):
            query = part | {"operator": "gt", "value": "a"}
            await prefilled_db.search(["IntField"], [query], [])


async def test_unknown_operator(prefilled_db: DummyOSDB):
    with pytest.raises(InvalidQueryError):
        await prefilled_db.search(
            None, [{"parameter": "IntField", "operator": "unknown"}], []
        )


async def test_unindexed_field(prefilled_db: DummyOSDB):
    with pytest.raises(InvalidQueryError):
        await prefilled_db.search(
            None,
            [{"parameter": "UnknownField", "operator": "eq", "value": "foobar"}],
            [],
        )


async def test_sort_long(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    results = await prefilled_db.search(
        None, [], [{"parameter": "IntField", "direction": "asc"}]
    )
    assert results == [doc3, doc2, doc1]
    results = await prefilled_db.search(
        None, [], [{"parameter": "IntField", "direction": "desc"}]
    )
    assert results == [doc1, doc2, doc3]


async def test_sort_date(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    results = await prefilled_db.search(
        None, [], [{"parameter": "DateField", "direction": "asc"}]
    )
    assert results == [doc2, doc3, doc1]
    results = await prefilled_db.search(
        None, [], [{"parameter": "DateField", "direction": "desc"}]
    )
    assert results == [doc1, doc3, doc2]


async def test_sort_keyword(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    results = await prefilled_db.search(
        None, [], [{"parameter": "KeywordField0", "direction": "asc"}]
    )
    assert results == [doc1, doc3, doc2]
    results = await prefilled_db.search(
        None, [], [{"parameter": "KeywordField0", "direction": "desc"}]
    )
    assert results == [doc2, doc3, doc1]


async def test_sort_text(prefilled_db: DummyOSDB):
    # The MockOSDBMixin doesn't validate if types are indexed correctly
    if not isinstance(prefilled_db, MockOSDBMixin):
        with pytest.raises(InvalidQueryError):
            await prefilled_db.search(
                None, [], [{"parameter": "TextField", "direction": "asc"}]
            )


async def test_sort_unknown(prefilled_db: DummyOSDB):
    with pytest.raises(InvalidQueryError):
        await prefilled_db.search(
            None, [], [{"parameter": "UnknownField", "direction": "asc"}]
        )


async def test_sort_multiple(prefilled_db: DummyOSDB):
    doc1, doc2, doc3 = prefilled_db.test_docs
    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "asc"},
            {"parameter": "IntField", "direction": "asc"},
        ],
    )
    assert results == [doc2, doc1, doc3]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "asc"},
            {"parameter": "IntField", "direction": "desc"},
        ],
    )
    assert results == [doc1, doc2, doc3]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "desc"},
            {"parameter": "IntField", "direction": "asc"},
        ],
    )
    assert results == [doc3, doc2, doc1]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "IntField", "direction": "asc"},
            {"parameter": "KeywordField1", "direction": "asc"},
        ],
    )
    assert results == [doc3, doc2, doc1]
