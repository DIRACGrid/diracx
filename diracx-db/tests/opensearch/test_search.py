from __future__ import annotations

import contextlib
from datetime import datetime, timedelta, timezone
from functools import partial

import pytest

from diracx.core.exceptions import InvalidQueryError
from diracx.testing.mock_osdb import MockOSDBMixin
from diracx.testing.osdb import DummyOSDB

DOC1 = {
    "DateField": datetime.now(tz=timezone.utc),
    "IntField": 1234,
    "KeywordField0": "a",
    "KeywordField1": "keyword1",
    "KeywordField2": "keyword one",
    "TextField": "text value",
    "UnknownField": "unknown field 1",
}
DOC2 = {
    "DateField": datetime.now(tz=timezone.utc) - timedelta(days=1, minutes=34),
    "IntField": 679,
    "KeywordField0": "c",
    "KeywordField1": "keyword1",
    "KeywordField2": "keyword two",
    "TextField": "another text value",
    "UnknownField": "unknown field 2",
}
DOC3 = {
    "DateField": datetime.now(tz=timezone.utc) - timedelta(days=1),
    "IntField": 42,
    "KeywordField0": "b",
    "KeywordField1": "keyword2",
    "KeywordField2": "keyword two",
    "TextField": "yet another text value",
}


@contextlib.asynccontextmanager
async def resolve_fixtures_hack(request, name):
    """Resolves a fixture from `diracx.testing.osdb`.

    This is a hack to work around pytest-asyncio not supporting the use of
    request.getfixturevalue() from within an async function.

    See: https://github.com/pytest-dev/pytest-asyncio/issues/112
    """
    import inspect

    import diracx.testing.osdb

    # Track cleanup generators to ensure they are all exhausted
    # i.e. we return control to the caller so cleanup can be performed
    to_cleanup = []
    # As we rely on recursion to resolve fixtures, we need to use an async
    # context stack to ensure cleanup is performed in the correct order
    async with contextlib.AsyncExitStack() as stack:
        # If the given function name is available in diracx.testing.osdb, resolve
        # it manually, else assume it's safe to use request.getfixturevalue()
        if func := getattr(diracx.testing.osdb, name, None):
            if not hasattr(func, "__wrapped__"):
                raise NotImplementedError(f"resolve_fixtures({func=})")
            func = func.__wrapped__
            # Only resolve the arguments manually if the function is marked
            # as an asyncio fixture
            if getattr(func, "_force_asyncio_fixture", False):
                args = [
                    await stack.enter_async_context(
                        resolve_fixtures_hack(request, arg_name)
                    )
                    for arg_name in inspect.signature(func).parameters
                ]
                result = func(*args)
                if inspect.isawaitable(result):
                    result = await result
                elif inspect.isasyncgen(result):
                    to_cleanup.append(partial(anext, result))
                    result = await anext(result)
            else:
                result = request.getfixturevalue(name)
        else:
            result = request.getfixturevalue(name)

        # Yield the resolved fixture result to the caller
        try:
            yield result
        finally:
            # Cleanup all resources in the correct order
            for cleanup_func in reversed(to_cleanup):
                try:
                    await cleanup_func()
                except StopAsyncIteration:
                    pass
                else:
                    raise NotImplementedError(
                        "Cleanup generator did not stop as expected"
                    )


@pytest.fixture(params=["dummy_opensearch_db", "sql_opensearch_db"])
async def prefilled_db(request):
    """Fill the database with dummy records for testing."""
    impl = request.param
    async with resolve_fixtures_hack(request, impl) as dummy_opensearch_db:
        await dummy_opensearch_db.upsert("dummyvo", 798811211, DOC1)
        await dummy_opensearch_db.upsert("dummyvo", 998811211, DOC2)
        await dummy_opensearch_db.upsert("dummyvo", 798811212, DOC3)

        # Force a refresh to make sure the documents are available
        if not impl == "sql_opensearch_db":
            await dummy_opensearch_db.client.indices.refresh(
                index=f"{dummy_opensearch_db.index_prefix}*"
            )

        yield dummy_opensearch_db


async def test_specified_parameters(prefilled_db: DummyOSDB):
    results = await prefilled_db.search(None, [], [])
    assert len(results) == 3
    assert DOC1 in results and DOC2 in results and DOC3 in results

    results = await prefilled_db.search([], [], [])
    assert len(results) == 3
    assert DOC1 in results and DOC2 in results and DOC3 in results

    results = await prefilled_db.search(["IntField"], [], [])
    expected_results = []
    for doc in [DOC1, DOC2, DOC3]:
        expected_doc = {key: doc[key] for key in {"IntField"}}
        # Ensure the document is not already in the list
        # If it is the all() check below no longer makes sense
        assert expected_doc not in expected_results
        expected_results.append(expected_doc)
    assert len(results) == len(expected_results)
    assert all(result in expected_results for result in results)

    results = await prefilled_db.search(["IntField", "UnknownField"], [], [])
    expected_results = [
        {"IntField": DOC1["IntField"], "UnknownField": DOC1["UnknownField"]},
        {"IntField": DOC2["IntField"], "UnknownField": DOC2["UnknownField"]},
        {"IntField": DOC3["IntField"]},
    ]
    assert len(results) == len(expected_results)
    assert all(result in expected_results for result in results)


async def test_pagination_asc(prefilled_db: DummyOSDB):
    sort = [{"parameter": "IntField", "direction": "asc"}]

    results = await prefilled_db.search(None, [], sort)
    assert results == [DOC3, DOC2, DOC1]

    # Pagination has no effect if a specific page isn't requested
    results = await prefilled_db.search(None, [], sort, per_page=2)
    assert results == [DOC3, DOC2, DOC1]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=1)
    assert results == [DOC3, DOC2]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=2)
    assert results == [DOC1]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=3)
    assert results == []

    results = await prefilled_db.search(None, [], sort, per_page=1, page=1)
    assert results == [DOC3]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=2)
    assert results == [DOC2]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=3)
    assert results == [DOC1]

    results = await prefilled_db.search(None, [], sort, per_page=1, page=4)
    assert results == []


async def test_pagination_desc(prefilled_db: DummyOSDB):
    sort = [{"parameter": "IntField", "direction": "desc"}]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=1)
    assert results == [DOC1, DOC2]

    results = await prefilled_db.search(None, [], sort, per_page=2, page=2)
    assert results == [DOC3]


async def test_eq_filter_long(prefilled_db: DummyOSDB):
    part = {"parameter": "IntField", "operator": "eq"}

    # Search for an ID which doesn't exist
    results = await prefilled_db.search(None, [part | {"value": "78"}], [])
    assert results == []

    # Check the DB contains what we expect when not filtering
    results = await prefilled_db.search(None, [], [])
    assert len(results) == 3
    assert DOC1 in results
    assert DOC2 in results
    assert DOC3 in results

    # Search separately for the two documents which do exist
    results = await prefilled_db.search(None, [part | {"value": "1234"}], [])
    assert results == [DOC1]
    results = await prefilled_db.search(None, [part | {"value": "679"}], [])
    assert results == [DOC2]
    results = await prefilled_db.search(None, [part | {"value": "42"}], [])
    assert results == [DOC3]


async def test_operators_long(prefilled_db: DummyOSDB):
    part = {"parameter": "IntField"}

    query = part | {"operator": "neq", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"], DOC3["IntField"]}

    query = part | {"operator": "in", "values": ["1234", "42"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC3["IntField"]}

    query = part | {"operator": "not in", "values": ["1234", "42"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"]}

    query = part | {"operator": "lt", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"], DOC3["IntField"]}

    query = part | {"operator": "lt", "value": "679"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC3["IntField"]}

    query = part | {"operator": "gt", "value": "1234"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "lt", "value": "42"}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()


async def test_operators_date(prefilled_db: DummyOSDB):
    part = {"parameter": "DateField"}

    query = part | {"operator": "eq", "value": DOC3["DateField"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC3["IntField"]}

    query = part | {"operator": "neq", "value": DOC2["DateField"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC3["IntField"]}

    doc1_time = DOC1["DateField"].strftime("%Y-%m-%dT%H:%M")
    doc2_time = DOC2["DateField"].strftime("%Y-%m-%dT%H:%M")
    doc3_time = DOC3["DateField"].strftime("%Y-%m-%dT%H:%M")

    query = part | {"operator": "in", "values": [doc1_time, doc2_time]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC2["IntField"]}

    query = part | {"operator": "not in", "values": [doc1_time, doc2_time]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC3["IntField"]}

    query = part | {"operator": "lt", "value": doc1_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"], DOC3["IntField"]}

    query = part | {"operator": "lt", "value": doc3_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"]}

    query = part | {"operator": "lt", "value": doc2_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "gt", "value": doc1_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {"operator": "gt", "value": doc3_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"]}

    query = part | {"operator": "gt", "value": doc2_time}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC3["IntField"]}


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

    The parameterized date_format argument should match DOC1 but not DOC2 or DOC3.
    """
    formatted_date = DOC1["DateField"].strftime(date_format)

    query = {"parameter": "DateField", "operator": "eq", "value": formatted_date}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"]}

    query = {"parameter": "DateField", "operator": "neq", "value": formatted_date}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"], DOC3["IntField"]}


async def test_operators_keyword(prefilled_db: DummyOSDB):
    part = {"parameter": "KeywordField1"}

    query = part | {"operator": "eq", "value": DOC1["KeywordField1"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC2["IntField"]}

    query = part | {"operator": "neq", "value": DOC1["KeywordField1"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC3["IntField"]}

    part = {"parameter": "KeywordField0"}

    query = part | {
        "operator": "in",
        "values": [DOC1["KeywordField0"], DOC3["KeywordField0"]],
    }
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC1["IntField"], DOC3["IntField"]}

    query = part | {"operator": "in", "values": ["missing"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == set()

    query = part | {
        "operator": "not in",
        "values": [DOC1["KeywordField0"], DOC3["KeywordField0"]],
    }
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {DOC2["IntField"]}

    query = part | {"operator": "not in", "values": ["missing"]}
    results = await prefilled_db.search(["IntField"], [query], [])
    assert {x["IntField"] for x in results} == {
        DOC1["IntField"],
        DOC2["IntField"],
        DOC3["IntField"],
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
    results = await prefilled_db.search(
        None, [], [{"parameter": "IntField", "direction": "asc"}]
    )
    assert results == [DOC3, DOC2, DOC1]
    results = await prefilled_db.search(
        None, [], [{"parameter": "IntField", "direction": "desc"}]
    )
    assert results == [DOC1, DOC2, DOC3]


async def test_sort_date(prefilled_db: DummyOSDB):
    results = await prefilled_db.search(
        None, [], [{"parameter": "DateField", "direction": "asc"}]
    )
    assert results == [DOC2, DOC3, DOC1]
    results = await prefilled_db.search(
        None, [], [{"parameter": "DateField", "direction": "desc"}]
    )
    assert results == [DOC1, DOC3, DOC2]


async def test_sort_keyword(prefilled_db: DummyOSDB):
    results = await prefilled_db.search(
        None, [], [{"parameter": "KeywordField0", "direction": "asc"}]
    )
    assert results == [DOC1, DOC3, DOC2]
    results = await prefilled_db.search(
        None, [], [{"parameter": "KeywordField0", "direction": "desc"}]
    )
    assert results == [DOC2, DOC3, DOC1]


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
    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "asc"},
            {"parameter": "IntField", "direction": "asc"},
        ],
    )
    assert results == [DOC2, DOC1, DOC3]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "asc"},
            {"parameter": "IntField", "direction": "desc"},
        ],
    )
    assert results == [DOC1, DOC2, DOC3]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "KeywordField1", "direction": "desc"},
            {"parameter": "IntField", "direction": "asc"},
        ],
    )
    assert results == [DOC3, DOC2, DOC1]

    results = await prefilled_db.search(
        None,
        [],
        [
            {"parameter": "IntField", "direction": "asc"},
            {"parameter": "KeywordField1", "direction": "asc"},
        ],
    )
    assert results == [DOC3, DOC2, DOC1]
