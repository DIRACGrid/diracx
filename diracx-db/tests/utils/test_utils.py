"""Unit tests for BaseOSDB and OpenSearch helper functions."""

from __future__ import annotations

import gc
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from diracx.core.exceptions import InvalidQueryError
from diracx.db.os.utils import (
    BaseOSDB,
    OpenSearchDBUnavailableError,
    apply_search_filters,
    require_type,
)

DB_FIELDS = {
    "status": {"type": "keyword"},
    "job_id": {"type": "long"},
    "timestamp": {"type": "date"},
}


@pytest.fixture(autouse=True)
def force_gc():
    """Collect any leaked async resources right after each testself.

    This avoids ResourceWarnings from unrelated aiosqlite connections
    bleeding into the next test's output.
    """
    yield
    gc.collect()


class DummyOSDB(BaseOSDB):
    fields = {
        "job_id": {"type": "long"},
        "status": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "vo": {"type": "keyword"},
    }
    index_prefix = "dummy"

    def index_name(self, vo: str, doc_id: int) -> str:
        return f"{self.index_prefix}-{vo}-{doc_id % 10}"


@pytest.fixture
def connection_kwargs() -> dict[str, Any]:
    return {"hosts": ["https://localhost:9200"], "verify_certs": False}


@pytest.fixture
def db(connection_kwargs):
    return DummyOSDB(connection_kwargs)


@pytest.fixture
def mock_client():
    """Return a fully-mocked AsyncOpenSearch client."""
    client = MagicMock()
    client.ping = AsyncMock(return_value=True)
    client.indices = MagicMock()
    client.indices.put_index_template = AsyncMock(return_value={"acknowledged": True})
    client.update = AsyncMock(return_value={"result": "updated"})
    client.search = AsyncMock(return_value={"hits": {"hits": []}})
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


@pytest_asyncio.fixture
async def live_db(db, mock_client):
    """DummyOSDB with client_context and __aenter__ already entered."""
    with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
        async with db.client_context():
            async with db:
                yield db, mock_client


class TestRequireType:
    def test_passes_when_type_is_allowed(self):
        require_type("eq", "status", "keyword", {"keyword", "long"})  # must not raise

    def test_raises_for_disallowed_type(self):
        with pytest.raises(InvalidQueryError, match="Cannot apply"):
            require_type("eq", "status", "text", {"keyword", "long"})

    def test_raises_when_type_is_none(self):
        with pytest.raises(InvalidQueryError):
            require_type("sort", "missing_field", None, {"keyword"})

    @pytest.mark.parametrize(
        "op,ftype,allowed",
        [
            ("gt", "long", {"long", "date"}),
            ("lt", "date", {"long", "date"}),
            ("in", "keyword", {"keyword", "long", "date"}),
            ("neq", "long", {"keyword", "long", "date"}),
        ],
    )
    def test_valid_combinations(self, op, ftype, allowed):
        require_type(op, "field", ftype, allowed)  # must not raise


class TestApplySearchFilters:
    def test_empty_search_returns_empty_bool(self):
        result = apply_search_filters(DB_FIELDS, [])
        assert result == {"bool": {"must": [], "must_not": []}}

    def test_eq_operator(self):
        result = apply_search_filters(
            DB_FIELDS, [{"parameter": "status", "operator": "eq", "value": "Running"}]
        )
        assert {"term": {"status": {"value": "Running"}}} in result["bool"]["must"]

    def test_neq_operator(self):
        result = apply_search_filters(
            DB_FIELDS, [{"parameter": "status", "operator": "neq", "value": "Failed"}]
        )
        assert {"term": {"status": {"value": "Failed"}}} in result["bool"]["must_not"]

    def test_gt_operator(self):
        result = apply_search_filters(
            DB_FIELDS, [{"parameter": "job_id", "operator": "gt", "value": 100}]
        )
        assert {"range": {"job_id": {"gt": 100}}} in result["bool"]["must"]

    def test_lt_operator(self):
        result = apply_search_filters(
            DB_FIELDS, [{"parameter": "job_id", "operator": "lt", "value": 500}]
        )
        assert {"range": {"job_id": {"lt": 500}}} in result["bool"]["must"]

    def test_in_operator(self):
        values = ["Running", "Waiting"]
        result = apply_search_filters(
            DB_FIELDS,
            [{"parameter": "status", "operator": "in", "values": values}],
        )
        assert {"terms": {"status": values}} in result["bool"]["must"]

    def test_not_in_operator(self):
        values = ["Failed", "Killed"]
        result = apply_search_filters(
            DB_FIELDS,
            [{"parameter": "status", "operator": "not in", "values": values}],
        )
        assert {"terms": {"status": values}} in result["bool"]["must_not"]

    def test_multiple_filters_combined(self):
        search = [
            {"parameter": "status", "operator": "eq", "value": "Running"},
            {"parameter": "job_id", "operator": "gt", "value": 10},
            {"parameter": "status", "operator": "neq", "value": "Failed"},
        ]
        result = apply_search_filters(DB_FIELDS, search)
        assert len(result["bool"]["must"]) == 2
        assert len(result["bool"]["must_not"]) == 1

    def test_raises_for_unknown_field(self):
        with pytest.raises(InvalidQueryError, match="not included"):
            apply_search_filters(
                DB_FIELDS,
                [{"parameter": "nonexistent", "operator": "eq", "value": "x"}],
            )

    def test_raises_for_unknown_operator(self):
        with pytest.raises(InvalidQueryError, match="Unknown filter"):
            apply_search_filters(
                DB_FIELDS,
                [{"parameter": "status", "operator": "like", "value": "Run%"}],
            )

    def test_raises_for_wrong_type_on_gt(self):
        with pytest.raises(InvalidQueryError, match="Cannot apply"):
            apply_search_filters(
                DB_FIELDS,
                [{"parameter": "status", "operator": "gt", "value": "something"}],
            )

    def test_raises_for_wrong_type_on_range(self):
        with pytest.raises(InvalidQueryError):
            apply_search_filters(
                DB_FIELDS,
                [{"parameter": "status", "operator": "lt", "value": "x"}],
            )


class TestBaseOSDBInit:
    def test_client_raises_before_context(self, db):
        with pytest.raises(RuntimeError, match="used before entering"):
            _ = db.client

    def test_connection_kwargs_stored(self, db, connection_kwargs):
        assert db._connection_kwargs == connection_kwargs

    def test_index_name(self, db):
        assert db.index_name("lhcb", 42) == "dummy-lhcb-2"

    def test_session_raises(self):
        with pytest.raises(NotImplementedError):
            DummyOSDB.session()


class TestAvailableUrls:
    def test_returns_parsed_env_var(self):
        kwargs = {"hosts": ["https://os:9200"]}
        ep = MagicMock()
        ep.name = "dummy"
        mock_settings = MagicMock()
        mock_settings.opensearch_dbs = {"dummy": json.dumps(kwargs)}

        with (
            patch("diracx.db.os.utils.select_from_extension", return_value=[ep]),
            patch("diracx.db.os.utils.FactorySettings", return_value=mock_settings),
        ):
            result = DummyOSDB.available_urls()
        assert result == {"dummy": kwargs}

    def test_skips_missing_env_var(self):
        ep = MagicMock()
        ep.name = "dummy"
        mock_settings = MagicMock()
        mock_settings.opensearch_dbs = {}
        with (
            patch("diracx.db.os.utils.select_from_extension", return_value=[ep]),
            patch("diracx.db.os.utils.FactorySettings", return_value=mock_settings),
        ):
            result = DummyOSDB.available_urls()
        assert result == {}

    def test_raises_on_invalid_json(self):
        ep = MagicMock()
        ep.name = "dummy"
        mock_settings = MagicMock()
        mock_settings.opensearch_dbs = {"dummy": "not-json"}
        with (
            patch("diracx.db.os.utils.select_from_extension", return_value=[ep]),
            patch("diracx.db.os.utils.FactorySettings", return_value=mock_settings),
        ):
            with pytest.raises(json.JSONDecodeError):
                DummyOSDB.available_urls()


class TestAvailableImplementations:
    def test_returns_loaded_classes(self):
        ep = MagicMock()
        ep.load.return_value = DummyOSDB
        with patch("diracx.db.os.utils.select_from_extension", return_value=[ep]):
            result = DummyOSDB.available_implementations("dummy")
        assert result == [DummyOSDB]

    def test_raises_when_no_match(self):
        with patch("diracx.db.os.utils.select_from_extension", return_value=[]):
            with pytest.raises(NotImplementedError, match="db_name"):
                DummyOSDB.available_implementations("nonexistent")


class TestClientContext:
    @pytest.mark.asyncio
    async def test_client_available_inside_context(self, db, mock_client):
        with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
            async with db.client_context():
                assert db.client is mock_client

    @pytest.mark.asyncio
    async def test_client_none_after_context_exits(self, db, mock_client):
        with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
            async with db.client_context():
                pass
        assert db._client is None

    @pytest.mark.asyncio
    async def test_nesting_raises(self, db, mock_client):
        with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
            async with db.client_context():
                with pytest.raises(AssertionError):
                    async with db.client_context():
                        pass


class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_enters_successfully(self, db, mock_client):
        with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
            async with db.client_context():
                async with db:
                    assert db._conn.get() is True
        assert db._conn.get() is False

    @pytest.mark.asyncio
    async def test_nesting_aenter_raises(self, db, mock_client):
        with patch("diracx.db.os.utils.AsyncOpenSearch", return_value=mock_client):
            async with db.client_context():
                async with db:
                    with pytest.raises(AssertionError):
                        async with db:
                            pass

    @pytest.mark.asyncio
    async def test_requires_client_context_first(self, db):
        with pytest.raises(AssertionError, match="client_context"):
            async with db:
                pass


class TestPing:
    @pytest.mark.asyncio
    async def test_ping_success(self, live_db):
        db, client = live_db
        client.ping.return_value = True
        await db.ping()  # must not raise

    @pytest.mark.asyncio
    async def test_ping_raises_when_unreachable(self, live_db):
        db, client = live_db
        client.ping.return_value = False
        with pytest.raises(OpenSearchDBUnavailableError):
            await db.ping()


class TestCreateIndexTemplate:
    @pytest.mark.asyncio
    async def test_calls_put_index_template(self, live_db):
        db, client = live_db
        await db.create_index_template()
        client.indices.put_index_template.assert_awaited_once()
        call_kwargs = client.indices.put_index_template.call_args
        assert call_kwargs.kwargs["name"] == db.index_prefix
        body = call_kwargs.kwargs["body"]
        assert body["index_patterns"] == [f"{db.index_prefix}*"]
        assert body["template"]["mappings"]["properties"] == db.fields

    @pytest.mark.asyncio
    async def test_raises_when_not_acknowledged(self, live_db):
        db, client = live_db
        client.indices.put_index_template.return_value = {"acknowledged": False}
        with pytest.raises(AssertionError):
            await db.create_index_template()


class TestUpsert:
    @pytest.mark.asyncio
    async def test_upsert_calls_client_update(self, live_db):
        db, client = live_db
        await db.upsert("lhcb", 42, {"status": "Running"})
        client.update.assert_awaited_once()
        kwargs = client.update.call_args.kwargs
        assert kwargs["id"] == 42
        assert kwargs["body"]["doc"] == {"status": "Running"}
        assert kwargs["body"]["doc_as_upsert"] is True

    @pytest.mark.asyncio
    async def test_upsert_uses_correct_index(self, live_db):
        db, client = live_db
        await db.upsert("lhcb", 42, {"status": "Running"})
        kwargs = client.update.call_args.kwargs
        assert kwargs["index"] == db.index_name("lhcb", 42)

    @pytest.mark.asyncio
    async def test_upsert_sets_retry_on_conflict(self, live_db):
        db, client = live_db
        await db.upsert("lhcb", 1, {})
        kwargs = client.update.call_args.kwargs
        assert kwargs["params"]["retry_on_conflict"] == 10


class TestBulkUpsert:
    @pytest.mark.asyncio
    async def test_bulk_upsert_success(self, live_db):
        db, _ = live_db
        documents = [
            ("lhcb", 1, {"status": "Running"}),
            ("lhcb", 2, {"status": "Done"}),
        ]

        with patch(
            "diracx.db.os.utils.async_bulk", new_callable=AsyncMock
        ) as mock_bulk:
            mock_bulk.return_value = (2, [])
            success, errors = await db.bulk_upsert(documents)

        assert success == 2
        assert errors == []

    @pytest.mark.asyncio
    async def test_bulk_upsert_returns_errors(self, live_db):
        db, _ = live_db
        error_entry = {"update": {"error": "some error", "_id": 99}}

        with patch(
            "diracx.db.os.utils.async_bulk", new_callable=AsyncMock
        ) as mock_bulk:
            mock_bulk.return_value = (0, [error_entry])
            success, errors = await db.bulk_upsert([("lhcb", 99, {})])

        assert success == 0
        assert errors == [error_entry]

    @pytest.mark.asyncio
    async def test_bulk_upsert_builds_correct_actions(self, live_db):
        db, _ = live_db
        documents = [("lhcb", 5, {"status": "Waiting"})]
        captured_actions = []

        async def fake_bulk(client, actions, **kwargs):
            captured_actions.extend(list(actions))
            return (1, [])

        with patch("diracx.db.os.utils.async_bulk", new=fake_bulk):
            await db.bulk_upsert(documents)

        assert len(captured_actions) == 1
        action = captured_actions[0]
        assert action["_op_type"] == "update"
        assert action["_id"] == 5
        assert action["doc"] == {"status": "Waiting"}
        assert action["doc_as_upsert"] is True
        assert action["retry_on_conflict"] == 10


class TestSearch:
    def _make_hit(self, source: dict) -> dict:
        return {"_source": source}

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_hits(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        result = await db.search([], [], [])
        assert result == []

    @pytest.mark.asyncio
    async def test_returns_source_from_hits(self, live_db):
        db, client = live_db
        client.search.return_value = {
            "hits": {"hits": [self._make_hit({"status": "Running", "job_id": 1})]}
        }
        result = await db.search([], [], [])
        assert result == [{"status": "Running", "job_id": 1}]

    @pytest.mark.asyncio
    async def test_converts_date_strings_to_datetime(self, live_db):
        db, client = live_db
        client.search.return_value = {
            "hits": {
                "hits": [
                    self._make_hit({"timestamp": "2024-01-15T10:30:00.000000+00:00"})
                ]
            }
        }
        result = await db.search([], [], [])
        assert result[0]["timestamp"].tzinfo is not None
        assert result[0]["timestamp"].year == 2024

    @pytest.mark.asyncio
    async def test_search_with_parameters_sets_source(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search(["status", "job_id"], [], [])
        body = client.search.call_args.kwargs["body"]
        assert body["_source"] == ["status", "job_id"]

    @pytest.mark.asyncio
    async def test_search_without_parameters_omits_source(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search([], [], [])
        body = client.search.call_args.kwargs["body"]
        assert "_source" not in body

    @pytest.mark.asyncio
    async def test_search_applies_sort(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search([], [], [{"parameter": "job_id", "direction": "asc"}])
        body = client.search.call_args.kwargs["body"]
        assert {"job_id": {"order": "asc"}} in body["sort"]

    @pytest.mark.asyncio
    async def test_search_raises_for_unsortable_field_type(self, live_db):
        db, _ = live_db
        with pytest.raises(InvalidQueryError, match="Cannot apply sort"):
            # "text" type is not in the allowed sort types
            original_fields = db.fields
            db.fields = {**original_fields, "text_field": {"type": "text"}}
            try:
                await db.search(
                    [], [], [{"parameter": "text_field", "direction": "asc"}]
                )
            finally:
                db.fields = original_fields

    @pytest.mark.asyncio
    async def test_search_with_pagination(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search([], [], [], per_page=25, page=3)
        params = client.search.call_args.kwargs["params"]
        assert params["from"] == 50  # (3-1) * 25
        assert params["size"] == 25

    @pytest.mark.asyncio
    async def test_search_without_page_omits_pagination_params(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search([], [], [], page=None)
        params = client.search.call_args.kwargs["params"]
        assert "from" not in params
        assert "size" not in params

    @pytest.mark.asyncio
    async def test_search_queries_correct_index_pattern(self, live_db):
        db, client = live_db
        client.search.return_value = {"hits": {"hits": []}}
        await db.search([], [], [])
        index_arg = client.search.call_args.kwargs["index"]
        assert index_arg == f"{db.index_prefix}*"
