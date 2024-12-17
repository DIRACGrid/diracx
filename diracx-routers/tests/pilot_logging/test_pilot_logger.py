from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import inspect, update

from diracx.core.properties import PILOT
from diracx.db.os import PilotLogsDB
from diracx.db.os.utils import BaseOSDB
from diracx.db.sql import PilotAgentsDB
from diracx.db.sql.pilot_agents.schema import PilotAgents
from diracx.routers.pilot_logging.remote_logger import (
    LogLine,
    LogMessage,
    get_logs,
    send_message,
)
from diracx.testing.mock_osdb import MockOSDBMixin, fake_available_osdb_implementations


class PilotLogsDB(MockOSDBMixin, PilotLogsDB):
    pass


@pytest.fixture
async def pilot_agents_db(tmp_path) -> PilotAgentsDB:
    agents_db = PilotAgentsDB("sqlite+aiosqlite:///:memory:")
    async with agents_db.engine_context():
        async with agents_db.engine.begin() as conn:
            await conn.run_sync(agents_db.metadata.create_all)
        yield agents_db


@pytest.fixture
async def pilot_logs_db():
    fake_implementation = fake_available_osdb_implementations(
        "PilotLogsDB", real_available_implementations=BaseOSDB.available_implementations
    )[0]
    db = fake_implementation(
        connection_kwargs={"sqlalchemy_dsn": "sqlite+aiosqlite:///:memory:"}
    )
    async with db.client_context():
        await db.create_index_template()
        yield db


@patch("diracx.routers.pilot_logging.remote_logger.BaseSQLDB.available_implementations")
@patch("diracx.routers.pilot_logging.remote_logger.BaseSQLDB.available_urls")
async def test_remote_logger(
    mock_url, mock_impl, pilot_logs_db: PilotLogsDB, pilot_agents_db: PilotAgentsDB
):

    async with pilot_agents_db as db:
        # Add a pilot reference
        upper_limit = 6
        refs = [f"ref_{i}" for i in range(1, upper_limit)]
        stamps = [f"stamp_{i}" for i in range(1, upper_limit)]
        stamp_dict = dict(zip(refs, stamps))

        await db.add_pilot_references(
            refs, "test_vo", grid_type="DIRAC", pilot_stamps=stamp_dict
        )
        tables = await db.conn.run_sync(
            lambda sync_conn: inspect(sync_conn).get_table_names()
        )
        assert "PilotAgents" in tables

        # move submission time back in time
        now = datetime.now(tz=timezone.utc)
        for i in range(1, upper_limit):
            sub_time = now - timedelta(hours=2 * i - 1)
            stmt = (
                update(PilotAgents)
                .where(PilotAgents.PilotStamp == f"stamp_{i}")
                .values(SubmissionTime=sub_time)
            )
            await db.conn.execute(stmt)
    # 4 message records for the first pilot.
    line = [{"Message": f"Message_no_{i}"} for i in range(1, 4)]
    log_lines = [LogLine(line_no=i + 1, line=line[i]["Message"]) for i in range(3)]
    message = LogMessage(pilot_stamp="stamp_1", lines=log_lines, vo="gridpp")

    check_permissions_mock = AsyncMock()
    check_permissions_mock.return_value.vo = "gridpp"
    # TODO add user properties dict return_value above
    mock_url.return_value = {"PilotAgentsDB": "sqlite+aiosqlite:///:memory:"}
    # use the existing context (we have a DB already):
    pilot_agents_db.engine_context = nullcontext
    mock_impl.return_value = [lambda x: pilot_agents_db]
    # send logs for stamp_1, pilot id = 1
    pilot_id = await send_message(message, pilot_logs_db, check_permissions_mock)
    assert pilot_id == 1
    # get logs for pilot_id=1
    log_records = await get_logs(pilot_id, pilot_logs_db, check_permissions_mock)
    assert log_records == line
    # delete logs for pilot_id = 1
    check_permissions_mock.return_value.properties = [PILOT]
    # TODO: await mock_osdb delete implementation...
    # res = await delete(pilot_id, DateRange(), pilot_logs_db, check_permissions_mock)
