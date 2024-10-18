from __future__ import annotations

import argparse
import asyncio
import logging

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    init_sql_parser = subparsers.add_parser(
        "init-sql", help="Initialise schema for SQL databases"
    )
    init_sql_parser.set_defaults(func=init_sql)

    init_os_parser = subparsers.add_parser(
        "init-os", help="Initialise schema for OpenSearch databases"
    )
    init_os_parser.set_defaults(func=init_os)

    args = parser.parse_args()
    logger.setLevel(logging.INFO)
    asyncio.run(args.func())


async def init_sql():
    logger.info("Initialising SQL databases")
    from diracx.db.sql.utils import BaseSQLDB

    for db_name, db_url in BaseSQLDB.available_urls().items():

        logger.info("Initialising %s", db_name)
        db = BaseSQLDB.available_implementations(db_name)[0](db_url)
        async with db.engine_context():
            async with db.engine.begin() as conn:
                # set PRAGMA foreign_keys=ON if sqlite
                if db._db_url.startswith("sqlite"):
                    await conn.exec_driver_sql("PRAGMA foreign_keys=ON")
                await conn.run_sync(db.metadata.create_all)


async def init_os():
    logger.info("Initialising OpenSearch databases")
    from diracx.db.os.utils import BaseOSDB

    for db_name, db_url in BaseOSDB.available_urls().items():
        logger.info("Initialising %s", db_name)
        db = BaseOSDB.available_implementations(db_name)[0](db_url)
        async with db.client_context():
            await db.create_index_template()


if __name__ == "__main__":
    parse_args()
