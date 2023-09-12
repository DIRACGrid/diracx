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

    args = parser.parse_args()
    logger.setLevel(logging.INFO)
    asyncio.run(args.func())


async def init_sql():
    logger.info("Initialising SQL databases")
    from diracx.db.utils import BaseDB

    for db_name, db_url in BaseDB.available_urls().items():
        logger.info("Initialising %s", db_name)
        db = BaseDB.available_implementations(db_name)[0](db_url)
        async with db.engine_context():
            async with db.engine.begin() as conn:
                await conn.run_sync(db.metadata.create_all)


if __name__ == "__main__":
    parse_args()
