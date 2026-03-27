from __future__ import annotations

import argparse
import asyncio
import logging

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True, dest="command")

    subparsers.add_parser("init-sql", help="Initialise schema for SQL databases")
    subparsers.add_parser("init-os", help="Initialise schema for OpenSearch databases")

    local_urls_parser = subparsers.add_parser(
        "generate-local-urls",
        help="Print shell exports for all registered DB URLs using sqlite",
    )
    local_urls_parser.add_argument(
        "tmp_dir", help="Temporary directory for database files"
    )

    args = parser.parse_args()
    logger.setLevel(logging.INFO)

    if args.command == "init-sql":
        asyncio.run(init_sql())
    elif args.command == "init-os":
        asyncio.run(init_os())
    elif args.command == "generate-local-urls":
        generate_local_urls(args.tmp_dir)


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
                await db.post_create(conn)


async def init_os():
    logger.info("Initialising OpenSearch databases")
    from diracx.db.os.utils import BaseOSDB

    for db_name, db_url in BaseOSDB.available_urls().items():
        logger.info("Initialising %s", db_name)
        db = BaseOSDB.available_implementations(db_name)[0](db_url)
        async with db.client_context():
            await db.create_index_template()


def generate_local_urls(tmp_dir: str) -> None:
    """Print shell export statements for all registered DB URLs using sqlite.

    Intended for use with eval in shell scripts::

        eval "$(python -m diracx.db generate-local-urls /tmp/dir)"
    """
    import json

    from diracx.core.extensions import DiracEntryPoint, select_from_extension

    seen: set[str] = set()
    for ep in select_from_extension(group=DiracEntryPoint.SQL_DB):
        if ep.name in seen:
            continue
        seen.add(ep.name)
        url = f"sqlite+aiosqlite:///{tmp_dir}/{ep.name.lower()}.db"
        print(f'export DIRACX_DB_URL_{ep.name.upper()}="{url}"')

    seen.clear()
    for ep in select_from_extension(group=DiracEntryPoint.OS_DB):
        if ep.name in seen:
            continue
        seen.add(ep.name)
        v = json.dumps(
            {"sqlalchemy_dsn": f"sqlite+aiosqlite:///{tmp_dir}/{ep.name.lower()}.db"}
        )
        print(f"export DIRACX_OS_DB_{ep.name.upper()}='{v}'")


if __name__ == "__main__":
    parse_args()
