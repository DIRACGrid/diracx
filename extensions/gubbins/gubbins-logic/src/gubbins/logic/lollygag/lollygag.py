from __future__ import annotations

from gubbins.db.sql import LollygagDB


async def insert_owner_object(
    lollygag_db: LollygagDB,
    owner_name: str,
):
    return await lollygag_db.insert_owner(owner_name)


async def get_owner_object(
    lollygag_db: LollygagDB,
):
    return await lollygag_db.get_owner()


async def get_gubbins_secrets(
    lollygag_db: LollygagDB,
):
    """Does nothing but expects a GUBBINS_SENSEI permission"""
    return await lollygag_db.get_owner()
