"""Logic of the Lollygag example."""

from __future__ import annotations

from gubbins.db.sql import LollygagDB


async def insert_owner_object(
    lollygag_db: LollygagDB,
    owner_name: str,
):
    """Insert an owner.

    Args:
        lollygag_db: Lollygag database.
        owner_name: Name of the owner.

    Returns:
        The ID of the new owner.
    """
    return await lollygag_db.insert_owner(owner_name)


async def get_owner_object(
    lollygag_db: LollygagDB,
):
    """Get the names of all the owners.

    Args:
        lollygag_db: Lollygag database.

    Returns:
        The owner names.
    """
    return await lollygag_db.get_owner()


async def get_gubbins_secrets(
    lollygag_db: LollygagDB,
):
    """Does nothing but expects a GUBBINS_SENSEI permission.

    Args:
        lollygag_db: Lollygag database.

    Returns:
        The owner names.
    """
    return await lollygag_db.get_owner()
