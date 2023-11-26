from __future__ import annotations

import pytest

from diracx.db.sql.auth.db import AuthDB
from diracx.db.sql.auth.schema import RefreshTokenStatus


@pytest.fixture
async def auth_db(tmp_path):
    auth_db = AuthDB("sqlite+aiosqlite:///:memory:")
    async with auth_db.engine_context():
        async with auth_db.engine.begin() as conn:
            await conn.run_sync(auth_db.metadata.create_all)
        yield auth_db


async def test_insert(auth_db: AuthDB):
    """Insert two refresh tokens in the DB and check that they don't share the same JWT ID"""

    # Insert a first refresh token
    async with auth_db as auth_db:
        jti1, _ = await auth_db.insert_refresh_token(
            "subject",
            "username",
            "vo:lhcb property:NormalUser",
        )

    # Insert a second refresh token
    async with auth_db as auth_db:
        jti2, _ = await auth_db.insert_refresh_token(
            "subject",
            "username",
            "vo:lhcb property:NormalUser",
        )

    # Make sure they don't have the same JWT ID
    assert jti1 != jti2


async def test_get(auth_db: AuthDB):
    """Insert a refresh token in the DB and get it"""
    # Refresh token details we want to insert
    refresh_token_details = {
        "sub": "12345",
        "preferred_username": "John Doe",
        "scope": "vo:lhcb property:NormalUser",
    }

    # Insert refresh token details
    async with auth_db as auth_db:
        jti, creation_time = await auth_db.insert_refresh_token(
            refresh_token_details["sub"],
            refresh_token_details["preferred_username"],
            refresh_token_details["scope"],
        )

    # Enrich the dict with the generated refresh token attributes
    refresh_token_details["jti"] = jti
    refresh_token_details["status"] = RefreshTokenStatus.CREATED
    refresh_token_details["creation_time"] = creation_time

    # Get refresh token details
    async with auth_db as auth_db:
        result = await auth_db.get_refresh_token(jti)

    # Make sure they are identical
    assert result == refresh_token_details


async def test_get_user_refresh_tokens(auth_db: AuthDB):
    """Insert refresh tokens belonging to different users in the DB and
    get the refresh tokens of each user
    """
    # Two users
    sub1 = "subject1"
    sub2 = "subject2"

    # Insert tokens
    # - 2 of them belongs to sub1
    # - 1 of them belongs to sub2
    subjects = [sub1, sub1, sub2]
    async with auth_db as auth_db:
        for sub in subjects:
            await auth_db.insert_refresh_token(
                sub,
                "username",
                "scope",
            )

    # Get the refresh tokens of each user
    async with auth_db as auth_db:
        refresh_tokens_user1 = await auth_db.get_user_refresh_tokens(sub1)
        refresh_tokens_user2 = await auth_db.get_user_refresh_tokens(sub2)

    # Check the number of refresh tokens belonging to the users
    # And check that the subject value corresponds to the user's subject
    assert len(refresh_tokens_user1) == 2
    for refresh_token in refresh_tokens_user1:
        assert refresh_token["sub"] == sub1

    assert len(refresh_tokens_user2) == 1
    for refresh_token in refresh_tokens_user2:
        assert refresh_token["sub"] == sub2


async def test_revoke(auth_db: AuthDB):
    """Insert a refresh token in the DB, revoke it, and make sure it appears as REVOKED in the db"""
    # Insert a refresh token details
    async with auth_db as auth_db:
        jti, _ = await auth_db.insert_refresh_token(
            "subject",
            "username",
            "scope",
        )

    # Revoke the token
    async with auth_db as auth_db:
        await auth_db.revoke_refresh_token(jti)

    # Make sure it is revoked
    async with auth_db as auth_db:
        refresh_token_details = await auth_db.get_refresh_token(jti)

    assert refresh_token_details["status"] == RefreshTokenStatus.REVOKED


async def test_revoke_user_refresh_tokens(auth_db: AuthDB):
    """Insert refresh tokens in the DB, revoke them, and make sure it appears as REVOKED in the db"""
    # Two users
    sub1 = "subject1"
    sub2 = "subject2"

    # Insert tokens
    # - 2 of them belongs to sub1
    # - 1 of them belongs to sub2
    subjects = [sub1, sub1, sub2]
    async with auth_db as auth_db:
        for sub in subjects:
            await auth_db.insert_refresh_token(
                sub,
                "username",
                "scope",
            )

    # Revoke the tokens of sub1
    async with auth_db as auth_db:
        await auth_db.revoke_user_refresh_tokens(sub1)

    # Make sure they are revoked (but not the ones belonging to sub2)
    async with auth_db as auth_db:
        refresh_token_details = await auth_db.get_user_refresh_tokens(sub1)
        assert len(refresh_token_details) == 0
        refresh_token_details = await auth_db.get_user_refresh_tokens(sub2)
        assert len(refresh_token_details) == 1

    # Revoke the tokens of sub2
    async with auth_db as auth_db:
        await auth_db.revoke_user_refresh_tokens(sub2)

    # Make sure they are all revoked
    async with auth_db as auth_db:
        refresh_token_details = await auth_db.get_user_refresh_tokens(sub1)
        assert len(refresh_token_details) == 0
        refresh_token_details = await auth_db.get_user_refresh_tokens(sub2)
        assert len(refresh_token_details) == 0


async def test_revoke_and_get_user_refresh_tokens(auth_db: AuthDB):
    """Insert refresh tokens belonging to a user, revoke one of them and
    make sure that only the active tokens appear
    """
    # User
    sub = "subject"

    # Number of tokens to insert
    nb_tokens = 2

    # Insert tokens
    jtis = []
    async with auth_db as auth_db:
        for _ in range(nb_tokens):
            jti, _ = await auth_db.insert_refresh_token(
                sub,
                "username",
                "scope",
            )
            jtis.append(jti)

    # Get the refresh tokens of the user
    async with auth_db as auth_db:
        refresh_tokens_user = await auth_db.get_user_refresh_tokens(sub)

    # Check the number of refresh tokens belonging to the user
    # And check that the subject value corresponds to the user's subject
    assert len(refresh_tokens_user) == nb_tokens
    for refresh_token in refresh_tokens_user:
        assert refresh_token["sub"] == sub

    # Revoke one of the tokens
    async with auth_db as auth_db:
        await auth_db.revoke_refresh_token(jtis[0])

    # Get the refresh tokens of the user again
    async with auth_db as auth_db:
        refresh_tokens_user = await auth_db.get_user_refresh_tokens(sub)

    # Check that there is less refresh tokens returned
    # And check that the subject value corresponds to the user's subject
    assert len(refresh_tokens_user) == nb_tokens - 1
    for refresh_token in refresh_tokens_user:
        assert refresh_token["sub"] == sub
        assert refresh_token["jti"] != jtis[0]


async def test_get_refresh_tokens(auth_db: AuthDB):
    """Insert refresh tokens belonging to different users in the DB and
    get the refresh tokens
    """
    # Two users
    sub1 = "subject1"
    sub2 = "subject2"

    # Insert tokens
    # - 2 of them belongs to sub1
    # - 1 of them belongs to sub2
    subjects = [sub1, sub1, sub2]
    async with auth_db as auth_db:
        for sub in subjects:
            await auth_db.insert_refresh_token(
                sub,
                "username",
                "scope",
            )

    # Get all refresh tokens (Admin)
    async with auth_db as auth_db:
        refresh_tokens = await auth_db.get_user_refresh_tokens()

    # Check the number of retrieved refresh tokens (should be 3 refresh tokens)
    assert len(refresh_tokens) == 3

    # Get user refresh tokens (sub1)
    async with auth_db as auth_db:
        refresh_tokens = await auth_db.get_user_refresh_tokens(sub1)

    # Check the number of retrieved refresh tokens (should be 3 refresh tokens)
    assert len(refresh_tokens) == 2
