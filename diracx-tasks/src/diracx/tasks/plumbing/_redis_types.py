"""Role-specific Redis type aliases.

Zero runtime cost — purely for readability and grep-ability.
Each alias documents *why* a function needs a Redis connection:

- ``LockCoordinator`` — acquiring / releasing / extending locks
- ``MessageTransport`` — enqueuing, reading, or promoting task messages
- ``ResultCache`` — storing / retrieving task results
- ``CallbackRegistry`` — tracking callback groups and firing callbacks
"""

from __future__ import annotations

from typing import TypeAlias

from redis.asyncio import Redis

LockCoordinator: TypeAlias = Redis
MessageTransport: TypeAlias = Redis
ResultCache: TypeAlias = Redis
CallbackRegistry: TypeAlias = Redis
