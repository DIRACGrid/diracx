from __future__ import annotations

import functools
from enum import StrEnum, auto
from typing import Annotated, Self

from fastapi import Depends


class ActionType(StrEnum):
    CREATE = auto()
    READ = auto()
    MANAGE = auto()
    QUERY = auto()


def get_user_name():
    return "toto"


class BaseAccessPolicy:
    @classmethod
    def check(cls) -> Self:
        raise NotImplementedError("This should never be called")


def policy_implementation(user_name):
    print(f"Is {user_name} allowed ? ")


class WMSAccessPolicy(BaseAccessPolicy):
    # policy = staticmethod(policy_implementation)
    @staticmethod
    def policy(user_name):
        print(f"Is {user_name} allowed ? ")

    # def depend_real(sub: Annotated[str, Depends(get_user_name)]):
    #     return f"That's the real deal {sub}"


# def check_permissions_wrapper(an_int: int, sub: Annotated[str, Depends(get_user_name)]):
#     return f"I am a fraud {an_int} {sub}"


def check_permissions(
    obj: BaseAccessPolicy, user_name: Annotated[str, Depends(get_user_name)]
):
    @functools.wraps(obj.policy)
    async def wrapped_policy(**kwargs):
        return obj.policy(user_name, **kwargs)

    yield wrapped_policy
    # return obj.policy(sub)
