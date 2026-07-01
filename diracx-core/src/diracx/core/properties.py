"""Security property definitions for user and group authorization.

This module defines the built-in DIRACX security properties and provides
helpers for evaluating combinations of those properties. These properties are
used to express access rights in the authorization subsystem.
"""

from __future__ import annotations

__all__ = [
    # Properties
    "ACCOUNTING_MONITOR",
    "ALARMS_MANAGEMENT",
    "BOOKKEEPING_MANAGEMENT",
    "CS_ADMINISTRATOR",
    "FC_MANAGEMENT",
    "FULL_DELEGATION",
    "GENERIC_PILOT",
    "JOB_ADMINISTRATOR",
    "JOB_MONITOR",
    "JOB_SHARING",
    "LIMITED_DELEGATION",
    "NORMAL_USER",
    "OPERATOR",
    "PILOT",
    "PPG_AUTHORITY",
    "PRIVATE_LIMITED_DELEGATION",
    "PRODUCTION_MANAGEMENT",
    "PROXY_MANAGEMENT",
    "SERVICE_ADMINISTRATOR",
    "SITE_MANAGER",
    "TRUSTED_HOST",
    "USER_MANAGER",
    # Classes
    "SecurityProperty",
    "UnevaluatedProperty",
]

import inspect
import operator
from collections.abc import Callable
from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from .extensions import select_from_extension


class SecurityProperty(str):
    """A named security property value used for authorization checks.

    `SecurityProperty` is a thin wrapper around `str` that enables symbolic
    handling of DIRACX security properties and interoperates with Pydantic.
    """

    @classmethod
    def available_properties(cls) -> set[SecurityProperty]:
        """Return all security properties discovered from extensions.

        Returns:
            set[SecurityProperty]: All configured security property values.
        """
        properties = set()
        for entry_point in select_from_extension(
            group="diracx", name="properties_module"
        ):
            properties_module = entry_point.load()
            for _, obj in inspect.getmembers(properties_module):
                if isinstance(obj, SecurityProperty):
                    properties.add(obj)
        return properties

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Provide a Pydantic core schema for this property type."""
        return core_schema.no_info_after_validator_function(cls, handler(str))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self})"

    def __and__(
        self, value: SecurityProperty | UnevaluatedProperty
    ) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) & value

    def __or__(
        self, value: SecurityProperty | UnevaluatedProperty
    ) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) | value

    def __xor__(
        self, value: SecurityProperty | UnevaluatedProperty
    ) -> UnevaluatedExpression:
        if not isinstance(value, UnevaluatedProperty):
            value = UnevaluatedProperty(value)
        return UnevaluatedProperty(self) ^ value

    def __invert__(self: SecurityProperty) -> UnevaluatedExpression:
        return ~UnevaluatedProperty(self)


class UnevaluatedProperty:
    """A property expression that can be evaluated against allowed properties.

    Instances represent a single property or a compound property expression
    that can later be called with a list of allowed properties to determine if
    it is satisfied.
    """

    def __init__(self, property: SecurityProperty):
        self.property = property

    def __str__(self) -> str:
        return str(self.property)

    def __repr__(self) -> str:
        return repr(self.property)

    def __call__(self, allowed_properties: list[SecurityProperty]) -> bool:
        """Evaluate whether this property is allowed.

        Args:
            allowed_properties (list[SecurityProperty]): The properties that are
                permitted in the current context.

        Returns:
            bool: True if this property is included in allowed_properties.
        """
        return self.property in allowed_properties

    def __and__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__and__, self, value)

    def __or__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__or__, self, value)

    def __xor__(self, value: UnevaluatedProperty) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__xor__, self, value)

    def __invert__(self) -> UnevaluatedExpression:
        return UnevaluatedExpression(operator.__invert__, self)


class UnevaluatedExpression(UnevaluatedProperty):
    """A logical expression of unevaluated properties.

    `UnevaluatedExpression` composes property operands with logical operators
    such as AND, OR, XOR, and NOT, and can be evaluated against a list of
    allowed properties.
    """

    def __init__(self, operator: Callable[..., bool], *args: UnevaluatedProperty):
        self.operator = operator
        self.args = args

    def __str__(self) -> str:
        if self.operator == operator.__invert__:
            return f"~{self.args[0]}"
        symbol = {
            operator.__and__: "&",
            operator.__or__: "|",
            operator.__xor__: "^",
        }[self.operator]
        return f"({self.args[0]} {symbol} {self.args[1]})"

    def __repr__(self) -> str:
        return f"{self.operator.__name__}({', '.join(map(repr, self.args))})"

    def __call__(self, properties: list[SecurityProperty]) -> bool:
        """Evaluate the expression against a set of properties.

        Args:
            properties (list[SecurityProperty]): The allowed security properties.

        Returns:
            bool: True if the expression evaluates to true for the given properties.
        """
        return self.operator(*(a(properties) for a in self.args))


# A host property. This property is used::
# * For a host to forward credentials in an RPC call
TRUSTED_HOST = SecurityProperty("TrustedHost")
# Normal user operations
NORMAL_USER = SecurityProperty("NormalUser")
# CS Administrator - possibility to edit the Configuration Service
CS_ADMINISTRATOR = SecurityProperty("CSAdministrator")
# Job sharing among members of a group
JOB_SHARING = SecurityProperty("JobSharing")
# DIRAC Service Administrator
SERVICE_ADMINISTRATOR = SecurityProperty("ServiceAdministrator")
# Job Administrator can manipulate everybody's jobs
JOB_ADMINISTRATOR = SecurityProperty("JobAdministrator")
# Job Monitor - can get job monitoring information
JOB_MONITOR = SecurityProperty("JobMonitor")
# Accounting Monitor - can see accounting data for all groups
ACCOUNTING_MONITOR = SecurityProperty("AccountingMonitor")
# Private pilot
PILOT = SecurityProperty("Pilot")
# Generic pilot
GENERIC_PILOT = SecurityProperty("GenericPilot")
# Site Manager
SITE_MANAGER = SecurityProperty("SiteManager")
# User, group, VO Registry management
USER_MANAGER = SecurityProperty("UserManager")
# Operator
OPERATOR = SecurityProperty("Operator")
# Allow getting full delegated proxies
FULL_DELEGATION = SecurityProperty("FullDelegation")
# Allow getting only limited proxies (ie. pilots)
LIMITED_DELEGATION = SecurityProperty("LimitedDelegation")
# Allow getting only limited proxies for one self
PRIVATE_LIMITED_DELEGATION = SecurityProperty("PrivateLimitedDelegation")
# Allow managing proxies
PROXY_MANAGEMENT = SecurityProperty("ProxyManagement")
# Allow managing production
PRODUCTION_MANAGEMENT = SecurityProperty("ProductionManagement")
# Allow production request approval on behalf of PPG
PPG_AUTHORITY = SecurityProperty("PPGAuthority")
# Allow Bookkeeping Management
BOOKKEEPING_MANAGEMENT = SecurityProperty("BookkeepingManagement")
# Allow to set notifications and manage alarms
ALARMS_MANAGEMENT = SecurityProperty("AlarmsManagement")
# Allow FC Management - FC root user
FC_MANAGEMENT = SecurityProperty("FileCatalogManagement")
# Allow staging files
STAGE_ALLOWED = SecurityProperty("StageAllowed")
# # TODO: LHCb specific
# STEP_ADMINISTRATOR = SecurityProperty("StepAdministrator")
