"""Response translation for the read adapter module.

This module translates responses from diracx Pydantic models to legacy format.
"""

from __future__ import annotations

from typing import Any, Dict

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)


def translate_storage_element_status(
    response: Dict[str, StorageElementStatus]
) -> Dict[str, Dict[str, Any]]:
    """Translate storage element status from diracx format to legacy format.

    Args:
        response: Dictionary of storage element names to StorageElementStatus

    Returns:
        Dictionary in legacy format
    """
    legacy_format = {}
    for name, status in response.items():
        legacy_format[name] = {
            "ReadAccess": _translate_resource_status(status.read),
            "WriteAccess": _translate_resource_status(status.write),
            "CheckAccess": _translate_resource_status(status.check),
            "RemoveAccess": _translate_resource_status(status.remove),
        }
    return legacy_format


def translate_computing_element_status(
    response: Dict[str, ComputeElementStatus]
) -> Dict[str, Dict[str, Any]]:
    """Translate computing element status from diracx format to legacy format.

    Args:
        response: Dictionary of computing element names to ComputeElementStatus

    Returns:
        Dictionary in legacy format
    """
    legacy_format = {}
    for name, status in response.items():
        legacy_format[name] = {
            "Status": _translate_resource_status(status.all),
        }
    return legacy_format


def translate_fts_status(
    response: Dict[str, FTSStatus]
) -> Dict[str, Dict[str, Any]]:
    """Translate FTS server status from diracx format to legacy format.

    Args:
        response: Dictionary of FTS server names to FTSStatus

    Returns:
        Dictionary in legacy format
    """
    legacy_format = {}
    for name, status in response.items():
        legacy_format[name] = {
            "Status": _translate_resource_status(status.all),
        }
    return legacy_format


def translate_site_status(
    response: Dict[str, SiteStatus]
) -> Dict[str, Dict[str, Any]]:
    """Translate site status from diracx format to legacy format.

    Args:
        response: Dictionary of site names to SiteStatus

    Returns:
        Dictionary in legacy format
    """
    legacy_format = {}
    for name, status in response.items():
        legacy_format[name] = {
            "Status": _translate_resource_status(status.all),
        }
    return legacy_format


def _translate_resource_status(status: AllowedStatus | BannedStatus) -> str:
    """Translate a single resource status from diracx format to legacy format.

    Args:
        status: ResourceStatus (AllowedStatus or BannedStatus)

    Returns:
        Legacy status string
    """
    if isinstance(status, AllowedStatus):
        if status.warnings:
            return "Degraded"
        return "Active"
    else:  # BannedStatus
        return status.reason or "Banned"