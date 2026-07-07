"""Response translation for the read adapter module.

This module translates responses from diracx Pydantic models to legacy format.
"""

from __future__ import annotations

from typing import Dict

from diracx.core.models.rss import (
    BANNED,
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)


def translate_storage_element_status(
    response: Dict[str, StorageElementStatus],
) -> list[tuple]:
    """Translate storage element status from diracx format to legacy format.

    Args:
        response: Dictionary of storage element names to StorageElementStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "StorageElement",
                "ReadAccess",
                _translate_resource_status(status.read),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "WriteAccess",
                _translate_resource_status(status.write),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "CheckAccess",
                _translate_resource_status(status.check),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "RemoveAccess",
                _translate_resource_status(status.remove),
                None,
            )
        )

    return legacy_format


def translate_computing_element_status(
    response: Dict[str, ComputeElementStatus],
) -> list[tuple]:
    """Translate computing element status from diracx format to legacy format.

    Args:
        response: Dictionary of computing element names to ComputeElementStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "ComputeElement",
                "all",
                _translate_resource_status(status.all),
                None,
            )
        )
    return legacy_format


def translate_fts_status(response: Dict[str, FTSStatus]) -> list[tuple]:
    """Translate FTS server status from diracx format to legacy format.

    Args:
        response: Dictionary of FTS server names to FTSStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "FTS",
                "all",
                _translate_resource_status(status.all),
                None,
            )
        )
    return legacy_format


def translate_site_status(response: Dict[str, SiteStatus]) -> list[tuple]:
    """Translate site status from diracx format to legacy format.

    Args:
        response: Dictionary of site names to SiteStatus

    Returns:
        List of tuples in legacy format: (site, status)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append((name, _translate_resource_status(status.all)))
    return legacy_format


def _translate_resource_status(status: AllowedStatus | BannedStatus) -> str:
    """Translate a single resource status from diracx format to legacy format.

    Args:
        status: ResourceStatus (AllowedStatus or BannedStatus)

    Returns:
        Legacy status string

    """
    if isinstance(status, AllowedStatus):
        if status.warnings == "Degraded":
            return "Degraded"
        return "Active"
    else:  # BannedStatus
        return status.reason if status.reason in BANNED - {"Unknown"} else "Banned"
