"""RSS API calling and apply translation for the read adapter module."""

from __future__ import annotations

from typing import Any, Dict

# Direct client calls - no need for separate client_calls module
from rss_read_adapter.response_translation import (
    translate_computing_element_status,
    translate_fts_status,
    translate_site_status,
    translate_storage_element_status,
)


async def get_storage_element_status(client: Any) -> Dict[str, Dict[str, Any]]:
    """Get storage element status from the RSS API and translate to legacy format.

    Args:
        client: The diracx client instance

    Returns:
        Dictionary in legacy format with element names as keys

    """
    response = await client.rss.get_storage_status()
    return translate_storage_element_status(response)


async def get_computing_element_status(client: Any) -> Dict[str, Dict[str, Any]]:
    """Get computing element status from the RSS API and translate to legacy format.

    Args:
        client: The diracx client instance

    Returns:
        Dictionary in legacy format with element names as keys

    """
    response = await client.rss.get_compute_status()
    return translate_computing_element_status(response)


async def get_fts_status(client: Any) -> Dict[str, Dict[str, Any]]:
    """Get merged FTS server status from all VOs.

    Args:
        client: The diracx client instance

    Returns:
        Merged dictionary in legacy format

    """
    response = await client.rss.get_fts_status()
    return translate_fts_status(response)


async def get_site_status(client: Any) -> Dict[str, Dict[str, Any]]:
    """Get site status from the RSS API and translate to legacy format.

    Args:
        client: The diracx client instance

    Returns:
        Dictionary in legacy format with site names as keys

    """
    response = await client.rss.get_site_status()
    return translate_site_status(response)
