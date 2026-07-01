"""Module to implement the logic of the configuration server side.

This module provides classes and helpers for reading configuration from
various backends such as local or remote Git repositories. It also manages
caching of revision metadata and content to minimize backend load.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Generic, TypeVar
from urllib.parse import urlparse, urlunparse

import sh
import yaml
from cachetools import Cache, LRUCache
from pydantic import AnyUrl, BeforeValidator, TypeAdapter, UrlConstraints

from diracx.core.exceptions import BadConfigurationVersionError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.utils import TwoLevelCache

from .schema import Config

DEFAULT_CONFIG_FILE = "default.yml"
DEFAULT_GIT_BRANCH = "master"
DEFAULT_CS_REV_CACHE_SOFT_TTL = 5
# TODO: Reduce the hard TTL when we have more redundancy around the source of truth
DEFAULT_CS_REV_CACHE_HARD_TTL = 60 * 60
DEFAULT_CS_CONTENT_HARD_TTL = 15

logger = logging.getLogger(__name__)


def is_running_in_async_context() -> bool:
    """Return whether the current code is executing inside an asyncio event loop.

    Returns:
        bool: True when running inside an async context, False otherwise.
    """
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _apply_default_scheme(value: str) -> str:
    """Normalize a config backend string into a full git URL.

    Args:
        value (str): A backend URL or path.

    Returns:
        str: A backend URL with the default ``git+file://`` scheme applied when
            no scheme is present.
    """
    if "://" not in value:
        value = f"git+file://{value}"
    return value


class AnyUrlWithoutHost(AnyUrl):
    """URL type that allows missing host components for local file backends."""

    _constraints = UrlConstraints(host_required=False)


ConfigSourceUrl = Annotated[AnyUrlWithoutHost, BeforeValidator(_apply_default_scheme)]

T = TypeVar("T")


class CacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for sources that can be cached.

    Handles the caching of the latest revision and its content using a two-level cache.
    """

    def __init__(self):
        # Revision cache is used to store the latest revision and its
        # modification date. This cache has two TTLs, one which triggers the
        # background refresh and the other which is results in a hard failure.
        # This allows us to avoid blocking while the refresh is done, while
        # maintaining strong guarantees on the data freshness.
        self._revision_cache = TwoLevelCache(
            soft_ttl=DEFAULT_CS_REV_CACHE_SOFT_TTL,
            hard_ttl=DEFAULT_CS_REV_CACHE_HARD_TTL,
            max_workers=1,
            max_items=1,
        )
        # The content of a given revision can be stored in a simple LRU cache
        # We keep the last two versions in memory to avoid any potential to flip
        # flop between two versions when it changes.
        self._content_cache: Cache = LRUCache(maxsize=2)

    @abstractmethod
    def latest_revision(self) -> tuple[str, datetime]:
        """Return the latest revision identifier and its modification time.

        Returns:
            tuple[str, datetime]: A tuple of revision hash and modification timestamp.
        """

    @abstractmethod
    def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Read the raw configuration content for a specific revision.

        Args:
            hexsha (str): The revision hash to read.
            modified (datetime): The revision modification timestamp.

        Returns:
            T: The parsed configuration object for the requested revision.
        """

    def read(self) -> T:
        """Load the source from the backend using blocking cache refresh.

        Returns:
            T: The parsed configuration object for the latest revision.

        Raises:
            diracx.core.exceptions.NotReadyError: If the source is still loading.
            git.exc.BadName: If the requested revision does not exist.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Load the source from the backend without blocking on freshness.

        Returns:
            T: The parsed configuration object for the latest revision.

        Raises:
            diracx.core.exceptions.NotReadyError: If the source is still loading.
            git.exc.BadName: If the requested revision does not exist.
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_work, blocking=False
        )
        return self._content_cache[hexsha]

    def _read_work(self) -> str:
        """Refresh the latest revision and populate the content cache.

        This function ensures that the latest revision is loaded into the
        content cache before it is admitted into the revision cache.

        Returns:
            str: The latest revision hash loaded into the cache.
        """
        hexsha, modified = self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = self.read_raw(hexsha, modified)
        return hexsha

    def clear_caches(self):
        """Clear both revision and content caches.

        This forces future reads to reload configuration metadata and content
        from the backend.
        """
        self._revision_cache.clear()
        self._content_cache.clear()


class ConfigSource(CacheableSource[Config]):
    """Abstract configuration source supporting backend-specific implementations.

    Subclasses are expected to implement revision discovery and raw content
    reading while the shared base class handles caching and refresh logic.
    """

    # Keep a mapping between the scheme and the class
    __registry: dict[str, type["ConfigSource"]] = {}
    scheme: str

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__()

    def __init_subclass__(cls) -> None:
        """Register a configuration source subclass by its URL scheme."""
        if cls.scheme in cls.__registry:
            raise TypeError(f"{cls.scheme=} is already define")
        cls.__registry[cls.scheme] = cls

    @classmethod
    def create(cls):
        """Create a config source from the environment backend URL."""
        return cls.create_from_url(backend_url=os.environ["DIRACX_CONFIG_BACKEND_URL"])

    @classmethod
    def create_from_url(
        cls, *, backend_url: ConfigSourceUrl | Path | str
    ) -> "ConfigSource":
        """Produce a concrete config source instance based on URL scheme.

        Args:
            backend_url (ConfigSourceUrl | Path | str): The backend URL to resolve.

        Returns:
            ConfigSource: A concrete source implementation for the scheme.
        """
        url = TypeAdapter(ConfigSourceUrl).validate_python(str(backend_url))
        return cls.__registry[url.scheme](backend_url=url)


class BaseGitConfigSource(ConfigSource):
    """Base class for Git-backed configuration sources."""

    repo_location: Path

    # Needed because of the ConfigSource.__init_subclass__
    scheme = "basegit"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        self.remote_url = self.extract_remote_url(backend_url)
        self.git_revision = self.get_git_revision_from_url(backend_url)

    def latest_revision(self) -> tuple[str, datetime]:
        try:
            rev = sh.git(
                "rev-parse",
                self.git_revision,
                _cwd=self.repo_location,
                _tty_out=False,
                _async=is_running_in_async_context(),
            ).strip()
            commit_info = sh.git.show(
                "-s",
                "--format=%ct",
                rev,
                _cwd=self.repo_location,
                _tty_out=False,
                _async=is_running_in_async_context(),
            ).strip()
            modified = datetime.fromtimestamp(int(commit_info), tz=timezone.utc)
        except sh.ErrorReturnCode as e:
            raise BadConfigurationVersionError(
                f"Error parsing latest revision: {e}"
            ) from e
        logger.debug("Latest revision for %s is %s with mtime %s", self, rev, modified)
        return rev, modified

    def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """:param: hexsha commit hash"""
        logger.debug("Reading %s for %s with mtime %s", self, hexsha, modified)
        try:
            blob = sh.git.show(
                f"{hexsha}:{DEFAULT_CONFIG_FILE}",
                _cwd=self.repo_location,
                _tty_out=False,
                _async=False,
            )
            raw_obj = yaml.safe_load(blob)
        except sh.ErrorReturnCode as e:
            raise BadConfigurationVersionError(
                f"Error reading configuration: {e}"
            ) from e

        config_class: Config = select_from_extension(
            group=DiracEntryPoint.CORE, name="config"
        )[0].load()
        config = config_class.model_validate(raw_obj)
        config._hexsha = hexsha
        config._modified = modified
        return config

    def extract_remote_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the remote repository URL from a backend URL.

        Args:
            backend_url (ConfigSourceUrl): The backend URL to parse.

        Returns:
            str: The remote URL without the ``git+`` prefix or query parameters.
        """
        parsed_url = urlparse(str(backend_url).replace("git+", ""))
        remote_url = urlunparse(parsed_url._replace(query=""))
        return remote_url

    def get_git_revision_from_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the requested Git revision from a backend URL.

        Args:
            backend_url (ConfigSourceUrl): The backend URL to inspect.

        Returns:
            str: The requested Git revision or the default branch if none is provided.
        """
        return dict(backend_url.query_params()).get("revision", DEFAULT_GIT_BRANCH)


class LocalGitConfigSource(BaseGitConfigSource):
    """Configuration source backed by a local Git repository.

    When running on multiple servers, the filesystem must be shared.
    """

    scheme = "git+file"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        if not backend_url.path:
            raise ValueError("Empty path for LocalGitConfigSource")

        self.repo_location = Path(backend_url.path)
        # Check if it's a valid git repository
        try:
            sh.git(
                "rev-parse",
                "--git-dir",
                _cwd=self.repo_location,
                _tty_out=False,
                _async=False,
            )
        except sh.ErrorReturnCode as e:
            raise ValueError(
                f"{self.repo_location} is not a valid git repository"
            ) from e
        sh.git.checkout(self.git_revision, _cwd=self.repo_location, _async=False)

    def __hash__(self):
        return hash(self.repo_location)


class RemoteGitConfigSource(BaseGitConfigSource):
    """Configuration source backed by a remote Git repository."""

    scheme = "git+https"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        if not backend_url:
            raise ValueError("No remote url for RemoteGitConfigSource")

        self._temp_dir = TemporaryDirectory()
        self.repo_location = Path(self._temp_dir.name)
        sh.git.clone(self.remote_url, self.repo_location, _async=False)
        sh.git.checkout(self.git_revision, _cwd=self.repo_location, _async=False)

    def __hash__(self):
        return hash(self.repo_location)

    def latest_revision(self) -> tuple[str, datetime]:
        """Pull the latest revision from the remote repository before returning it.

        Returns:
            tuple[str, datetime]: The latest revision hash and its modification time.
        """
        logger.debug("Pulling latest version from %s", self)
        try:
            sh.git.pull(_cwd=self.repo_location, _async=False)
        except sh.ErrorReturnCode as err:
            logger.exception(err)

        return super().latest_revision()
