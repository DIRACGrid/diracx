"""Module to implement the logic of the configuration server side.

This is where all the backend abstraction and the caching logic takes place.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
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


def _apply_default_scheme(value: str) -> str:
    """Apply the default git+file:// scheme if not present."""
    if "://" not in value:
        value = f"git+file://{value}"
    return value


class AnyUrlWithoutHost(AnyUrl):
    _constraints = UrlConstraints(host_required=False)


ConfigSourceUrl = Annotated[AnyUrlWithoutHost, BeforeValidator(_apply_default_scheme)]

T = TypeVar("T")


@dataclass(frozen=True)
class Snapshot(Generic[T]):
    """Wraps a cached data payload with its cache metadata.

    Decouples cache plumbing (hexsha, modified) from the data models themselves,
    replacing the old CachedModel._hexsha / _modified private-attribute pattern.
    """

    data: T
    hexsha: str
    modified: datetime


class CacheableSource(Generic[T], metaclass=ABCMeta):
    """Abstract base class for async sources that can be cached.

    Handles the caching of the latest revision and its content using a two-level cache.
    Subclasses implement async `latest_revision` and `read_raw`; the base class
    provides the single-flight refresh logic via asyncio.Event and asyncio.Task.
    """

    def __init__(self):
        # Revision cache stores (hexsha → content) with two TTLs.
        # soft_ttl: triggers a background refresh while serving the stale value.
        # hard_ttl: absolute deadline; missing it causes a hard miss (await refresh).
        self._revision_cache = TwoLevelCache(
            soft_ttl=DEFAULT_CS_REV_CACHE_SOFT_TTL,
            hard_ttl=DEFAULT_CS_REV_CACHE_HARD_TTL,
            max_workers=1,
            max_items=1,
        )
        # Keep the last two content versions so there is no flip-flop during a transition.
        self._content_cache: Cache = LRUCache(maxsize=2)

        # Single-flight refresh state: at most one Task is in flight at a time.
        self._refresh_task: asyncio.Task | None = None
        self._refresh_lock = asyncio.Lock()

    @abstractmethod
    async def latest_revision(self) -> tuple[str, datetime]:
        """Return (hexsha, modified) for the current revision."""

    @abstractmethod
    async def read_raw(self, hexsha: str, modified: datetime) -> T:
        """Fetch and return the data for *hexsha*."""

    async def _refresh(self) -> str:
        """Fetch the latest revision and populate the content cache.

        Returns the hexsha so callers can look up self._content_cache[hexsha].
        """
        hexsha, modified = await self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = await self.read_raw(hexsha, modified)
        return hexsha

    async def _ensure_refresh_task(self) -> asyncio.Task:
        """Start a background refresh task if one is not already running."""
        async with self._refresh_lock:
            if self._refresh_task is None or self._refresh_task.done():
                self._refresh_task = asyncio.create_task(self._refresh())
            return self._refresh_task

    async def read(self) -> T:
        """Load the source with caching; awaits a refresh on a hard cache miss.

        :raises: git.exc.BadName if version does not exist
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._sync_refresh_shim, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_non_blocking(self) -> T:
        """Load the source with caching; raises NotReadyError while a refresh is in flight.

        Triggers a background refresh when the soft TTL has expired so that
        subsequent requests benefit from fresh data without paying the latency now.

        :raises: diracx.core.exceptions.NotReadyError if the cache is cold
        """
        # Try the revision cache first (non-blocking).  On a soft-miss or hard-miss
        # we kick off an async background refresh and either serve stale or raise.
        try:
            hexsha = self._revision_cache.get(
                "latest_revision", self._sync_refresh_shim, blocking=False
            )
            return self._content_cache[hexsha]
        except KeyError:
            # The revision cache returned a hexsha not yet in the content cache —
            # shouldn't happen in normal operation; treat as not-ready.
            pass

        # Hard miss: nothing in either cache yet.  Start (or reuse) a background
        # refresh task and raise NotReadyError so the router can serve a 503.
        asyncio.create_task(self._ensure_refresh_task())
        from diracx.core.exceptions import NotReadyError

        raise NotReadyError("Cache is not yet populated; a refresh is in progress.")

    def _sync_refresh_shim(self) -> str:
        """Synchronous shim used by TwoLevelCache's thread-pool worker.

        Runs the async _refresh coroutine on the running event loop via
        run_coroutine_threadsafe so the engine's loop is respected.
        """
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(self._refresh(), loop)
        hexsha = future.result()
        return hexsha

    def clear_caches(self):
        """Clear the caches."""
        self._revision_cache.clear()
        self._content_cache.clear()


class ConfigSource(CacheableSource[Config]):
    """Abstract class for the configuration source.

    This class takes care of the expected caching and locking logic. Subclasses
    are responsible for implementing the actual logic to find revisions and
    reading the configuration.
    """

    # Keep a mapping between the scheme and the class
    __registry: dict[str, type["ConfigSource"]] = {}
    scheme: str

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__()

    def __init_subclass__(cls) -> None:
        """Keep a record of <scheme: class>."""
        if cls.scheme in cls.__registry:
            raise TypeError(f"{cls.scheme=} is already define")
        cls.__registry[cls.scheme] = cls

    @classmethod
    def create(cls):
        return cls.create_from_url(backend_url=os.environ["DIRACX_CONFIG_BACKEND_URL"])

    @classmethod
    def create_from_url(
        cls, *, backend_url: ConfigSourceUrl | Path | str
    ) -> "ConfigSource":
        """Produce a concrete instance depending on the backend URL scheme."""
        url = TypeAdapter(ConfigSourceUrl).validate_python(str(backend_url))
        return cls.__registry[url.scheme](backend_url=url)


class BaseGitConfigSource(ConfigSource):
    """Base class for the git based config source."""

    repo_location: Path

    # Needed because of the ConfigSource.__init_subclass__
    scheme = "basegit"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        self.remote_url = self.extract_remote_url(backend_url)
        self.git_revision = self.get_git_revision_from_url(backend_url)

    async def latest_revision(self) -> tuple[str, datetime]:
        """Return the latest git revision hash and its commit timestamp."""
        try:
            rev = (
                await asyncio.to_thread(
                    sh.git,
                    "rev-parse",
                    self.git_revision,
                    _cwd=self.repo_location,
                    _tty_out=False,
                )
            ).strip()
            commit_info = (
                await asyncio.to_thread(
                    sh.git.show,
                    "-s",
                    "--format=%ct",
                    rev,
                    _cwd=self.repo_location,
                    _tty_out=False,
                )
            ).strip()
            modified = datetime.fromtimestamp(int(commit_info), tz=timezone.utc)
        except sh.ErrorReturnCode as e:
            raise BadConfigurationVersionError(
                f"Error parsing latest revision: {e}"
            ) from e
        logger.debug("Latest revision for %s is %s with mtime %s", self, rev, modified)
        return rev, modified

    async def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """:param: hexsha commit hash"""
        logger.debug("Reading %s for %s with mtime %s", self, hexsha, modified)
        try:
            blob = await asyncio.to_thread(
                sh.git.show,
                f"{hexsha}:{DEFAULT_CONFIG_FILE}",
                _cwd=self.repo_location,
                _tty_out=False,
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
        return config

    def extract_remote_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the base URL without the 'git+' prefix and query parameters."""
        parsed_url = urlparse(str(backend_url).replace("git+", ""))
        remote_url = urlunparse(parsed_url._replace(query=""))
        return remote_url

    def get_git_revision_from_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the branch from the query parameters."""
        return dict(backend_url.query_params()).get("revision", DEFAULT_GIT_BRANCH)


class LocalGitConfigSource(BaseGitConfigSource):
    """The configuration is stored on a local git repository.

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
    """Use a remote directory as a config source."""

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

    async def latest_revision(self) -> tuple[str, datetime]:
        logger.debug("Pulling latest version from %s", self)
        try:
            await asyncio.to_thread(sh.git.pull, _cwd=self.repo_location)
        except sh.ErrorReturnCode as err:
            logger.exception(err)

        return await super().latest_revision()
