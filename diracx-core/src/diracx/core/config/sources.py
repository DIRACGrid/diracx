"""This module implements the logic of the configuration server side.

This is where all the backend abstraction and the caching logic takes place.
"""

from __future__ import annotations

import asyncio
import logging
import os
from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated
from urllib.parse import urlparse, urlunparse

import sh
import yaml
from cachetools import Cache, LRUCache
from pydantic import AnyUrl, BeforeValidator, TypeAdapter, UrlConstraints

from ..exceptions import BadConfigurationVersionError
from ..extensions import select_from_extension
from ..utils import TwoLevelCache
from .schema import Config

DEFAULT_CONFIG_FILE = "default.yml"
DEFAULT_GIT_BRANCH = "master"
DEFAULT_CS_REV_CACHE_SOFT_TTL = 5
# TODO: Reduce the hard TTL when we have more redundancy around the source of truth
DEFAULT_CS_REV_CACHE_HARD_TTL = 60 * 60
DEFAULT_CS_CONTENT_HARD_TTL = 15

logger = logging.getLogger(__name__)


def is_running_in_async_context():
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _apply_default_scheme(value: str) -> str:
    """Applies the default git+file:// scheme if not present."""
    if isinstance(value, str) and "://" not in value:
        value = f"git+file://{value}"
    return value


class AnyUrlWithoutHost(AnyUrl):

    _constraints = UrlConstraints(host_required=False)


ConfigSourceUrl = Annotated[AnyUrlWithoutHost, BeforeValidator(_apply_default_scheme)]


class ConfigSource(metaclass=ABCMeta):
    """Abstract class for the configuration source.

    This class takes care of the expected caching and locking logic. Subclasses
    are responsible for implementing the actual logic to find revisions and
    reading the configuration.
    """

    # Keep a mapping between the scheme and the class
    __registry: dict[str, type["ConfigSource"]] = {}
    scheme: str

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
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
        """Must return:
        * a unique hash as a string, representing the last version
        * a datetime object corresponding to when the version dates.
        """

    @abstractmethod
    def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """Return the Config object that corresponds to the
        specific hash
        The `modified` parameter is just added as a attribute to the config.
        """

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
        """Factory method to produce a concrete instance depending on
        the backend URL scheme.

        """
        url = TypeAdapter(ConfigSourceUrl).validate_python(str(backend_url))
        return cls.__registry[url.scheme](backend_url=url)

    def read_config(self) -> Config:
        """Load the configuration from the backend with appropriate caching.

        :raises: diracx.core.exceptions.NotReadyError if the config is being loaded still
        :raises: git.exc.BadName if version does not exist
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_config_work, blocking=True
        )
        return self._content_cache[hexsha]

    async def read_config_non_blocking(self) -> Config:
        """Load the configuration from the backend with appropriate caching.

        :raises: diracx.core.exceptions.NotReadyError if the config is being loaded still
        :raises: git.exc.BadName if version does not exist
        """
        hexsha = self._revision_cache.get(
            "latest_revision", self._read_config_work, blocking=False
        )
        return self._content_cache[hexsha]

    def _read_config_work(self) -> str:
        """Work function for the thread pool of `self._revision_cache`.

        This function ensures that the latest revision is loaded into the
        content cache before it is admitted into the revision cache.
        """
        hexsha, modified = self.latest_revision()
        if hexsha not in self._content_cache:
            self._content_cache[hexsha] = self.read_raw(hexsha, modified)
        return hexsha

    def clear_caches(self):
        """Clear the caches."""
        self._revision_cache.clear()
        self._content_cache.clear()


class BaseGitConfigSource(ConfigSource):
    """Base class for the git based config source."""

    repo_location: Path

    # Needed because of the ConfigSource.__init_subclass__
    scheme = "basegit"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        self.remote_url = self.extract_remote_url(backend_url)
        self.git_branch = self.get_git_branch_from_url(backend_url)

    def latest_revision(self) -> tuple[str, datetime]:
        try:
            rev = sh.git(
                "rev-parse",
                self.git_branch,
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

        config_class: Config = select_from_extension(group="diracx", name="config")[
            0
        ].load()
        config = config_class.model_validate(raw_obj)
        config._hexsha = hexsha
        config._modified = modified
        return config

    def extract_remote_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the base URL without the 'git+' prefix and query parameters."""
        parsed_url = urlparse(str(backend_url).replace("git+", ""))
        remote_url = urlunparse(parsed_url._replace(query=""))
        return remote_url

    def get_git_branch_from_url(self, backend_url: ConfigSourceUrl) -> str:
        """Extract the branch from the query parameters."""
        return dict(backend_url.query_params()).get("branch", DEFAULT_GIT_BRANCH)


class LocalGitConfigSource(BaseGitConfigSource):
    """The configuration is stored on a local git repository
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
        sh.git.checkout(self.git_branch, _cwd=self.repo_location, _async=False)

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
        sh.git.clone(
            self.remote_url, self.repo_location, branch=self.git_branch, _async=False
        )

    def __hash__(self):
        return hash(self.repo_location)

    def latest_revision(self) -> tuple[str, datetime]:
        logger.debug("Pulling latest version from %s", self)
        try:
            sh.git.pull(_cwd=self.repo_location, _async=False)
        except sh.ErrorReturnCode as err:
            logger.exception(err)

        return super().latest_revision()
