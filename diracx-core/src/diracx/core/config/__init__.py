"""This module implements the logic of the configuration server side.

This is where all the backend abstraction and the caching logic takes place.
"""

from __future__ import annotations

__all__ = ("Config", "ConfigSource", "LocalGitConfigSource", "RemoteGitConfigSource")

import logging
import os
from abc import ABCMeta, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated

import git
import yaml
from cachetools import Cache, LRUCache, TTLCache, cachedmethod
from pydantic import AnyUrl, BeforeValidator, TypeAdapter, UrlConstraints

from ..exceptions import BadConfigurationVersion
from ..extensions import select_from_extension
from .schema import Config

DEFAULT_CONFIG_FILE = "default.yml"
DEFAULT_GIT_BRANCH = "master"
DEFAULT_CS_CACHE_TTL = 5
MAX_CS_CACHED_VERSIONS = 1
DEFAULT_PULL_CACHE_TTL = 5
MAX_PULL_CACHED_VERSIONS = 1

logger = logging.getLogger(__name__)


def _apply_default_scheme(value: str) -> str:
    """Applies the default git+file:// scheme if not present."""
    if isinstance(value, str) and "://" not in value:
        value = f"git+file://{value}"
    return value


ConfigSourceUrl = Annotated[
    AnyUrl, UrlConstraints(host_required=False), BeforeValidator(_apply_default_scheme)
]


class ConfigSource(metaclass=ABCMeta):
    """This class is the abstract base class that should be used everywhere
    throughout the code.
    It acts as a factory for concrete implementations
    See the abstractmethods to implement a concrete class.
    """

    # Keep a mapping between the scheme and the class
    __registry: dict[str, type[ConfigSource]] = {}
    scheme: str

    @abstractmethod
    def __init__(self, *, backend_url: ConfigSourceUrl) -> None: ...

    @abstractmethod
    def latest_revision(self) -> tuple[str, datetime]:
        """Must return:
        * a unique hash as a string, representing the last version
        * a datetime object corresponding to when the version dates.
        """
        ...

    @abstractmethod
    def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """Return the Config object that corresponds to the
        specific hash
        The `modified` parameter is just added as a attribute to the config.

        """
        ...

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
    ) -> ConfigSource:
        """Factory method to produce a concrete instance depending on
        the backend URL scheme.

        """
        url = TypeAdapter(ConfigSourceUrl).validate_python(str(backend_url))
        return cls.__registry[url.scheme](backend_url=url)

    def read_config(self) -> Config:
        """:raises:
        git.exc.BadName if version does not exist
        """
        hexsha, modified = self.latest_revision()
        return self.read_raw(hexsha, modified)

    @abstractmethod
    def clear_caches(self): ...


class BaseGitConfigSource(ConfigSource):
    """Base class for the git based config source
    The caching is based on 2 caches:
    * TTL to find the latest commit hashes
    * LRU to keep in memory the last few versions.

    """

    repo: git.Repo

    # Needed because of the ConfigSource.__init_subclass__
    scheme = "basegit"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        self._latest_revision_cache: Cache = TTLCache(
            MAX_CS_CACHED_VERSIONS, DEFAULT_CS_CACHE_TTL
        )
        self._read_raw_cache: Cache = LRUCache(MAX_CS_CACHED_VERSIONS)

    @cachedmethod(lambda self: self._latest_revision_cache)
    def latest_revision(self) -> tuple[str, datetime]:
        try:
            rev = self.repo.rev_parse(DEFAULT_GIT_BRANCH)
        except git.exc.ODBError as e:  # type: ignore
            raise BadConfigurationVersion(f"Error parsing latest revision: {e}") from e
        modified = rev.committed_datetime.astimezone(timezone.utc)
        logger.debug(
            "Latest revision for %s is %s with mtime %s", self, rev.hexsha, modified
        )
        return rev.hexsha, modified

    @cachedmethod(lambda self: self._read_raw_cache)
    def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """:param: hexsha commit hash"""
        logger.debug("Reading %s for %s with mtime %s", self, hexsha, modified)
        rev = self.repo.rev_parse(hexsha)
        blob = rev.tree / DEFAULT_CONFIG_FILE
        raw_obj = yaml.safe_load(blob.data_stream.read().decode())

        config_class: Config = select_from_extension(group="diracx", name="config")[
            0
        ].load()
        config = config_class.model_validate(raw_obj)
        config._hexsha = hexsha
        config._modified = modified
        return config

    def clear_caches(self):
        self._latest_revision_cache.clear()
        self._read_raw_cache.clear()


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
        self.repo = git.Repo(self.repo_location)

    def __hash__(self):
        return hash(self.repo_location)


class RemoteGitConfigSource(BaseGitConfigSource):
    """Use a remote directory as a config source."""

    scheme = "git+https"

    def __init__(self, *, backend_url: ConfigSourceUrl) -> None:
        super().__init__(backend_url=backend_url)
        if not backend_url:
            raise ValueError("No remote url for RemoteGitConfigSource")

        # git does not understand `git+https`, so we remove the `git+` part
        self.remote_url = str(backend_url).replace("git+", "")
        self._temp_dir = TemporaryDirectory()
        self.repo_location = Path(self._temp_dir.name)
        self.repo = git.Repo.clone_from(self.remote_url, self.repo_location)
        self._pull_cache: Cache = TTLCache(
            MAX_PULL_CACHED_VERSIONS, DEFAULT_PULL_CACHE_TTL
        )

    def clear_caches(self):
        super().clear_caches()
        self._pull_cache.clear()

    def __hash__(self):
        return hash(self.repo_location)

    @cachedmethod(lambda self: self._pull_cache)
    def _pull(self):
        """Git pull from remote repo."""
        self.repo.remotes.origin.pull()

    def latest_revision(self) -> tuple[str, datetime]:
        self._pull()
        return super().latest_revision()
