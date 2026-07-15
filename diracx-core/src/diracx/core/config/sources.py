"""Module to implement the logic of the configuration server side.

This is where all the backend abstraction and the caching logic takes place.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated
from urllib.parse import urlparse, urlunparse

import sh
import yaml
from pydantic import AnyUrl, BeforeValidator, TypeAdapter, UrlConstraints

from diracx.core.exceptions import BadConfigurationVersionError
from diracx.core.extensions import DiracEntryPoint, select_from_extension
from diracx.core.sources import CacheableSource

from .schema import Config

DEFAULT_CONFIG_FILE = "default.yml"
DEFAULT_GIT_BRANCH = "master"
DEFAULT_CS_CONTENT_HARD_TTL = 15

logger = logging.getLogger(__name__)


def is_running_in_async_context():
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


def _apply_default_scheme(value: str) -> str:
    """Apply the default git+file:// scheme if not present."""
    if "://" not in value:
        value = f"git+file://{value}"
    return value


class AnyUrlWithoutHost(AnyUrl):
    _constraints = UrlConstraints(host_required=False)


ConfigSourceUrl = Annotated[AnyUrlWithoutHost, BeforeValidator(_apply_default_scheme)]


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
        # Avoid circular import
        from diracx.core.settings import FactorySettings

        return cls.create_from_url(backend_url=FactorySettings().config_backend_url)

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

    def latest_revision(self) -> tuple[str, datetime]:
        logger.debug("Pulling latest version from %s", self)
        try:
            sh.git.pull(_cwd=self.repo_location, _async=False)
        except sh.ErrorReturnCode as err:
            logger.exception(err)

        return super().latest_revision()
