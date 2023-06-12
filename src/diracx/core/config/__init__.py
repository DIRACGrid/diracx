from __future__ import annotations

__all__ = ("Config", "get_config", "LocalGitConfigSource")

from datetime import datetime, timezone
from pathlib import Path

import git
import yaml
from cachetools import Cache, LRUCache, TTLCache, cachedmethod

from ..exceptions import BadConfigurationVersion
from ..secrets import DiracxSecrets
from .schema import Config

DEFAULT_CONFIG_FILE = "default.yml"
DEFAULT_CS_CACHE_TTL = 5
MAX_CS_CACHED_VERSIONS = 1


class LocalGitConfigSource:
    def __init__(self, repo_location: Path):
        self.repo_location = repo_location
        self.repo = git.Repo(repo_location)

    def __hash__(self):
        return hash(self.repo_location)

    @classmethod
    def clear_caches(cls):
        cls._latest_revision_cache.clear()
        cls._read_raw_cache.clear()

    _latest_revision_cache: Cache = TTLCache(
        MAX_CS_CACHED_VERSIONS, DEFAULT_CS_CACHE_TTL
    )

    @cachedmethod(lambda self: self._latest_revision_cache)
    def latest_revision(self) -> tuple[str, datetime]:
        print("config latest_revision")
        try:
            rev = self.repo.rev_parse("master")
        except git.exc.ODBError as e:  # type: ignore
            raise BadConfigurationVersion(f"Error parsing latest revision: {e}") from e
        return rev.hexsha, rev.committed_datetime.astimezone(timezone.utc)

    _read_raw_cache: Cache = LRUCache(MAX_CS_CACHED_VERSIONS)

    @cachedmethod(lambda self: self._read_raw_cache)
    def read_raw(self, hexsha: str, modified: datetime) -> Config:
        """ "
        Returns the raw data from the git repo

        :returns hexsha, commit time, data
        """
        print("config read_raw")
        rev = self.repo.rev_parse(hexsha)
        blob = rev.tree / DEFAULT_CONFIG_FILE
        raw_obj = yaml.safe_load(blob.data_stream.read().decode())
        config = Config.parse_obj(raw_obj)
        config._hexsha = hexsha
        config._modified = modified
        return config

    def read_config(self) -> Config:
        """
        :raises:
            git.exc.BadName if version does not exist
        """
        hexsha, modified = self.latest_revision()
        return self.read_raw(hexsha, modified)


def get_config() -> Config:
    secrets = DiracxSecrets.from_env()
    if secrets.config.scheme == "file":
        return LocalGitConfigSource(Path(secrets.config.path)).read_config()
    else:
        raise NotImplementedError(secrets.config.scheme)
