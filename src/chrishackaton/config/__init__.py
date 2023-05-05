import os
from datetime import datetime, timezone
from pathlib import Path

import git
import yaml
from cachetools import LRUCache, TTLCache, cachedmethod

from .schema import Config
from ..exceptions import BadConfigurationVersion
from ..properties import SecurityProperty

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

    _latest_revision_cache = TTLCache(MAX_CS_CACHED_VERSIONS, DEFAULT_CS_CACHE_TTL)

    @cachedmethod(lambda self: self._latest_revision_cache)
    def latest_revision(self) -> tuple[str, datetime]:
        print("config latest_revision")
        try:
            rev = self.repo.rev_parse("master")
        except git.exc.ODBError as e:
            raise BadConfigurationVersion(f"Error parsing latest revision: {e}") from e
        return rev.hexsha, rev.committed_datetime.astimezone(timezone.utc)

    _read_raw_cache = LRUCache(MAX_CS_CACHED_VERSIONS)

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
        return Config.parse_obj(raw_obj | {"hexsha": hexsha, "modified": modified})

    def read_config(self) -> Config:
        """
        :raises:
            git.exc.BadName if version does not exist
        """
        hexsha, modified = self.latest_revision()
        return self.read_raw(hexsha, modified)


def get_config() -> Config:
    return LocalGitConfigSource(Path(os.environ["DIRAC_CS_SOURCE"])).read_config()


Registry = {
    "lhcb": {
        "Users": [
            {
                "nickname": "chaen",
                "email": "somewhere@cern.ch",
                "sub": "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041",
            }
        ],
        "Groups": {
            "lhcb_user": {
                "members": ["cburr", "b824d4dc-1f9d-4ee8-8df5-c0ae55d46041"],
                "properties": [
                    SecurityProperty.NORMAL_USER,
                    SecurityProperty.CS_ADMINISTRATOR,
                ],
            }
        },
    }
}
