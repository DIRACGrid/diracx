from datetime import datetime, timezone
from pathlib import Path

import git
import yaml
from cachetools import LRUCache, TTLCache, cachedmethod

from .exceptions import BadConfigurationVersion
from .properties import SecurityProperty

DEFAULT_CONFIG_FILE = "default.yml"


class Config(dict):
    """Object that represents a configuration
    Attributes are the way to access the elements

    Make it a dict such that it is json serializable

    TODO: hack until we have proper pydantic model
    """

    def __init__(self, obj, hexsha: str, modified: datetime):
        self._obj = obj
        self.hexsha = hexsha
        self.modified = modified
        super().__init__(obj)

    # def __contains__(self, key):
    #     return key in self._obj

    def __getitem__(self, key):
        value = super(self).__getitem__[key]
        if isinstance(value, dict):
            return self.__class__(value, self.hexsha, self.modified)
        return value

    def __getattr__(self, key):
        try:
            value = self[key]
        except KeyError:
            raise AttributeError(
                f"type object '{self.__class__.__name__}' has no attribute '{key}'"
            ) from None
        return value


DEFAULT_CS_CACHE_TTL = 5
MAX_CS_CACHED_VERSIONS = 1


class LocalGitConfigSource:
    def __init__(self, repo_location: Path):
        self.repo_location = repo_location
        self.repo = git.Repo(repo_location)

    def __hash__(self):
        return hash(self.repo_location)

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
    def read_raw(self, hexsha: str, modified: datetime) -> str:
        """ "
        Returns the raw data from the git repo

        :returns hexsha, commit time, data
        """
        print("config read_raw")
        rev = self.repo.rev_parse(hexsha)
        blob = rev.tree / DEFAULT_CONFIG_FILE
        return Config(
            yaml.safe_load(blob.data_stream.read().decode()), hexsha, modified
        )

    def read_config(self, version: str = "master") -> Config:
        """
        :raises:
            git.exc.BadName if version does not exist
        """
        hexsha, modified = self.latest_revision()
        return self.read_raw(hexsha, modified)


def get_config() -> Config:
    return LocalGitConfigSource(Path("/tmp/csRepo")).read_config("master")


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
