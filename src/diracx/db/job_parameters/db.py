from __future__ import annotations

from opensearchpy import OpenSearch

try:
    from opensearchpy import A, Q, Search
except ImportError:
    pass

from ..utils import BaseDB


class OpenSearchJobParametersDB(BaseDB):
    async def set_client():
        # TODO: get connection secrets from SettingsClass.create and connection details from configuration
        host = "opensearch-cluster-master"
        port = 9200
        # For now credentials are hard coded, to be set here
        auth = ("user", "pwd")
        client = OpenSearch(
            hosts=[{"host": host, "port": port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=False,
        )
        return client
