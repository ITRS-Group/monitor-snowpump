import pysnow

from snowpump.client import BaseClient
from snowpump.schema import HostSchema


class SnowClient(BaseClient, pysnow.Client):
    def __init__(self, config):
        config["user"] = config.pop("username")
        super(SnowClient, self).__init__(**config)

    def query(self, table, **kwargs):
        query = kwargs.pop("query", {})
        kwargs["stream"] = kwargs.pop("stream", False)
        return self.resource(
            f"/table/{table}",
            chunk_size=4096,
        ).get(query, fields=list(HostSchema._declared_fields.keys()), **kwargs)

    def count(self, table):
        resource = self.resource(base_path="/api/now/stats", api_path=f"/{table}")
        resource.parameters.add_custom(dict(sysparm_count=True))
        response = resource.get({}).one()
        return response["stats"]["count"]


class HostCollection:
    def __init__(self, config):
        self._client = SnowClient(config)

    def get_one(self, *args, **kwargs):
        data = self._client.query(*args, **kwargs).one()
        return HostSchema().load(data)

    def get_many(self, *args, **kwargs):
        for host_data in self._client.query(*args, **kwargs).all():
            yield HostSchema().load(host_data)

    def get_count(self, *args, **kwargs):
        return int(self._client.count(*args, **kwargs))


def init(*args, **kwargs):
    return HostCollection(*args, **kwargs)
