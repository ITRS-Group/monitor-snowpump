import requests
import urllib3

from snowpump.exceptions import RequestError
from snowpump.client import BaseClient
from snowpump.schema import HostSchema

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MonitorClient(BaseClient):
    def __init__(self, config):
        self.url_base = f"https://{config['host']}/api/config/host"
        self.config = config

    @property
    def request_base(self):
        return dict(
            auth=(self.config["username"], self.config["password"]),
            verify=self.config["ssl_verify"],
        )

    def _request(self, method, url, **kwargs):
        response = requests.request(method, url, **kwargs).json()

        if "error" in response:
            raise RequestError(response["full_error"])

        return response

    def get(self, hostgroup=None, **kwargs):
        name = kwargs.pop("name", None)
        url = self.url_base + f"/{name}" if name else self.url_base
        request = self.request_base

        if hostgroup:
            request["params"] = hostgroup

        return self._request("GET", url, **request)

    def create(self, payload):
        return self._request("POST", self.url_base, data=payload, **self.request_base)


class HostApi:
    def __init__(self, config):
        self._client = MonitorClient(config)

    def get_one(self, host_name):
        data = self._client.get(name=host_name)
        return HostSchema(many=False).load(data)

    def get_many(self, *args, **kwargs):
        data = self._client.get(*args, **kwargs)
        return HostSchema(many=True).load(data)

    def create(self, payload):
        return self._client.create(payload)


def init(*args, **kwargs):
    return HostApi(*args, **kwargs)
