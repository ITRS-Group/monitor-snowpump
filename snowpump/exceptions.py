from requests import ConnectionError


class HTTPConnectionError(ConnectionError):
    pass


class RequestError(Exception):
    pass
