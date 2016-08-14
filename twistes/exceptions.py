class ElasticsearchException(Exception):
    """
    Base class for all exceptions raised by this package's operations (doesn't
    apply to :class:`~elasticsearch.ImproperlyConfigured`).
    """


class ImproperlyConfigured(Exception):
    """
    Exception raised when the config passed to the client is inconsistent or invalid.
    """


class ScanError(ElasticsearchException):
    """An Error eccure during the scan and scroll request"""


class BulkIndexError(ElasticsearchException):

    @property
    def errors(self):
        """ List of errors from execution of the last chunk. """
        return self.args[1]


class SerializationError(ElasticsearchException):
    """
    Data passed in failed to serialize properly in the Serializer being used.
    """


class TransportError(ElasticsearchException):
    """
    Exception raised when ES returns a non-OK (>=400) HTTP status code.
    Or when an actual connection error happens;  in that case the status_code
    will be set to 'N/A'.
    """

    def __init__(self, error, status_code=None, info=None):
        super(TransportError, self).__init__(error)
        self.error = error
        self.status_code = status_code or 'N/A'
        self.info = info or {}


class ConnectionError(TransportError):
    """
    Error raised when there was an exception while talking to ES.
    Original exception from the underlying Connection implementation
    is available as .info.
    """

    def __init__(self, error):
        super(ConnectionError, self).__init__(error=error)


class ConnectionTimeout(ConnectionError):
    """
    A network timeout. DOesn't cause a node retry by default.
    """


class SSLError(ConnectionError):
    """
    Error raised when encountering SSL errors.
    """


class NotFoundError(TransportError):
    """
    Exception representing a 404 status code.
    """

    def __init__(self, info):
        super(NotFoundError, self).__init__(error="not found",
                                            info=info,
                                            status_code=404)


class ConflictError(TransportError):
    """
    Exception representing a 409 status code.
    """

    def __init__(self, info):
        super(ConflictError, self).__init__(error="conflict",
                                            info=info,
                                            status_code=409)


class RequestError(TransportError):
    """
    Exception representing a 400 status code.
    """

    def __init__(self, info):
        super(RequestError, self).__init__(error="bad request",
                                           info=info,
                                           status_code=400)
