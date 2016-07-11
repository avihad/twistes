class ElasticsearchException(Exception):
    """
    Base class for all exceptions raised by this package's operations (doesn't
    apply to :class:`~elasticsearch.ImproperlyConfigured`).
    """


class TransportError(ElasticsearchException):
    """`
    Exception raised when ES returns a non-OK (>=400) HTTP status code. Or when
    an actual connection error happens;.
    """


class NotFoundError(ElasticsearchException):
    """ Exception representing a 404 status code. """


class ConnectionTimeout(ElasticsearchException):
    """ A network timeout. Doesn't cause a node retry by default. """


class ScanError(ElasticsearchException):
    """An Error eccure during the scan and scroll request"""


class BulkIndexError(ElasticsearchException):
    @property
    def errors(self):
        """ List of errors from execution of the last chunk. """
        return self.args[1]
