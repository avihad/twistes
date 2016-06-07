class NotFoundError(Exception):
    """ Exception representing a 404 status code. """


class ConnectionTimeout(Exception):
    """ A network timeout. Doesn't cause a node retry by default. """


class ScanError(Exception):
    """An Error eccure during the scan and scroll request"""
