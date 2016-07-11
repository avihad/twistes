from twisted.internet.defer import succeed, inlineCallbacks, returnValue

from twistes.consts import EsDocProperties
from twistes.utilities import EsUtils


class Scroller(object):
    """
    Handle scrolling through scan and scroll API.

    Usage: after the creation of the Scroller,
    Iterate over it and yield each item to get the results.
    Example:
        scroller = es.scan(...)
        for item in scroller:
            results = yield item
            for hit in results:
                ...
    """

    def __init__(self, es, results, scroll, size):
        self._first_results = results
        self._scroll_id = results.get(EsDocProperties.SCROLL_ID, None)
        self._scroll = scroll
        self._size = size
        self._es = es

    def __iter__(self):
        return self

    def next(self):
        """Fetch next page from scroll API."""
        d = None
        if self._first_results:
            d = succeed(EsUtils.extract_hits(self._first_results))
            self._first_results = None
        elif self._scroll_id:
            d = self._scroll_next_results()
        else:
            raise StopIteration()
        return d

    def __next__(self):
        return self.next()

    @inlineCallbacks
    def _scroll_next_results(self):
        results = yield self._es.scroll(str(self._scroll_id), scroll=self._scroll)
        hits = EsUtils.extract_hits(results)

        # No more results
        if len(hits) < self._size:
            self._scroll_id = None
        else:
            self._scroll_id = results.get(EsDocProperties.SCROLL_ID, None)

        returnValue(hits)
