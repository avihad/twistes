import treq
import json

from twisted.internet.defer import inlineCallbacks, returnValue, CancelledError
from twisted.internet.error import ConnectingCancelledError
from twisted.web._newclient import ResponseNeverReceived

from scroller import Scroller
from exceptions import NotFoundError, ConnectionTimeout
from consts import HttpMethod, EsMethods, EsConst, NULL_VALUES
from parser import EsParser
from consts import ResponseCodes


class Elasticsearch(object):
    """
    Elastic search asynchronous http client implemented with treq and twisted
    """

    def __init__(self, hosts, timeout=10, _=treq):
        self._es_parser = EsParser()
        self._hostname, self._auth = self._es_parser.parse_host(hosts)
        self._timeout = timeout
        self._async_http_client = _

    @inlineCallbacks
    def get(self, index, id, fields=None, doc_type=EsConst.ANY_DOC_TYPE, **query_params):
        """
        Retrieve specific record by id
        :param index: the index name to query
        :param id: the id of the record
        :param fields: the fields you what to fetch from the record (str separated by comma's)
        :param doc_type: the doc type to search in
        :param query_params: params
        :return:
        """
        if fields:
            query_params[EsConst.FIELDS] = fields

        path = self._es_parser.make_path(index, doc_type, id)
        result = yield self._perform_request(HttpMethod.GET, path, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def search(self, index=None, doc_type=None, body=None, **query_params):
        """
        Make a search query on the elastic search
        :param index: the index name to query
        :param doc_type: he doc type to search in
        :param body: the query
        :param query_params: params
        :return:
        """
        path = self._es_parser.make_path(index, doc_type, EsMethods.SEARCH)
        result = yield self._perform_request(HttpMethod.POST, path, body=body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def delete(self, index, doc_type, id, **query_params):
        """
        Delete specific record by id
        :param index: the index name to delete from
        :param doc_type: the doc type to delete from
        :param id: the id of the record
        :param query_params: params
        :return:
        """
        path = self._es_parser.make_path(index, doc_type, id)
        result = yield self._perform_request(HttpMethod.DELETE, path, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def index(self, index, doc_type, body, id=None, params=None):
        """
        Adds or updates a typed JSON document in a specific index, making it searchable.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html>`_

        :param index: The name of the index
        :param doc_type: The type of the document
        :param body: The document
        :param id: Document ID

        :arg consistency: Explicit write consistency setting for the operation,
            valid choices are: 'one', 'quorum', 'all'
        :arg op_type: Explicit operation type, default 'index', valid choices
            are: 'index', 'create'
        :arg parent: ID of the parent document
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg timeout: Explicit operation timeout
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'external', 'external_gte', 'force'
        """
        self._es_parser.is_not_empty_params(index, doc_type, body)

        method = HttpMethod.POST if id in NULL_VALUES else HttpMethod.PUT
        path = self._es_parser.make_path(index, doc_type, id)
        result = yield self._perform_request(method, path, body, params)
        returnValue(result)

    @inlineCallbacks
    def scroll(self, scroll_id=None, body=None, params=None):
        """
        Scroll a search request created by specifying the scroll parameter.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html>`_

        :param scroll_id: The scroll ID
        :param body: The scroll ID if not passed by URL or query parameter.
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        """
        if scroll_id in NULL_VALUES and body in NULL_VALUES:
            raise ValueError("You need to supply scroll_id or body.")
        elif scroll_id and not body:
            body = scroll_id
        elif scroll_id:
            params[EsConst.SCROLL_ID] = scroll_id

        result = yield self._perform_request(HttpMethod.GET, EsMethods.SCROLL, body, params)
        returnValue(result)

    @inlineCallbacks
    def scan(self, index, doc_type, query=None, scroll='5m', preserve_order=False, **kwargs):
        """
        Simple abstraction on top of the
        :meth:`~elasticsearch.Elasticsearch.scroll` api - a simple iterator that
        yields all hits as returned by underlining scroll requests.

        By default scan does not return results in any pre-determined order. To
        have a standard order in the returned documents (either by score or
        explicit sort definition) when scrolling, use ``preserve_order=True``. This
        may be an expensive operation and will negate the performance benefits of
        using ``scan``.
        :param index: the index to query on
        :param doc_type: the doc_type to query on
        :param query: body for the :meth:`~elasticsearch.Elasticsearch.search` api
        :param scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :param preserve_order: don't set the ``search_type`` to ``scan`` - this will
            cause the scroll to paginate with preserving the order. Note that this
            can be an extremely expensive operation and can easily lead to
            unpredictable results, use with caution.

        Any additional keyword arguments will be passed to the initial
        :meth:`~elasticsearch.Elasticsearch.search` call::

            scan(index="coding_languages",
                doc_type="languages_description",
                query={"query": {"match": {"title": "python"}}},
                index="orders-*",
                doc_type="books"
            )

        """
        if not preserve_order:
            kwargs['search_type'] = 'scan'
        # initial search
        results = yield self.search(index=index, doc_type=doc_type, body=query, scroll=scroll, **kwargs)

        returnValue(Scroller(self, results))

    @inlineCallbacks
    def _perform_request(self, method, path, body=None, params=None):
        url = self._es_parser.prepare_url(self._hostname, path, params)

        if body is not None and not isinstance(body, basestring):
            body = json.dumps(body)
        try:
            response = yield self._async_http_client.request(method, url, data=body, timeout=self._timeout, auth=self._auth)
            if response.code in (ResponseCodes.OK, ResponseCodes.CREATED, ResponseCodes.ACCEPTED):
                content = yield self._get_content(response)
                returnValue(content)
            elif response.code == ResponseCodes.NOT_FOUND:
                raise NotFoundError()
            else:
                # This is a place holder that will change after we implement the whole Es interface
                raise Exception(response.code)
        except (ResponseNeverReceived, CancelledError, ConnectingCancelledError):
            raise ConnectionTimeout()

    @inlineCallbacks
    def _get_content(self, response):
        try:
            content = yield response.json()
        except ValueError, e:
            content = yield response.content()
            content = json.loads(content)
        returnValue(content) if content else returnValue(None)
