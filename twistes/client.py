from twisted.internet.task import deferLater

import treq
import json
from twisted.internet.defer import inlineCallbacks, returnValue, CancelledError
from twisted.internet.error import ConnectingCancelledError
from twisted.web._newclient import ResponseNeverReceived
from twistes.compatability import string_types, urlparse
from twistes.exceptions import (NotFoundError,
                                ConnectionTimeout,
                                RequestError,
                                ElasticsearchException)
from twistes.scroller import Scroller
from twistes.consts import HttpMethod, EsMethods, EsConst, NULL_VALUES, TREQ_POOL_DEFAULT_PARAMS
from twistes.parser import EsParser
from twistes.consts import ResponseCodes
from twistes.bulk_utils import BulkUtility

from twisted.web.client import HTTPConnectionPool
from twisted.internet import reactor
from twisted.internet.tcp import Client


class Elasticsearch(object):
    """
    Elastic search asynchronous http client implemented with treq and twisted
    """

    def __init__(self, hosts, timeout=10,
                 async_http_client=None,
                 async_http_client_params=None):
        self._es_parser = EsParser()
        self._hostname, self._auth = self._es_parser.parse_host(hosts)
        self._timeout = timeout
        self._async_http_client = async_http_client or treq
        self._async_http_client_params = async_http_client_params or {}
        self.bulk_utils = BulkUtility(self)

        if self._async_http_client == treq \
                and 'pool' not in self._async_http_client_params:
            self.inject_pool_to_treq(self._async_http_client_params)

    @staticmethod
    def inject_pool_to_treq(params):
        params["pool"] = HTTPConnectionPool(reactor, params.pop("persistent", True))

        for key, default_value in TREQ_POOL_DEFAULT_PARAMS.items():
            setattr(params["pool"], key, params.pop(key, default_value))

    @inlineCallbacks
    def info(self, **query_params):
        """
        Get basic information about the cluster
        `<http://www.elastic.co/guide/>`_
        """
        yield self._perform_request(HttpMethod.GET, '/', params=query_params)

    @inlineCallbacks
    def get(self, index, id, fields=None, doc_type=EsConst.ALL_VALUES, **query_params):
        """
        Retrieve specific record by id
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html>`_
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
    def exists(self, index, doc_type, id, **query_params):
        """
        Check if the doc exist in the elastic search
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html>`_
        :param index: the index name
        :param doc_type: the document type
        :param id: the id of the doc type
        :arg parent: The ID of the parent document
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg realtime: Specify whether to perform the operation in realtime or
            search mode
        :arg refresh: Refresh the shard containing the document before
            performing the operation
        :arg routing: Specific routing value
        """
        self._es_parser.is_not_empty_params(index, doc_type, id)
        path = self._es_parser.make_path(index, doc_type, id)
        result = yield self._perform_request(HttpMethod.HEAD,
                                             path,
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def get_source(self, index, doc_type, id, **query_params):
        """
        Get the _source of the document
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-get.html>`_
        :param index: the index name
        :param doc_type: the document type
        :param id: the id of the doc type
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg parent: The ID of the parent document
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg realtime: Specify whether to perform the operation in realtime or
            search mode
        :arg refresh: Refresh the shard containing the document before
            performing the operation
        :arg routing: Specific routing value
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'external', 'external_gte', 'force'
        """
        self._es_parser.is_not_empty_params(index, doc_type, id)
        path = self._es_parser.make_path(index, doc_type, id, EsMethods.SOURCE)
        result = yield self._perform_request(HttpMethod.GET,
                                             path,
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def mget(self, body, index=None, doc_type=None, **query_params):
        """
        Get multiple document from the same index and doc_type (optionally) by ids
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-multi-get.html>`

        :param body: list of docs with or without the index and type parameters
        or list of ids
        :param index: the name of the index
        :param doc_type: the document type
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg fields: A comma-separated list of fields to return in the response
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg realtime: Specify whether to perform the operation in realtime or
            search mode
        :arg refresh: Refresh the shard containing the document before
            performing the operation
        """
        self._es_parser.is_not_empty_params(body)

        path = self._es_parser.make_path(index,
                                         doc_type,
                                         EsMethods.MULTIPLE_GET)

        result = yield self._perform_request(HttpMethod.GET,
                                             path,
                                             body=body,
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def update(self, index, doc_type, id, body=None, **query_params):
        """
        Update a document with the body param or list of ids
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update.html>`_

        :param index: the name of the index
        :param doc_type: the document type
        :param id: the id of the doc type
        :param body: the data to update in the index
        :arg consistency: Explicit write consistency setting for the operation,
            valid choices are: 'one', 'quorum', 'all'
        :arg fields: A comma-separated list of fields to return in the response
        :arg lang: The script language (default: groovy)
        :arg parent: ID of the parent document. Is is only used for routing and
            when for the upsert request
        :arg refresh: Refresh the index after performing the operation
        :arg retry_on_conflict: Specify how many times should the operation be
            retried when a conflict occurs (default: 0)
        :arg routing: Specific routing value
        :arg script: The URL-encoded script definition (instead of using request
            body)
        :arg script_id: The id of a stored script
        :arg scripted_upsert: True if the script referenced in script or
            script_id should be called to perform inserts - defaults to false
        :arg timeout: Explicit operation timeout
        :arg timestamp: Explicit timestamp for the document
        :arg ttl: Expiration time for the document
        :arg version: Explicit version number for concurrency control
        :arg version_type: Specific version type, valid choices are: 'internal',
            'force'
        """
        self._es_parser.is_not_empty_params(index, doc_type, id)
        path = self._es_parser.make_path(index, doc_type, id, EsMethods.UPDATE)
        result = yield self._perform_request(HttpMethod.POST, path, body=body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def search(self, index=None, doc_type=None, body=None, **query_params):
        """
        Make a search query on the elastic search
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html>`_

        :param index: the index name to query
        :param doc_type: he doc type to search in
        :param body: the query
        :param query_params: params
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg analyze_wildcard: Specify whether wildcard and prefix queries
            should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg explain: Specify whether to return detailed information about score
            computation as part of a hit
        :arg fielddata_fields: A comma-separated list of fields to return as the
            field data representation of a field for each hit
        :arg fields: A comma-separated list of fields to return as part of a hit
        :arg from\_: Starting offset (default: 0)
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg lowercase_expanded_terms: Specify whether query terms should be
            lowercased
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg request_cache: Specify if request cache should be used for this
            request or not, defaults to index level setting
        :arg routing: A comma-separated list of specific routing values
        :arg scroll: Specify how long a consistent view of the index should be
            maintained for scrolled search
        :arg search_type: Search operation type, valid choices are:
            'query_then_fetch', 'dfs_query_then_fetch'
        :arg size: Number of hits to return (default: 10)
        :arg sort: A comma-separated list of <field>:<direction> pairs
        :arg stats: Specific 'tag' of the request for logging and statistical
            purposes
        :arg suggest_field: Specify which field to use for suggestions
        :arg suggest_mode: Specify suggest mode, default 'missing', valid
            choices are: 'missing', 'popular', 'always'
        :arg suggest_size: How many suggestions to return in response
        :arg suggest_text: The source text for which the suggestions should be
            returned
        :arg terminate_after: The maximum number of documents to collect for
            each shard, upon reaching which the query execution will terminate
            early.
        :arg timeout: Explicit operation timeout
        :arg track_scores: Whether to calculate and return scores even if they
            are not used for sorting
        :arg version: Specify whether to return document version as part of a
            hit
        """
        path = self._es_parser.make_path(index, doc_type, EsMethods.SEARCH)
        result = yield self._perform_request(HttpMethod.POST, path, body=body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def explain(self, index, doc_type, id, body=None, **query_params):
        """
        The explain api computes a score explanation for a query and a specific
        document. This can give useful feedback whether a document matches or
        didn't match a specific query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-explain.html>`_
        :param index: The name of the index
        :param doc_type: The type of the document
        :param id: The document ID
        :param body: The query definition using the Query DSL
        :arg _source: True or false to return the _source field or not, or a
            list of fields to return
        :arg _source_exclude: A list of fields to exclude from the returned
            _source field
        :arg _source_include: A list of fields to extract and return from the
            _source field
        :arg analyze_wildcard: Specify whether wildcards and prefix queries in
            the query string query should be analyzed (default: false)
        :arg analyzer: The analyzer for the query string query
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The default field for query string query (default: _all)
        :arg fields: A comma-separated list of fields to return in the response
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg lowercase_expanded_terms: Specify whether query terms should be
            lowercased
        :arg parent: The ID of the parent document
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg routing: Specific routing value
        """
        self._es_parser.is_not_empty_params(index, doc_type, id)

        path = self._es_parser.make_path(index,
                                         doc_type,
                                         id,
                                         EsMethods.EXPLAIN)

        result = yield self._perform_request(HttpMethod.GET,
                                             path,
                                             body,
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def delete(self, index, doc_type, id, **query_params):
        """
        Delete specific record by id
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete.html>`_
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
    def index(self, index, doc_type, body, id=None, **query_params):
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
        result = yield self._perform_request(method, path, body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def create(self, index, doc_type, body, id=None, **query_params):
        """
        Adds a typed JSON document in a specific index, making it searchable.
        Behind the scenes this method calls index(..., op_type='create')
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
        query_params['op_type'] = 'create'
        result = yield self.index(index, doc_type, body, id=id, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def scroll(self, scroll_id=None, body=None, **query_params):
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
            query_params[EsConst.SCROLL_ID] = scroll_id

        path = self._es_parser.make_path(EsMethods.SEARCH, EsMethods.SCROLL)
        result = yield self._perform_request(HttpMethod.GET, path, body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def clear_scroll(self, scroll_id=None, body=None, **query_params):
        """
        Clear the scroll request created by specifying the scroll parameter to
        search.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html>`_
        :param scroll_id: A comma-separated list of scroll IDs to clear
        :param body: A comma-separated list of scroll IDs to clear if none was
            specified via the scroll_id parameter
        """
        if scroll_id in NULL_VALUES and body in NULL_VALUES:
            raise ValueError("You need to supply scroll_id or body.")
        elif scroll_id and not body:
            body = scroll_id
        elif scroll_id:
            query_params[EsConst.SCROLL_ID] = scroll_id

        path = self._es_parser.make_path(EsMethods.SEARCH, EsMethods.SCROLL)
        result = yield self._perform_request(HttpMethod.DELETE, path, body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def scan(self, index, doc_type, query=None, scroll='5m', preserve_order=False, size=10, **kwargs):
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
        :param size: the number of results to fetch in each scroll query

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
        results = yield self.search(index=index,
                                    doc_type=doc_type,
                                    body=query,
                                    size=size,
                                    scroll=scroll,
                                    **kwargs)

        returnValue(Scroller(self, results, scroll, size))

    @inlineCallbacks
    def count(self, index=None, doc_type=None, body=None, **query_params):
        """
        Execute a query and get the number of matches for that query.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html>`_
        :param index: A comma-separated list of indices to restrict the results
        :param doc_type: A comma-separated list of types to restrict the results
        :param body: A query to restrict the results specified with the Query DSL
            (optional)
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg analyze_wildcard: Specify whether wildcard and prefix queries
            should be analyzed (default: false)
        :arg analyzer: The analyzer to use for the query string
        :arg default_operator: The default operator for query string query (AND
            or OR), default 'OR', valid choices are: 'AND', 'OR'
        :arg df: The field to use as default where no field prefix is given in
            the query string
        :arg expand_wildcards: Whether to expand wildcard expression to concrete
            indices that are open, closed or both., default 'open', valid
            choices are: 'open', 'closed', 'none', 'all'
        :arg ignore_unavailable: Whether specified concrete indices should be
            ignored when unavailable (missing or closed)
        :arg lenient: Specify whether format-based query failures (such as
            providing text to a numeric field) should be ignored
        :arg lowercase_expanded_terms: Specify whether query terms should be
            lowercased
        :arg min_score: Include only documents with a specific `_score` value in
            the result
        :arg preference: Specify the node or shard the operation should be
            performed on (default: random)
        :arg q: Query in the Lucene query string syntax
        :arg routing: Specific routing value
        """
        if doc_type and not index:
            index = EsConst.ALL_VALUES

        path = self._es_parser.make_path(index, doc_type, EsMethods.COUNT)
        result = yield self._perform_request(HttpMethod.GET, path, body, params=query_params)
        returnValue(result)

    @inlineCallbacks
    def bulk(self, body, index=None, doc_type=None, **query_params):
        """
        Perform many index/delete operations in a single API call.
        See the :func:`~elasticsearch.helpers.bulk` helper function for a more
        friendly API.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html>`_
        :param body: The operation definition and data (action-data pairs),
            separated by newlines
        :param index: Default index for items which don't provide one
        :param doc_type: Default document type for items which don't provide one
        :arg consistency: Explicit write consistency setting for the operation,
            valid choices are: 'one', 'quorum', 'all'
        :arg fields: Default comma-separated list of fields to return in the
            response for updates
        :arg pipeline: The pipeline id to preprocess incoming documents with
        :arg refresh: Refresh the index after performing the operation
        :arg routing: Specific routing value
        :arg timeout: Explicit operation timeout
        """
        self._es_parser.is_not_empty_params(body)
        path = self._es_parser.make_path(index, doc_type, EsMethods.BULK)
        result = yield self._perform_request(HttpMethod.POST,
                                             path,
                                             self._bulk_body(body),
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def msearch(self, body, index=None, doc_type=None, **query_params):
        """
        Execute several search requests within the same API.
        `<http://www.elastic.co/guide/en/elasticsearch/reference/current/search-multi-search.html>`_
        :arg body: The request definitions (metadata-search request definition
            pairs), separated by newlines
        :arg index: A comma-separated list of index names to use as default
        :arg doc_type: A comma-separated list of document types to use as
            default
        :arg search_type: Search operation type, valid choices are:
            'query_then_fetch', 'query_and_fetch', 'dfs_query_then_fetch',
            'dfs_query_and_fetch'
        """
        self._es_parser.is_not_empty_params(body)
        path = self._es_parser.make_path(index,
                                         doc_type,
                                         EsMethods.MULTIPLE_SEARCH)

        result = yield self._perform_request(HttpMethod.GET,
                                             path,
                                             self._bulk_body(body),
                                             params=query_params)
        returnValue(result)

    @inlineCallbacks
    def _perform_request(self, method, path, body=None, params=None):
        url = self._es_parser.prepare_url(self._hostname, path, params)

        if body is not None and not isinstance(body, string_types):
            body = json.dumps(body)
        try:
            response = yield self._async_http_client.request(method,
                                                             url,
                                                             data=body,
                                                             timeout=self._timeout,
                                                             auth=self._auth,
                                                             **self._async_http_client_params)

            content = yield self._get_content(response)

            if response.code in (ResponseCodes.OK,
                                 ResponseCodes.CREATED,
                                 ResponseCodes.ACCEPTED):
                returnValue(content)

            if response.code == ResponseCodes.NOT_FOUND:
                raise NotFoundError(content)

            if response.code == ResponseCodes.BAD_REQUEST:
                raise RequestError(content)

            # This is a place holder for unknown exceptions
            # that haven't been encaplulated yet
            msg_fmt = "unknown error; code: {code} | message: {msg}"
            raise ElasticsearchException(msg_fmt.format(code=response.code,
                                                        msg=str(content)))

        except (ResponseNeverReceived,
                CancelledError,
                ConnectingCancelledError) as e:
            raise ConnectionTimeout(str(e))

    @inlineCallbacks
    def _get_content(self, response):
        content = None
        try:
            content = yield response.json()
        except ValueError as e:
            content_txt = yield response.content()
            content = json.loads(content_txt)
        except Exception as e:
            # unknown exceptions are ignored
            # and the content is set to None
            pass
        finally:
            returnValue(content)

    @staticmethod
    def _bulk_body(body):
        # if not passed in a string, serialize items and join by newline
        line_feed = '\n'
        if not isinstance(body, str):
            body = line_feed.join(map(json.dumps, body))

        # bulk body must end with a newline
        if not body.endswith(line_feed):
            body += line_feed

        return body

    def close(self):
        """
        close all http connections.
        returns a deferred that fires once they're all closed.
        """

        def validate_client(client):
            """
            Validate that the connection is for the current client
            :param client:
            :return:
            """
            host, port = client.addr
            parsed_url = urlparse(self._hostname)
            return host == parsed_url.hostname and port == parsed_url.port

        # read https://github.com/twisted/treq/issues/86
        # to understand the following...
        def _check_fds(_):
            fds = set(reactor.getReaders() + reactor.getReaders())
            if not [fd for fd in fds if isinstance(fd, Client) and validate_client(fd)]:
                return

            return deferLater(reactor, 0, _check_fds, None)

        pool = self._async_http_client_params["pool"]
        return pool.closeCachedConnections().addBoth(_check_fds)
