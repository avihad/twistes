import json
from twistes.compatability import urlencode, quote
from mock import MagicMock
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web._newclient import ResponseNeverReceived

from twistes.client import Elasticsearch
from twistes.consts import HttpMethod, EsConst, ResponseCodes, EsMethods
from twistes.exceptions import (NotFoundError,
                                ConnectionTimeout,
                                ElasticsearchException,
                                RequestError)
from twistes.scroller import Scroller

FIELD_2 = 'FIELD_2'
FIELD_1 = 'FIELD_2'

SOME_FIELDS = [FIELD_1, FIELD_2]

TIMEOUT = 123

SOME_ID = 'SOME_ID'

SOME_PORT = 9200
SOME_HOST = 'http://SOME_HOST'

SOME_USER = 'sonic'
SOME_PASS = 'blast'
SOME_HOSTS_CONFIG = [{
    'host': SOME_HOST,
    'port': SOME_PORT,
    'http_auth': '{user}:{pwd}'.format(user=SOME_USER, pwd=SOME_PASS)
}]

SOME_INDEX = 'SOME_INDEX'
SOME_DOC_TYPE = 'SOME_DOC_TYPE'


class TestElasticsearch(TestCase):

    def setUp(self):
        self.es = Elasticsearch(SOME_HOSTS_CONFIG, TIMEOUT, MagicMock())

    def test_default_async_class(self):
        import treq
        es = Elasticsearch(SOME_HOSTS_CONFIG, TIMEOUT)
        self.assertEqual(es._async_http_client, treq)

    def test_treq_creates_internal_pool(self):
        es = Elasticsearch(SOME_HOSTS_CONFIG, TIMEOUT)
        self.assertTrue("pool" in es._async_http_client_params)

    def test_generic_async_class_params_are_not_rewritten(self):
        es = Elasticsearch(SOME_HOSTS_CONFIG, TIMEOUT, MagicMock())
        self.assertTrue("pool" not in es._async_http_client_params)

    @inlineCallbacks
    def test_async_class_params_are_passed_to_requests(self):
        async_client = MagicMock()
        ok_response = self.generate_response(ResponseCodes.OK)

        async_client.request = MagicMock(return_value=ok_response)

        async_client_params = {"test": "test"}
        es = Elasticsearch(SOME_HOSTS_CONFIG,
                           TIMEOUT,
                           async_client,
                           async_client_params)

        yield es.info()

        auth = (SOME_USER, SOME_PASS)

        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None)
        async_client.request.assert_called_once_with(HttpMethod.GET,
                                                     expected_url,
                                                     auth=auth,
                                                     data=None,
                                                     timeout=TIMEOUT,
                                                     **async_client_params)

    @inlineCallbacks
    def test_close_closes_the_pool(self):
        pool = MagicMock()
        es = Elasticsearch(SOME_HOSTS_CONFIG,
                           TIMEOUT,
                           MagicMock(),
                           dict(pool=pool))
        yield es.close()

        pool.closeCachedConnections.assert_called_once_with()

    @inlineCallbacks
    def test_info(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.info()
        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_exists(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.exists(SOME_INDEX, SOME_DOC_TYPE, id=SOME_ID)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.HEAD, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @staticmethod
    def _generate_url(host, port, uri_params, *args):
        """
        Generate the elastic search url in the format
            <host>:<port>/<sub path 1>/.../<sub path n>?<param 1>&...&<param n>
            where the sub paths is from kwargs and the params is from uri_params list
        :param host: the hostname
        :param port: port
        :param uri_params: the url parameters list
        :param args: all the sub paths
        :return: the formatted url
        """
        fields = ''
        if uri_params:
            fields = '?' + '&'.join(map(urlencode, uri_params))

        path = '/'.join([quote(c, '') for c in args])
        return '{host}:{port}/{path}{fields}'.format(host=host, port=port, path=path, fields=fields).encode('utf-8')

    @inlineCallbacks
    def test_get_source(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.get_source(SOME_INDEX, SOME_DOC_TYPE, id=SOME_ID)
        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID,
                                          EsMethods.SOURCE)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_mget(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        query = {'ids': ['1', '2']}
        yield self.es.mget(body=query, index=SOME_INDEX, doc_type=SOME_DOC_TYPE)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, EsMethods.MULTIPLE_GET)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(SOME_USER, SOME_PASS), data=json.dumps(
                                                                       query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_mget_empty_body(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.assertFailure(self.es.mget(body=None, index=SOME_INDEX, doc_type=SOME_DOC_TYPE), ValueError)

    @inlineCallbacks
    def test_update(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        query = {'name': 'joseph', 'last_name': 'smith'}
        yield self.es.update(index=SOME_INDEX, doc_type=SOME_DOC_TYPE, id=SOME_ID, body=query)
        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID,
                                          EsMethods.UPDATE)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url,
                                                                   auth=(SOME_USER, SOME_PASS), data=json.dumps(
                                                                       query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_explain(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        query = {'name': 'joseph', 'last_name': 'smith'}
        yield self.es.explain(index=SOME_INDEX, doc_type=SOME_DOC_TYPE, id=SOME_ID, body=query)
        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID,
                                          EsMethods.EXPLAIN)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(SOME_USER, SOME_PASS), data=json.dumps(
                                                                       query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_get_simple(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.get(SOME_INDEX, id=SOME_ID)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, EsConst.ALL_VALUES, SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_get_not_found(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.NOT_FOUND))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), NotFoundError)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, EsConst.ALL_VALUES, SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_bad_request_raises_bad_request_error(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(400))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), RequestError)

    @inlineCallbacks
    def test_get_unknown_error(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(504))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), ElasticsearchException)

    @inlineCallbacks
    def test_get_connection_timeout(self):
        self.es._async_http_client.request = MagicMock(
            side_effect=ResponseNeverReceived('test'))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), ConnectionTimeout)

    @inlineCallbacks
    def test_get_with_fields(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.get(SOME_INDEX, id=SOME_ID, doc_type=SOME_DOC_TYPE, fields=SOME_FIELDS)
        url_fields = [{EsConst.FIELDS: SOME_FIELDS}]
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, url_fields, SOME_INDEX, SOME_DOC_TYPE, SOME_ID, )

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @staticmethod
    def generate_response(response_code):
        response = MagicMock()
        response.code = response_code
        return response

    @inlineCallbacks
    def test_search(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        some_search_query = {"query": {"match": {FIELD_1: "blabla"}}}
        yield self.es.search(SOME_INDEX, SOME_DOC_TYPE, body=some_search_query)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, EsMethods.SEARCH)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=json.dumps(
                                                                       some_search_query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_delete(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.delete(SOME_INDEX, SOME_DOC_TYPE, id=SOME_ID)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.DELETE, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_with_id(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        doc_to_index = {FIELD_1: 'bla', FIELD_2: 'bla2'}
        yield self.es.index(SOME_INDEX, SOME_DOC_TYPE, doc_to_index, id=SOME_ID)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.PUT, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=json.dumps(
                                                                       doc_to_index),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_without_id(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        doc_to_index = {FIELD_1: 'bla', FIELD_2: 'bla2'}
        yield self.es.index(SOME_INDEX, SOME_DOC_TYPE, doc_to_index)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=json.dumps(
                                                                       doc_to_index),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_empty_index_param(self):
        self.es._async_http_client.request = MagicMock()
        doc_to_index = {FIELD_1: 'bla', FIELD_2: 'bla2'}
        yield self.assertFailure(self.es.index(None, SOME_DOC_TYPE, doc_to_index), ValueError)

    @inlineCallbacks
    def test_index_empty_doc_type_param(self):
        self.es._async_http_client.request = MagicMock()
        doc_to_index = {FIELD_1: 'bla', FIELD_2: 'bla2'}
        yield self.assertFailure(self.es.index(SOME_INDEX, None, doc_to_index), ValueError)

    @inlineCallbacks
    def test_index_empty_body_param(self):
        self.es._async_http_client.request = MagicMock()
        yield self.assertFailure(self.es.index(SOME_INDEX, SOME_DOC_TYPE, None), ValueError)

    @inlineCallbacks
    def test_create(self):
        self.es.index = MagicMock()
        doc_to_index = {FIELD_1: 'bla', FIELD_2: 'bla2'}
        yield self.es.create(SOME_INDEX, SOME_DOC_TYPE, doc_to_index, id=SOME_ID)
        self.es.index.assert_called_once_with(SOME_INDEX, SOME_DOC_TYPE, doc_to_index, id=SOME_ID,
                                              params={'op_type': 'create'})

    @inlineCallbacks
    def test_scroll(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        scroll_id = '12345'
        yield self.es.scroll(scroll_id)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, EsMethods.SEARCH, EsMethods.SCROLL)
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=scroll_id,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_clear_scroll(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        scroll_id = '12345'
        yield self.es.clear_scroll(scroll_id)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, EsMethods.SEARCH, EsMethods.SCROLL)
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.DELETE, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=scroll_id,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_scroll_no_scroll_id_or_body(self):
        yield self.assertFailure(self.es.scroll(), ValueError)

    @inlineCallbacks
    def test_scan(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        some_search_result = {'hits': {'hits': []}}
        self.es.search = MagicMock(return_value=some_search_result)
        some_search_query = {"query": {"match": {FIELD_1: "blabla"}}}
        scroll_ttl = '1m'
        scroll_size = 10
        scroll_result = yield self.es.scan(SOME_INDEX, SOME_DOC_TYPE, query=some_search_query, scroll=scroll_ttl)
        self.es.search.assert_called_once_with(index=SOME_INDEX, doc_type=SOME_DOC_TYPE, body=some_search_query,
                                               scroll=scroll_ttl, size=scroll_size, search_type='scan')
        self.assertEqual(Scroller(self.es, some_search_result,
                                  scroll_ttl, scroll_size).__dict__, scroll_result.__dict__)

    @inlineCallbacks
    def test_count(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        some_search_query = {"query": {"match": {FIELD_1: "blabla"}}}
        yield self.es.count(SOME_INDEX, SOME_DOC_TYPE, body=some_search_query)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, EsMethods.COUNT)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=json.dumps(
                                                                       some_search_query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_bulk_list(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        index_query1 = {FIELD_1: "blabla1"}
        index_query2 = {FIELD_1: "blabla2"}
        yield self.es.bulk([index_query1, index_query2], SOME_INDEX, SOME_DOC_TYPE)
        expected_url = self._generate_url(
            SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE, EsMethods.BULK)

        expected_body = '{q1}\n{q2}\n'.format(
            q1=json.dumps(index_query1), q2=json.dumps(index_query2))
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=expected_body,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_msearch_list(self):
        self.es._async_http_client.request = MagicMock(
            return_value=self.generate_response(ResponseCodes.OK))
        search_query1 = {'query': {'match': {FIELD_1: "blabla1"}}}
        search_query2 = {'query': {'match': {FIELD_1: "blabla2"}}}
        yield self.es.msearch([search_query1, search_query2], SOME_INDEX, SOME_DOC_TYPE)
        expected_url = self._generate_url(SOME_HOST, SOME_PORT, None, SOME_INDEX, SOME_DOC_TYPE,
                                          EsMethods.MULTIPLE_SEARCH)

        expected_body = '{q1}\n{q2}\n'.format(q1=json.dumps(
            search_query1), q2=json.dumps(search_query2))
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(
                                                                       SOME_USER, SOME_PASS),
                                                                   data=expected_body,
                                                                   timeout=TIMEOUT)
