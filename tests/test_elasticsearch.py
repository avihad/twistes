import json
import urllib

from mock import MagicMock, ANY
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web._newclient import ResponseNeverReceived

from twistes.client import Elasticsearch
from twistes.consts import HttpMethod, EsConst, ResponseCodes, EsMethods
from twistes.exceptions import NotFoundError, ConnectionTimeout
from twistes.scroller import Scroller

FILED_2 = 'FILED_2'
FILED_1 = 'FILED_1'

SOME_FIELDS = [FILED_1, FILED_2]

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

    @inlineCallbacks
    def test_get_simple(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.get(SOME_INDEX, id=SOME_ID)
        expected_url = '{host}:{port}/{index}/{doc_type}/{id}'.format(host=SOME_HOST, port=SOME_PORT, index=SOME_INDEX,
                                                                      doc_type=EsConst.ANY_DOC_TYPE, id=SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url,
                                                                   auth=(SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_get_not_found(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.NOT_FOUND))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), NotFoundError)

        expected_url = '{host}:{port}/{index}/{doc_type}/{id}'.format(host=SOME_HOST, port=SOME_PORT, index=SOME_INDEX,
                                                                      doc_type=EsConst.ANY_DOC_TYPE, id=SOME_ID)
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url, auth=(SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_get_unknown_error(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(504))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), Exception)

    @inlineCallbacks
    def test_get_connection_timeout(self):
        self.es._async_http_client.request = MagicMock(side_effect=ResponseNeverReceived('test'))

        yield self.assertFailure(self.es.get(SOME_INDEX, id=SOME_ID), ConnectionTimeout)

    @inlineCallbacks
    def test_get_with_fields(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.get(SOME_INDEX, id=SOME_ID, doc_type=SOME_DOC_TYPE, fields=SOME_FIELDS)
        expected_url = '{host}:{port}/{index}/{doc_type}/{id}?{fileds}'.format(host=SOME_HOST, port=SOME_PORT,
                                                                               index=SOME_INDEX,
                                                                               doc_type=SOME_DOC_TYPE, id=SOME_ID,
                                                                               fileds=urllib.urlencode(
                                                                                   {EsConst.FIELDS: SOME_FIELDS}))

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url, auth=(SOME_USER, SOME_PASS), data=None,
                                                                   timeout=TIMEOUT)

    @staticmethod
    def generate_response(response_code):
        response = MagicMock()
        response.code = response_code
        return response

    @inlineCallbacks
    def test_search(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        some_search_query = {"query": {"match": {FILED_1: "blabla"}}}
        yield self.es.search(SOME_INDEX, SOME_DOC_TYPE, body=some_search_query)
        expected_url = '{host}:{port}/{index}/{doc_type}/{method}'.format(host=SOME_HOST, port=SOME_PORT,
                                                                          index=SOME_INDEX,
                                                                          doc_type=SOME_DOC_TYPE,
                                                                          method=EsMethods.SEARCH)
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url, auth=(SOME_USER, SOME_PASS),
                                                                   data=json.dumps(some_search_query),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_delete(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        yield self.es.delete(SOME_INDEX, SOME_DOC_TYPE, id=SOME_ID)
        expected_url = '{host}:{port}/{index}/{doc_type}/{id}'.format(host=SOME_HOST, port=SOME_PORT, index=SOME_INDEX,
                                                                      doc_type=SOME_DOC_TYPE, id=SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.DELETE, expected_url, auth=(SOME_USER, SOME_PASS),
                                                                   data=None,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_with_id(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        doc_to_index = {FILED_1: 'bla', FILED_2: 'bla2'}
        yield self.es.index(SOME_INDEX, SOME_DOC_TYPE, doc_to_index, id=SOME_ID)
        expected_url = '{host}:{port}/{index}/{doc_type}/{id}'.format(host=SOME_HOST, port=SOME_PORT, index=SOME_INDEX,
                                                                      doc_type=SOME_DOC_TYPE, id=SOME_ID)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.PUT, expected_url, auth=(SOME_USER, SOME_PASS),
                                                                   data=json.dumps(doc_to_index),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_without_id(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        doc_to_index = {FILED_1: 'bla', FILED_2: 'bla2'}
        yield self.es.index(SOME_INDEX, SOME_DOC_TYPE, doc_to_index)
        expected_url = '{host}:{port}/{index}/{doc_type}'.format(host=SOME_HOST, port=SOME_PORT, index=SOME_INDEX,
                                                                 doc_type=SOME_DOC_TYPE)

        self.es._async_http_client.request.assert_called_once_with(HttpMethod.POST, expected_url, auth=(SOME_USER, SOME_PASS),
                                                                   data=json.dumps(doc_to_index),
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_index_empty_index_param(self):
        self.es._async_http_client.request = MagicMock()
        doc_to_index = {FILED_1: 'bla', FILED_2: 'bla2'}
        yield self.assertFailure(self.es.index(None, SOME_DOC_TYPE, doc_to_index), ValueError)

    @inlineCallbacks
    def test_index_empty_doc_type_param(self):
        self.es._async_http_client.request = MagicMock()
        doc_to_index = {FILED_1: 'bla', FILED_2: 'bla2'}
        yield self.assertFailure(self.es.index(SOME_INDEX, None, doc_to_index), ValueError)

    @inlineCallbacks
    def test_index_empty_body_param(self):
        self.es._async_http_client.request = MagicMock()
        yield self.assertFailure(self.es.index(SOME_INDEX, SOME_DOC_TYPE, None), ValueError)

    @inlineCallbacks
    def test_scroll(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        scroll_id = '12345'
        yield self.es.scroll(scroll_id)
        expected_url = '{host}:{port}{method}'.format(host=SOME_HOST, port=SOME_PORT, method=EsMethods.SCROLL)
        self.es._async_http_client.request.assert_called_once_with(HttpMethod.GET, expected_url, auth=(SOME_USER, SOME_PASS),
                                                                   data=scroll_id,
                                                                   timeout=TIMEOUT)

    @inlineCallbacks
    def test_scroll_no_scroll_id_or_body(self):
        yield self.assertFailure(self.es.scroll(), ValueError)

    @inlineCallbacks
    def test_scan(self):
        self.es._async_http_client.request = MagicMock(return_value=self.generate_response(ResponseCodes.OK))
        some_search_result = {'hits': {'hits': []}}
        self.es.search = MagicMock(return_value=some_search_result)
        some_search_query = {"query": {"match": {FILED_1: "blabla"}}}
        scroll_size = '1m'
        scroll_result = yield self.es.scan(SOME_INDEX, SOME_DOC_TYPE, query=some_search_query, scroll=scroll_size)
        self.es.search.assert_called_once_with(index=SOME_INDEX, doc_type=SOME_DOC_TYPE, body=some_search_query,
                                               scroll=scroll_size, search_type='scan')

        self.assertEqual(Scroller(self.es, some_search_result).__dict__, scroll_result.__dict__)
