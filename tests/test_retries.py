
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from mock import MagicMock
from twisted.web._newclient import ResponseNeverReceived

from twistes.client import Elasticsearch
from twistes.consts import ResponseCodes
from twistes.exceptions import ConnectionTimeout

SOME_HOSTS_CONFIG = [{
    'host': "http://SOME_HOST",
    'port': 9200,
    'http_auth': '{user}:{pwd}'.format(user="BURN", pwd="SANDERS")
}]

METHOD = "method"
PATH = "/path"
BODY = {"some": "json"}
SOME_CONTENT = "Some content"

NUM_RETRIES = 3


class TestRetries(TestCase):

    @inlineCallbacks
    def test_perform_request_fail(self):
        async_http_client = MagicMock()
        async_http_client.request = MagicMock(side_effect=ResponseNeverReceived("test"))
        es = self.get_es(async_http_client)

        yield self.assertFailure(es._perform_request(METHOD, PATH, BODY), ConnectionTimeout)
        self.assertEqual(NUM_RETRIES + 1, async_http_client.request.call_count)

    @inlineCallbacks
    def test_perform_request_success_after_retries(self):
        async_http_client = MagicMock()
        async_http_client.request = MagicMock(side_effect=[ResponseNeverReceived("test"),
                                                           ResponseNeverReceived("test"),
                                                           self.generate_response(ResponseCodes.OK)])

        es = self.get_es(async_http_client)
        es._get_content = MagicMock(return_value=SOME_CONTENT)

        r = yield es._perform_request(METHOD, PATH, BODY)
        self.assertEqual(NUM_RETRIES, async_http_client.request.call_count)
        self.assertEqual(SOME_CONTENT, r)

    @staticmethod
    def generate_response(response_code):
        response = MagicMock()
        response.code = response_code
        return response

    @staticmethod
    def get_es(async_http_client):
        return Elasticsearch(SOME_HOSTS_CONFIG, 10, async_http_client, None, True, NUM_RETRIES)