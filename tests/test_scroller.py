from mock import MagicMock
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from twistes.consts import EsConst, EsDocProperties
from twistes.scroller import Scroller

SOME_VALUE_1 = "SOME_VALUE_1"
SOME_VALUE_2 = "SOME_VALUE_2"
SOME_VALUE_3 = "SOME_VALUE_3"
SOME_ID_1 = "SOME_ID_1"
SOME_ID_2 = "SOME_ID_2"
SOME_ID_3 = "SOME_ID_3"
SOME_ID_4 = "SOME_ID_4"
SOME_SCROLL = "2m"


class TestScroller(TestCase):
    @inlineCallbacks
    def test_scroll_return_results(self):
        expected_results = [{SOME_VALUE_1: SOME_VALUE_2}]
        some_results = self.create_valid_es_result(expected_results, SOME_ID_1)
        scroller = Scroller(MagicMock(), some_results, SOME_SCROLL, 1)
        results = yield scroller.next()
        self.assertEqual(expected_results, results)

    @inlineCallbacks
    def test_scroll_next_scroller_iteration_call_es(self):
        scroll_id = SOME_ID_1
        some_results1 = self.create_valid_es_result([{SOME_VALUE_1: SOME_VALUE_2}], scroll_id)

        expected_results = [{SOME_VALUE_2: SOME_VALUE_1}]
        es = MagicMock()
        es.scroll = MagicMock(return_value=self.create_valid_es_result(expected_results, scroll_id))
        scroller = Scroller(es, some_results1, SOME_SCROLL, 1)
        yield scroller.next()
        results = yield scroller.next()
        self.assertEqual(results, expected_results)
        es.scroll.assert_called_once_with(scroll_id, scroll=SOME_SCROLL)

    @inlineCallbacks
    def test_scroll_iterator(self):
        expected_result_1 = [{
            SOME_VALUE_1: SOME_VALUE_1
        }]
        es_results_1 = self.create_valid_es_result(expected_result_1, SOME_ID_1)

        expected_result_2 = [{
            SOME_VALUE_2: SOME_VALUE_2
        }]
        expected_result_3 = [{
            SOME_VALUE_3: SOME_VALUE_3
        }]
        expected_result_4 = []

        es = MagicMock()
        scroll_side_effect = self.create_scroll_side_effect({
            SOME_ID_1: {
                "next_scroll_id": SOME_ID_2,
                "results": expected_result_2
            },
            SOME_ID_2: {
                "next_scroll_id": SOME_ID_3,
                "results": expected_result_3
            },
            SOME_ID_3: {
                "next_scroll_id": SOME_ID_4,
                "results": expected_result_4
            }
        })
        es.scroll = MagicMock(side_effect=scroll_side_effect)

        scroller = Scroller(es, es_results_1, SOME_SCROLL, 1)

        results = []
        for defer_results in scroller:
            data = yield defer_results
            results.append(data)
        expected = [expected_result_1, expected_result_2, expected_result_3, expected_result_4]
        self.assertEqual(expected, results)

    def create_scroll_side_effect(self, expected_results):
        """
        :param expected_results: Object where the key is a scroll_id and the value is the value to return for that id
        :return: Scroll side_effect method (stub)
        """
        def scroll_side_effect(scroll_id=None, body=None, **query_params):
            if scroll_id in expected_results:
                next_scroll_id = expected_results[scroll_id]['next_scroll_id']
                results = expected_results[scroll_id]['results']
                return self.create_valid_es_result(results, next_scroll_id)

        return scroll_side_effect

    @staticmethod
    def create_valid_es_result(expected_results, scroll_id):
        return {
            EsDocProperties.SCROLL_ID: scroll_id,
            EsConst.HITS: {
                EsConst.HITS: expected_results,
                EsConst.TOTAL: len(expected_results)
            },
            EsConst.SHARDS: {
                EsConst.FAILED: 0,
                EsConst.TOTAL: 2
            }
        }
