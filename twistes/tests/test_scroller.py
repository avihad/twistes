from mock import MagicMock
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from twistes.consts import EsConst
from twistes.scroller import Scroller

SOME_VALUE_1 = "SOME_VALUE_1"
SOME_VALUE_2 = "SOME_VALUE_2"
SOME_ID = "SOME_ID"


class TestScroller(TestCase):
    @inlineCallbacks
    def test_scroll_return_results(self):
        expected_results = [{SOME_VALUE_1: SOME_VALUE_2}]
        some_results = self.wrap_good_result(expected_results, SOME_ID)
        scroller = Scroller(MagicMock(), some_results)
        results = yield scroller.next()
        self.assertEqual(expected_results, results)

    @inlineCallbacks
    def test_scroll_next_scroller_iteration_call_es(self):
        scroll_id = SOME_ID
        some_results1 = self.wrap_good_result([{SOME_VALUE_1: SOME_VALUE_2}], scroll_id)

        expected_results = [{SOME_VALUE_2: SOME_VALUE_1}]
        es = MagicMock()
        es.scroll = MagicMock(return_value=self.wrap_good_result(expected_results, scroll_id))
        scroller = Scroller(es, some_results1)
        yield scroller.next()
        results = yield scroller.next()
        self.assertEqual(results, expected_results)
        es.scroll.assert_called_once_with(scroll_id)

    @inlineCallbacks
    def test_scroll_iterator(self):
        scroll_id = SOME_ID
        expected_result_1 = [{SOME_VALUE_1: SOME_VALUE_2}]
        es_results_1 = self.wrap_good_result(expected_result_1, scroll_id)

        expected_result_2 = [{SOME_VALUE_2: SOME_VALUE_1}]
        es = MagicMock()
        es.scroll = MagicMock(return_value=self.wrap_good_result(expected_result_2, None))
        scroller = Scroller(es, es_results_1)

        results = []
        for defer_results in scroller:
            data = yield defer_results
            results.append(data)
        expected = [expected_result_1, expected_result_2]
        self.assertEqual(expected, results)

    def test_scroll_end_of_scan_when_scroll_id_is_none(self):
        expected_results = [{SOME_VALUE_1: SOME_VALUE_2}]
        some_results = self.wrap_good_result(expected_results, None)
        scroller = Scroller(MagicMock(), some_results)
        scroller.next()
        self.assertRaises(StopIteration, scroller.next)

    @staticmethod
    def wrap_good_result(expected_results, scroll_id):
        return {EsConst.UNDERSCORE_SCROLL_ID: scroll_id,
                EsConst.HITS: {
                    EsConst.HITS: expected_results,
                    EsConst.TOTAL: 2
                },
                EsConst.SHARDS: {
                    EsConst.FAILED: 0,
                    EsConst.TOTAL: 2
                }}
