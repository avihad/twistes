from unittest import TestCase
from twistes.consts import EsConst
from twistes.exceptions import ScanError
from twistes.utilities import EsUtils


class TestEsUtils(TestCase):
    def test_extract_hits(self):
        result_hits = [{'filed': 'value'}]
        good_result = self.create_results(result_hits)
        extracted_results = EsUtils.extract_hits(good_result)
        self.assertEqual(result_hits, extracted_results)

    def test_extract_hits_bad_results(self):
        not_hits_results = {EsConst.UNDERSCORE_SCROLL_ID: 12345,
                            EsConst.SHARDS: {
                                EsConst.FAILED: 0,
                                EsConst.TOTAL: 2
                            }}
        extracted_results = EsUtils.extract_hits(not_hits_results)
        self.assertEqual([], extracted_results)

    def test_has_results_good_results(self):
        result_hits = [{'filed': 'value'}]
        good_result = self.create_results(result_hits)
        self.assertTrue(EsUtils.has_results(good_result))

    def test_has_results_invalid_results(self):
        invalid_result = {EsConst.UNDERSCORE_SCROLL_ID: 12345}
        self.assertFalse(EsUtils.has_results(invalid_result))

    def test_validate_scan_result_shard_failed(self):
        result_hits = [{'filed': 'value'}]
        bad_result = self.create_results(result_hits, failures=1)
        self.assertRaises(ScanError, EsUtils.validate_scan_result, bad_result)

    @staticmethod
    def create_results(result_hits, failures=0):
        return {EsConst.UNDERSCORE_SCROLL_ID: 12345,
                EsConst.HITS: {
                    EsConst.HITS: result_hits,
                    EsConst.TOTAL: 2
                },
                EsConst.SHARDS: {
                    EsConst.FAILED: failures,
                    EsConst.TOTAL: 2
                }}

    def test_is_get_query_with_results_valid_response_returns_true(self):
        es_get_response = {"_type": "type", "_index": "index", "fields": {}, "_version": 1, "found": True, "_id": "id"}
        self.assertTrue(EsUtils.is_get_query_with_results(es_get_response))
