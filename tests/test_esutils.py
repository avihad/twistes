from unittest import TestCase

from twistes.consts import EsConst, EsDocProperties, EsAggregation
from twistes.exceptions import ScanError
from twistes.utilities import EsUtils


class TestEsUtils(TestCase):
    def test_extract_hits(self):
        result_hits = [{'filed': 'value'}]
        good_result = self.create_results(result_hits)
        extracted_results = EsUtils.extract_hits(good_result)
        self.assertEqual(result_hits, extracted_results)

    def test_extract_hits_bad_results(self):
        not_hits_results = {EsDocProperties.SCROLL_ID: 12345,
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
        invalid_result = {EsDocProperties.SCROLL_ID: 12345}
        self.assertFalse(EsUtils.has_results(invalid_result))

    def test_validate_scan_result_shard_failed(self):
        result_hits = [{'filed': 'value'}]
        bad_result = self.create_results(result_hits, failures=1)
        self.assertRaises(ScanError, EsUtils.validate_scan_result, bad_result)

    @staticmethod
    def create_results(result_hits, failures=0):
        return {EsDocProperties.SCROLL_ID: 12345,
                EsConst.HITS: {
                    EsConst.HITS: result_hits,
                    EsConst.TOTAL: 2
                },
                EsConst.SHARDS: {
                    EsConst.FAILED: failures,
                    EsConst.TOTAL: 2
                }}

    def test_is_get_query_with_results_valid_response_returns_true(self):
        es_get_response = {EsDocProperties.TYPE: "type", EsDocProperties.INDEX: "index", EsConst.FIELDS: {},
                           EsDocProperties.VERSION: 1, EsConst.FOUND: True, EsDocProperties.ID: "id"}
        self.assertTrue(EsUtils.is_get_query_with_results(es_get_response))

    def test_has_aggregation_results_true(self):
        agg_name = "results"
        actual_results = [
            {
                EsAggregation.KEY: "https://www.linkedin.com/company/forward-philippines",
                EsAggregation.DOC_COUNT: 1
            }
        ]
        agg_results = self.create_agg_results(agg_name, actual_results)
        result = EsUtils.has_aggregation_results(agg_results, agg_name)
        self.assertTrue(result)

    def test_has_aggregation_results_false(self):
        agg_name = "results"
        agg_results = self.create_agg_results(agg_name, [])
        result = EsUtils.has_aggregation_results(agg_results, agg_name)
        self.assertFalse(result)

    def test_extract_aggregation_results_has_results(self):
        agg_name = "results"
        expected = [
            {
                EsAggregation.KEY: "https://www.linkedin.com/company/forward-philippines",
                EsAggregation.DOC_COUNT: 1
            }
        ]
        agg_results = self.create_agg_results(agg_name, expected)
        result = EsUtils.extract_aggregation_results(agg_results, agg_name)
        self.assertEquals(result, expected)

    def test_extract_aggregation_results_no_results(self):
        agg_name = "results"
        expected = []
        agg_results = self.create_agg_results(agg_name, expected)
        result = EsUtils.extract_aggregation_results(agg_results, agg_name)
        self.assertEquals(result, expected)

    @staticmethod
    def create_agg_results(agg_name, actual_results):
        agg_results = {
            EsConst.HITS: {
                EsConst.TOTAL: 1,
                EsConst.HITS: []
            },
            EsAggregation.AGGREGATIONS: {
                agg_name: {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    EsAggregation.BUCKETS: actual_results
                }
            }
        }
        return agg_results
