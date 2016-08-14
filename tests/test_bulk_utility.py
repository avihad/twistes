from __future__ import division
import json

import math
from mock import MagicMock
from twisted.internet.defer import succeed, inlineCallbacks
from twisted.trial.unittest import TestCase

from twistes.bulk_utils import BulkUtility, ActionParser
from twistes.consts import EsBulk, EsDocProperties
from twistes.exceptions import BulkIndexError, ConnectionTimeout

SOME_INDEX = "some_index"
SOME_DOC_TYPE = "some_doc_type"
SOME_DOC = {"field1": "value1", "field2": "value2"}
SOME_ID = "some_id"

ERROR_MSG = "error_msg"
ITEM_FAILED = (False, ERROR_MSG)

ITEM_SUCCESS = (True, None)


class TestBulkUtility(TestCase):

    def setUp(self):
        self.bulk_utility = BulkUtility(MagicMock())

    @inlineCallbacks
    def test_bulk_stats_only(self):
        return_value = [succeed([ITEM_SUCCESS, ITEM_SUCCESS, ITEM_FAILED])]
        self.bulk_utility.streaming_bulk = MagicMock(return_value=return_value)
        success, faileds = yield self.bulk_utility.bulk(None, stats_only=True)
        self.assertEqual(2, success)
        self.assertEqual(1, faileds)

    @inlineCallbacks
    def test_bulk(self):
        return_value = [succeed([ITEM_SUCCESS, ITEM_SUCCESS, ITEM_FAILED])]
        self.bulk_utility.streaming_bulk = MagicMock(return_value=return_value)
        errors = yield self.bulk_utility.bulk(None)
        self.assertEqual((2, [ERROR_MSG]), errors)

    def test_streaming_bulk(self):
        self.bulk_utility._process_bulk_chunk = MagicMock()
        num_of_actions = 30
        mock_sub_actions = [1, 2, 3]
        num_of_chunks = 4
        return_value = [mock_sub_actions for i in range(num_of_chunks)]
        self.bulk_utility._chunk_actions = MagicMock(return_value=return_value)

        cb = MagicMock()

        bulk = list(self.bulk_utility.streaming_bulk(range(num_of_actions),
                                                     expand_action_callback=cb))

        self.assertEqual(num_of_actions,
                         cb.call_count)
        self.assertEqual(num_of_chunks,
                         self.bulk_utility._process_bulk_chunk.call_count)

    def test_action_parser(self):
        update_record = {EsBulk.OP_TYPE: EsBulk.UPDATE,
                         EsDocProperties.INDEX: SOME_INDEX,
                         EsDocProperties.TYPE: SOME_DOC_TYPE,
                         EsDocProperties.ID: SOME_ID,
                         EsDocProperties.SOURCE: SOME_DOC}

        result = ActionParser.expand_action(update_record)
        row = [EsBulk.UPDATE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        expected = (self._create_action_row(*row), SOME_DOC)

        self.assertEqual(expected, result)

    @staticmethod
    def _create_action_row(op_type, index, doc_type, id):
        return {op_type: {EsDocProperties.INDEX: index,
                          EsDocProperties.TYPE: doc_type,
                          EsDocProperties.ID: id}
                }

    def test__expand_action_doc_str(self):
        doc_dumps = json.dumps(SOME_DOC)
        result = ActionParser.expand_action(doc_dumps)

        expected_first_row = json.dumps({EsBulk.INDEX: {}})
        expected = expected_first_row, doc_dumps

        self.assertEqual(expected, result)

    def test__expand_action_delete(self):
        delete_record = {EsBulk.OP_TYPE: EsBulk.DELETE,
                         EsDocProperties.INDEX: SOME_INDEX,
                         EsDocProperties.TYPE: SOME_DOC_TYPE,
                         EsDocProperties.ID: SOME_ID}

        result = ActionParser.expand_action(delete_record)
        row = [EsBulk.DELETE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        expected = (self._create_action_row(*row), None)

        self.assertEqual(expected, result)

    def test__chunk_actions_by_chunk_size(self):
        num_of_tasks = 10
        tasks_per_chunk = 3

        row = [EsBulk.UPDATE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]

        actions = [(self._create_action_row(*row), SOME_DOC)
                   for i in range(num_of_tasks)]

        chunks = list(self.bulk_utility._chunk_actions(actions,
                                                       chunk_size=tasks_per_chunk,
                                                       max_chunk_bytes=100000))
        self.assertEqual(math.ceil(num_of_tasks / tasks_per_chunk),
                         len(chunks))

    def test__chunk_actions_by_chunk_bytes(self):
        num_of_tasks = 10
        row = [EsBulk.UPDATE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [(self._create_action_row(*row), SOME_DOC) for _ in
                   range(num_of_tasks)]

        chunks = list(self.bulk_utility._chunk_actions(actions,
                                                       chunk_size=20,
                                                       max_chunk_bytes=350))

        # Every 2 records is about 350 bytes so we will have 5 chunks
        self.assertEqual(5, len(chunks))

    def test__chunk_actions_serialize(self):
        num_of_tasks = 10
        row = [EsBulk.UPDATE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [(self._create_action_row(*row), SOME_DOC) for _ in
                   range(num_of_tasks)]
        chunks = list(self.bulk_utility._chunk_actions(actions,
                                                       chunk_size=20,
                                                       max_chunk_bytes=100000))
        # Every 2 records is about 350 bytes so we will have 5 chunks
        expected = []
        for a, d in actions:
            expected.append(json.dumps(a))
            expected.append(json.dumps(d))

        self.assertEqual([expected], chunks)

    @inlineCallbacks
    def test__process_bulk_chunk_good_results(self):
        op_type1 = EsBulk.INDEX
        op_type2 = EsBulk.DELETE
        good_index_result = {op_type1: {'status': 200}}
        good_delete_result = {op_type2: {'status': 200}}
        bulk_mock_result = {'items': [good_index_result.copy(),
                                      good_delete_result.copy()]}

        self.bulk_utility.client.bulk = MagicMock(
            return_value=bulk_mock_result)

        row = [op_type1, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [self._create_action_row(*row)]

        results = yield self.bulk_utility._process_bulk_chunk(json.dumps(actions))
        self.assertEqual([(True, good_index_result),
                          (True, good_delete_result)], results)

    @inlineCallbacks
    def test__process_bulk_chunk(self):
        op_type1 = EsBulk.INDEX
        op_type2 = EsBulk.DELETE
        good_index_result = {op_type1: {'status': 200}}
        good_delete_result = {op_type2: {'status': 200}}
        bulk_mock_result = {'items': [good_index_result.copy(),
                                      good_delete_result.copy()]}

        self.bulk_utility.client.bulk = MagicMock(
            return_value=bulk_mock_result)
        row = [op_type1, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [self._create_action_row(*row)]
        results = yield self.bulk_utility._process_bulk_chunk(json.dumps(actions))
        self.assertEqual([(True, good_index_result),
                          (True, good_delete_result)],
                         results)

    @inlineCallbacks
    def test__process_bulk_chunk_error(self):
        op_type1 = EsBulk.INDEX
        op_type2 = EsBulk.DELETE
        good_index_result = {op_type1: {'status': 200}}
        bad_delete_result = {op_type2: {'status': 500}}
        bulk_mock_result = {'items': [good_index_result, bad_delete_result]}

        self.bulk_utility.client.bulk = MagicMock(
            return_value=bulk_mock_result)
        row = [op_type1, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [self._create_action_row(*row)]
        yield self.assertFailure(self.bulk_utility._process_bulk_chunk(json.dumps(actions)),
                                 BulkIndexError)

    @inlineCallbacks
    def test__process_bulk_connection_timeout_raise(self):
        self.bulk_utility.client.bulk = MagicMock(
            side_effect=ConnectionTimeout("test-timeout"))
        self.bulk_utility._handle_transport_error = MagicMock()
        yield self.assertFailure(self.bulk_utility._process_bulk_chunk([]),
                                 ConnectionTimeout)

    @inlineCallbacks
    def test__process_bulk_transport_error_raise_false(self):
        self.bulk_utility.client.bulk = MagicMock(
            side_effect=ConnectionTimeout("test-timeout"))
        self.bulk_utility._handle_transport_error = MagicMock()
        results = yield self.bulk_utility._process_bulk_chunk([],
                                                              raise_on_exception=False)
        self.assertEqual([], results)

    def test__handle_transport_error(self):
        row = [EsBulk.UPDATE, SOME_INDEX, SOME_DOC_TYPE, SOME_ID]
        actions = [(self._create_action_row(*row), SOME_DOC) for i in
                   range(10)]
        dump_actions = []
        for a, d in actions:
            dump_actions.append(json.dumps(a))
            dump_actions.append(json.dumps(d))

        self.assertRaises(BulkIndexError,
                          self.bulk_utility._handle_transport_error,
                          dump_actions,
                          Exception(),
                          raise_on_error=True)
