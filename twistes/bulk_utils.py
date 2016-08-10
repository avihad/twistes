import json
from operator import methodcaller

from twisted.internet.defer import inlineCallbacks, returnValue

from twistes.compatability import string_types
from twistes.consts import EsBulk, EsDocProperties
from twistes.exceptions import BulkIndexError, ConnectionTimeout


class ActionParser(object):
    ES_OPERATIONS_PARAMS = (
        EsDocProperties.INDEX, EsDocProperties.PARENT, EsDocProperties.PERCOLATE, EsDocProperties.ROUTING,
        EsDocProperties.TIMESTAMP, EsDocProperties.TTL, EsDocProperties.TYPE, EsDocProperties.VERSION,
        EsDocProperties.VERSION_TYPE, EsDocProperties.ID, EsDocProperties.RETRY_ON_CONFLICT)

    @staticmethod
    def expand_action(data):
        """
        From one document or action definition passed in by the user extract the
        action/data lines needed for elasticsearch's
        :meth:`~elasticsearch.Elasticsearch.bulk` api.
        :return es format to bulk doc
        """
        # when given a string, assume user wants to index raw json
        if isinstance(data, string_types):
            return '{"index": {}}', data

        # make sure we don't alter the action
        data = data.copy()
        op_type = data.pop(EsBulk.OP_TYPE, EsBulk.INDEX)

        action = ActionParser._get_relevant_action_params(data, op_type)

        # no data payload for delete
        if op_type == EsBulk.DELETE:
            return action, None

        return action, data.get(EsDocProperties.SOURCE, data)

    @staticmethod
    def _get_relevant_action_params(data, op_type):
        action = {op_type: {}}
        for key in ActionParser.ES_OPERATIONS_PARAMS:
            if key in data:
                action[op_type][key] = data.pop(key)

        return action


class BulkUtility(object):

    def __init__(self, es):
        self.client = es

    @inlineCallbacks
    def bulk(self, actions, stats_only=False, **kwargs):
        """
        Helper for the :meth:`~elasticsearch.Elasticsearch.bulk` api that provides
        a more human friendly interface - it consumes an iterator of actions and
        sends them to elasticsearch in chunks. It returns a tuple with summary
        information - number of successfully executed actions and either list of
        errors or number of errors if `stats_only` is set to `True`.
        See :func:`~elasticsearch.helpers.streaming_bulk` for more accepted
        parameters
        :arg actions: iterator containing the actions
        :arg stats_only: if `True` only report number of successful/failed
            operations instead of just number of successful and a list of error responses
        Any additional keyword arguments will be passed to
        :func:`~elasticsearch.helpers.streaming_bulk` which is used to execute
        the operation.
        """
        success, failed = 0, 0

        # list of errors to be collected is not stats_only
        errors = []

        for deferred_bulk in self.streaming_bulk(actions, **kwargs):
            bulk_results = yield deferred_bulk
            for ok, item in bulk_results:
                # go through request-response pairs and detect failures
                if not ok:
                    if not stats_only:
                        errors.append(item)
                    failed += 1
                else:
                    success += 1
        summarized_results = success, failed if stats_only else errors
        returnValue(summarized_results)

    def streaming_bulk(self, actions, chunk_size=500, max_chunk_bytes=100 * 1024 * 1024,
                       raise_on_error=True, expand_action_callback=ActionParser.expand_action,
                       raise_on_exception=True, **kwargs):
        """
        Streaming bulk consumes actions from the iterable passed in and return the results of all bulk data
        :func:`~elasticsearch.helpers.bulk` which is a wrapper around streaming
        bulk that returns summary information about the bulk operation once the
        entire input is consumed and sent.
        :arg actions: iterable containing the actions to be executed
        :arg chunk_size: number of docs in one chunk sent to es (default: 500)
        :arg max_chunk_bytes: the maximum size of the request in bytes (default: 100MB)
        :arg raise_on_error: raise ``BulkIndexError`` containing errors (as `.errors`)
            from the execution of the last chunk when some occur. By default we raise.
        :arg raise_on_exception: if ``False`` then don't propagate exceptions from
            call to ``bulk`` and just report the items that failed as failed.
        :arg expand_action_callback: callback executed on each action passed in,
            should return a tuple containing the action line and the data line
            (`None` if data line should be omitted).
        """
        actions = list(map(expand_action_callback, actions))

        for bulk_actions in self._chunk_actions(actions, chunk_size, max_chunk_bytes):
            yield self._process_bulk_chunk(bulk_actions, raise_on_exception, raise_on_error, **kwargs)

    @staticmethod
    def _chunk_actions(actions, chunk_size, max_chunk_bytes):
        """
        Split actions into chunks by number or size, serialize them into strings in
        the process.
        """
        bulk_actions = []
        size, action_count = 0, 0
        for action, data in actions:
            action = json.dumps(action)
            cur_size = len(action) + 1

            if data is not None:
                data = json.dumps(data)
                cur_size += len(data) + 1

            # full chunk, send it and start a new one
            if bulk_actions and (size + cur_size > max_chunk_bytes or action_count == chunk_size):
                yield bulk_actions
                bulk_actions = []
                size, action_count = 0, 0

            bulk_actions.append(action)
            if data is not None:
                bulk_actions.append(data)
            size += cur_size
            action_count += 1

        if bulk_actions:
            yield bulk_actions

    @inlineCallbacks
    def _process_bulk_chunk(self, bulk_actions, raise_on_exception=True, raise_on_error=True, **kwargs):
        """
        Send a bulk request to elasticsearch and process the output.
        """
        # if raise on error is set, we need to collect errors per chunk before
        # raising them

        resp = None
        try:
            # send the actual request
            actions = "{}\n".format('\n'.join(bulk_actions))
            resp = yield self.client.bulk(actions, **kwargs)
        except ConnectionTimeout as e:
            # default behavior - just propagate exception
            if raise_on_exception:
                raise

            self._handle_transport_error(bulk_actions, e, raise_on_error)
            returnValue([])

        # go through request-response pairs and detect failures
        errors = []
        results = []
        for op_type, item in map(methodcaller('popitem'), resp['items']):
            ok = 200 <= item.get('status', 500) < 300
            if not ok and raise_on_error:
                errors.append({op_type: item})

            if ok or not errors:
                # if we are not just recording all errors to be able to raise
                # them all at once, yield items individually
                results.append((ok, {op_type: item}))

        if errors:
            msg_fmt = '{num} document(s) failed to index.'
            raise BulkIndexError(msg_fmt.format(num=len(errors)), errors)
        else:
            returnValue(results)

    @staticmethod
    def _handle_transport_error(bulk_actions, e, raise_on_error):
        # if we are not propagating, mark all actions in current chunk as
        # failed
        exc_errors = []
        # deserialize the data back, this is expensive but only run on
        # errors if raise_on_exception is false, so shouldn't be a real
        # issue
        bulk_data = iter(map(json.loads, bulk_actions))
        while True:
            try:
                # collect all the information about failed actions
                action = next(bulk_data)
                op_type, action = action.popitem()
                # TODO:: add the status code after we add it to the exception
                info = {"error": str(e), "exception": e}
                if op_type != 'delete':
                    info['data'] = next(bulk_data)
                info.update(action)
                exc_errors.append({op_type: info})
            except StopIteration:
                break

        # emulate standard behavior for failed actions
        if raise_on_error:
            msg_fmt = '{num} document(s) failed to index.'
            raise BulkIndexError(msg_fmt.format(num=len(exc_errors)),
                                 exc_errors)
