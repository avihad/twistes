class HttpMethod(object):
    HEAD = 'HEAD'
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    DELETE = 'DELETE'


class ResponseCodes(object):
    OK = 200
    CREATED = 201
    ACCEPTED = 202
    BAD_REQUEST = 400
    NOT_FOUND = 404


class EsMethods(object):
    MULTIPLE_SEARCH = '_msearch'
    BULK = '_bulk'
    COUNT = '_count'
    EXPLAIN = '_explain'
    MULTIPLE_GET = '_mget'
    UPDATE = '_update'
    SOURCE = '_source'
    SEARCH = '_search'
    SCROLL = '/_search/scroll'


class EsConst(object):
    TOTAL = 'total'
    FAILED = 'failed'
    SHARDS = '_shards'
    ALL_VALUES = '_all'
    FIELDS = 'fields'
    SCROLL_ID = 'scroll_id'
    UNDERSCORE_SCROLL_ID = '_scroll_id'
    HITS = 'hits'


# parts of URL to be omitted
NULL_VALUES = (None, '', b'', [], ())
