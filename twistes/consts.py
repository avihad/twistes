class HttpMethod(object):
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
    SEARCH = '_search'
    SCROLL = '/_search/scroll'


class EsConst(object):
    TOTAL = 'total'
    FAILED = 'failed'
    SHARDS = '_shards'
    ANY_DOC_TYPE = '_all'
    FIELDS = 'fields'
    SCROLL_ID = 'scroll_id'
    UNDERSCORE_SCROLL_ID = '_scroll_id'
    HITS = 'hits'


# parts of URL to be omitted
NULL_VALUES = (None, '', b'', [], ())
