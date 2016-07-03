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
    SCROLL = '_search/scroll'


class EsConst(object):
    TOTAL = 'total'
    FAILED = 'failed'
    SHARDS = '_shards'
    ALL_VALUES = '_all'
    FIELDS = 'fields'
    SCROLL_ID = 'scroll_id'
    HITS = 'hits'


class EsBulk(object):
    OP_TYPE = '_op_type'
    INDEX = 'index'
    CREATE = 'create'
    DELETE = 'delete'
    UPDATE = 'update'


class EsDocProperties(object):
    INDEX = '_index'
    TYPE = '_type'
    PARENT = '_parent'
    PERCOLATE = '_percolate'
    ROUTING = '_routing'
    TIMESTAMP = '_timestamp'
    TTL = '_ttl'
    VERSION = '_version'
    VERSION_TYPE = '_version_type'
    ID = '_id'
    RETRY_ON_CONFLICT = '_retry_on_conflict'
    SOURCE = '_source'
    SCROLL_ID = '_scroll_id'

# parts of URL to be omitted
NULL_VALUES = (None, '', b'', [], ())
