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


class HostParsing(object):
    HTTP = 'http'
    HTTPS = 'https'
    USE_SSL = 'use_ssl'
    URL_PREFIX = 'url_prefix'
    SCHEME = 'scheme'
    HTTP_AUTH = 'http_auth'
    PORT = 'port'
    HOST = 'host'


class EsMethods(object):
    MULTIPLE_SEARCH = '_msearch'
    BULK = '_bulk'
    COUNT = '_count'
    EXPLAIN = '_explain'
    MULTIPLE_GET = '_mget'
    UPDATE = '_update'
    SOURCE = '_source'
    SEARCH = '_search'
    SCROLL = 'scroll'


class EsConst(object):
    TOTAL = 'total'
    FAILED = 'failed'
    SHARDS = '_shards'
    ALL_VALUES = '_all'
    FIELD = 'field'
    FIELDS = 'fields'
    SCROLL_ID = 'scroll_id'
    HITS = 'hits'
    FOUND = 'found'


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


class EsAggregation(object):
    AGGREGATIONS = 'aggregations'
    BUCKETS = 'buckets'
    DOC_COUNT = 'doc_count'
    KEY = 'key'
    AGGS = 'aggs'


class EsQuery(object):
    SIZE = 'size'
    QUERY = 'query'
    BOOL = 'bool'
    MUST = 'must'
    MUST_NOT = 'must_not'
    SHOULD = 'should'
    MATCH_ALL = 'match_all'
    MATCH = 'match'
    TERMS = 'terms'
    FILTER = 'filter'
    CONSTANT_SCORE = 'constant_score'


TREQ_POOL_DEFAULT_PARAMS = {
    "maxPersistentPerHost": 10,
    "cachedConnectionTimeout": 30,
    "retryAutomatically": False
}

# parts of URL to be omitted
NULL_VALUES = (None, '', b'', [], ())
