import sys

PY2 = sys.version_info.major == 2

if PY2:
    string_types = basestring,
    from urllib import quote, urlencode
    from urlparse import  urlparse
    from itertools import imap as map
else:
    string_types = str, bytes
    from urllib.parse import quote, urlencode, urlparse
    map = map