from unittest import TestCase

from twistes.consts import HostParsing
from twistes.parser import EsParser

HTTPS_SCHEME = 'https'

SOME_URL = 'someurl.com'

SOME_PASS = 'pass'

SOME_USER = 'user'


class TestEsParser(TestCase):
    def test_normalize_hosts_by_string(self):
        port = '123'
        hosts = EsParser._normalize_hosts(
            '{scheme}://{user}:{passwd}@{url}:{port}'.format(scheme=HTTPS_SCHEME, user=SOME_USER, passwd=(SOME_PASS),
                                                             url=SOME_URL,
                                                             port=port))

        first_host = hosts[0]
        self.assertEqual(HTTPS_SCHEME, first_host[HostParsing.SCHEME])
        self.assertEqual(SOME_URL, first_host[HostParsing.HOST])
        self.assertEqual(int(port), first_host[HostParsing.PORT])
        self.assertEqual('{user}:{passwd}'.format(user=SOME_USER, passwd=SOME_PASS),
                         first_host[HostParsing.HTTP_AUTH])

    def test_normalize_hosts_by_string_https_without_port(self):
        hosts = EsParser._normalize_hosts(
            '{scheme}://{user}:{passwd}@{url}'.format(scheme=HTTPS_SCHEME, user=SOME_USER, passwd=SOME_PASS,
                                                      url=SOME_URL))

        first_host = hosts[0]
        self.assertEqual(HTTPS_SCHEME, first_host[HostParsing.SCHEME])
        self.assertEqual(SOME_URL, first_host[HostParsing.HOST])
        self.assertEqual(443, first_host[HostParsing.PORT])
        self.assertEqual('{user}:{passwd}'.format(user=SOME_USER, passwd=SOME_PASS),
                         first_host[HostParsing.HTTP_AUTH])

    def test_parse_host_https_without_port(self):
        full_host, auth = EsParser.parse_host(
            [{HostParsing.HOST: '{scheme}://{url}'.format(scheme=HTTPS_SCHEME,
                                                                          user=SOME_USER,
                                                                          passwd=SOME_PASS,
                                                                          url=SOME_URL)}])

        self.assertEqual('443', full_host[-3:])
        self.assertEqual(HostParsing.HTTPS, full_host[0:5])
        self.assertEqual(SOME_URL, full_host[8:-4])
