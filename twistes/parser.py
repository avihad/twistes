from twistes.compatability import quote, urlencode, string_types, urlparse

from twistes.consts import NULL_VALUES, HostParsing


class EsParser(object):
    SSL_DEFAULT_PORT = 443

    @staticmethod
    def parse_host(hosts):
        """
        Parsing the hosts parameter,
        * currently support only one host
        :param hosts: the hosts json to parse
        :return: the full host and the authentication if exists
        """
        hosts = EsParser._normalize_hosts(hosts)
        host = hosts[0]

        host_name = host[HostParsing.HOST]
        host_port = host[HostParsing.PORT]

        auth = None
        if HostParsing.HTTP_AUTH in host:
            http_auth = host[HostParsing.HTTP_AUTH]
            user_pass = http_auth.split(':')
            auth = (user_pass[0], user_pass[1])

        full_host = "{host}:{port}".format(host=host_name, port=host_port)
        if not host_name.startswith((HostParsing.HTTP + ':', HostParsing.HTTPS + ':')):
            scheme = HostParsing.HTTPS if host.get(HostParsing.USE_SSL) else HostParsing.HTTP
            full_host = "{scheme}://{full_host}".format(full_host=full_host, scheme=scheme)

        return full_host, auth

    @staticmethod
    def _normalize_hosts(hosts):
        if hosts is None:
            hosts = [{}]

        if isinstance(hosts, string_types):
            hosts = [hosts]

        return list(map(EsParser._normalize_host, hosts))

    @staticmethod
    def _normalize_host(host):
        if isinstance(host, string_types):
            return EsParser._parse_string_host(host)
        else:
            return EsParser._update_ssl_params(host)

    @staticmethod
    def _update_ssl_params(host):
        """
        Update the host ssl params (port or scheme) if needed.
        :param host:
        :return:
        """
        if host[HostParsing.HOST] \
                and EsParser._is_secure_connection_type(host):
            host[HostParsing.PORT] = EsParser.SSL_DEFAULT_PORT
            host[HostParsing.USE_SSL] = True
            parsed_url = urlparse(EsParser._fix_host_prefix(host[HostParsing.HOST]))
            host[HostParsing.HOST] = parsed_url.hostname
            host[HostParsing.SCHEME] = HostParsing.HTTPS
        return host

    @staticmethod
    def _parse_string_host(host_str):
        """
        Parse host string into a dictionary host
        :param host_str:
        :return:
        """
        host_str = EsParser._fix_host_prefix(host_str)
        parsed_url = urlparse(host_str)
        host = {HostParsing.HOST: parsed_url.hostname}
        if parsed_url.port:
            host[HostParsing.PORT] = parsed_url.port
        if parsed_url.scheme == HostParsing.HTTPS:
            host[HostParsing.PORT] = parsed_url.port or EsParser.SSL_DEFAULT_PORT
            host[HostParsing.USE_SSL] = True
            host[HostParsing.SCHEME] = HostParsing.HTTPS
        elif parsed_url.scheme:
            host[HostParsing.SCHEME] = parsed_url.scheme
        if parsed_url.username or parsed_url.password:
            host[HostParsing.HTTP_AUTH] = '%s:%s' % (parsed_url.username, parsed_url.password)
        if parsed_url.path and parsed_url.path != '/':
            host[HostParsing.URL_PREFIX] = parsed_url.path
        return host

    @staticmethod
    def _fix_host_prefix(host):
        if '://' not in host:
            host = "//%s" % host
        return host

    @staticmethod
    def _is_secure_connection_type(host):
        return (HostParsing.HTTPS in host[HostParsing.HOST] and not host.get(HostParsing.PORT)) \
                or (HostParsing.HTTPS not in host[HostParsing.HOST] and host.get(HostParsing.PORT, 0) == 443)

    @staticmethod
    def make_path(*sub_paths):
        """
        Create a path from a list of sub paths.
        :param sub_paths: a list of sub paths
        :return:
        """
        queued_params = [quote(c.encode('utf-8'), '') for c in sub_paths if c not in NULL_VALUES]
        queued_params.insert(0, '')
        return '/'.join(queued_params)

    @staticmethod
    def prepare_url(hostname, path, params=None):
        """
        Prepare Elasticsearch request url.
        :param hostname: host name
        :param path: request path
        :param params: optional url params
        :return:
        """
        url = hostname + path

        if params:
            url = url + '?' + urlencode(params)

        if not url.startswith(('http:', 'https:')):
            url = "http://" + url

        return url.encode('utf-8')

    @staticmethod
    def is_not_empty_params(*kwargs):
        for param in kwargs:
            if param in NULL_VALUES:
                raise ValueError("Empty value passed for a required argument {empty_param}.".format(empty_param=param))
