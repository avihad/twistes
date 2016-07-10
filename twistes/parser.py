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

        out = []
        # normalize hosts to dicts
        for host in hosts:
            if isinstance(host, string_types):
                if '://' not in host:
                    host = "//%s" % host

                parsed_url = urlparse(host)
                h = {HostParsing.HOST: parsed_url.hostname}

                if parsed_url.port:
                    h[HostParsing.PORT] = parsed_url.port

                if parsed_url.scheme == HostParsing.HTTPS:
                    h[HostParsing.PORT] = parsed_url.port or EsParser.SSL_DEFAULT_PORT
                    h[HostParsing.USE_SSL] = True
                    h[HostParsing.SCHEME] = HostParsing.HTTPS
                elif parsed_url.scheme:
                    h[HostParsing.SCHEME] = parsed_url.scheme

                if parsed_url.username or parsed_url.password:
                    h[HostParsing.HTTP_AUTH] = '%s:%s' % (parsed_url.username, parsed_url.password)

                if parsed_url.path and parsed_url.path != '/':
                    h[HostParsing.URL_PREFIX] = parsed_url.path

                out.append(h)
            else:
                if host[HostParsing.HOST] \
                        and EsParser._is_secure_connection_type(host):
                    host[HostParsing.PORT] = EsParser.SSL_DEFAULT_PORT
                    host[HostParsing.USE_SSL] = True
                    parsed_url = urlparse(host[HostParsing.HOST])
                    host[HostParsing.HOST] = parsed_url.hostname
                    host[HostParsing.SCHEME] = HostParsing.HTTPS

                out.append(host)
        return out

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
        queued_params = [quote(str(c), '') for c in sub_paths if c not in NULL_VALUES]
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
