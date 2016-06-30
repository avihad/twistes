from twistes.compatability import quote, urlencode


from twistes.consts import NULL_VALUES


class EsParser(object):

    @staticmethod
    def parse_host(hosts):
        """
        Parsing the hosts parameter,
        * currently support only one host
        :param hosts: the hosts json to parse
        :return: the full host and the authentication if exists
        """
        host = hosts[0]

        host_name = host['host']
        host_port = host['port']

        auth = None
        if 'http_auth' in host:
            http_auth = host['http_auth']
            user_pass = http_auth.split(':')
            auth = (user_pass[0], user_pass[1])

        full_host = "{host}:{port}".format(host=host_name, port=host_port)
        return full_host, auth

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
