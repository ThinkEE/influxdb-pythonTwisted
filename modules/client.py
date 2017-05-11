################################################################################
# MIT License
#
# Copyright (c) 2013 InfluxDB
# Copyright (c) 2017 Jean-Charles Fosse & Johann Bigler
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
################################################################################

import json, treq

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList

from utils import quote_ident, make_lines
from resultset import ResultSet

class InfluxDBClient(object):

    def __init__(self, reactor, config):
        self._host = config["host"] if "host" in config else "localhost"
        self._port = int(config["port"]) if "port" in config else 8086
        self._username = config["username"] if "username" in config else "root"
        self._password = config["password"] if "password" in config else "root"
        self._database = config["database"] if "database" in config else None
        self._timeout = config["timeout"] if "timeout" in config else None
        self._retries = config["retries"] if "retries" in config else 3

        if self._retries < 1:
            self._retries = 1

        self._verify_ssl = config["verify_ssl"] if "verify_ssl" in config else False

        # XXX UDP Not valid Yet. To be implemented
        self.use_udp = config["use_udp"] if "use_udp" in config else False
        self.udp_port = int(config["udp_port"]) if "udp_port" in config else 4444

        # if use_udp:
        #     self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        ssl = config["ssl"] if "ssl" in config else False
        self._scheme = "http"
        if ssl is True:
            self._scheme = "https"

        proxies = config["proxies"] if "proxies" in config else None
        self._proxies = {}
        if proxies:
            self._proxies = proxies

        self._baseurl = "{0}://{1}:{2}".format(self._scheme,
                                                self._host,
                                                self._port)

        self._headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain'
        }

    @inlineCallbacks
    def request(self, url, method='GET', params=None, data=None,
                expected_response_code=200, headers=None):
        """
            Make a HTTP request to the InfluxDB API.
            :param url: the path of the HTTP request, e.g. write, query, etc.
            :type url: str
            :param method: the HTTP method for the request, defaults to GET
            :type method: str
            :param params: additional parameters for the request, defaults to None
            :type params: dict
            :param data: the data of the request, defaults to None
            :type data: str
            :param expected_response_code: the expected response code of
                the request, defaults to 200
            :type expected_response_code: int
            :param headers: headers to add to the request
            :type headers: dict
            :returns: the response from the request
            :rtype: :class:`requests.Response`
            :raises InfluxDBServerError: if the response code is any server error
                code (5xx)
            :raises InfluxDBClientError: if the response code is not the
                same as `expected_response_code` and is not a server error code
        """
        url = "{0}/{1}".format(self._baseurl, url)

        if headers is None:
            headers = self._headers

        if params is None:
            params = {}

        if isinstance(data, (dict, list)):
            data = json.dumps(data)

        for _try in range(0, self._retries):
            try:
                response = yield treq.request( method=method,
                                                url=url,
                                                auth=(self._username, self._password),
                                                params=params,
                                                data=data,
                                                headers=headers,
                                                proxies=self._proxies,
                                                verify=self._verify_ssl,
                                                timeout=self._timeout)
                break
            except:
                print("ERROR: Connection error while sending request")
                response = None

        if response:
            if 500 <= response.code < 600:
                err = yield response.json()
                print("ERROR: %s"%(err))
                response = None
            elif response.code == expected_response_code:
                returnValue(response)
            else:
                err = yield response.json()
                print("ERROR: %s"%(err))
                response = None

        returnValue(response)

    @inlineCallbacks
    def write(self, data, params=None, expected_response_code=204, protocol='json'):
        """
            Write data to InfluxDB.
            :param data: the data to be written
            :type data: (if protocol is 'json') dict
                        (if protocol is 'line') sequence of line protocol strings
            :param params: additional parameters for the request, defaults to None
            :type params: dict
            :param expected_response_code: the expected response code of the write
                operation, defaults to 204
            :type expected_response_code: int
            :param protocol: protocol of input data, either 'json' or 'line'
            :type protocol: str
            :returns: True, if the write operation is successful
            :rtype: bool
        """

        headers = self._headers
        headers['Content-type'] = 'application/octet-stream'

        if params:
            precision = params.get('precision')
        else:
            precision = None

        if protocol == 'json':
            data = make_lines(data, precision).encode('utf-8')
        elif protocol == 'line':
            data = ('\n'.join(data) + '\n').encode('utf-8')

        yield self.request( url="write",
                            method='POST',
                            params=params,
                            data=data,
                            expected_response_code=expected_response_code,
                            headers=headers )
        returnValue(True)

    @inlineCallbacks
    def write_points(self, points,
                           time_precision=None,
                           database=None,
                           retention_policy=None,
                           tags=None,
                           batch_size=None,
                           protocol='json'):
        """
            Write to multiple time series names.
            :param points: the list of points to be written in the database
            :type points: list of dictionaries, each dictionary represents a point
            :type points: (if protocol is 'json') list of dicts, where each dict
                                                represents a point.
                        (if protocol is 'line') sequence of line protocol strings.
            :param time_precision: Either 's', 'm', 'ms' or 'u', defaults to None
            :type time_precision: str
            :param database: the database to write the points to. Defaults to
                the client's current database
            :type database: str
            :param tags: a set of key-value pairs associated with each point. Both
                keys and values must be strings. These are shared tags and will be
                merged with point-specific tags, defaults to None
            :type tags: dict
            :param retention_policy: the retention policy for the points. Defaults
                to None
            :type retention_policy: str
            :param batch_size: value to write the points in batches
                instead of all at one time. Useful for when doing data dumps from
                one database to another or when doing a massive write operation,
                defaults to None
            :type batch_size: int
            :param protocol: Protocol for writing data. Either 'line' or 'json'.
            :type protocol: str
            :returns: True, if the operation is successful
            :rtype: bool
            .. note:: if no retention policy is specified, the default retention
                policy for the database is used
        """

        if batch_size and batch_size > 0:
            dList = []
            for i in xrange(0, len(iterable), size):
                batch = points[i:i + batch_size]
                self._write_points(points=batch,
                                   time_precision=time_precision,
                                   database=database,
                                   retention_policy=retention_policy,
                                   tags=tags, protocol=protocol)
            yield DeferredList(dList)
            returnValue(True)
        else:
            ans = yield self._write_points(points=points,
                                           time_precision=time_precision,
                                           database=database,
                                           retention_policy=retention_policy,
                                           tags=tags, protocol=protocol)
            returnValue(ans)

    @inlineCallbacks
    def _write_points(self, points,
                            time_precision,
                            database,
                            retention_policy,
                            tags,
                            protocol='json'):

        if time_precision not in ['n', 'u', 'ms', 's', 'm', 'h', None]:
            raise ValueError ("Invalid time precision is given. "
                             "(use 'n', 'u', 'ms', 's', 'm' or 'h')")

        if self.use_udp and time_precision and time_precision != 's':
            raise ValueError("InfluxDB only supports seconds precision for udp writes")

        if protocol == 'json':
            data = {
                'points': points
            }

            if tags is not None:
                data['tags'] = tags
        else:
            data = points

        params = {
            'db': database or self._database
        }

        if time_precision is not None:
            params['precision'] = time_precision

        if retention_policy is not None:
            params['rp'] = retention_policy

        if self.use_udp:
            yield self.send_packet(data, protocol=protocol)
        else:
            yield self.write(data=data,
                             params=params,
                             expected_response_code=204,
                             protocol=protocol)

        returnValue(True)

    @inlineCallbacks
    def create_database(self, dbname):
        """Create a new database in InfluxDB.
        :param dbname: the name of the database to create
        :type dbname: str
        """
        yield self.query("CREATE DATABASE {0}".format(quote_ident(dbname)))

    @inlineCallbacks
    def drop_database(self, dbname):
        """Drop a database from InfluxDB.
        :param dbname: the name of the database to drop
        :type dbname: str
        """
        yield self.query("DROP DATABASE {0}".format(quote_ident(dbname)))

    @inlineCallbacks
    def create_retention_policy(self, name, duration, replication,
                                database=None, default=False):
        """
            Create a retention policy for a database.
            :param name: the name of the new retention policy
            :type name: str
            :param duration: the duration of the new retention policy.
                Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
                and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
                respectively. For infinite retention - meaning the data will
                never be deleted - use 'INF' for duration.
                The minimum retention period is 1 hour.
            :type duration: str
            :param replication: the replication of the retention policy
            :type replication: str
            :param database: the database for which the retention policy is
                created. Defaults to current client's database
            :type database: str
            :param default: whether or not to set the policy as default
            :type default: bool
        """
        query_string = \
            "CREATE RETENTION POLICY {0} ON {1} " \
            "DURATION {2} REPLICATION {3}".format(
                quote_ident(name), quote_ident(database or self._database),
                duration, replication)

        if default is True:
            query_string += " DEFAULT"

        yield self.query(query_string)

    @inlineCallbacks
    def rawQuery(self, query,
                params=None,
                epoch=None,
                expected_response_code=200,
                database=None,
                raise_errors=True,
                chunked=False,
                chunk_size=0):
        """
            Send a query to InfluxDB.
            :param query: the actual query string
            :type query: str
            :param params: additional parameters for the request, defaults to {}
            :type params: dict
            :param epoch: response timestamps to be in epoch format either 'h',
                'm', 's', 'ms', 'u', or 'ns',defaults to `None` which is
                RFC3339 UTC format with nanosecond precision
            :type epoch: str
            :param expected_response_code: the expected status code of response,
                defaults to 200
            :type expected_response_code: int
            :param database: database to query, defaults to None
            :type database: str
            :param raise_errors: Whether or not to raise exceptions when InfluxDB
                returns errors, defaults to True
            :type raise_errors: bool
            :param chunked: Enable to use chunked responses from InfluxDB.
                With ``chunked`` enabled, one ResultSet is returned per chunk
                containing all results within that chunk
            :type chunked: bool
            :param chunk_size: Size of each chunk to tell InfluxDB to use.
            :type chunk_size: int
            :returns: the queried data
            :rtype: :class:`~.ResultSet`
        """
        if params is None:
            params = {}

        params['q'] = query
        params['db'] = database or self._database

        if epoch is not None:
            params['epoch'] = epoch

        # if chunked:
        #     params['chunked'] = 'true'
        #     if chunk_size > 0:
        #         params['chunk_size'] = chunk_size

        response = yield self.request(url="query",
                                    method='POST',
                                    params=params,
                                    data=None,
                                    expected_response_code=expected_response_code)

        # XXX Chuncked not implemented
        # if chunked:
        #     return self._read_chunked_response(response)

        if response:
            data = yield response.json()
        else:
            data = {u'results': []}

        returnValue(data)

    @inlineCallbacks
    def parsedQuery(self, query,
                    params=None,
                    epoch=None,
                    expected_response_code=200,
                    database=None,
                    raise_errors=True,
                    chunked=False,
                    chunk_size=0):

        data = yield self.rawQuery(query, params, epoch,
                                   expected_response_code, database,
                                   raise_errors, chunked, chunk_size)

        results = [ ResultSet(result, raise_errors=raise_errors)
                    for result in data.get('results', [])]

        returnValue(results)
