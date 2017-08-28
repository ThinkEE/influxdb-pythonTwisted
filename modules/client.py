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

import json

from twisted.internet.defer import inlineCallbacks, returnValue, DeferredList

from utils import quote_ident, make_lines
from resultset import ResultSet

class InfluxDBClient(object):

    def __init__(self, config):
        import treq
        self.treq = treq
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

    @inlineCallbacks
    def request(self, url, data=None, method='GET', expected_response_code=200, headers=None):
        """
            Make a HTTP request to the InfluxDB API.
            :param url: the path of the HTTP request, e.g. write, query, etc.
            :type url: str
            :param method: the HTTP method for the request, defaults to GET
            :type method: str
            :param expected_response_code: the expected response code of
                the request, defaults to 200
            :type expected_response_code: int
            :param headers: headers to add to the request
            :type headers: dict
            :returns: the response from the request
            :rtype: :class:`requests.Response`
        """
        url = "{0}/{1}".format(self._baseurl, url)

        for _try in range(0, self._retries):
            try:
                response = yield self.treq.request( method=method,
                                                    url=url,
                                                    headers=headers,
                                                    data=data,
                                                    proxies=self._proxies,
                                                    verify=self._verify_ssl,
                                                    timeout=self._timeout)
                break
            except Exception as err:
                print(err)
                print("ERROR: Connection error while sending request")
                response = None

        if response:
            if 400 <= response.code < 500:
                print("ERROR: InfluxDB could not understand the request. Error code: {0}".format(response.code))
                err = yield response.json()
                print("ERROR: {0}".format(err))
            elif 500 <= response.code < 600:
                err = yield response.json()
                print("ERROR: {0}".format(err))
                response = None
            elif response.code == expected_response_code:
                returnValue(response)
            else:
                err = yield response.json()
                print("ERROR: {0}".format(err))
                response = None

        returnValue(response)

    @inlineCallbacks
    def query(self, query, parsed=True, params=None, expected_response_code=200):

        query_url = "query?db={0}&u={1}&p={2}&q={3}".format(self._database, self._username, self._password, query.replace(" ", "+"))

        response = yield self.request(url=query_url, expected_response_code=expected_response_code)

        if response:
            data = yield response.json()
        else:
            data = {u'results': []}

        if parsed:
            data = [ ResultSet(result) for result in data.get('results', [])]

        returnValue(data)

    @inlineCallbacks
    def write(self, data, expected_response_code=204):
        url = "write?db={0}&u={1}&p={2}".format(self._database, self._username, self._password)
        data = make_lines(data).encode("utf-8")

        response = yield self.request(url=url, data=data, method="POST", expected_response_code=expected_response_code)
        returnValue(True)

    # @inlineCallbacks
    # def write(self, data, params=None, expected_response_code=204, protocol='json'):
    #     """
    #         Write data to InfluxDB.
    #         :param data: the data to be written
    #         :type data: (if protocol is 'json') dict
    #                     (if protocol is 'line') sequence of line protocol strings
    #         :param params: additional parameters for the request, defaults to None
    #         :type params: dict
    #         :param expected_response_code: the expected response code of the write
    #             operation, defaults to 204
    #         :type expected_response_code: int
    #         :param protocol: protocol of input data, either 'json' or 'line'
    #         :type protocol: str
    #         :returns: True, if the write operation is successful
    #         :rtype: bool
    #     """
    #
    #     headers = self._headers
    #     headers['Content-type'] = 'application/octet-stream'
    #
    #     if params:
    #         precision = params.get('precision')
    #     else:
    #         precision = None
    #
    #     if protocol == 'json':
    #         data = make_lines(data, precision).encode('utf-8')
    #     elif protocol == 'line':
    #         data = ('\n'.join(data) + '\n').encode('utf-8')
    #
    #     yield self.request( url="write",
    #                         method='POST',
    #                         params=params,
    #                         data=data,
    #                         expected_response_code=expected_response_code,
    #                         headers=headers )
    #     returnValue(True)
    #
    # @inlineCallbacks
    # def write_points(self, points,
    #                        time_precision=None,
    #                        database=None,
    #                        retention_policy=None,
    #                        tags=None,
    #                        batch_size=None,
    #                        protocol='json'):
    #     """
    #         Write to multiple time series names.
    #         :param points: the list of points to be written in the database
    #         :type points: list of dictionaries, each dictionary represents a point
    #         :type points: (if protocol is 'json') list of dicts, where each dict
    #                                             represents a point.
    #                     (if protocol is 'line') sequence of line protocol strings.
    #         :param time_precision: Either 's', 'm', 'ms' or 'u', defaults to None
    #         :type time_precision: str
    #         :param database: the database to write the points to. Defaults to
    #             the client's current database
    #         :type database: str
    #         :param tags: a set of key-value pairs associated with each point. Both
    #             keys and values must be strings. These are shared tags and will be
    #             merged with point-specific tags, defaults to None
    #         :type tags: dict
    #         :param retention_policy: the retention policy for the points. Defaults
    #             to None
    #         :type retention_policy: str
    #         :param batch_size: value to write the points in batches
    #             instead of all at one time. Useful for when doing data dumps from
    #             one database to another or when doing a massive write operation,
    #             defaults to None
    #         :type batch_size: int
    #         :param protocol: Protocol for writing data. Either 'line' or 'json'.
    #         :type protocol: str
    #         :returns: True, if the operation is successful
    #         :rtype: bool
    #         .. note:: if no retention policy is specified, the default retention
    #             policy for the database is used
    #     """
    #
    #     if batch_size and batch_size > 0:
    #         dList = []
    #         for i in xrange(0, len(iterable), size):
    #             batch = points[i:i + batch_size]
    #             self._write_points(points=batch,
    #                                time_precision=time_precision,
    #                                database=database,
    #                                retention_policy=retention_policy,
    #                                tags=tags, protocol=protocol)
    #         yield DeferredList(dList)
    #         returnValue(True)
    #     else:
    #         ans = yield self._write_points(points=points,
    #                                        time_precision=time_precision,
    #                                        database=database,
    #                                        retention_policy=retention_policy,
    #                                        tags=tags, protocol=protocol)
    #         returnValue(ans)
    #
    # @inlineCallbacks
    # def _write_points(self, points,
    #                         time_precision,
    #                         database,
    #                         retention_policy,
    #                         tags,
    #                         protocol='json'):
    #
    #     if time_precision not in ['n', 'u', 'ms', 's', 'm', 'h', None]:
    #         raise ValueError ("Invalid time precision is given. "
    #                          "(use 'n', 'u', 'ms', 's', 'm' or 'h')")
    #
    #     if self.use_udp and time_precision and time_precision != 's':
    #         raise ValueError("InfluxDB only supports seconds precision for udp writes")
    #
    #     if protocol == 'json':
    #         data = {
    #             'points': points
    #         }
    #
    #         if tags is not None:
    #             data['tags'] = tags
    #     else:
    #         data = points
    #
    #     params = {
    #         'db': database or self._database
    #     }
    #
    #     if time_precision is not None:
    #         params['precision'] = time_precision
    #
    #     if retention_policy is not None:
    #         params['rp'] = retention_policy
    #
    #     if self.use_udp:
    #         yield self.send_packet(data, protocol=protocol)
    #     else:
    #         yield self.write(data=data,
    #                          params=params,
    #                          expected_response_code=204,
    #                          protocol=protocol)
    #
    #     returnValue(True)
    #
    # @inlineCallbacks
    # def create_database(self, dbname):
    #     """Create a new database in InfluxDB.
    #     :param dbname: the name of the database to create
    #     :type dbname: str
    #     """
    #     yield self.rawQuery("CREATE DATABASE {0}".format(quote_ident(dbname)))
    #
    # @inlineCallbacks
    # def drop_database(self, dbname):
    #     """Drop a database from InfluxDB.
    #     :param dbname: the name of the database to drop
    #     :type dbname: str
    #     """
    #     yield self.rawQuery("DROP DATABASE {0}".format(quote_ident(dbname)))

    # @inlineCallbacks
    # def create_retention_policy(self, name, duration, replication,
    #                             database=None, default=False):
    #     """
    #         Create a retention policy for a database.
    #         :param name: the name of the new retention policy
    #         :type name: str
    #         :param duration: the duration of the new retention policy.
    #             Durations such as 1h, 90m, 12h, 7d, and 4w, are all supported
    #             and mean 1 hour, 90 minutes, 12 hours, 7 day, and 4 weeks,
    #             respectively. For infinite retention - meaning the data will
    #             never be deleted - use 'INF' for duration.
    #             The minimum retention period is 1 hour.
    #         :type duration: str
    #         :param replication: the replication of the retention policy
    #         :type replication: str
    #         :param database: the database for which the retention policy is
    #             created. Defaults to current client's database
    #         :type database: str
    #         :param default: whether or not to set the policy as default
    #         :type default: bool
    #     """
    #     query_string = \
    #         "CREATE RETENTION POLICY {0} ON {1} " \
    #         "DURATION {2} REPLICATION {3}".format(
    #             quote_ident(name), quote_ident(database or self._database),
    #             duration, replication)
    #
    #     if default is True:
    #         query_string += " DEFAULT"
    #
    #     yield self.rawQuery(query_string)
