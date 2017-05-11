################################################################################
# MIT License
#
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

import sys, os, json

from datetime import datetime
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue

from modules.client import InfluxDBClient

#http://patorjk.com/software/taag/#p=display&h=1&f=Stick%20Letters&t=INFLUXDB%20Twisted%20Example
BANNER = r"""
      ___           __  __    ___     ________ __     ___            __      ___
||\ ||__|   |  |\_/|  \|__)    ||  ||/__`||__ |  \   |__ \_/ /\ |\/||__)|   |__
|| \||  |___\__// \|__/|__)    ||/\||.__/||___|__/   |___/ \/~~\|  ||   |___|___

"""

@inlineCallbacks
def run():

    # Config to used by client.
    # Minimal required config parameters
    config = {
      "host": "127.0.0.1",
      "port": "8086",
      "username": "lyla",
      "password": "lylabox",
      "database": "statementStorageDB"
     }

    # Additional configuration parameters
    #  config["timeout"] = ""
    #  config["retries"] = ""
    #  config["verify_ssl"] = ""
    #  config["use_udp"] = ""
    #  config["udp_port"] = ""
    #  config["ssl"] = ""
    #  config["proxies"] = ""

    # Creates a Influxdb client
    client = InfluxDBClient(reactor, config)
    print("INFO: Client Created")

    statement = [
        {
            "measurement": "test1",
            "tags": {
                "tag_demo": "demo1",
            },
            "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
            "fields": {
                "value": 10
            }
        }
    ]

    print("INFO: Writing points")
    # Write points to influxdb
    try:
        yield client.write_points(statement)
    except Exception as err:
        print("ERROR")
        print(err)

    print("INFO: Starting query")
    # Query points from measurement test1
    try:
        results = yield client.parsedQuery('SELECT * FROM test1;')
    except Exception as err:
        print("ERROR")
        print(err)
    else:
        print("Results")
        print(results)

    reactor.stop()
# ------------------------------------------------------------------------------
if __name__ == '__main__':

    print("")
    print("-------------------------------------------------------------------")
    print(BANNER)

    run()

    # Start Twisted reactor
    reactor.run()

    print("")
    print("-------------------------------------------------------------------")
    print("DEBUG: Shuting down Twisted Influx Db Client")
