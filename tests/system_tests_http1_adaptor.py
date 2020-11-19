#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#
# Test the HTTP/1.x Adaptor
#

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function


import socket
import sys
from threading import Thread
from time import sleep
try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from http.client import HTTPConnection
    from http.client import HTTPException
except ImportError:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
    from httplib import HTTPConnection, HTTPException

from proton.handlers import MessagingHandler
from proton.reactor import Container
from system_test import TestCase, unittest, main_module, Qdrouterd, QdManager
from system_test import TIMEOUT, Logger


class RequestMsg(object):
    """
    A 'hardcoded' HTTP request message.  This class writes its request
    message to the HTTPConnection.
    """
    def __init__(self, method, target, headers=None, body=None):
        self.method = method
        self.target = target
        self.headers = headers or {}
        self.body = body

    def send_request(self, conn):
        conn.putrequest(self.method, self.target)
        for key, value in self.headers.items():
            conn.putheader(key, value)
        conn.endheaders()
        if self.body:
            conn.send(self.body)


class ResponseMsg(object):
    """
    A 'hardcoded' HTTP response message.  This class writes its response
    message when called by the HTTPServer via the BaseHTTPRequestHandler
    """
    def __init__(self, status, version=None, reason=None,
                 headers=None, body=None, error=False):
        self.status = status
        self.version = version or "HTTP/1.1"
        self.reason = reason
        self.headers = headers or []
        self.body = body
        self.error = error

    def send_response(self, handler):
        if self.error:
            handler.send_error(self.status,
                               message=self.reason)
            return

        handler.send_response(self.status, self.reason)
        for key, value in self.headers.items():
            handler.send_header(key, value)
        handler.end_headers()

        if self.body:
            handler.wfile.write(self.body)
            handler.wfile.flush()


class ResponseValidator(object):
    """
    Validate a response as received by the HTTP client
    """
    def __init__(self, status=200, expect_headers=None, expect_body=None):
        if expect_headers is None:
            expect_headers = {}
        self.status = status
        self.expect_headers = expect_headers
        self.expect_body = expect_body

    def check_response(self, rsp):
        if self.status and rsp.status != self.status:
            raise Exception("Bad response code, expected %s got %s"
                            % (self.status, rsp.status))
        for key, value in self.expect_headers.items():
            if rsp.getheader(key) != value:
                raise Exception("Missing/bad header (%s), expected %s got %s"
                                % (key, value, rsp.getheader(key)))

        body = rsp.read()
        if (self.expect_body and self.expect_body != body):
            raise Exception("Bad response body expected %s got %s"
                            % (self.expect_body, body))
        return body


class RequestHandler(BaseHTTPRequestHandler):
    """
    Dispatches requests received by the HTTPServer based on the method
    """
    protocol_version = 'HTTP/1.1'

    def _execute_request(self, tests):
        for req, resp, val in tests:
            if req.target == self.path:
                self._consume_body()
                if not isinstance(resp, list):
                    resp = [resp]
                for r in resp:
                    r.send_response(self)
                return
        self.send_error(404, "Not Found")

    def do_GET(self):
        self._execute_request(self.server.system_tests["GET"])

    def do_HEAD(self):
        self._execute_request(self.server.system_tests["HEAD"])

    def do_POST(self):
        if self.path == "/SHUTDOWN":
            self.send_response(200, "OK")
            self.send_header("Content-Length", "13")
            self.end_headers()
            self.wfile.write(b'Server Closed')
            self.wfile.flush()
            self.close_connection = True
            self.server.server_killed = True
            return
        self._execute_request(self.server.system_tests["POST"])

    def do_PUT(self):
        self._execute_request(self.server.system_tests["PUT"])

    # these overrides just quiet the test output
    # comment them out to help debug:
    def log_request(self, code=None, size=None):
        pass

    def log_message(self, format=None, *args):
        pass

    def _consume_body(self):
        """
        Read the entire body off the rfile.  This must be done to allow
        multiple requests on the same socket
        """
        if self.command == 'HEAD':
            return b''

        for key, value in self.headers.items():
            if key.lower() == 'content-length':
                return self.rfile.read(int(value))

            if key.lower() == 'transfer-encoding'  \
               and 'chunked' in value.lower():
                body = b''
                while True:
                    header = self.rfile.readline().strip().split(b';')[0]
                    hlen = int(header, base=16)
                    if hlen > 0:
                        data = self.rfile.read(hlen + 2)  # 2 = \r\n
                        body += data[:-2]
                    else:
                        self.rfile.readline()  # discard last \r\n
                        break;
                return body
        return self.rfile.read()


class RequestHandler10(RequestHandler):
    """
    RequestHandler that forces the server to use HTTP version 1.0 semantics
    """
    protocol_version = 'HTTP/1.0'


class MyHTTPServer(HTTPServer):
    """
    Adds a switch to the HTTPServer to allow it to exit gracefully
    """
    def __init__(self, addr, handler_cls, testcases):
        self.system_tests = testcases
        HTTPServer.__init__(self, addr, handler_cls)


class TestServer(object):
    """
    A HTTPServer running in a separate thread
    """
    def __init__(self, server_port, client_port, tests, handler_cls=None):
        self._logger = Logger(title="TestServer", print_to_console=False)
        self._client_port = client_port
        self._server_addr = ("", server_port)
        self._server = MyHTTPServer(self._server_addr,
                                    handler_cls or RequestHandler,
                                    tests)
        self._server.allow_reuse_address = True
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        self._logger.log("TestServer listening on %s:%s" % self._server_addr)
        try:
            self._server.server_killed = False
            while not self._server.server_killed:
                self._server.handle_request()
        except Exception as exc:
            self._logger.log("TestServer %s crash: %s" %
                             (self._server_addr, exc))
            raise
        self._logger.log("TestServer %s:%s closed" % self._server_addr)

    def wait(self, timeout=TIMEOUT):
        self._logger.log("TestServer %s:%s shutting down" % self._server_addr)
        if self._thread.is_alive():
            client = HTTPConnection("127.0.0.1:%s" % self._client_port,
                                    timeout=TIMEOUT)
            client.putrequest("POST", "/SHUTDOWN")
            client.putheader("Content-Length", "0")
            client.endheaders()
            # 13 == len('Server Closed')
            client.getresponse().read(13)
            client.close()
            self._thread.join(timeout=TIMEOUT)
        if self._server:
            self._server.server_close()


class ThreadedTestClient(object):
    """
    An HTTP client running in a separate thread
    """
    def __init__(self, tests, port, repeat=1):
        self._conn_addr = ("127.0.0.1:%s" % port)
        self._tests = tests
        self._repeat = repeat
        self._logger = Logger(title="TestClient", print_to_console=False)
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self.error = None
        self.count = 0
        self._thread.start()

    def _run(self):
        self._logger.log("TestClient connecting on %s" % self._conn_addr)
        client = HTTPConnection(self._conn_addr, timeout=TIMEOUT)
        for loop in range(self._repeat):
            for op, tests in self._tests.items():
                for req, _, val in tests:
                    self._logger.log("TestClient sending %s %s request" % (op, req.target))
                    req.send_request(client)
                    self._logger.log("TestClient getting %s response" % op)
                    rsp = client.getresponse()
                    self._logger.log("TestClient response %s received" % op)
                    if val:
                        try:
                            body = val.check_response(rsp)
                        except Exception as exc:
                            self._logger.log("TestClient response invalid: %s"
                                             % str(exc))
                            self.error = "client failed: %s" % str(exc)
                            return

                        if req.method == "BODY" and body != b'':
                            self._logger.log("TestClient response invalid: %s"
                                             % "body present!")
                            self.error = "error: body present!"
                            return
                    self.count += 1
        client.close()
        self._logger.log("TestClient to %s closed" % self._conn_addr)

    def wait(self, timeout=TIMEOUT):
        self._thread.join(timeout=TIMEOUT)
        self._logger.log("TestClient %s shut down" % self._conn_addr)


def http1_ping(sport, cport):
    """
    Test the HTTP path by doing a simple GET request
    """
    TEST = {
        "GET": [
            (RequestMsg("GET", "/GET/ping",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 4,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'pong'),
             ResponseValidator(expect_body=b'pong'))
        ]
    }

    server = TestServer(server_port=sport,
                        client_port=cport,
                        tests=TEST)
    client = ThreadedTestClient(tests=TEST, port=cport)
    client.wait()
    server.wait()
    return (client.count, client.error)


class Http1AdaptorManagementTest(TestCase):
    """
    Test Creation and deletion of HTTP1 management entities
    """
    @classmethod
    def setUpClass(cls):
        super(Http1AdaptorManagementTest, cls).setUpClass()

        cls.http_server_port = cls.tester.get_port()
        cls.http_listener_port = cls.tester.get_port()

        config = [
            ('router', {'mode': 'standalone',
                            'id': 'HTTP1MgmtTest',
                        'allowUnsettledMulticast': 'yes'}),
            ('listener', {'role': 'normal',
                          'port': cls.tester.get_port()}),
            ('address', {'prefix': 'closest',   'distribution': 'closest'}),
            ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ]

        config = Qdrouterd.Config(config)
        cls.router = cls.tester.qdrouterd('HTTP1MgmtTest', config, wait=True)

    def test_01_mgmt(self):
        """
        Create and delete HTTP1 connectors and listeners
        """
        LISTENER_TYPE = 'org.apache.qpid.dispatch.httpListener'
        CONNECTOR_TYPE = 'org.apache.qpid.dispatch.httpConnector'
        CONNECTION_TYPE = 'org.apache.qpid.dispatch.connection'

        mgmt = self.router.management
        self.assertEqual(0, len(mgmt.query(type=LISTENER_TYPE).results))
        self.assertEqual(0, len(mgmt.query(type=CONNECTOR_TYPE).results))

        mgmt.create(type=CONNECTOR_TYPE,
                    name="ServerConnector",
                    attributes={'address': 'http1',
                                'port': self.http_server_port,
                                'protocolVersion': 'HTTP1'})

        mgmt.create(type=LISTENER_TYPE,
                    name="ClientListener",
                    attributes={'address': 'http1',
                                'port': self.http_listener_port,
                                'protocolVersion': 'HTTP1'})

        # verify the entities have been created and http traffic works

        self.assertEqual(1, len(mgmt.query(type=LISTENER_TYPE).results))
        self.assertEqual(1, len(mgmt.query(type=CONNECTOR_TYPE).results))

        count, error = http1_ping(sport=self.http_server_port,
                                  cport=self.http_listener_port)
        self.assertIsNone(error)
        self.assertEqual(1, count)

        #
        # delete the connector and wait for the associated connection to be
        # removed
        #

        mgmt.delete(type=CONNECTOR_TYPE, name="ServerConnector")
        self.assertEqual(0, len(mgmt.query(type=CONNECTOR_TYPE).results))

        hconns = 0
        retry = 20  # 20 * 0.25 = 5 sec
        while retry:
            obj = mgmt.query(type=CONNECTION_TYPE,
                             attribute_names=["protocol"])
            for item in obj.get_dicts():
                if "http/1.x" in item["protocol"]:
                    hconns += 1
            if hconns == 0:
                break;
            sleep(0.25)
            retry -= 1

        self.assertEqual(0, hconns, msg="HTTP connection not deleted")

        # When a connector is configured the router will periodically attempt
        # to connect to the server address. To prove that the connector has
        # been completely removed listen for connection attempts on the server
        # port.
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("", self.http_server_port))
        s.setblocking(1)
        s.settimeout(3)  # reconnect attempts every 2.5 seconds
        s.listen(1)
        with self.assertRaises(socket.timeout):
            conn, addr = s.accept()
        s.close()

        #
        # re-create the connector and verify it works
        #
        mgmt.create(type=CONNECTOR_TYPE,
                    name="ServerConnector",
                    attributes={'address': 'http1',
                                'port': self.http_server_port,
                                'protocolVersion': 'HTTP1'})

        self.assertEqual(1, len(mgmt.query(type=CONNECTOR_TYPE).results))

        count, error = http1_ping(sport=self.http_server_port,
                                  cport=self.http_listener_port)
        self.assertIsNone(error)
        self.assertEqual(1, count)


class Http1AdaptorOneRouterTest(TestCase):
    """
    Test HTTP servers and clients attached to a standalone router
    """

    # HTTP/1.1 compliant test cases
    TESTS_11 = {
        #
        # GET
        #
        "GET": [
            (RequestMsg("GET", "/GET/error",
                        headers={"Content-Length": 0}),
             ResponseMsg(400, reason="Bad breath", error=True),
             ResponseValidator(status=400)),

            (RequestMsg("GET", "/GET/content_len",
                        headers={"Content-Length": "00"}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 1,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'?'),
             ResponseValidator(expect_headers={'Content-Length': '1'},
                               expect_body=b'?')),

            (RequestMsg("GET", "/GET/content_len_511",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 511,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 511),
             ResponseValidator(expect_headers={'Content-Length': '511'},
                               expect_body=b'X' * 511)),

            (RequestMsg("GET", "/GET/content_len_4096",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 4096,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 4096),
             ResponseValidator(expect_headers={'Content-Length': '4096'},
                               expect_body=b'X' * 4096)),

            (RequestMsg("GET", "/GET/chunked",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"transfer-encoding": "chunked",
                                  "Content-Type": "text/plain;charset=utf-8"},
                         # note: the chunk length does not count the trailing CRLF
                         body=b'16\r\n'
                         + b'Mary had a little pug \r\n'
                         + b'1b\r\n'
                         + b'Its name was "Skupper-Jack"\r\n'
                         + b'0\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_headers={'transfer-encoding': 'chunked'},
                               expect_body=b'Mary had a little pug Its name was "Skupper-Jack"')),

            (RequestMsg("GET", "/GET/chunked_large",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"transfer-encoding": "chunked",
                                  "Content-Type": "text/plain;charset=utf-8"},
                         # note: the chunk length does not count the trailing CRLF
                         body=b'1\r\n'
                         + b'?\r\n'
                         + b'800\r\n'
                         + b'X' * 0x800 + b'\r\n'
                         + b'13\r\n'
                         + b'Y' * 0x13  + b'\r\n'
                         + b'0\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'Optional: Trailer\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_headers={'transfer-encoding': 'chunked'},
                               expect_body=b'?' + b'X' * 0x800 + b'Y' * 0x13)),

            (RequestMsg("GET", "/GET/info_content_len",
                        headers={"Content-Length": 0}),
             [ResponseMsg(100, reason="Continue",
                          headers={"Blab": 1, "Blob": "?"}),
              ResponseMsg(200, reason="OK",
                          headers={"Content-Length": 1,
                                   "Content-Type": "text/plain;charset=utf-8"},
                          body=b'?')],
             ResponseValidator(expect_headers={'Content-Type': "text/plain;charset=utf-8"},
                               expect_body=b'?')),

            # (RequestMsg("GET", "/GET/no_length",
            #             headers={"Content-Length": "0"}),
            #  ResponseMsg(200, reason="OK",
            #              headers={"Content-Type": "text/plain;charset=utf-8",
            #                       "connection": "close"
            #              },
            #              body=b'Hi! ' * 1024 + b'X'),
            #  ResponseValidator(expect_body=b'Hi! ' * 1024 + b'X')),
        ],
        #
        # HEAD
        #
        "HEAD": [
            (RequestMsg("HEAD", "/HEAD/test_01",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Content-Length": "10",
                                       "App-Header-2": "Value 02"},
                         body=None),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Content-Length": "10",
                                               "App-Header-2": "Value 02"})
            ),
            (RequestMsg("HEAD", "/HEAD/test_02",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Transfer-Encoding": "chunked",
                                       "App-Header-2": "Value 02"}),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Transfer-Encoding": "chunked",
                                               "App-Header-2": "Value 02"})),

            (RequestMsg("HEAD", "/HEAD/test_03",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-3": "Value 03"}),
             ResponseValidator(expect_headers={"App-Header-3": "Value 03"})),
        ],
        #
        # POST
        #
        "POST": [
            (RequestMsg("POST", "/POST/test_01",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "19",
                                 "Content-Type": "application/x-www-form-urlencoded"},
                        body=b'one=1&two=2&three=3'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Transfer-Encoding": "chunked"},
                         body=b'8\r\n'
                         + b'12345678\r\n'
                         + b'f\r\n'
                         + b'abcdefghijklmno\r\n'
                         + b'000\r\n'
                         + b'\r\n'),
             ResponseValidator(expect_body=b'12345678abcdefghijklmno')
            ),
            (RequestMsg("POST", "/POST/test_02",
                        headers={"App-Header-1": "Value 01",
                                 "Transfer-Encoding": "chunked"},
                        body=b'01\r\n'
                        + b'!\r\n'
                        + b'0\r\n\r\n'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Content-Length": "9"},
                         body=b'Hi There!'),
             ResponseValidator(expect_body=b'Hi There!')
            ),
        ],
        #
        # PUT
        #
        "PUT": [
            (RequestMsg("PUT", "/PUT/test_01",
                        headers={"Put-Header-1": "Value 01",
                                 "Transfer-Encoding": "chunked",
                                 "Content-Type": "text/plain;charset=utf-8"},
                        body=b'80\r\n'
                        + b'$' * 0x80 + b'\r\n'
                        + b'0\r\n\r\n'),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Content-length": "3"},
                         body=b'ABC'),
             ResponseValidator(status=201, expect_body=b'ABC')
            ),

            (RequestMsg("PUT", "/PUT/test_02",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-length": "0",
                                 "Content-Type": "text/plain;charset=utf-8"}),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Transfer-Encoding": "chunked"},
                         body=b'1\r\n$\r\n0\r\n\r\n'),
             ResponseValidator(status=201, expect_body=b'$')
            ),
        ]
    }

    # HTTP/1.0 compliant test cases (no chunked, response length unspecified)
    TESTS_10 = {
        #
        # GET
        #
        "GET": [
            (RequestMsg("GET", "/GET/error",
                        headers={"Content-Length": 0}),
             ResponseMsg(400, reason="Bad breath", error=True),
             ResponseValidator(status=400)),

            (RequestMsg("GET", "/GET/content_len_511",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Length": 511,
                                  "Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 511),
             ResponseValidator(expect_headers={'Content-Length': '511'},
                               expect_body=b'X' * 511)),

            (RequestMsg("GET", "/GET/content_len_4096",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8"},
                         body=b'X' * 4096),
             ResponseValidator(expect_headers={"Content-Type": "text/plain;charset=utf-8"},
                               expect_body=b'X' * 4096)),

            (RequestMsg("GET", "/GET/info_content_len",
                        headers={"Content-Length": 0}),
             [ResponseMsg(100, reason="Continue",
                          headers={"Blab": 1, "Blob": "?"}),
              ResponseMsg(200, reason="OK",
                          headers={"Content-Type": "text/plain;charset=utf-8"},
                          body=b'?')],
             ResponseValidator(expect_headers={'Content-Type': "text/plain;charset=utf-8"},
                               expect_body=b'?')),

            # test support for "folded headers"

            (RequestMsg("GET", "/GET/folded_header_01",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8",
                                  "Content-Length": 1,
                                  "folded-header": "One\r\n \r\n\tTwo"},
                         body=b'X'),
             ResponseValidator(expect_headers={"Content-Type":
                                               "text/plain;charset=utf-8",
                                               "folded-header":
                                               "One     \tTwo"},
                               expect_body=b'X')),

            (RequestMsg("GET", "/GET/folded_header_02",
                        headers={"Content-Length": 0}),
             ResponseMsg(200, reason="OK",
                         headers={"Content-Type": "text/plain;charset=utf-8",
                                  "Content-Length": 1,
                                  "folded-header": "\r\n \r\n\tTwo",
                                  "another-header": "three"},
                         body=b'X'),
             ResponseValidator(expect_headers={"Content-Type":
                                               "text/plain;charset=utf-8",
                                               # trim leading and
                                               # trailing ws:
                                               "folded-header":
                                               "Two",
                                               "another-header":
                                               "three"},
                               expect_body=b'X')),
        ],
        #
        # HEAD
        #
        "HEAD": [
            (RequestMsg("HEAD", "/HEAD/test_01",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-1": "Value 01",
                                       "Content-Length": "10",
                                       "App-Header-2": "Value 02"},
                         body=None),
             ResponseValidator(expect_headers={"App-Header-1": "Value 01",
                                               "Content-Length": "10",
                                               "App-Header-2": "Value 02"})
            ),

            (RequestMsg("HEAD", "/HEAD/test_03",
                        headers={"Content-Length": "0"}),
             ResponseMsg(200, headers={"App-Header-3": "Value 03"}),
             ResponseValidator(expect_headers={"App-Header-3": "Value 03"})),
        ],
        #
        # POST
        #
        "POST": [
            (RequestMsg("POST", "/POST/test_01",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "19",
                                 "Content-Type": "application/x-www-form-urlencoded"},
                        body=b'one=1&two=2&three=3'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever"},
                         body=b'12345678abcdefghijklmno'),
             ResponseValidator(expect_body=b'12345678abcdefghijklmno')
            ),
            (RequestMsg("POST", "/POST/test_02",
                        headers={"App-Header-1": "Value 01",
                                 "Content-Length": "5"},
                        body=b'01234'),
             ResponseMsg(200, reason="OK",
                         headers={"Response-Header": "whatever",
                                  "Content-Length": "9"},
                         body=b'Hi There!'),
             ResponseValidator(expect_body=b'Hi There!')
            ),
        ],
        #
        # PUT
        #
        "PUT": [
            (RequestMsg("PUT", "/PUT/test_01",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-Length": "513",
                                 "Content-Type": "text/plain;charset=utf-8"},
                        body=b'$' * 513),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever",
                                  "Content-length": "3"},
                         body=b'ABC'),
             ResponseValidator(status=201, expect_body=b'ABC')
            ),

            (RequestMsg("PUT", "/PUT/test_02",
                        headers={"Put-Header-1": "Value 01",
                                 "Content-length": "0",
                                 "Content-Type": "text/plain;charset=utf-8"}),
             ResponseMsg(201, reason="Created",
                         headers={"Response-Header": "whatever"},
                         body=b'No Content Length'),
             ResponseValidator(status=201, expect_body=b'No Content Length')
            ),
        ]
    }


    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorOneRouterTest, cls).setUpClass()

        def router(name, mode, extra):
            config = [
                ('router', {'mode': mode,
                            'id': name,
                            'allowUnsettledMulticast': 'yes'}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port()}),
                ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ]

            if extra:
                config.extend(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))
            return cls.routers[-1]

        # configuration:
        #  One interior router, two servers (one running as HTTP/1.0)
        #
        #  +----------------+
        #  |     INT.A      |
        #  +----------------+
        #      ^         ^
        #      |         |
        #      V         V
        #  <clients>  <servers>

        cls.routers = []
        cls.http_server11_port = cls.tester.get_port()
        cls.http_server10_port = cls.tester.get_port()
        cls.http_listener11_port = cls.tester.get_port()
        cls.http_listener10_port = cls.tester.get_port()

        router('INT.A', 'standalone',
               [('httpConnector', {'port': cls.http_server11_port,
                                   'protocolVersion': 'HTTP1',
                                   'address': 'testServer11'}),
                ('httpConnector', {'port': cls.http_server10_port,
                                   'protocolVersion': 'HTTP1',
                                   'address': 'testServer10'}),
                ('httpListener', {'port': cls.http_listener11_port,
                                  'protocolVersion': 'HTTP1',
                                  'address': 'testServer11'}),
                ('httpListener', {'port': cls.http_listener10_port,
                                  'protocolVersion': 'HTTP1',
                                  'address': 'testServer10'})
               ])

        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        cls.http11_server = TestServer(server_port=cls.http_server11_port,
                                       client_port=cls.http_listener11_port,
                                       tests=cls.TESTS_11)
        cls.http10_server = TestServer(server_port=cls.http_server10_port,
                                       client_port=cls.http_listener10_port,
                                       tests=cls.TESTS_10,
                                       handler_cls=RequestHandler10)
        cls.INT_A.wait_connectors()

    @classmethod
    def tearDownClass(cls):
        if cls.http11_server:
            cls.http11_server.wait()
        if cls.http10_server:
            cls.http10_server.wait()
        super(Http1AdaptorOneRouterTest, cls).tearDownClass()

    def _do_request(self, client, tests):
        for req, _, val in tests:
            req.send_request(client)
            rsp = client.getresponse()
            try:
                body = val.check_response(rsp)
            except Exception as exc:
                self.fail("request failed:  %s" % str(exc))

            if req.method == "BODY":
                self.assertEqual(b'', body)

    def test_001_get(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_11["GET"])
        client.close()

    def test_002_head(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_11["HEAD"])
        client.close()

    def test_003_post(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_11["POST"])
        client.close()

    def test_004_put(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_11["PUT"])
        client.close()

    def test_005_get_10(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener10_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_10["GET"])
        client.close()

    def test_006_head_10(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener10_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_10["HEAD"])
        client.close()

    def test_007_post_10(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener10_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_10["POST"])
        client.close()

    def test_008_put_10(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener10_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_10["PUT"])
        client.close()

    def test_000_stats(self):
        client = HTTPConnection("127.0.0.1:%s" % self.http_listener11_port,
                                timeout=TIMEOUT)
        self._do_request(client, self.TESTS_11["GET"])
        self._do_request(client, self.TESTS_11["POST"])
        client.close()
        qd_manager = QdManager(self, address=self.INT_A.listener)
        stats = qd_manager.query('org.apache.qpid.dispatch.httpRequestInfo')
        self.assertEqual(len(stats), 2)
        for s in stats:
            self.assertEqual(s.get('requests'), 9)
            self.assertEqual(s.get('details').get('GET:400'), 1)
            self.assertEqual(s.get('details').get('GET:200'), 6)
            self.assertEqual(s.get('details').get('POST:200'), 2)
        def assert_approximately_equal(a, b):
            self.assertTrue((abs(a - b) / a) < 0.1)
        if stats[0].get('direction') == 'out':
            self.assertEqual(stats[1].get('direction'), 'in')
            assert_approximately_equal(stats[0].get('bytesOut'), 1059)
            assert_approximately_equal(stats[0].get('bytesIn'), 8849)
            assert_approximately_equal(stats[1].get('bytesOut'), 8830)
            assert_approximately_equal(stats[1].get('bytesIn'), 1059)
        else:
            self.assertEqual(stats[0].get('direction'), 'in')
            self.assertEqual(stats[1].get('direction'), 'out')
            assert_approximately_equal(stats[0].get('bytesOut'), 8849)
            assert_approximately_equal(stats[0].get('bytesIn'), 1059)
            assert_approximately_equal(stats[1].get('bytesOut'), 1059)
            assert_approximately_equal(stats[1].get('bytesIn'), 8830)


class Http1AdaptorEdge2EdgeTest(TestCase):
    """
    Test an HTTP servers and clients attached to edge routers separated by an
    interior router
    """

    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorEdge2EdgeTest, cls).setUpClass()

        def router(name, mode, extra):
            config = [
                ('router', {'mode': mode,
                            'id': name,
                            'allowUnsettledMulticast': 'yes'}),
                ('listener', {'role': 'normal',
                              'port': cls.tester.get_port()}),
                ('address', {'prefix': 'closest',   'distribution': 'closest'}),
                ('address', {'prefix': 'multicast', 'distribution': 'multicast'}),
            ]

            if extra:
                config.extend(extra)
            config = Qdrouterd.Config(config)
            cls.routers.append(cls.tester.qdrouterd(name, config, wait=True))
            return cls.routers[-1]

        # configuration:
        # one edge, one interior
        #
        #  +-------+    +---------+    +-------+
        #  |  EA1  |<==>|  INT.A  |<==>|  EA2  |
        #  +-------+    +---------+    +-------+
        #      ^                           ^
        #      |                           |
        #      V                           V
        #  <clients>                   <servers>

        cls.routers = []
        cls.INTA_edge1_port   = cls.tester.get_port()
        cls.INTA_edge2_port   = cls.tester.get_port()
        cls.http_server11_port = cls.tester.get_port()
        cls.http_listener11_port = cls.tester.get_port()
        cls.http_server10_port = cls.tester.get_port()
        cls.http_listener10_port = cls.tester.get_port()

        router('INT.A', 'interior',
               [('listener', {'role': 'edge', 'port': cls.INTA_edge1_port}),
                ('listener', {'role': 'edge', 'port': cls.INTA_edge2_port}),
               ])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        router('EA1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge',
                               'port': cls.INTA_edge1_port}),
                ('httpListener', {'port': cls.http_listener11_port,
                                  'protocolVersion': 'HTTP1',
                                  'address': 'testServer11'}),
                ('httpListener', {'port': cls.http_listener10_port,
                                  'protocolVersion': 'HTTP1',
                                  'address': 'testServer10'})
               ])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        router('EA2', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge',
                               'port': cls.INTA_edge2_port}),
                ('httpConnector', {'port': cls.http_server11_port,
                                   'protocolVersion': 'HTTP1',
                                   'address': 'testServer11'}),
                ('httpConnector', {'port': cls.http_server10_port,
                                   'protocolVersion': 'HTTP1',
                                   'address': 'testServer10'})
               ])
        cls.EA2 = cls.routers[-1]
        cls.EA2.listener = cls.EA2.addresses[0]

        cls.INT_A.wait_address('EA1')
        cls.INT_A.wait_address('EA2')

    def test_01_streaming(self):
        """
        Test multiple clients sending streaming messages in parallel
        """
        TESTS_11 = {
            "PUT": [
                (RequestMsg("PUT", "/PUT/streaming_test",
                            headers={
                                "Transfer-encoding": "chunked",
                                "Content-Type": "text/plain;charset=utf-8"
                            },

                            # Note: due to DISPATCH-1791 the test cannot send
                            # messages of length > Q2 without stalling. Until
                            # this bug is fixed use a smaller message body
                            # length:
                            #
                            #body=b'aBcDe\r\n' + b'1' * 0xabcde + b'\r\n'
                            #+ b'a9B8C\r\n' + b'2' * 0xa9b8c + b'\r\n'
                            #+ b'0\r\n\r\n'),

                            body=b'FFFF\r\n' + b'1' * 0xFFFE + b'X\r\n'
                            + b'0\r\n\r\n'),

                 ResponseMsg(201, reason="Created",
                             headers={"Response-Header": "data",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201)
                )],

            "GET": [
                (RequestMsg("GET", "/GET/test",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={
                                 "transfer-Encoding": "chunked",
                                 "Content-Type": "text/plain;charset=utf-8"
                             },

                             # See DISPATCH-1791
                             body=b'FFFF\r\n' + b'2' * 0xFFFF + b'\r\n'
                             + b'0\r\n\r\n'),

                             # body=b'20000\r\n' + b'0' * 0x20000 + b'\r\n'
                             # + b'20019\r\n' + b'1' * 0x20019 + b'\r\n'
                             # + b'20028\r\n' + b'2' * 0x20028 + b'\r\n'
                             # + b'20037\r\n' + b'3' * 0x20037 + b'\r\n'
                             # + b'20046\r\n' + b'4' * 0x20046 + b'\r\n'
                             # + b'20054\r\n' + b'5' * 0x20054 + b'\r\n'
                             # + b'20063\r\n' + b'6' * 0x20063 + b'\r\n'
                             # + b'20072\r\n' + b'7' * 0x20072 + b'\r\n'
                             # + b'20081\r\n' + b'8' * 0x20081 + b'\r\n'
                             # + b'20090\r\n' + b'9' * 0x20090 + b'\r\n'
                             # + b'0\r\n\r\n'),
                 ResponseValidator(status=200)
                )],
        }

        TESTS_10 = {
            "POST": [
                # See DISPATCH-1791: once fixed make body length > Q2
                (RequestMsg("POST", "/POST/streaming_test",
                            headers={"Header-1": "H" * 2048,
                                     #"Content-Length": "1048577",
                                     "Content-Length": "65536",
                                     "Content-Type": "text/plain;charset=utf-8"},
                            # body=b'P' * 1048577),
                            body=b'P' * 65536),
                 ResponseMsg(201, reason="Created",
                             headers={"Response-Header": "data",
                                      "Content-Length": "0"}),
                 ResponseValidator(status=201)
                )],

            "GET": [
                (RequestMsg("GET", "/GET/streaming_test",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             #headers={"Content-Length": "999999",
                             headers={"Content-Length": "65533",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             #body=b'G' * 999999),
                             body=b'G' * 65533),
                 ResponseValidator(status=200),
                ),
                (RequestMsg("GET", "/GET/streaming_test_close",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Type": "text/plain;charset=utf-8"},
                             #body=b'C' * 999999),
                             body=b'C' * 65537),
                 ResponseValidator(status=200),
                )],
        }
        server11 = TestServer(server_port=self.http_server11_port,
                              client_port=self.http_listener11_port,
                              tests=TESTS_11)
        server10 = TestServer(server_port=self.http_server10_port,
                              client_port=self.http_listener10_port,
                              tests=TESTS_10,
                              handler_cls=RequestHandler10)

        self.EA2.wait_connectors()

        clients = []
        for _ in range(2):
            clients.append(ThreadedTestClient(TESTS_11,
                                              self.http_listener11_port,
                                              repeat=10))
            clients.append(ThreadedTestClient(TESTS_10,
                                              self.http_listener10_port,
                                              repeat=10))
        for client in clients:
            client.wait()
            self.assertIsNone(client.error)

        server11.wait()
        server10.wait()

    def test_02_credit_replenish(self):
        """
        Verify credit is replenished by sending > the default credit window
        requests across the routers.  The default credit window is 250
        """

        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_02_credit_replenish",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "24",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_02_credit_replenish'),
                 ResponseValidator(status=200),
                ),
            ]
        }
        server = TestServer(server_port=self.http_server11_port,
                            client_port=self.http_listener11_port,
                            tests=TESTS)
        self.EA2.wait_connectors()

        client = ThreadedTestClient(TESTS,
                                    self.http_listener11_port,
                                    repeat=300)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(300, client.count)
        server.wait()

    def test_03_server_reconnect(self):
        """
        Verify server reconnect logic.
        """
        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_03_server_reconnect",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "24",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_03_server_reconnect'),
                 ResponseValidator(status=200),
                ),
            ]
        }

        # bring up the server and send some requests. This will cause the
        # router to grant credit for clients
        server = TestServer(server_port=self.http_server11_port,
                            client_port=self.http_listener11_port,
                            tests=TESTS)
        self.EA2.wait_connectors()

        client = ThreadedTestClient(TESTS,
                                    self.http_listener11_port,
                                    repeat=2)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(2, client.count)

        # simulate server loss.  Fire up a client which should be granted
        # credit since the adaptor does not immediately teardown the server
        # links.  This will cause the adaptor to run qdr_connection_process
        # without a raw connection available to wake the I/O thread..
        server.wait()
        client = ThreadedTestClient(TESTS,
                                    self.http_listener11_port,
                                    repeat=2)
        # the adaptor will detach the links to the server if the connection
        # cannot be reestablished after 2.5 seconds.  Restart the server before
        # that occurrs to prevent client messages from being released
        server = TestServer(server_port=self.http_server11_port,
                            client_port=self.http_listener11_port,
                            tests=TESTS)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(2, client.count)
        server.wait()

    def test_04_server_pining_for_the_fjords(self):
        """
        Test permanent loss of server
        """
        TESTS = {
            "GET": [
                (RequestMsg("GET", "/GET/test_04_fjord_pining",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "20",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_04_fjord_pining'),
                 ResponseValidator(status=200),
                ),
            ]
        }

        # bring up the server and send some requests. This will cause the
        # router to grant credit for clients
        server = TestServer(server_port=self.http_server11_port,
                            client_port=self.http_listener11_port,
                            tests=TESTS)
        self.EA2.wait_connectors()

        client = ThreadedTestClient(TESTS, self.http_listener11_port)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(1, client.count)

        TESTS_FAIL = {
            "GET": [
                (RequestMsg("GET", "/GET/test_04_fjord_pining",
                            headers={"Content-Length": "000"}),
                 ResponseMsg(200, reason="OK",
                             headers={"Content-Length": "20",
                                      "Content-Type": "text/plain;charset=utf-8"},
                             body=b'test_04_fjord_pining'),
                 ResponseValidator(status=503),
                ),
            ]
        }

        # Kill the server then issue client requests, expect 503 response
        server.wait()
        client = ThreadedTestClient(TESTS_FAIL, self.http_listener11_port)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(1, client.count)

        # ensure links recover once the server re-appears
        server = TestServer(server_port=self.http_server11_port,
                            client_port=self.http_listener11_port,
                            tests=TESTS)
        self.EA2.wait_connectors()

        client = ThreadedTestClient(TESTS, self.http_listener11_port)
        client.wait()
        self.assertIsNone(client.error)
        self.assertEqual(1, client.count)


if __name__ == '__main__':
    unittest.main(main_module())
