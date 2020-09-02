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

import sys
from threading import Thread
try:
    from http.server import HTTPServer, BaseHTTPRequestHandler
except ImportError:
    from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

from proton.handlers import MessagingHandler
from proton.reactor import Container
from system_test import TestCase, unittest, main_module, Qdrouterd


class HttpResponse(object):
    def __init__(self, status, version=None, reason=None,
                 headers=None, body=None, eom_close=False, error=False):
        self.status = status
        self.version = version or "HTTP/1.1"
        self.reason = reason
        self.headers = headers or []
        self.body = body
        self.eom_close = eom_close
        self.error = error

    def do_response(self, handler):
        handler.protocol_version = self.version
        if self.error:
            handler.send_error(self.status,
                               message=self.reason,
                               explain=self.body)
            return

        handler.send_response(self.status, self.reason)
        for header in self.headers:
            handler.send_header(header[0], header[1])
        handler.end_headers()

        if self.body:
            handler.wfile.write(self.body)

        return self.eom_close


GET_RESPONSES = {
    "/get_error":
    HttpResponse(400, reason="Bad breath", error=True),

    "/get_content_len":
    HttpResponse(200, reason="OK",
                 headers=[("Content-Length", 1),
                          ("Content-Type", "text/plain;charset=utf-8")],
                 body=b'?'),

    "/get_content_len_511":
    HttpResponse(200, reason="OK",
                 headers=[("Content-Length", 511),
                          ("Content-Type", "text/plain;charset=utf-8")],
                 body=b'X' * 511),

    "/get_content_len_4096":
    HttpResponse(200, reason="OK",
                 headers=[("Content-Length", 4096),
                          ("Content-Type", "text/plain;charset=utf-8")],
                 body=b'X' * 4096),

    "/get_chunked":
    HttpResponse(200, reason="OK",
                 headers=[("transfer-encoding", "chunked"),
                          ("Content-Type", "text/plain;charset=utf-8")],
                 # note: the chunk length does not count the trailing CRLF
                 body=b'16\r\n'
                 + b'Mary had a little pug \r\n'
                 + b'1b\r\n'
                 + b'Its name was "Skupper-Jack"\r\n'
                 + b'0\r\n'
                 + b'Optional: Trailer\r\n'
                 + b'Optional: Trailer\r\n'
                 + b'\r\n'),

    "/get_chunked_large":
    HttpResponse(200, reason="OK",
                 headers=[("transfer-encoding", "chunked"),
                          ("Content-Type", "text/plain;charset=utf-8")],
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

    "/get_info_content_len":
    [HttpResponse(100, reason="Continue",
                  headers=[("Blab", 1), ("Blob", "?")]),
     HttpResponse(200, reason="OK",
                  headers=[("Content-Length", 1),
                           ("Content-Type", "text/plain;charset=utf-8")],
                  body=b'?')],

    "/get_no_length":
    HttpResponse(200, reason="OK",
                 headers=[("Content-Type", "text/plain;charset=utf-8")],
                 body=b'Hi! ' * 1024,
                 eom_close=True),

}




class RequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        print("GET %s (%s:%s)" % (self.path, self.client_address[0],
                                  self.client_address[1]))

        close = False
        rsp = GET_RESPONSES.get(self.path)
        if rsp is None:
            rsp = HttpResponse(404, reason="Not Found", error=True)
        if isinstance(rsp, list):
            for r in rsp:
                close = r.do_response(self);
                if close:
                    break;
        else:
            close = rsp.do_response(self)

        if close:
            print("Closing connection at EOM")
            self.close_connection = True



class TestServer(object):
    def __init__(self, port=8080):
        self._server_addr = ("", port)
        self._server = HTTPServer(self._server_addr, RequestHandler)
        self._stop_thread = False
        self._thread = Thread(target=self._run)
        self._thread.daemon = True
        self._thread.start()

    def _run(self):
        print("TestServer listening on 0.0.0.0:%d" % self._server_addr[1])
        self._server.serve_forever()
        print("TestServer shutting down...")

    def wait(self):
        self._thread.join()


class Http1AdaptorTest(TestCase):
    """
    Blah
    """
    @classmethod
    def setUpClass(cls):
        """Start a router"""
        super(Http1AdaptorTest, cls).setUpClass()

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
        #  +-------+    +---------+
        #  |  EA1  |<==>|  INT.A  |
        #  +-------+    +---------+
        #

        cls.routers = []
        cls.http_server = "0.0.0.0:%d" % cls.tester.get_port()
        cls.INTA_edge_port   = cls.tester.get_port()

        router('INT.A', 'interior',
               [('listener', {'role': 'edge', 'port': cls.INTA_edge_port})])
        cls.INT_A = cls.routers[0]
        cls.INT_A.listener = cls.INT_A.addresses[0]

        router('EA1', 'edge',
               [('connector', {'name': 'uplink', 'role': 'edge',
                               'port': cls.INTA_edge_port})])
        cls.EA1 = cls.routers[1]
        cls.EA1.listener = cls.EA1.addresses[0]

        cls.EA1.wait_connectors()
        cls.INT_A.wait_address('EA1')

    def test_001_get(self):
        import time
        time.sleep(100)
        print("DONE!", flush=True)


if __name__ == '__main__':
    unittest.main(main_module())
