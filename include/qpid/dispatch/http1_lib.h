#ifndef http1_lib_H
#define http1_lib_H 1
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#include <qpid/dispatch/buffer.h>

#include <inttypes.h>
#include <stdbool.h>


// HTTP/1.x Encoder/Decoder Library
//
// This library provides an API for encoding and decoding HTTP/1.x messages.
//
// The decoder takes qd_buffer_t chains containing HTTP/1.x data read from the
// TCP connection and issues callbacks as various parts (headers, body, status)
// of the HTTP/1.x message are parsed.
//
// The encoder allows the application to construct an HTTP/1.x message. An API
// is provided for building the message and a callback is invoked when the
// encoder has full qd_buffer_t data to send out the TCP connection.
//
// This library provides two classes:
//
// * h1_lib_connection_t - a context for a single TCP connection over which
//   HTTP/1.x messages are exchanged.
//
// * h1_lib_request_state_t - a context which tracks the state of a single
//   HTTP/1.x Request <-> Response message exchange. Multiple
//   h1_lib_request_state_t can be associated with an h1_lib_connection_t due to
//   request pipelining.


#define HTTP1_VERSION_1_1  "HTTP/1.1"
#define HTTP1_VERSION_1_0  "HTTP/1.0"

typedef struct h1_lib_connection_t    h1_lib_connection_t;
typedef struct h1_lib_request_state_t h1_lib_request_state_t;


typedef enum {
    HTTP1_CONN_CLIENT,  // connection initiated by client
    HTTP1_CONN_SERVER,  // connection to server
} h1_lib_conn_type_t;

typedef enum {
    HTTP1_STATUS_BAD_REQ = 400,
    HTTP1_STATUS_SERVER_ERR = 500,
    HTTP1_STATUS_BAD_VERSION = 505,
} h1_lib_status_code_t;

typedef struct h1_lib_conn_config_t {

    h1_lib_conn_type_t type;

    // called with output data to write to the network
    void (*conn_tx_data)(h1_lib_connection_t *conn, qd_buffer_list_t *data, size_t offset, unsigned int len);

    //
    // RX message callbacks
    //

    // HTTP request received - new h1_lib_request_state_t created (hrs).  This
    // hrs must be supplied in the h1_lib_tx_response() method when sending the
    // response.
    int (*rx_request)(h1_lib_request_state_t *hrs,
                      const char *method,
                      const char *target,
                      uint32_t   version_major,
                      uint32_t   version_minor);

    // HTTP response received - the h1_lib_request_state_t comes from the return
    // value of the h1_lib_tx_request() method used to create the corresponding
    // request.  Note well that if status_code is Informational (1xx) then this
    // response is NOT the last response for the current request (See RFC7231,
    // 6.2 Informational 1xx).  The request_done callback will be called after
    // the LAST response has been received.
    //
    int (*rx_response)(h1_lib_request_state_t *hrs,
                       int status_code,
                       const char *reason_phrase,
                       uint32_t version_major,
                       uint32_t version_minor);

    int (*rx_header)(h1_lib_request_state_t *hrs, const char *key, const char *value);
    int (*rx_headers_done)(h1_lib_request_state_t *hrs, bool has_body);

    int (*rx_body)(h1_lib_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, uintmax_t len, bool more);

    void (*rx_done)(h1_lib_request_state_t *hrs);

    // Invoked when the request/response(s) exchange has completed.  The
    // h1_lib_request_state_t instance (hrs) is no longer valid on return from
    // this call so all context assoicated with hrs must be released during
    // this call.
    //
    void (*request_complete)(h1_lib_request_state_t *hrs);
} h1_lib_conn_config_t;


h1_lib_connection_t *h1_lib_connection(h1_lib_conn_config_t *config, void *context);
void h1_lib_connection_close(h1_lib_connection_t *conn);
void *h1_lib_connection_get_context(h1_lib_connection_t *conn);

// push inbound network data into the http1 library
int h1_lib_connection_rx_data(h1_lib_connection_t *conn, qd_buffer_list_t *data, uintmax_t len);


void h1_lib_request_state_set_context(h1_lib_request_state_t *hrs, void *context);
void *h1_lib_request_state_get_context(const h1_lib_request_state_t *hrs);
h1_lib_connection_t *h1_lib_request_state_get_connection(const h1_lib_request_state_t *hrs);

const char *h1_lib_request_state_method(const h1_lib_request_state_t *hrs);
//const char *h1_lib_request_state_target(const h1_lib_request_state_t *hrs);
//bool h1_lib_request_state_request_version(const h1_lib_request_state_t *hrs, int *major, int *minor);



//
// API for sending HTTP/1.x messages
//


// initiate a request - this creates a new request state context
h1_lib_request_state_t *h1_lib_tx_request(h1_lib_connection_t *conn, const char *method, const char *target,
                                          uint32_t version_major, uint32_t version_minor);

// respond to a received request - the request state context should be the one
// supplied during the corresponding rx_request callback
//
int h1_lib_tx_response(h1_lib_request_state_t *hrs, int status_code, const char *reason_phrase,
                       uint32_t version_major, uint32_t version_minor);

// add header to outgoing message
//
int h1_lib_tx_add_header(h1_lib_request_state_t *hrs, const char *key, const char *value);

// stream outgoing body data
//
int h1_lib_tx_body(h1_lib_request_state_t *hrs, qd_buffer_list_t *data, size_t offset, uintmax_t len);

// outgoing message construction complete
int h1_lib_tx_done(h1_lib_request_state_t *hrs);


#endif // http1_lib_H
