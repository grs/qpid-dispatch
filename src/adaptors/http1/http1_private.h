#ifndef http1_private_H
#define http1_private_H 1
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

//
// HTTP/1.x Adaptor Internals
//

#include <qpid/dispatch/http1_lib.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/message.h>
#include "adaptors/http_common.h"


typedef struct qdr_http1_adaptor_t {
    qdr_core_t               *core;
    qdr_protocol_adaptor_t   *adaptor;
    qd_http_lsnr_list_t       listeners;
    qd_http_connector_list_t  connectors;
    qd_log_source_t          *log;
} qdr_http1_adaptor_t;

extern qdr_http1_adaptor_t *qdr_http1_adaptor;

// Per HTTP request/response state.
// A reference is stored in qdr_delivery_get_context(dlv)
//
typedef struct qdr_http1_request_t qdr_http1_request_t;
struct qdr_http1_request_t {
    DEQ_LINKS(qdr_http1_request_t);

    uint64_t msg_id;
    h1_lib_request_state_t *lib_rs;
    struct qdr_http1_connection_t *hconn;  // parent connection
    char *response_addr;                   // request reply-to

    qdr_delivery_t        *request_dlv;
    qd_message_t          *request_msg;

    qdr_delivery_t        *response_dlv;
    qd_message_t          *response_msg;

    qd_composed_field_t   *app_props;  // for incoming message

    // flags

    bool headers_sent;  // HTTP headers written to raw connection
    bool connection_close_flag;
    bool receive_complete;
};
ALLOC_DECLARE(qdr_http1_request_t);
DEQ_DECLARE(qdr_http1_request_t, qdr_http1_request_list_t);


// Data to be written out the raw connection
//
typedef struct qdr_http1_out_data_t {
    DEQ_LINKS(struct qdr_http1_out_data_t);

    // data is either in a raw buffer chain
    // or a message body data (not both!)

    qd_buffer_list_t raw_buffers;
    qd_message_body_data_t *body_data;

    int buffer_count;  // # total buffers
    int next_buffer;   // offset to next buffer to send
    int free_count;    // # buffers returned from proton

} qdr_http1_out_data_t;
ALLOC_DECLARE(qdr_http1_out_data_t);
DEQ_DECLARE(qdr_http1_out_data_t, qdr_http1_out_data_list_t);


// A single HTTP adaptor connection.
//
typedef struct qdr_http1_connection_t {

    qd_server_t           *qd_server;
    h1_lib_connection_t   *http_conn;
    pn_raw_connection_t   *raw_conn;
    qdr_connection_t      *qdr_conn;
    void                  *user_context;
    qdr_http1_adaptor_t   *adaptor;

    uint64_t               conn_id;
    qd_handler_context_t   handler_context;
    h1_lib_conn_type_t     type;

    struct {
        char *host;
        char *port;
        char *address;
        char *host_port;
    } cfg;

    // State if connected to an HTTP client
    //
    struct {
        char *client_ip_addr;
        char *reply_to_addr;
        uint64_t next_msg_id;
    } client;

    // State if connected to an HTTP server
    struct {
        qd_timer_t *activate_timer;
    } server;

    // Outgoing link (router ==> HTTP app)
    //
    qdr_link_t            *out_link;
    uint64_t               out_link_id;
    int                    out_link_credit;  // provided by adaptor

    // Incoming link (HTTP app ==> router)
    //
    qdr_link_t            *in_link;
    uint64_t               in_link_id;
    int                    in_link_credit;  // provided by router

    // Oldest at HEAD
    //
    qdr_http1_request_list_t requests;

    // Pending data to write to proactor.  out_data is a FIFO queue. write_ptr
    // points to the out_data instance that is currently being written.  As
    // writes complete entries are released in order starting at HEAD.
    qdr_http1_out_data_list_t  out_data;
    qdr_http1_out_data_t      *write_ptr;

    bool trace;
    bool close_connection;

} qdr_http1_connection_t;
ALLOC_DECLARE(qdr_http1_connection_t);


// special AMQP application properties keys for HTTP1 metadata headers
//
#define HTTP1_HEADER_PREFIX  "http:"          // reserved prefix
#define REQUEST_HEADER_KEY   "http:request"   // request msg, value=version
#define RESPONSE_HEADER_KEY  "http:response"  // response msg, value=version
#define REASON_HEADER_KEY    "http:reason"    // from response (optional)
#define TARGET_HEADER_KEY    "http:target"    // request target
#define STATUS_HEADER_KEY    "http:status"    // response status (integer)


// http1_adaptor.c
//
void qdr_http1_write_flush(qdr_http1_connection_t *hconn);
void qdr_http1_write_buffer_list(qdr_http1_connection_t *hconn, qd_buffer_list_t *blist);
void qdr_http1_write_body_data(qdr_http1_connection_t *hconn, qd_message_body_data_t *body_data);


// http1_client.c protocol adaptor callbacks
//
void qdr_http1_client_link_flow(qdr_http1_adaptor_t    *adaptor,
                                qdr_http1_connection_t *hconn,
                                qdr_link_t             *link,
                                int                     credit);
uint64_t qdr_http1_client_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                       qdr_http1_connection_t *hconn,
                                       qdr_link_t             *link,
                                       qdr_delivery_t         *delivery,
                                       bool                    settled);
void qdr_http1_client_delivery_update(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn,
                                      qdr_http1_request_t    *hreq,
                                      qdr_delivery_t         *dlv,
                                      uint64_t                disp,
                                      bool                    settled);


// http1_server.c protocol adaptor callbacks
//
void qdr_http1_server_link_flow(qdr_http1_adaptor_t    *adaptor,
                                qdr_http1_connection_t *hconn,
                                qdr_link_t             *link,
                                int                     credit);
uint64_t qdr_http1_server_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                       qdr_http1_connection_t *hconn,
                                       qdr_link_t             *link,
                                       qdr_delivery_t         *delivery,
                                       bool                    settled);
void qdr_http1_server_delivery_update(qdr_http1_adaptor_t *adaptor,
                                      qdr_http1_connection_t *hconn,
                                      qdr_http1_request_t    *hreq,
                                      qdr_delivery_t         *dlv,
                                      uint64_t                disp,
                                      bool                    settled);

#endif // http1_private_H
