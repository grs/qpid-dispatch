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
// Nomenclature:
//  "in": for information flowing from the endpoint into the router
//        (from proactor to core)
//  "out": for information flowing from the router out to the endpoint
//         (from core to proactor)
//
#include <qpid/dispatch/http1_lib.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/message.h>
#include "adaptors/http_common.h"

typedef struct qdr_http1_out_data_t   qdr_http1_out_data_t;
typedef struct qdr_http1_request_t    qdr_http1_request_t;
typedef struct qdr_http1_connection_t qdr_http1_connection_t;


typedef struct qdr_http1_adaptor_t {
    qdr_core_t               *core;
    qdr_protocol_adaptor_t   *adaptor;
    qd_http_lsnr_list_t       listeners;
    qd_http_connector_list_t  connectors;
    qd_log_source_t          *log;
} qdr_http1_adaptor_t;

extern qdr_http1_adaptor_t *qdr_http1_adaptor;


// Data to be written out the raw connection.
//
// This adaptor has to cope with two different data sources: the HTTP1 encoder
// and the qd_message_body_data_t list.  The HTTP1 encoder produces a simple
// qd_buffer_list_t for outgoing header data whose ownership is given to the
// adaptor: the adaptor is free to deque/free these buffers as needed.  The
// qd_message_body_data_t buffers are shared with the owning message and the
// buffer list must not be modified by the adaptor.  The qdr_http1_out_data_t
// is used to manage both types of data sources.
//
struct qdr_http1_out_data_t {
    DEQ_LINKS(qdr_http1_out_data_t);
    qdr_http1_request_t *hreq;   // owning request/response

    // data is either in a raw buffer chain
    // or a message body data (not both!)

    qd_buffer_list_t raw_buffers;
    qd_message_body_data_t *body_data;

    int buffer_count;  // # total buffers
    int next_buffer;   // offset to next buffer to send
    int free_count;    // # buffers returned from proton

};
ALLOC_DECLARE(qdr_http1_out_data_t);
DEQ_DECLARE(qdr_http1_out_data_t, qdr_http1_out_data_list_t);


// Per HTTP request/response state.
// A reference is stored in qdr_delivery_get_context(dlv)
//
struct qdr_http1_request_t {
    DEQ_LINKS(qdr_http1_request_t);

    uint64_t                msg_id;
    h1_lib_request_state_t *lib_rs;
    qdr_http1_connection_t *hconn;  // parent connection
    char                   *response_addr; // request reply-to

    qdr_delivery_t         *request_dlv;
    qdr_delivery_t         *response_dlv;

    // incoming message construction
    // in_msg is created once all the Application properties have arrived.
    // Once the in_msg is delivered to the core it is owned by one of the above
    // deliveries and this pointer is cleared.
    qd_composed_field_t    *app_props;
    qd_message_t           *in_msg;

    // incoming message disposition - set by router
    uint64_t                in_dispo;
    // outgoing message disposition - set by adaptor
    uint64_t                out_dispo;

    // Outgoing data to write to proactor raw connection.  out_data is a FIFO
    // queue. An out_data entry is freed once all associated buffers have
    // completed writing.  Note well: the raw connection API does NOT guarantee
    // that written buffers are returned in the same order as they have been
    // queued for writing!
    qdr_http1_out_data_list_t  out_data;

    // flags

    bool headers_sent;  // HTTP headers written to raw connection
    bool connection_close_flag;
    bool completed;     // encoder/decoder done
};
ALLOC_DECLARE(qdr_http1_request_t);
DEQ_DECLARE(qdr_http1_request_t, qdr_http1_request_list_t);


// A single HTTP adaptor connection.
//
struct qdr_http1_connection_t {
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

    bool trace;
    bool close_connection;
    bool completed;  // set by library when done
};
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
void qdr_http1_write_out_data(qdr_http1_request_t *hreq);
void qdr_http1_write_buffer_list(qdr_http1_request_t *hreq, qd_buffer_list_t *blist);
void qdr_http1_write_body_data(qdr_http1_request_t *hreq, qd_message_body_data_t *body_data);
void qdr_http1_free_written_buffers(qdr_http1_connection_t *hconn);
void qdr_http1_close_connection(qdr_http1_connection_t *hconn, const char *error);
void qdr_http1_request_free(qdr_http1_request_t *hreq);
void qdr_http1_connection_free(qdr_http1_connection_t *hconn);
void qdr_http1_error_response(qdr_http1_request_t *hreq,
                              int error_code,
                              const char *reason);
void qdr_http1_rejected_response(qdr_http1_request_t *hreq,
                                 const qdr_error_t *error);



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
