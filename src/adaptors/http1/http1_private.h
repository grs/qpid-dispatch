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
#include <qpid/dispatch/http1_codec.h>
#include <qpid/dispatch/protocol_adaptor.h>
#include <qpid/dispatch/message.h>
#include "adaptors/http_common.h"

typedef struct qdr_http1_out_data_t     qdr_http1_out_data_t;
typedef struct qdr_http1_response_msg_t qdr_http1_response_msg_t;
typedef struct qdr_http1_request_t      qdr_http1_request_t;
typedef struct qdr_http1_connection_t   qdr_http1_connection_t;

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


// State for a single HTTP Response message.
// An HTTP client may receive zero or more "100 Continue"
// responses prior to the final response message.  This structure
// holds the state for one response message
//
struct qdr_http1_response_msg_t {
    DEQ_LINKS(qdr_http1_response_msg_t);

    qd_message_t   *in_msg;
    qdr_delivery_t *dlv;
    uint64_t        dispo;
    bool            headers_encoded;
};
ALLOC_DECLARE(qdr_http1_response_msg_t);
DEQ_DECLARE(qdr_http1_response_msg_t, qdr_http1_response_msg_list_t);


// Per HTTP request/response(s) state.
// A reference is stored in qdr_delivery_get_context(dlv)
//
struct qdr_http1_request_t {
    DEQ_LINKS(qdr_http1_request_t);

    uint64_t                  msg_id;
    h1_codec_request_state_t *lib_rs;
    qdr_http1_connection_t   *hconn;  // parent connection
    char                     *response_addr; // request reply-to
    char                     *site;
    qd_timestamp_t            start;
    qd_timestamp_t            stop;

    // The request message.
    //
    // For connections to an HTTP client, request_msg is used to build the
    // incoming request.  Once enough of the message arrives and credit is
    // available a delivery is created (request_dlv) and request_msg pointer is
    // cleared (the delivery now holds the message).
    //
    // For connections to an HTTP server request_msg is not used.  request_dlv
    // holds the delivery sent by the router.
    //
    qd_message_t   *request_msg;
    qdr_delivery_t *request_dlv;
    uint64_t        request_dispo;
    bool            request_headers_encoded;

    // The response message(s).
    //
    // A client may receive zero or more "100 Continue" responses by the server
    // prior to receiving the terminal response. This list holds these
    // responses in order of delivery (oldest at HEAD).
    //
    qdr_http1_response_msg_list_t responses;

    // Incoming message construction.
    //
    // app_props is used to temporarily hold header fields as they arrive from
    // the HTTP endpoint prior to creating a qd_message_t
    //
    qd_composed_field_t      *app_props;

    // incoming message disposition - set by router
    uint64_t                  in_dispo;
    // outgoing message disposition - set by adaptor
    uint64_t                  out_dispo;

    // Outgoing data to write to proactor raw connection.
    // Out_fifo holds the data to be written in order HEAD to TAIL.
    // Write_ptr tracks the entry that holds the next buffers to send.
    // Entries are removed from out_fifo after proton completes
    // writing the buffers to the raw connection.
    qdr_http1_out_data_list_t out_fifo;
    qdr_http1_out_data_t     *write_ptr;

    // statistics
    //
    uint64_t  in_http1_octets;    // read from raw conn
    uint64_t  out_http1_octets;   // written to raw conn

    // flags

    bool headers_sent; // HTTP headers written to raw connection
    bool codec_completed;     // encoder/decoder done
    bool cancelled;
    bool close_on_complete;   // close the conn when this request is complete
};
ALLOC_DECLARE(qdr_http1_request_t);
DEQ_DECLARE(qdr_http1_request_t, qdr_http1_request_list_t);


// A single HTTP adaptor connection.
//
struct qdr_http1_connection_t {
    qd_server_t           *qd_server;
    h1_codec_connection_t *http_conn;
    pn_raw_connection_t   *raw_conn;
    qdr_connection_t      *qdr_conn;
    void                  *user_context;
    qdr_http1_adaptor_t   *adaptor;

    uint64_t               conn_id;
    qd_handler_context_t   handler_context;
    h1_codec_connection_type_t     type;

    struct {
        char *host;
        char *port;
        char *address;
        char *site;
        char *host_port;
    } cfg;

    // State if connected to an HTTP client
    //
    struct {
        char *client_ip_addr;
        char *reply_to_addr;   // set once link is up
        uint64_t next_msg_id;
    } client;

    // State if connected to an HTTP server
    struct {
        qd_timer_t *reconnect_timer;
        int         reconnect_count;
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

    // statistics
    //
    uint64_t  in_http1_octets;
    uint64_t  out_http1_octets;

    // flags
    //
    bool trace;
    bool close_connection;
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
int qdr_http1_write_out_data(qdr_http1_connection_t *hconn);
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
bool qdr_http1_request_complete(const qdr_http1_request_t *hreq);

// return the next HTTP token in a comma separated list of tokens
//
// start - search for token start pointer
// len - length of token if non-null returned
// next - address of start of next token
//
const char *qdr_http1_token_list_next(const char *start, size_t *len, const char **next);



// http1_client.c protocol adaptor callbacks
//
void qdr_http1_client_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit);
uint64_t qdr_http1_client_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled);
void qdr_http1_client_core_second_attach(qdr_http1_adaptor_t    *adaptor,
                                         qdr_http1_connection_t *hconn,
                                         qdr_link_t             *link,
                                         qdr_terminus_t         *source,
                                         qdr_terminus_t         *target);
void qdr_http1_client_core_delivery_update(qdr_http1_adaptor_t *adaptor,
                                           qdr_http1_connection_t *hconn,
                                           qdr_http1_request_t    *hreq,
                                           qdr_delivery_t         *dlv,
                                           uint64_t                disp,
                                           bool                    settled);


// http1_server.c protocol adaptor callbacks
//
void qdr_http1_server_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit);
uint64_t qdr_http1_server_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled);
void qdr_http1_server_core_delivery_update(qdr_http1_adaptor_t *adaptor,
                                           qdr_http1_connection_t *hconn,
                                           qdr_http1_request_t    *hreq,
                                           qdr_delivery_t         *dlv,
                                           uint64_t                disp,
                                           bool                    settled);


// management info retrieval:

void qdr_http1_record_client_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_t *request, qdr_http1_response_msg_t *response);
void qdr_http1_record_server_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_t *request, int status, const char *reason);


void qdra_http_request_info_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset);
void qdra_http_request_info_get_next_CT(qdr_core_t *core, qdr_query_t *query);
void qdra_http_request_info_get_CT(qdr_core_t          *core,
                                   qd_iterator_t       *name,
                                   qd_iterator_t       *identity,
                                   qdr_query_t         *query,
                                   const char          *qdr_http_request_info_columns[]);

#define QDR_HTTP_REQUEST_INFO_COLUMN_COUNT 10
extern const char *qdr_http_request_info_columns[QDR_HTTP_REQUEST_INFO_COLUMN_COUNT + 1];

#endif // http1_private_H
