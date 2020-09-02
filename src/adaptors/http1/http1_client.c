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
 */

#include "http1_private.h"
#include "adaptors/adaptor_utils.h"

#include <proton/listener.h>
#include <proton/proactor.h>


#define REPLY_TO_CAPACITY 10
#define LISTENER_BACKLOG  16


static void _client_tx_msg_headers_cb(h1_lib_request_state_t *lib_hrs, qd_buffer_list_t *blist, unsigned int len);
static void _client_tx_msg_body_cb(h1_lib_request_state_t *lib_hrs, qd_message_body_data_t *body_data);
static int _client_rx_request_cb(h1_lib_request_state_t *lib_rs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor);
static int _client_rx_response_cb(h1_lib_request_state_t *lib_rs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor);
static int _client_rx_header_cb(h1_lib_request_state_t *lib_rs, const char *key, const char *value);
static int _client_rx_headers_done_cb(h1_lib_request_state_t *lib_rs, bool has_body);
static int _client_rx_body_cb(h1_lib_request_state_t *lib_rs, qd_buffer_list_t *body, size_t offset, uintmax_t len,
                              bool more);
static void _client_rx_done_cb(h1_lib_request_state_t *lib_rs);
static void _client_request_complete_cb(h1_lib_request_state_t *lib_rs, bool cancelled);
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);


// Process terminal outcome set by core for request message
//
static void _handle_request_disposition_update(qdr_http1_request_t *req)
{
    assert(qd_message_receive_complete(qdr_delivery_message(req->request_dlv)));
    assert(req->in_dispo);

    if (req->in_dispo != PN_ACCEPTED) {
        // no response will be received - generate one
        if (req->in_dispo == PN_REJECTED) {
            // invalid request message
            qdr_error_t *error = qdr_delivery_error(req->request_dlv);
            qdr_http1_rejected_response(req, error);
        } else {
            // modified/released: in doubt
            qdr_http1_error_response(req, HTTP1_STATUS_SERVICE_UNAVAILABLE,
                                     "Cannot forward request to origin server");
        }
    }
}


// The request has completed.
// Advance to the next pending request if possible
//
static bool _handle_request_done(qdr_http1_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->hconn;
    bool close_conn = false;

    // @TODO(kgiusti) remove asserts?
    assert(hreq == DEQ_HEAD(hconn->requests));
    assert(hreq->completed);
    if (DEQ_IS_EMPTY(hreq->out_data)) {
        close_conn = hreq->connection_close_flag;
        qdr_http1_request_free(hreq);
        if (!close_conn && DEQ_HEAD(hconn->requests)) {
            // start sending the next request message
            hreq = DEQ_HEAD(hconn->requests);
            if (hreq->in_msg && hconn->in_link_credit) {
                hconn->in_link_credit -= 1;
                hreq->request_dlv = qdr_link_deliver(hconn->in_link, hreq->in_msg, 0, false, 0, 0, 0, 0);
                qdr_delivery_set_context(hreq->request_dlv, (void*) hreq);
                qdr_delivery_incref(hreq->request_dlv, "referenced by HTTP1 adaptor");
                hreq->in_msg = 0;
            }
        }
    }
    return close_conn;
}


////////////////////////////////////////////////////////
// HTTP/1.x Client Listener
////////////////////////////////////////////////////////


// Listener received connection request from client
//
static void _accept_client_connection(qd_http_lsnr_t *li)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();

    ZERO(hconn);
    hconn->type = HTTP1_CONN_CLIENT;
    hconn->qd_server = li->server;
    hconn->handler_context.handler = &_handle_connection_events;
    hconn->handler_context.context = hconn;

    hconn->cfg.host = qd_strdup(li->config.host);
    hconn->cfg.port = qd_strdup(li->config.port);
    hconn->cfg.address = qd_strdup(li->config.address);

    hconn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(hconn->raw_conn, hconn);
    pn_listener_raw_accept(li->pn_listener, hconn->raw_conn);

    // configure the HTTP/1.x library

    h1_lib_conn_config_t config = {0};
    config.type             = HTTP1_CONN_CLIENT;
    config.tx_msg_headers   = _client_tx_msg_headers_cb;
    config.tx_msg_body      = _client_tx_msg_body_cb;
    config.rx_request       = _client_rx_request_cb;
    config.rx_response      = _client_rx_response_cb;
    config.rx_header        = _client_rx_header_cb;
    config.rx_headers_done  = _client_rx_headers_done_cb;
    config.rx_body          = _client_rx_body_cb;
    config.rx_done          = _client_rx_done_cb;
    config.request_complete = _client_request_complete_cb;

    hconn->http_conn = h1_lib_connection(&config, hconn);
    if (!hconn->http_conn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
               "Failed to initialize HTTP/1.x library - connection refused.");
        pn_raw_connection_close(hconn->raw_conn);
        free_qdr_http1_connection_t(hconn);
        // @TODO(kgiusti) proper cleanup
    }

    // we'll create a QDR connection and links once the raw connection activates
}


// Process proactor events for the client listener
//
static void _handle_listener_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qd_log_source_t *log = qdr_http1_adaptor->log;
    qd_http_lsnr_t *li = (qd_http_lsnr_t*) context;
    const char *host_port = li->config.host_port;

    qd_log(log, QD_LOG_DEBUG, "HTTP/1.x Client Listener Event %s\n", pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_LISTENER_OPEN: {
        qd_log(log, QD_LOG_NOTICE, "Listening for HTTP/1.x client requests on %s", host_port);
        break;
    }

    case PN_LISTENER_ACCEPT: {
        qd_log(log, QD_LOG_INFO, "Accepting HTTP/1.x connection on %s", host_port);
        _accept_client_connection(li);
        break;
    }

    case PN_LISTENER_CLOSE: {
        if (li->pn_listener) {
            pn_condition_t *cond = pn_listener_condition(li->pn_listener);
            if (pn_condition_is_set(cond)) {
                qd_log(log, QD_LOG_ERROR, "Listener error on %s: %s (%s)", host_port,
                       pn_condition_get_description(cond),
                       pn_condition_get_name(cond));
            } else {
                qd_log(log, QD_LOG_TRACE, "Listener closed on %s", host_port);
            }
            pn_listener_set_context(li->pn_listener, 0);
            li->pn_listener = 0;
        }
        break;
    }

    default:
        break;
    }
}


// Management Agent API - Create
//
qd_http_lsnr_t *qd_http1_configure_listener(qd_dispatch_t *qd, const qd_http_bridge_config_t *config, qd_entity_t *entity)
{
    qd_http_lsnr_t *li = qd_http_lsnr(qd->server, &_handle_listener_events);
    if (!li) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR, "Unable to create http listener: no memory");
        return 0;
    }
    li->config = *config;

    sys_atomic_inc(&li->ref_count);
    pn_proactor_listen(qd_server_proactor(li->server), li->pn_listener, li->config.host_port, LISTENER_BACKLOG);
    DEQ_ITEM_INIT(li);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->listeners, li);
    qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Configured HTTP_ADAPTOR listener on %s", (&li->config)->host_port);
    return li;
}


// Management Agent API - Delete
//
void qd_http1_delete_listener(qd_dispatch_t *qd, qd_http_lsnr_t *li)
{
    if (li) {
        if (li->pn_listener) {
            pn_listener_close(li->pn_listener);
            li->pn_listener = 0;
        }
        DEQ_REMOVE(qdr_http1_adaptor->listeners, li);
        qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Deleted HttpListener for %s, %s:%s", li->config.address, li->config.host, li->config.port);
        qd_http_listener_decref(li);
    }
}


////////////////////////////////////////////////////////
// Raw Connector Events
////////////////////////////////////////////////////////


// Raw Connection Initialization
//
static void _setup_client_connection(qdr_http1_connection_t *hconn)
{
    hconn->client.client_ip_addr = qda_raw_conn_get_address(hconn->raw_conn);
    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_INCOMING, //qd_direction_t   dir,
                                                      hconn->client.client_ip_addr,    //const char      *host,
                                                      "",    //const char      *ssl_proto,
                                                      "",    //const char      *ssl_cipher,
                                                      "",    //const char      *user,
                                                      "HTTP/1.x Adaptor",    //const char      *container,
                                                      pn_data(0),     //pn_data_t       *connection_properties,
                                                      0,     //int              ssl_ssf,
                                                      false, //bool             ssl,
                                                      // set if remote is a qdrouter
                                                      0);    //const qdr_router_version_t *version)

    hconn->conn_id = qd_server_allocate_connection_id(hconn->qd_server);
    hconn->qdr_conn = qdr_connection_opened(qdr_http1_adaptor->core,
                                            qdr_http1_adaptor->adaptor,
                                            true,  // incoming
                                            QDR_ROLE_NORMAL,
                                            1,     //cost
                                            hconn->conn_id,
                                            0,  // label
                                            0,  // remote container id
                                            false,  // strip annotations in
                                            false,  // strip annotations out
                                            false,  // allow dynamic link routes
                                            false,  // allow admin status update
                                            250,    // capacity
                                            0,      // vhost
                                            info,
                                            0,      // bind context
                                            0);     // bind token
    qdr_connection_set_context(hconn->qdr_conn, hconn);

    // simulate a client subscription for reply-to
    qdr_terminus_t *dynamic_source = qdr_terminus(0);
    qdr_terminus_set_dynamic(dynamic_source);
    hconn->out_link = qdr_link_first_attach(hconn->qdr_conn,
                                            QD_OUTGOING,
                                            dynamic_source,   //qdr_terminus_t   *source,
                                            qdr_terminus(0),  //qdr_terminus_t   *target,
                                            "http1.client.reply-to", //const char       *name,
                                            0,                  //const char       *terminus_addr,
                                            false,              // no-route
                                            NULL,               // initial delivery
                                            &(hconn->out_link_id));
    qdr_link_set_context(hconn->out_link, hconn);

    // simulate a client publisher link to the HTTP server:
    qdr_terminus_t *target = qdr_terminus(0);
    qdr_terminus_set_address(target, hconn->cfg.address);
    hconn->in_link = qdr_link_first_attach(hconn->qdr_conn,
                                           QD_INCOMING,
                                           qdr_terminus(0),  //qdr_terminus_t   *source,
                                           target,           //qdr_terminus_t   *target,
                                           "http1.client.in", //const char       *name,
                                           0,                //const char       *terminus_addr,
                                           false,
                                           NULL,
                                           &(hconn->in_link_id));
    qdr_link_set_context(hconn->in_link, hconn);

    // wait until the dynamic reply-to address is returned in the second attach
    // to grant buffers to the raw connection
}


// Proton Connection Event Handler
//
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log_source_t *log = qdr_http1_adaptor->log;

    qd_log(log, QD_LOG_DEBUG, "RAW CONNECTION EVENT %s\n", pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        _setup_client_connection(hconn);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ:
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Closed for %s", hconn->conn_id,
               pn_event_type(e) == PN_RAW_CONNECTION_CLOSED_READ
               ? "reading" : "writing");
        pn_raw_connection_close(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qd_log(log, QD_LOG_INFO, "[C%i] Disconnected", hconn->conn_id);
        hconn->raw_conn = 0;
        qdr_http1_connection_free(hconn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need write buffers", hconn->conn_id);
        if (!hconn->close_connection) {
            qdr_http1_request_t *hreq = DEQ_HEAD(hconn->requests);
            if (hreq && DEQ_SIZE(hreq->out_data))
                qdr_http1_write_out_data(hreq);
        }
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need read buffers", hconn->conn_id);
        if (!hconn->close_connection && hconn->in_link_credit > 0)
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Wake-up", hconn->conn_id);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        qd_buffer_list_t blist;
        uintmax_t length;
        qda_raw_conn_get_read_buffers(hconn->raw_conn, &blist, &length);
        if (length) {
            int error = h1_lib_connection_rx_data(hconn->http_conn, &blist, length);
            if (error)
                qdr_http1_close_connection(hconn, "Incoming request message failed to parse");
            else
                qd_log(log, QD_LOG_DEBUG, "[C%i] Read %"PRIuMAX" bytes",
                       hconn->conn_id, length);
        }
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        bool close_conn = false;
        qdr_http1_free_written_buffers(hconn);
        qdr_http1_request_t *hreq = DEQ_HEAD(hconn->requests);
        if (hreq) {
            qdr_http1_write_out_data(hreq);
            if (DEQ_IS_EMPTY(hreq->out_data) && hreq->completed) {
                close_conn = _handle_request_done(hreq);
            }
        }
        if (close_conn)
            qdr_http1_close_connection(hconn, 0);
        break;
    }
    default:
        break;
    }
}




////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
////////////////////////////////////////////////////////


// Decoder callback: send blist buffers to client endpoint
//
static void _client_tx_msg_headers_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *blist, unsigned int len)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    if (!hreq->hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%i] Discarding outgoing data - client connection closed", hreq->hconn->conn_id);
        qd_buffer_list_free_buffers(blist);
        return;
    }

    qdr_http1_write_buffer_list(hreq, blist);
}


// Decoder callback: send body_data buffers to client endpoint
//
static void _client_tx_msg_body_cb(h1_lib_request_state_t *hrs, qd_message_body_data_t *body_data)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    if (!hreq->hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%i] Discarding outgoing data - client connection closed", hreq->hconn->conn_id);
        qd_message_body_data_release(body_data);
        return;
    }

    qdr_http1_write_body_data(hreq, body_data);
}


// Called when decoding an HTTP request from a client.  This indicates the
// start of a new request message.
//
static int _client_rx_request_cb(h1_lib_request_state_t *hrs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor)
{
    h1_lib_connection_t *h1c = h1_lib_request_state_get_connection(hrs);
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*)h1_lib_connection_get_context(h1c);

    if (hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request: method=%s target=%s version=%"PRIi32".%"PRIi32,
               hconn->conn_id, method, target, version_major, version_minor);

    qdr_http1_request_t *creq = new_qdr_http1_request_t();
    ZERO(creq);
    creq->msg_id = hconn->client.next_msg_id++;
    creq->lib_rs = hrs;
    creq->hconn = hconn;
    DEQ_INIT(creq->out_data);
    creq->app_props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(creq->app_props);
    {
        // OASIS specifies this value as "1.1" by default...
        char version[64];
        snprintf(version, 64, "%"PRIi32".%"PRIi32, version_major, version_minor);
        qd_compose_insert_symbol(creq->app_props, REQUEST_HEADER_KEY);
        qd_compose_insert_string(creq->app_props, version);

        qd_compose_insert_symbol(creq->app_props, TARGET_HEADER_KEY);
        qd_compose_insert_string(creq->app_props, target);
    }

    h1_lib_request_state_set_context(hrs, (void*) creq);
    DEQ_INSERT_TAIL(hconn->requests, creq);
    return 0;
}


// Cannot happen for a client connection!
static int _client_rx_response_cb(h1_lib_request_state_t *hrs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor)
{
    return HTTP1_STATUS_BAD_REQ;
}


// called for each decoded HTTP header.
//
static int _client_rx_header_cb(h1_lib_request_state_t *hrs, const char *key, const char *value)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (creq->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request header: key='%s' value='%s'",
               creq->hconn->conn_id, key, value);

    if (strcasecmp(key, "connection") == 0) {
        // We need to filter this header out after we check for the "close"
        // flag.  @TODO(kgiusti): does case matter?  Need to ensure "close" is
        // not part of a larger value
        creq->connection_close_flag = !!strstr(value, "close");
    } else {
        // lump everything else directly into application properties
        qd_compose_insert_symbol(creq->app_props, key);
        qd_compose_insert_string(creq->app_props, value);
    }

    return 0;
}


// Called after the last header is decoded, before decoding any body data.
// At this point there is enough data to start forwarding the message to
// the router.
//
static int _client_rx_headers_done_cb(h1_lib_request_state_t *hrs, bool has_body)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = creq->hconn;

    if (hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request headers done.",
               hconn->conn_id);

    // now that all the headers have been received we can construct
    // the AMQP message

    creq->in_msg = qd_message();

    qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
    qd_compose_start_list(props);

    qd_compose_insert_ulong(props, creq->msg_id);    // message-id
    qd_compose_insert_null(props);                 // user-id
    qd_compose_insert_string(props, hconn->cfg.address); // to
    qd_compose_insert_string(props, h1_lib_request_state_method(hrs));  // subject
    qd_compose_insert_string(props, hconn->client.reply_to_addr);   // reply-to
    qd_compose_end_list(props);

    qd_compose_end_map(creq->app_props);

    qd_message_compose_3(creq->in_msg, props, creq->app_props, !has_body);
    qd_compose_free(props);
    qd_compose_free(creq->app_props);
    creq->app_props = 0;

    // Use up one credit to obtain a delivery and forward to core.  If no
    // credit is available the request is stalled until the core grants more
    // flow.
    if (creq == DEQ_HEAD(hconn->requests) && hconn->in_link_credit > 0) {
        hconn->in_link_credit -= 1;
        creq->request_dlv = qdr_link_deliver(hconn->in_link, creq->in_msg, 0, false, 0, 0, 0, 0);
        qdr_delivery_set_context(creq->request_dlv, (void*) creq);
        qdr_delivery_incref(creq->request_dlv, "referenced by HTTP1 adaptor");
        creq->in_msg = 0;
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int _client_rx_body_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, size_t len,
                              bool more)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qd_message_t *msg = creq->in_msg ? creq->in_msg : qdr_delivery_message(creq->request_dlv);

    if (offset) {
        // dispatch assumes all body data starts at the buffer base so it cannot deal with offsets.
        // Remove the offset by shifting the content of the head buffer forward
        //
        qd_buffer_t *head = DEQ_HEAD(*body);
        memmove(qd_buffer_base(head), qd_buffer_base(head) + offset, qd_buffer_size(head) - offset);
    }

    //
    // Compose a DATA performative for this section of the stream
    //
    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_BODY_DATA, 0);
    qd_compose_insert_binary_buffers(field, body);

    //
    // Extend the streaming message and free the composed field
    //
    qd_message_extend(msg, field);
    qd_compose_free(field);

    //
    // Notify the router that more data is ready to be pushed out on the delivery
    //
    if (!more)
        qd_message_set_receive_complete(msg);

    if (creq->request_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, creq->request_dlv, false);

    return 0;
}


// Called at the completion of request message decoding.
//
static void _client_rx_done_cb(h1_lib_request_state_t *hrs)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qd_message_t *msg = creq->in_msg ? creq->in_msg : qdr_delivery_message(creq->request_dlv);

    if (creq->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request receive complete.",
               creq->hconn->conn_id);

    if (!qd_message_receive_complete(msg)) {
        qd_message_set_receive_complete(msg);
        if (creq->request_dlv) {
            qdr_delivery_continue(qdr_http1_adaptor->core, creq->request_dlv, false);
        }
    }

    // if a disposition update occurred during request processing
    // handle it now
    if (creq->in_dispo)
        _handle_request_disposition_update(creq);
}


// The library has completed processing the request and response.  This means
// that the request has been forwarded into the core and a response has been
// completely decoded (though may still be being written out the raw
// connection).
static void _client_request_complete_cb(h1_lib_request_state_t *lib_rs, bool cancelled)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(lib_rs);
    if (hreq) {
        qdr_http1_connection_t *hconn = hreq->hconn;

        hreq->lib_rs = 0;  // freed on return from this call
        hreq->completed = true;

        if (hreq->request_dlv) {
            if (hconn->in_link) {
                // can settle the request delivery now that the response
                // message has been completed.
                qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                  hreq->request_dlv,
                                                  0,      // remote dispo
                                                  true,   // settled
                                                  0,      // error
                                                  0,      // dispo data
                                                  false);
            }
            qdr_delivery_set_context(hreq->request_dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 adaptor request settled");
            hreq->request_dlv = 0;
        }

        if (hreq->response_dlv) {
            if (hconn->out_link) {
                // complete the outbound response delivery
                qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                  hreq->response_dlv,
                                                  hreq->out_dispo, // remote dispo
                                                  true,   // settled
                                                  0,      // error
                                                  0,      // dispo data
                                                  false);
            }
            qdr_delivery_set_context(hreq->response_dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, hreq->response_dlv, "HTTP1 adaptor response settled");
            hreq->response_dlv = 0;
        }

        bool close_conn = _handle_request_done(hreq);
        if (close_conn)
            qdr_http1_close_connection(hconn, 0);
    }
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


void qdr_http1_client_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit)
{
    assert(link == hconn->in_link);   // router only grants flow on incoming link

    hconn->in_link_credit += credit;
    if (hconn->in_link_credit > 0) {

        if (hconn->raw_conn)
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);

        // is the current request message blocked by lack of credit?

        qdr_http1_request_t *creq = DEQ_HEAD(hconn->requests);
        if (creq && creq->in_msg) {
            assert(!creq->request_dlv);
            hconn->in_link_credit -= 1;
            creq->request_dlv = qdr_link_deliver(hconn->in_link, creq->in_msg, 0, false, 0, 0, 0, 0);
            qdr_delivery_set_context(creq->request_dlv, (void*) creq);
            qdr_delivery_incref(creq->request_dlv, "referenced by HTTP1 adaptor");
            creq->in_msg = 0;
        }
    }

    // adjust the flow on the response link so we can accept as many responses
    // as we're allowed requests.  Add extra to account for multiple
    // "non-terminal" (code 1xx) responses for any one request.
    credit *= 2;
    hconn->out_link_credit += credit;
    qdr_link_flow(adaptor->core, link, credit, false);  // will activate conn
}


// Handle disposition/settlement update for the outstanding request msg
//
void qdr_http1_client_core_delivery_update(qdr_http1_adaptor_t    *adaptor,
                                           qdr_http1_connection_t *hconn,
                                           qdr_http1_request_t    *hreq,
                                           qdr_delivery_t         *dlv,
                                           uint64_t                disp,
                                           bool                    settled)
{
    qd_message_t *msg = qdr_delivery_message(dlv);
    assert(dlv == hreq->request_dlv);
    if (!hreq->in_dispo && disp && disp != PN_RECEIVED) {
        // terminal disposition
        hreq->in_dispo = disp;
        if (qd_message_receive_complete(msg)) {
            _handle_request_disposition_update(hreq);
        }
    }
}


//
// Response message forwarding
//


// use the correlation ID from the AMQP message containing the response to look
// up the original request context
//
static qdr_http1_request_t *_lookup_request_context(qdr_http1_connection_t *hconn,
                                                    qd_message_t *msg)
{
    qdr_http1_request_t *req = 0;

    qd_iterator_t *cid_iter = qd_message_field_iterator(msg, QD_FIELD_CORRELATION_ID);
    if (cid_iter) {
        qd_parsed_field_t *cid_pf = qd_parse(cid_iter);
        if (cid_pf) {
            uint64_t cid = qd_parse_as_ulong(cid_pf);
            if (qd_parse_ok(cid_pf)) {
                req = DEQ_HEAD(hconn->requests);
                while (req) {
                    if (req->msg_id == cid)
                        break;
                    req = DEQ_NEXT(req);
                }
            }
            qd_parse_free(cid_pf);
        }
        qd_iterator_free(cid_iter);
    }
    return req;
}


// Send the response status and all HTTP headers to the client
// The message has been validated to app properties depth
//
static bool _send_response_headers(qdr_http1_request_t *req)
{
    bool ok = false;
    qd_message_t *msg = qdr_delivery_message(req->response_dlv);
    qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (app_props_iter) {
        qd_parsed_field_t *app_props = qd_parse(app_props_iter);
        if (app_props && qd_parse_is_map(app_props)) {
            qd_parsed_field_t *tmp = qd_parse_value_by_key(app_props, STATUS_HEADER_KEY);
            if (tmp) {
                int32_t status_code = qd_parse_as_int(tmp);
                if (qd_parse_ok(tmp)) {

                    // the value for RESPONSE_HEADER_KEY is optional and is set
                    // to a string representation of the version of the server
                    // (e.g. "1.1"
                    uint32_t major = 1;
                    uint32_t minor = 1;
                    tmp = qd_parse_value_by_key(app_props, RESPONSE_HEADER_KEY);
                    if (tmp) {
                        char *version_str = (char*) qd_iterator_copy(qd_parse_raw(tmp));
                        if (version_str) {
                            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
                            free(version_str);
                        }
                    }
                    char *reason_str = 0;
                    tmp = qd_parse_value_by_key(app_props, REASON_HEADER_KEY);
                    if (tmp) {
                        reason_str = (char*) qd_iterator_copy(qd_parse_raw(tmp));
                    }

                    ok = !h1_lib_tx_response(req->lib_rs, (int)status_code, reason_str, major, minor);
                    free(reason_str);

                    // now send all headers in app properties
                    qd_parsed_field_t *key = qd_field_first_child(app_props);
                    while (ok && key) {
                        qd_parsed_field_t *value = qd_field_next_child(key);
                        if (!value)
                            break;

                        qd_iterator_t *i_key = qd_parse_raw(key);
                        if (!i_key)
                            break;

                        // ignore the special headers added by the mapping
                        if (!qd_iterator_prefix(i_key, HTTP1_HEADER_PREFIX)) {
                            qd_iterator_t *i_value = qd_parse_raw(value);
                            if (!i_value)
                                break;

                            char *header_key = (char*) qd_iterator_copy(i_key);
                            char *header_value = (char*) qd_iterator_copy(i_value);

                            ok = !h1_lib_tx_add_header(req->lib_rs, header_key, header_value);

                            free(header_key);
                            free(header_value);
                        }

                        key = qd_field_next_child(value);
                    }
                }
            }
        }
        qd_parse_free(app_props);
        qd_iterator_free(app_props_iter);
    }

    return ok;
}


static uint64_t _decode_response_message(qdr_http1_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->hconn;
    qd_message_t *msg = qdr_delivery_message(hreq->response_dlv);
    qd_message_depth_status_t status = qd_message_check_depth(msg, QD_DEPTH_BODY);

    if (status == QD_MESSAGE_DEPTH_INCOMPLETE)
        return 0;

    if (status == QD_MESSAGE_DEPTH_INVALID) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] body data depth check failed",
               hconn->conn_id, hconn->out_link_id);
        return PN_REJECTED;
    }

    assert(status == QD_MESSAGE_DEPTH_OK);
    // not responding to current request?
    assert(hreq == DEQ_HEAD(hreq->hconn->requests));

    if (!hreq->headers_sent) {
        hreq->headers_sent = true;
        if (!_send_response_headers(hreq)) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] message headers malformed - discarding.",
                   hconn->conn_id, hconn->out_link_id);
            return PN_REJECTED;
        }
    }

    qd_message_body_data_t *body_data = 0;

    while (true) {
        switch (qd_message_next_body_data(msg, &body_data)) {

        case QD_MESSAGE_BODY_DATA_OK:
            if (h1_lib_tx_body(hreq->lib_rs, body_data)) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] body data encode failed",
                       hconn->conn_id, hconn->out_link_id);
                return PN_REJECTED;
            }
            break;

        case QD_MESSAGE_BODY_DATA_NO_MORE:
            // indicate this message is complete
            return PN_ACCEPTED;

        case QD_MESSAGE_BODY_DATA_INCOMPLETE:
            return 0;  // wait for more

        case QD_MESSAGE_BODY_DATA_INVALID:
        case QD_MESSAGE_BODY_DATA_NOT_DATA:
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Rejecting corrupted body data.",
                   hconn->conn_id, hconn->out_link_id);
            return PN_REJECTED;
        }
    }
}


// The I/O thread wants to send this delivery out the link
//
uint64_t qdr_http1_client_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled)
{
    qd_message_t        *msg = qdr_delivery_message(delivery);
    qdr_http1_request_t *req = (qdr_http1_request_t*) qdr_delivery_get_context(delivery);
    if (!req) {
        // new delivery - look for corresponding request via correlation_id
        switch (qd_message_check_depth(msg, QD_DEPTH_PROPERTIES)) {
        case QD_MESSAGE_DEPTH_INCOMPLETE:
            return 0;

        case QD_MESSAGE_DEPTH_INVALID:
            // @TODO(kgiusti) how to handle this?
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Malformed HTTP/1.x message",
                   hconn->conn_id, link->identity);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            return PN_REJECTED;

        case QD_MESSAGE_DEPTH_OK:
            req = _lookup_request_context(hconn, msg);
            if (!req) {
                // No corresponding request found
                // @TODO(kgiusti) how to handle this?  - simply discard?
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Discarding malformed message.", hconn->conn_id, link->identity);
                qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
                return PN_REJECTED;
            }

            // link request state and delivery
            assert(!req->response_dlv);
            req->response_dlv = delivery;
            qdr_delivery_set_context(delivery, req);
            qdr_delivery_incref(delivery, "referenced by HTTP1 adaptor");
            break;
        }
    }

    // responses must be written out in order with respect to requests

    if (req == DEQ_HEAD(hconn->requests)) {
        req->out_dispo = _decode_response_message(req);
        if (req->out_dispo) {
            qd_message_set_send_complete(msg);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            h1_lib_tx_done(req->lib_rs);
            // note: disposition and settlement is handled in _client_request_complete_cb()
        }
    }

    return 0;
}



