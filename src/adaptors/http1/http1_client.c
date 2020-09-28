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


#define DEFAULT_CAPACITY 250
#define LISTENER_BACKLOG  16


static void _client_tx_buffers_cb(h1_codec_request_state_t *lib_hrs, qd_buffer_list_t *blist, unsigned int len);
static void _client_tx_body_data_cb(h1_codec_request_state_t *lib_hrs, qd_message_body_data_t *body_data);
static int _client_rx_request_cb(h1_codec_request_state_t *lib_rs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor);
static int _client_rx_response_cb(h1_codec_request_state_t *lib_rs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor);
static int _client_rx_header_cb(h1_codec_request_state_t *lib_rs, const char *key, const char *value);
static int _client_rx_headers_done_cb(h1_codec_request_state_t *lib_rs, bool has_body);
static int _client_rx_body_cb(h1_codec_request_state_t *lib_rs, qd_buffer_list_t *body, size_t offset, uintmax_t len,
                              bool more);
static void _client_rx_done_cb(h1_codec_request_state_t *lib_rs);
static void _client_request_complete_cb(h1_codec_request_state_t *lib_rs, bool cancelled);
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);


////////////////////////////////////////////////////////
// HTTP/1.x Client Listener
////////////////////////////////////////////////////////


// Listener received connection request from client
//
static qdr_http1_connection_t *_create_client_connection(qd_http_lsnr_t *li)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();

    ZERO(hconn);
    hconn->type = HTTP1_CONN_CLIENT;
    hconn->qd_server = li->server;
    hconn->adaptor = qdr_http1_adaptor;
    hconn->handler_context.handler = &_handle_connection_events;
    hconn->handler_context.context = hconn;

    hconn->client.next_msg_id = 99383939;

    // configure the HTTP/1.x library

    h1_codec_config_t config = {0};
    config.type             = HTTP1_CONN_CLIENT;
    config.tx_buffers       = _client_tx_buffers_cb;
    config.tx_body_data     = _client_tx_body_data_cb;
    config.rx_request       = _client_rx_request_cb;
    config.rx_response      = _client_rx_response_cb;
    config.rx_header        = _client_rx_header_cb;
    config.rx_headers_done  = _client_rx_headers_done_cb;
    config.rx_body          = _client_rx_body_cb;
    config.rx_done          = _client_rx_done_cb;
    config.request_complete = _client_request_complete_cb;

    hconn->http_conn = h1_codec_connection(&config, hconn);
    if (!hconn->http_conn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
               "Failed to initialize HTTP/1.x library - connection refused.");
        qdr_http1_connection_free(hconn);
        return 0;
    }

    hconn->cfg.host = qd_strdup(li->config.host);
    hconn->cfg.port = qd_strdup(li->config.port);
    hconn->cfg.address = qd_strdup(li->config.address);
    hconn->cfg.site = qd_strdup(li->config.site);

    hconn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(hconn->raw_conn, &hconn->handler_context);

    // we'll create a QDR connection and links once the raw connection activates
    return hconn;
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
        qdr_http1_connection_t *hconn = _create_client_connection(li);
        if (hconn) {
            // Note: the proactor may schedule the hconn on another thread
            // during this call!
            pn_listener_raw_accept(li->pn_listener, hconn->raw_conn);
        }
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
    DEQ_ITEM_INIT(li);
    DEQ_INSERT_TAIL(qdr_http1_adaptor->listeners, li);
    qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Configured HTTP_ADAPTOR listener on %s", (&li->config)->host_port);
    // Note: the proactor may schedule the pn_listener on another thread during this call
    pn_proactor_listen(qd_server_proactor(li->server), li->pn_listener, li->config.host_port, LISTENER_BACKLOG);
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
                                            DEFAULT_CAPACITY,
                                            0,      // vhost
                                            info,
                                            0,      // bind context
                                            0);     // bind token
    qdr_connection_set_context(hconn->qdr_conn, hconn);

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP connection to client created", hconn->conn_id);

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

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] HTTP client response link created",
           hconn->conn_id, hconn->out_link_id);

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

    qd_log(hconn->adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] HTTP client request link created",
           hconn->conn_id, hconn->in_link_id);

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

    if (!hconn) return;

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
        return;  // hconn no longer valid
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need write buffers", hconn->conn_id);
        if (!hconn->close_connection) {
            int written = qdr_http1_write_out_data(hconn);
            qd_log(log, QD_LOG_DEBUG, "[C%i] %d buffers written",
                   hconn->conn_id, written);
        }
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] Need read buffers", hconn->conn_id);
        // @TODO(kgiusti): backpressure if no credit
        if (hconn->client.reply_to_addr && !hconn->close_connection /* && hconn->in_link_credit > 0 */) {
            int granted = qda_raw_conn_grant_read_buffers(hconn->raw_conn);
            qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] %d read buffers granted",
                   hconn->conn_id, granted);
        }
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] Wake-up", hconn->conn_id);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] Processing done", hconn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        qd_buffer_list_t blist;
        uintmax_t length;
        qda_raw_conn_get_read_buffers(hconn->raw_conn, &blist, &length);
        if (length) {
            qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"][L%"PRIu64"] Read %"PRIuMAX" bytes from client",
                   hconn->conn_id, hconn->in_link_id, length);
            hconn->in_http1_octets += length;
            int error = h1_codec_connection_rx_data(hconn->http_conn, &blist, length);
            if (error)
                qdr_http1_close_connection(hconn, "Incoming request message failed to parse");
        }
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        // Response message data written
        qdr_http1_free_written_buffers(hconn);
        break;
    }
    default:
        break;
    }

    // Check the head request for completion and advance to next request if
    // done.

    // remove me:
    if (hconn) {
        qdr_http1_request_t *hreq = DEQ_HEAD(hconn->requests);
        if (hreq) {
            qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP is client request complete????", hconn->conn_id);
            qd_log(log, QD_LOG_DEBUG, "   codec=%s req-dlv=%p resp-dlv=%d req_msg=%p %s",
                   hreq->codec_completed ? "Done" : "Not Done",
                   (void*)hreq->request_dlv,
                   (int)DEQ_SIZE(hreq->responses),
                   (void*)hreq->request_msg,
                   hreq->cancelled ? "Cancelled" : "Not Cancelled");
        }
    }


    qdr_http1_request_t *hreq = hconn ? DEQ_HEAD(hconn->requests) : 0;
    if (hreq && qdr_http1_request_complete(hreq)) {

        qd_log(log, QD_LOG_DEBUG, "[C%"PRIu64"] HTTP request completed!", hconn->conn_id);

        bool need_close = hreq->close_on_complete;
        qdr_http1_request_free(hreq);

        if (need_close)
            qdr_http1_close_connection(hconn, "Connection: close");
        else {
            hreq = DEQ_HEAD(hconn->requests);
            if (hreq && hconn->in_link_credit > 0 && hreq->request_msg) {

                qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                       "[C%"PRIu64"][L%"PRIu64"] Delivering request to router",
                       hconn->conn_id, hconn->in_link_id);

                hconn->in_link_credit -= 1;
                hreq->request_dlv = qdr_link_deliver(hconn->in_link, hreq->request_msg, 0, false, 0, 0, 0, 0);
                qdr_delivery_set_context(hreq->request_dlv, (void*) hreq);
                qdr_delivery_incref(hreq->request_dlv, "referenced by HTTP1 adaptor");
                hreq->request_msg = 0;
            }
        }
    }
}




////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
////////////////////////////////////////////////////////


// Decoder callback: send blist buffers to client endpoint
//
static void _client_tx_buffers_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *blist, unsigned int len)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    if (!hreq->hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%i] Discarding outgoing data - client connection closed", hreq->hconn->conn_id);
        qd_buffer_list_free_buffers(blist);
        return;
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] Sending %u octets to client",
           hreq->hconn->conn_id, hreq->hconn->out_link_id, len);
    qdr_http1_write_buffer_list(hreq, blist);
}


// Decoder callback: send body_data buffers to client endpoint
//
static void _client_tx_body_data_cb(h1_codec_request_state_t *hrs, qd_message_body_data_t *body_data)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    if (!hreq->hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%i] Discarding outgoing data - client connection closed", hreq->hconn->conn_id);
        qd_message_body_data_release(body_data);
        return;
    }

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] Sending body data to client",
           hreq->hconn->conn_id, hreq->hconn->out_link_id);
    qdr_http1_write_body_data(hreq, body_data);
}


// Called when decoding an HTTP request from a client.  This indicates the
// start of a new request message.
//
static int _client_rx_request_cb(h1_codec_request_state_t *hrs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor)
{
    h1_codec_connection_t *h1c = h1_codec_request_state_get_connection(hrs);
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*)h1_codec_connection_get_context(h1c);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"] HTTP request received: method=%s target=%s version=%"PRIi32".%"PRIi32,
           hconn->conn_id, method, target, version_major, version_minor);

    qdr_http1_request_t *creq = new_qdr_http1_request_t();
    ZERO(creq);
    creq->start = qd_timer_now();
    creq->msg_id = hconn->client.next_msg_id++;
    creq->lib_rs = hrs;
    creq->hconn = hconn;
    DEQ_INIT(creq->out_fifo);
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

    h1_codec_request_state_set_context(hrs, (void*) creq);
    DEQ_INSERT_TAIL(hconn->requests, creq);
    return 0;
}


// Cannot happen for a client connection!
static int _client_rx_response_cb(h1_codec_request_state_t *hrs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
           "[C%"PRIu64"][L%"PRIu64"] Spurious HTTP response received from client",
           hconn->conn_id, hconn->in_link_id);
    return HTTP1_STATUS_BAD_REQ;
}


// called for each decoded HTTP header.
//
static int _client_rx_header_cb(h1_codec_request_state_t *hrs, const char *key, const char *value)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request header received: key='%s' value='%s'",
           creq->hconn->conn_id, creq->hconn->in_link_id, key, value);

    if (strcasecmp(key, "connection") == 0) {
        // We need to filter the connection header out.  See if client
        // requested 'close' - this means it expects us to close the connection
        // when the response has been sent
        //
        // @TODO(kgiusti): also have to remove other headers given in value!
        //
        size_t len;
        const char *token = qdr_http1_token_list_next(value, &len, &value);
        while (token) {
            if (len == 5 && strncasecmp(token, "close", 5) == 0) {
                creq->close_on_complete = true;
                break;
            }
            token = qdr_http1_token_list_next(value, &len, &value);
        }

    } else {
        qd_compose_insert_symbol(creq->app_props, key);
        qd_compose_insert_string(creq->app_props, value);
    }

    return 0;
}


// Called after the last header is decoded, before decoding any body data.
// At this point there is enough data to start forwarding the message to
// the router.
//
static int _client_rx_headers_done_cb(h1_codec_request_state_t *hrs, bool has_body)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->hconn;

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request headers done.",
           hconn->conn_id, hconn->in_link_id);

    // now that all the headers have been received we can construct
    // the AMQP message

    hreq->request_msg = qd_message();

    qd_composed_field_t *hdrs = qd_compose(QD_PERFORMATIVE_HEADER, 0);
    qd_compose_start_list(hdrs);
    qd_compose_insert_bool(hdrs, 0);     // durable
    qd_compose_insert_null(hdrs);        // priority
    //qd_compose_insert_null(hdrs);        // ttl
    //qd_compose_insert_bool(hdrs, 0);     // first-acquirer
    //qd_compose_insert_uint(hdrs, 0);     // delivery-count
    qd_compose_end_list(hdrs);

    qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, hdrs);
    qd_compose_start_list(props);

    qd_compose_insert_ulong(props, hreq->msg_id);    // message-id
    qd_compose_insert_null(props);                 // user-id
    qd_compose_insert_string(props, hconn->cfg.address); // to
    qd_compose_insert_string(props, h1_codec_request_state_method(hrs));  // subject
    qd_compose_insert_string(props, hconn->client.reply_to_addr);   // reply-to
    qd_compose_insert_null(props);                      // correlation-id
    qd_compose_insert_null(props);                      // content-type
    qd_compose_insert_null(props);                      // content-encoding
    qd_compose_insert_null(props);                      // absolute-expiry-time
    qd_compose_insert_null(props);                      // creation-time
    qd_compose_insert_string(props, hconn->cfg.site);   // group-id

    assert(hconn->client.reply_to_addr && strlen(hconn->client.reply_to_addr));  //remove me
    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "REPLY-TO: %s", hconn->client.reply_to_addr);

    qd_compose_end_list(props);

    qd_compose_end_map(hreq->app_props);

    if (!has_body) {
        // @TODO(kgiusti): fixme: tack on an empty body data performative.  The
        // message decoder will barf otherwise
        qd_buffer_list_t empty = DEQ_EMPTY;
        hreq->app_props = qd_compose(QD_PERFORMATIVE_BODY_DATA, hreq->app_props);
        qd_compose_insert_binary_buffers(hreq->app_props, &empty);
    }

    qd_message_compose_3(hreq->request_msg, props, hreq->app_props, !has_body);
    qd_compose_free(props);
    qd_compose_free(hreq->app_props);
    hreq->app_props = 0;

    // Use up one credit to obtain a delivery and forward to core.  If no
    // credit is available the request is stalled until the core grants more
    // flow.
    if (hreq == DEQ_HEAD(hconn->requests) && hconn->in_link_credit > 0) {
        hconn->in_link_credit -= 1;

        qd_log(hconn->adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"][L%"PRIu64"] Delivering request to router",
               hconn->conn_id, hconn->in_link_id);

        hreq->request_dlv = qdr_link_deliver(hconn->in_link, hreq->request_msg, 0, false, 0, 0, 0, 0);
        qdr_delivery_set_context(hreq->request_dlv, (void*) hreq);
        qdr_delivery_incref(hreq->request_dlv, "referenced by HTTP1 adaptor");
        hreq->request_msg = 0;
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int _client_rx_body_cb(h1_codec_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, size_t len,
                              bool more)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    qd_message_t *msg = hreq->request_msg ? hreq->request_msg : qdr_delivery_message(hreq->request_dlv);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request body received len=%zu.",
           hreq->hconn->conn_id, hreq->hconn->in_link_id, len);

    if (offset) {
        // dispatch assumes all body data starts at the buffer base so it cannot deal with offsets.
        // Remove the offset by shifting the content of the head buffer forward
        //
        qd_buffer_t *head = DEQ_HEAD(*body);
        memmove(qd_buffer_base(head), qd_buffer_base(head) + offset, qd_buffer_size(head) - offset);
        head->size -= offset;
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

    if (hreq->request_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, hreq->request_dlv, false);

    return 0;
}


// Called at the completion of request message decoding.
//
static void _client_rx_done_cb(h1_codec_request_state_t *hrs)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(hrs);
    qd_message_t *msg = hreq->request_msg ? hreq->request_msg : qdr_delivery_message(hreq->request_dlv);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request receive complete.",
           hreq->hconn->conn_id, hreq->hconn->in_link_id);

    if (!qd_message_receive_complete(msg)) {
        qd_message_set_receive_complete(msg);
        if (hreq->request_dlv) {
            qdr_delivery_continue(qdr_http1_adaptor->core, hreq->request_dlv, false);
        }
    }
}


// The coded has completed processing the request and response messages.
//
static void _client_request_complete_cb(h1_codec_request_state_t *lib_rs, bool cancelled)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_codec_request_state_get_context(lib_rs);
    if (hreq) {
        hreq->lib_rs = 0;  // freed on return from this call
        hreq->codec_completed = true;
        hreq->cancelled = hreq->cancelled || cancelled;

        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request/response %s.", hreq->hconn->conn_id,
               cancelled ? "cancelled!" : "codec done");
    }
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


void qdr_http1_client_core_second_attach(qdr_http1_adaptor_t    *adaptor,
                                         qdr_http1_connection_t *hconn,
                                         qdr_link_t             *link,
                                         qdr_terminus_t         *source,
                                         qdr_terminus_t         *target)
{
    if (link == hconn->out_link) {
        // this is the reply-to link for the client
        qd_iterator_t *reply_iter = qdr_terminus_get_address(source);
        hconn->client.reply_to_addr = (char*) qd_iterator_copy(reply_iter);

        assert(hconn->client.reply_to_addr);
        
        hconn->out_link_credit += DEFAULT_CAPACITY;
        qdr_link_flow(adaptor->core, link, DEFAULT_CAPACITY, false);
    }
}


void qdr_http1_client_core_link_flow(qdr_http1_adaptor_t    *adaptor,
                                     qdr_http1_connection_t *hconn,
                                     qdr_link_t             *link,
                                     int                     credit)
{
    qd_log(adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] Credit granted on request link %d",
           hconn->conn_id, hconn->in_link_id, credit);

    assert(link == hconn->in_link);   // router only grants flow on incoming link

    hconn->in_link_credit += credit;
    if (hconn->in_link_credit > 0) {

        if (hconn->raw_conn) {
            int granted = qda_raw_conn_grant_read_buffers(hconn->raw_conn);
            qd_log(adaptor->log, QD_LOG_DEBUG,
                   "[C%"PRIu64"] %d read buffers granted",
                   hconn->conn_id, granted);
        }

        // is the current request message blocked by lack of credit?

        qdr_http1_request_t *hreq = DEQ_HEAD(hconn->requests);
        if (hreq && hreq->request_msg) {
            assert(!hreq->request_dlv);
            hconn->in_link_credit -= 1;

            qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Delivering request to router",
                   hconn->conn_id, hconn->in_link_id);

            hreq->request_dlv = qdr_link_deliver(hconn->in_link, hreq->request_msg, 0, false, 0, 0, 0, 0);
            qdr_delivery_set_context(hreq->request_dlv, (void*) hreq);
            qdr_delivery_incref(hreq->request_dlv, "referenced by HTTP1 adaptor");
            hreq->request_msg = 0;
        }
    }
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
    assert(dlv == hreq->request_dlv);

    qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
           "[C%"PRIu64"][L%"PRIu64"] HTTP request delivery update, outcome=0x%"PRIx64"%s",
           hconn->conn_id, hconn->in_link_id, disp, settled ? " settled" : "");

    if (settled) {
        qdr_delivery_set_context(hreq->request_dlv, 0);
        qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 adaptor request settled");
        hreq->request_dlv = 0;
    }

    if (disp && disp != PN_RECEIVED && hreq->in_dispo == 0) {
        // terminal disposition
        hreq->in_dispo = disp;
        if (disp != PN_ACCEPTED) {
            // no response message is going to arrive.  Now what?  For now fake
            // a reply from the server by using the codec to write an error
            // response on the behalf of the server.
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] HTTP request failure, outcome=0x%"PRIx64,
                   hconn->conn_id, hconn->in_link_id, disp);
            if (disp == PN_REJECTED) {
                qdr_http1_error_response(hreq, 400, "Bad Request");
            } else {
                // total guess as to what the proper error code should be
                qdr_http1_error_response(hreq, 503, "Service Unavailable");
            }
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

    qd_parsed_field_t *cid_pf = 0;
    qd_iterator_t *cid_iter = qd_message_field_iterator_typed(msg, QD_FIELD_CORRELATION_ID);
    if (cid_iter) {
        cid_pf = qd_parse(cid_iter);
        if (cid_pf && qd_parse_ok(cid_pf)) {
            uint64_t cid = qd_parse_as_ulong(cid_pf);
            if (qd_parse_ok(cid_pf)) {
                req = DEQ_HEAD(hconn->requests);
                while (req) {
                    if (req->msg_id == cid)
                        break;
                    req = DEQ_NEXT(req);
                }
            }
        }
    }

    qd_parse_free(cid_pf);
    qd_iterator_free(cid_iter);

    return req;
}


// Encode the response status and all HTTP headers.
// The message has been validated to app properties depth
//
static bool _encode_response_headers(qdr_http1_request_t *hreq,
                                     qdr_http1_response_msg_t *rmsg)
{
    bool ok = false;
    qd_message_t *msg = qdr_delivery_message(rmsg->dlv);

    qd_iterator_t *group_id_itr = qd_message_field_iterator(msg, QD_FIELD_GROUP_ID);
    hreq->site = (char*) qd_iterator_copy(group_id_itr);
    qd_iterator_free(group_id_itr);

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

                    qd_log(hreq->hconn->adaptor->log, QD_LOG_TRACE,
                           "[C%"PRIu64"][L%"PRIu64"] Encoding response %d %s",
                           hreq->hconn->conn_id, hreq->hconn->out_link_id, (int)status_code,
                           reason_str ? reason_str : "");

                    ok = !h1_codec_tx_response(hreq->lib_rs, (int)status_code, reason_str, major, minor);
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


                            qd_log(hreq->hconn->adaptor->log, QD_LOG_TRACE,
                                   "[C%"PRIu64"][L%"PRIu64"] Encoding response header %s:%s",
                                   hreq->hconn->conn_id, hreq->hconn->out_link_id,
                                   header_key, header_value);

                            ok = !h1_codec_tx_add_header(hreq->lib_rs, header_key, header_value);

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


static uint64_t _encode_response_message(qdr_http1_request_t *hreq,
                                         qdr_http1_response_msg_t *rmsg)
{
    qdr_http1_connection_t *hconn = hreq->hconn;
    qd_message_t *msg = qdr_delivery_message(rmsg->dlv);
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

    if (!rmsg->headers_encoded) {
        rmsg->headers_encoded = true;
        if (!_encode_response_headers(hreq, rmsg)) {
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

            qd_log(hconn->adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"][L%"PRIu64"] Encoding response body data",
                   hconn->conn_id, hconn->out_link_id);

            if (h1_codec_tx_body(hreq->lib_rs, body_data)) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] body data encode failed",
                       hconn->conn_id, hconn->out_link_id);
                return PN_REJECTED;
            }
            break;

        case QD_MESSAGE_BODY_DATA_NO_MORE:
            // indicate this message is complete
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                   "[C%"PRIu64"][L%"PRIu64"] response message encoding completed",
                   hconn->conn_id, hconn->out_link_id);
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


// The I/O thread wants to send this delivery containing the response out the
// link.  It is unlikely that the parsing of this message will fail since the
// message was constructed by the ingress router.  However if the message fails
// to parse then there is probably no recovering as the client will now be out
// of sync.  For now close the connection if an error occurs.
//
uint64_t qdr_http1_client_core_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                            qdr_http1_connection_t *hconn,
                                            qdr_link_t             *link,
                                            qdr_delivery_t         *delivery,
                                            bool                    settled)
{
    qd_message_t        *msg = qdr_delivery_message(delivery);
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) qdr_delivery_get_context(delivery);
    if (!hreq) {
        // new delivery - look for corresponding request via correlation_id
        switch (qd_message_check_depth(msg, QD_DEPTH_PROPERTIES)) {
        case QD_MESSAGE_DEPTH_INCOMPLETE:
            return 0;

        case QD_MESSAGE_DEPTH_INVALID:
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Malformed HTTP/1.x message",
                   hconn->conn_id, link->identity);
            qd_message_set_send_complete(msg);
            qdr_http1_close_connection(hconn, "Malformed response message");
            return PN_REJECTED;

        case QD_MESSAGE_DEPTH_OK:
            hreq = _lookup_request_context(hconn, msg);
            if (!hreq) {
                // No corresponding request found
                // @TODO(kgiusti) how to handle this?  - simply discard?
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] Discarding malformed message.", hconn->conn_id, link->identity);
                qd_message_set_send_complete(msg);
                qdr_http1_close_connection(hconn, "Cannot correlate response message");
                return PN_REJECTED;
            }

            // link request state and delivery
            qdr_http1_response_msg_t *rmsg = new_qdr_http1_response_msg_t();
            ZERO(rmsg);
            rmsg->dlv = delivery;
            qdr_delivery_set_context(delivery, hreq);
            qdr_delivery_incref(delivery, "referenced by HTTP1 adaptor");
            DEQ_INSERT_TAIL(hreq->responses, rmsg);
            break;
        }
    }

    // expect: responses arrive in order and are completely received before
    // another can arrive!  The current delivery should be on the tail
    qdr_http1_response_msg_t *rmsg = DEQ_TAIL(hreq->responses);
    assert(rmsg && rmsg->dlv == delivery);

    rmsg->dispo = _encode_response_message(hreq, rmsg);
    hreq->stop = qd_timer_now();
    qdr_http1_record_client_request_info(adaptor, hreq, rmsg);
    if (rmsg->dispo == PN_ACCEPTED) {
        bool need_close = false;
        qd_message_set_send_complete(msg);
        qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
        h1_codec_tx_done(hreq->lib_rs, &need_close);
        hreq->close_on_complete = need_close || hreq->close_on_complete;

        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"][L%"PRIu64"] HTTP client settling response, dispo=0x%"PRIx64,
               hreq->hconn->conn_id, hreq->hconn->out_link_id, rmsg->dispo);

        qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                          rmsg->dlv,
                                          rmsg->dispo,
                                          true,   // settled
                                          0,      // error
                                          0,      // dispo data
                                          false);
        DEQ_REMOVE(hreq->responses, rmsg);
        qdr_delivery_set_context(rmsg->dlv, 0);
        qdr_delivery_decref(qdr_http1_adaptor->core, rmsg->dlv, "HTTP1 adaptor response settled");
        free_qdr_http1_response_msg_t(rmsg);

    } else if (rmsg->dispo) {

        // returning a non-zero value will cause the delivery to be settled, so
        // drop our reference.
        DEQ_REMOVE(hreq->responses, rmsg);
        qdr_delivery_set_context(rmsg->dlv, 0);
        qdr_delivery_decref(qdr_http1_adaptor->core, rmsg->dlv, "HTTP1 adaptor invalid response");
        uint64_t dispo = rmsg->dispo;
        free_qdr_http1_response_msg_t(rmsg);

        qd_message_set_send_complete(msg);
        qdr_http1_close_connection(hconn, "Cannot parse response message");
        return dispo;
    }

    return 0;
}
