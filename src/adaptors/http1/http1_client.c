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

// callbacks from HTTP1 decoder (incoming HTTP requests)
static void client_tx_data_cb(h1_lib_connection_t *lib_hconn, qd_buffer_list_t *blist, size_t offset, unsigned int len);
static int client_rx_request_cb(h1_lib_request_state_t *lib_rs,
                                const char *method,
                                const char *target,
                                uint32_t version_major,
                                uint32_t version_minor);
static int client_rx_response_cb(h1_lib_request_state_t *lib_rs,
                                 int status_code,
                                 const char *reason_phrase,
                                 uint32_t version_major,
                                 uint32_t version_minor);
static int client_rx_header_cb(h1_lib_request_state_t *lib_rs, const char *key, const char *value);
static int client_rx_headers_done_cb(h1_lib_request_state_t *lib_rs, bool has_body);
static int client_rx_body_cb(h1_lib_request_state_t *lib_rs, qd_buffer_list_t *body, size_t offset, uintmax_t len,
                             bool more);
static void client_rx_done_cb(h1_lib_request_state_t *lib_rs);
static void client_request_complete_cb(h1_lib_request_state_t *lib_rs);

static void handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);





////////////////////////////////////////////////////////
// HTTP/1.x Client Listener
////////////////////////////////////////////////////////


// Listener received connection request from client
//
static void accept_client_connection(qd_http_lsnr_t *li)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();

    ZERO(hconn);
    hconn->type = HTTP1_CONN_CLIENT;
    hconn->qd_server = li->server;
    hconn->handler_context.handler = &handle_connection_events;
    hconn->handler_context.context = hconn;

    DEQ_INIT(hconn->out_buffers);

    hconn->cfg.host = qd_strdup(li->config.host);
    hconn->cfg.port = qd_strdup(li->config.port);
    hconn->cfg.address = qd_strdup(li->config.address);

    hconn->raw_conn = pn_raw_connection();
    pn_raw_connection_set_context(hconn->raw_conn, hconn);
    pn_listener_raw_accept(li->pn_listener, hconn->raw_conn);

    // configure the HTTP/1.x library

    h1_lib_conn_config_t config = {0};
    config.type             = HTTP1_CONN_CLIENT;
    config.conn_tx_data     = client_tx_data_cb;
    config.rx_request       = client_rx_request_cb;
    config.rx_response      = client_rx_response_cb;
    config.rx_header        = client_rx_header_cb;
    config.rx_headers_done  = client_rx_headers_done_cb;
    config.rx_body          = client_rx_body_cb;
    config.rx_done          = client_rx_done_cb;
    config.request_complete = client_request_complete_cb;

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
static void handle_listener_events(pn_event_t *e, qd_server_t *qd_server, void *context)
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
        accept_client_connection(li);
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
            qd_http_listener_decref(li);
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
    qd_http_lsnr_t *li = qd_http_lsnr(qd->server, &handle_listener_events);
    if (!li) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR, "Unable to create http listener: no memory");
        return 0;
    }
    li->config = *config;

    sys_atomic_inc(&li->ref_count); /* In use by proactor, PN_LISTENER_CLOSE will dec */
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
static void setup_client_connection(qdr_http1_connection_t *hconn)
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
static void handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log_source_t *log = qdr_http1_adaptor->log;

    qd_log(log, QD_LOG_DEBUG, "RAW CONNECTION EVENT %s\n", pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        setup_client_connection(hconn);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_READ: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Closed for reading", hconn->conn_id);
        pn_raw_connection_close(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_CLOSED_WRITE: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Closed for writing", hconn->conn_id);
        pn_raw_connection_close(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_DISCONNECTED: {
        qd_log(log, QD_LOG_INFO, "[C%i] Disconnected", hconn->conn_id);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need write buffers", hconn->conn_id);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need read buffers", hconn->conn_id);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Wake-up", hconn->conn_id);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        // TCP data has arrived from the client
        qd_buffer_list_t blist;
        uintmax_t length;
        qda_raw_conn_get_read_buffers(hconn->raw_conn, &blist, &length);
        int error = h1_lib_connection_rx_data(hconn->http_conn, &blist, length);
        if (error) abort();  // TODO(kgiusti) FIXME: drop conn
        qda_raw_conn_grant_read_buffers(hconn->raw_conn);
        qd_log(log, QD_LOG_DEBUG, "[C%i] Read %"PRIuMAX" bytes", hconn->conn_id, length);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    case PN_RAW_CONNECTION_WRITTEN: {
        qda_raw_conn_free_write_buffers(hconn->raw_conn);
        while (qdr_connection_process(hconn->qdr_conn)) {}
        break;
    }
    default:
        break;
    }
}




////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
////////////////////////////////////////////////////////


// Called when the encoder has data to send out the raw connection.  This will
// take ownership of the buffer list and wake up the outgoing raw connection.
//
static void client_tx_data_cb(h1_lib_connection_t *hc, qd_buffer_list_t *blist, size_t offset, unsigned int len)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) h1_lib_connection_get_context(hc);
    if (!hconn->raw_conn) {
        // client connection has been lost
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%i] Discarding outgoing data - client connection closed", hconn->conn_id);
        qd_buffer_list_free_buffers(blist);
    } else {
        if (offset) {
            qd_buffer_t *head = DEQ_HEAD(*blist);
            memmove(qd_buffer_base(head), qd_buffer_base(head) + offset, qd_buffer_size(head));
        }
        DEQ_APPEND(hconn->out_buffers, *blist);
        pn_raw_connection_wake(hconn->raw_conn);
    }
}


// Called when decoding an HTTP request from a client.  This indicates the
// start of a new request message.
//
static int client_rx_request_cb(h1_lib_request_state_t *hrs,
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
static int client_rx_response_cb(h1_lib_request_state_t *hrs,
                                   int status_code,
                                   const char *reason_phrase,
                                   uint32_t version_major,
                                   uint32_t version_minor)
{
    return HTTP1_STATUS_BAD_REQ;
}


// called for each decoded HTTP header.
//
static int client_rx_header_cb(h1_lib_request_state_t *hrs, const char *key, const char *value)
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

// called after the last header is decoded, before decoding any body data.
//
static int client_rx_headers_done_cb(h1_lib_request_state_t *hrs, bool has_body)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = creq->hconn;

    if (hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request headers done.",
               hconn->conn_id);

    // now that all the headers have been received we can construct
    // the AMQP message

    creq->request_msg = qd_message();

    qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
    qd_compose_start_list(props);

    qd_compose_insert_ulong(props, creq->msg_id);    // message-id
    qd_compose_insert_null(props);                 // user-id
    qd_compose_insert_string(props, hconn->cfg.address); // to
    qd_compose_insert_string(props, h1_lib_request_state_method(hrs));  // subject
    qd_compose_insert_string(props, hconn->client.reply_to_addr);   // reply-to
    qd_compose_end_list(props);

    qd_compose_end_map(creq->app_props);

    qd_message_compose_3(creq->request_msg, props, creq->app_props, !has_body);

    // send the message to the core for forwarding
    assert(!creq->request_dlv);

    if (hconn->in_link_credit > 0) {
        creq->request_dlv = qdr_link_deliver(hconn->in_link, creq->request_msg, 0, false, 0, 0, 0, 0);
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int client_rx_body_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, size_t len,
                             bool more)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (offset) {
        // dispatch assumes all body data starts at the buffer base so it cannot deal with offsets.
        // Remove the offset by shifting the content of the head buffer forward
        //
        qd_buffer_t *head = DEQ_HEAD(*body);
        memmove(qd_buffer_base(head), qd_buffer_base(head) + offset, qd_buffer_size(head));
    }

    //
    // Compose a DATA performative for this section of the stream
    //
    qd_composed_field_t *field = qd_compose(QD_PERFORMATIVE_BODY_DATA, 0);
    qd_compose_insert_binary_buffers(field, body);

    //
    // Extend the streaming message and free the composed field
    //
    qd_message_extend(creq->request_msg, field);
    qd_compose_free(field);

    //
    // Notify the router that more data is ready to be pushed out on the delivery
    //
    if (!more)
        qd_message_set_receive_complete(creq->request_msg);

    if (creq->request_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, creq->request_dlv, false);

    return 0;
}

// Called at the completion of message decoding.  This indicates the message
// has been completely decoded.  No further calls will occur for this message.
//
static void client_rx_done_cb(h1_lib_request_state_t *hrs)
{
    qdr_http1_request_t *creq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (creq->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP request receive complete.",
               creq->hconn->conn_id);

    if (!qd_message_receive_complete(creq->request_msg)) {
        qd_message_set_receive_complete(creq->request_msg);
        if (creq->request_dlv) {
            qdr_delivery_continue(qdr_http1_adaptor->core, creq->request_dlv, false);
        }
    }

    creq->hconn->in_link_credit -= 1;
}


static void client_request_complete_cb(h1_lib_request_state_t *lib_rs)
{
    // TODO(kgiusti)
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


void qdr_http1_client_link_flow(qdr_http1_adaptor_t    *adaptor,
                                qdr_http1_connection_t *hconn,
                                qdr_link_t             *link,
                                int                     credit)
{
    assert(link == hconn->in_link);   // router only grants flow on incoming link

    int old = hconn->in_link_credit;
    hconn->in_link_credit += credit;
    if (old == 0 && hconn->in_link_credit > 0) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] link unblocked", hconn->conn_id, link->identity);

        if (hconn->raw_conn && !pn_raw_connection_is_read_closed(hconn->raw_conn))
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);

        // check for pending request messages that can now be delivered to
        // the router
        while (hconn->in_link_credit > 0) {
            qdr_http1_request_t *creq = DEQ_HEAD(hconn->requests);
            if (!creq)
                break;

            if (creq->request_msg && !creq->request_dlv) {
                creq->request_dlv = qdr_link_deliver(hconn->in_link, creq->request_msg, 0, false, 0, 0, 0, 0);
                if (qd_message_receive_complete(creq->request_msg)) {
                    DEQ_REMOVE_HEAD(hconn->requests);
                    // @TODO(kgiusti): settle? free?
                    hconn->in_link_credit -= 1;
                } else
                    break;
            } else
                break;
        }
    }

    // adjust the flow on the outgoing link so we keep the number of requests
    // that can be sent to the server approximately the same as the number of
    // responses the router will accept
    hconn->out_link_credit += credit;
    qdr_link_flow(qdr_http1_adaptor->core, link, credit, false);  // will activate conn
}


/*static*/ qdr_http1_request_t *lookup_request_context(qdr_http1_connection_t *hconn,
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



#ifdef KAG_TODO
// Start a new response to the client.  msg has been validated to at least
// application properties depth.
//
static bool start_send_response(qdr_http1_request_t *req)
{
    qd_message_t *msg = req->response_msg;
    qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    qd_parsed_field_t *app_props = qd_parse(app_props_iter);
    char *reason_str = 0;
    bool rc = false;

    if (!app_props || !qd_parse_is_map(app_props)) goto exit;

    // extract the status code, version (optional), and reason (optional)

    qd_parse_field_t *tmp = qd_parse_value_by_key(app_props, STATUS_HEADER_KEY);
    if (!tmp) goto exit;

    int32_t code = qd_parse_as_int(tmp);
    if (!qd_parse_ok(tmp)) goto exit;

    uint32_t major = 1;
    uint32_t minor = 1;
    tmp = qd_parse_value_by_key(app_props, RESPONSE_HEADER_KEY);
    if (tmp) {
        char *version_str = qd_iterator_copy(qd_parse_raw(tmp));
        if (version_str) {
            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
            free(version_str);
        }
    }

    tmp = qd_parse_value_by_key(app_props, REASON_HEADER_KEY);
    if (tmp) {
        reason_str = qd_iterator_copy(qd_parse_raw(tmp));
    }

    if (h1_lib_tx_response(req->lib_rs, (int)status_code, reason_str, major, minor))
        goto exit;

    // send all headers in app properties

    qd_parsed_field_t *key = qd_field_first_child(app_props);
    while (key) {
        qd_parsed_field_t *value = qd_field_next_child(key);
        if (!value)
            break;

        qd_iterator_t *i_key = qd_parse_raw(key);
        if (!i_key)
            break;

        // ignore the special headers added by the mapping
        if (!qd_iterator_prefix(ikey, HTTP1_HEADER_PREFIX)) {
            qd_iterator_t *i_value = qd_parse_raw(value);
            if (!i_value)
                break;

            char *header_key = qd_iterator_copy(i_key);
            char *header_value = qd_iterator_copy(i_value);

            int err = h1_lib_tx_add_header(req->lib_rs, header_key, header_value);

            free(header_key);
            free(header_value);

            if (err) goto exit;
        }

        key = qd_field_next_child(value);
    }

    rc = true;  // success

exit:

    qd_iterator_free(app_props_iter);
    qd_parse_free(app_props);
    free(reason_str);

    return rc;
}


static void send_request_headers(qdr_http1_server_response_t *resp)
{
    qd_iterator_t *app_props_iter = qd_message_field_iterator(resp->request_msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (app_props_iter) {
        qd_parsed_field_t *app_props = qd_parse(app_props_iter);
        if (app_props && qd_parse_is_map(app_props)) {
            qd_parsed_field_t *key = qd_field_first_child(app_props);
            while (key) {
                qd_parsed_field_t *value = qd_field_next_child(key);
                if (!value)
                    break;

                qd_iterator_t *i_key = qd_parse_raw(key);
                if (!i_key)
                    break;

                // ignore the special headers added by the mapping
                if (!qd_iterator_prefix(ikey, HTTP1_HEADER_PREFIX)) {
                    qd_iterator_t *i_value = qd_parse_raw(value);
                    if (!i_value)
                        break;

                    char *header_key = qd_iterator_copy(i_key);
                    char *header_value = qd_iterator_copy(i_value);

                    h1_lib_tx_add_header(resp->req_state, header_key, header_value);

                    free(header_key);
                    free(header_value);
                }

                key = qd_field_next_child(value);
            }
        }
        qd_parse_free(app_props);
        qd_iterator_free(app_props_iter);
    }
}

// helper routine to extract request body data from AMQP message and write it out to the server
//
static uint64_t send_request_body(qdr_http1_server_response_t *resp)
{
    qd_message_body_data_t        *body_data = 0;

    while (true) {
        switch (qd_message_next_body_data(resp->request_msg, &body_data)) {
        case QD_MESSAGE_BODY_DATA_OK: {
            //
            // We have a new valid body-data segment.  Handle it
            //

            // @TODO(kgiusti) - refactor HTTP1 library to handle body_data for output
            break;
        }

        case QD_MESSAGE_BODY_DATA_NO_MORE:
            //
            // We have already handled the last body-data segment for this delivery.
            // Complete the "sending" of this delivery and replenish credit.
            //
            // Note that depending on the adaptor, it might be desirable to delay the
            // acceptance and settlement of this delivery until a later event (i.e. when
            // a requested action has completed).
            //
            qd_message_set_send_complete(msg);
            qdr_link_flow(adaptor->core, link, 1, false);
            return PN_ACCEPTED; // This will cause the delivery to be settled

        case QD_MESSAGE_BODY_DATA_INCOMPLETE:
            //
            // A new segment has not completely arrived yet.  Check again later.
            //
            return 0;

        case QD_MESSAGE_BODY_DATA_INVALID:
        case QD_MESSAGE_BODY_DATA_NOT_DATA:
            qdr_link_flow(adaptor->core, link, 1, false);
            return PN_REJECTED;
        }
    }
}
#endif // KAG_TODO

// The I/O thread wants to send this delivery out the link
//
uint64_t qdr_http1_client_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                       qdr_http1_connection_t *hconn,
                                       qdr_link_t             *link,
                                       qdr_delivery_t         *delivery,
                                       bool                    settled)
{
#ifdef KAG_TODO

    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    qd_message_t             *msg = qdr_delivery_message(delivery);

    qd_message_depth_status_t status = qd_message_check_depth(msg, QD_DEPTH_BODY);
    if (status == QD_MESSAGE_DEPTH_INCOMPLETE)
        return 0;

    if (status == QD_MESSAGE_DEPTH_INVALID) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, link->identity);
        qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
        return PN_REJECTED;
    }

    assert(status == QD_MESSAGE_DEPTH_OK);

    qdr_http1_client_request_t *req = qdr_delivery_get_context(delivery);
    if (!req) {
        // start of response, find corresponding request
        req = lookup_request_context(hconn, msg);
        if (!req) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, link->identity);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            return PN_REJECTED;
        }
        req->response_dlv = delivery;
        req->response_msg = msg;
        qdr_delivery_set_context(delivery, req);
    }

    if (req != DEQ_HEAD(hconn->request)) {
        // responses must be sent back to the client in order...
        not first cannot decode;  need to remember if headers etc sent!;
    }

    if (!start_send_response(req)) {

    }

    uint64_t outcome = send_response_body(req);
    if (outcome == PN_REJECTED) {
        // cleanup
        // drop connection??
    }
#endif  // KAG_TODO
    return 0;
}


// Handle disposition/settlement update for the outstanding incoming HTTP message
//
void qdr_http1_client_delivery_update(qdr_http1_adaptor_t    *adaptor,
                                      qdr_http1_connection_t *hconn,
                                      qdr_http1_request_t    *hreq,
                                      qdr_delivery_t         *dlv,
                                      uint64_t                disp,
                                      bool                    settled)
{
    // find corresponding in delivery

    /*
      if is_client connection:
         if PN_ACCEPTED we need to wait for response
             (or has response already been received??)
         if other terminal outcome:
            Craft an HTTP response msg for client
            with error status

     else if is_server connection:
         server holds no response state
         simply log it?
         or accept corresponding msg?

     if (settled)
        qdr_delivery_decref(adaptor->core, dlv, "qdr_ref_delivery_update - settled delivery");
    */

}



