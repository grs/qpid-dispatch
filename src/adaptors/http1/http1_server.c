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

#include <proton/proactor.h>

static void _server_tx_msg_headers_cb(h1_lib_request_state_t *lib_hrs, qd_buffer_list_t *blist, unsigned int len);
static void _server_tx_msg_body_cb(h1_lib_request_state_t *lib_hrs, qd_message_body_data_t *body_data);
static int  _server_rx_request_cb(h1_lib_request_state_t *hrs,
                                  const char *method,
                                  const char *target,
                                  uint32_t version_major,
                                  uint32_t version_minor);
static int  _server_rx_response_cb(h1_lib_request_state_t *hrs,
                                   int status_code,
                                   const char *reason_phrase,
                                   uint32_t version_major,
                                   uint32_t version_minor);
static int _server_rx_header_cb(h1_lib_request_state_t *hrs, const char *key, const char *value);
static int _server_rx_headers_done_cb(h1_lib_request_state_t *hrs, bool has_body);
static int _server_rx_body_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, size_t len,
                              bool more);
static void _server_rx_done_cb(h1_lib_request_state_t *hrs);
static void _server_request_complete_cb(h1_lib_request_state_t *hrs, bool cancelled);
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);
static void _on_activate(void *context);


// Is this request complete?
//
// @TODO(kgiusti): determine method for progressing to next request to be serviced
static bool _handle_request_done(qdr_http1_request_t *req)
{
    return false;  // fixme
}
static void _handle_response_disposition_update(qdr_http1_request_t *req)
{
}
static void _check_request_complete(qdr_http1_request_t *req)
{
    // request:
    //  out_data empty
    //  request_delivery ?
    //  egress outcome
    //
    // response:
    //   ingress outcome

    // @TODO(kgiusti): not correct:
    assert(qd_message_receive_complete(qdr_delivery_message(req->response_dlv)));
    assert(req->in_dispo);

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "");

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


////////////////////////////////////////////////////////
// HTTP/1.x Server Connector
////////////////////////////////////////////////////////


// configure the HTTP/1.x library for the connection
//
static bool _setup_http1_lib(qdr_http1_connection_t *hconn)
{

    h1_lib_conn_config_t config = {0};
    config.type             = HTTP1_CONN_SERVER;
    config.tx_msg_headers   = _server_tx_msg_headers_cb;
    config.tx_msg_body      = _server_tx_msg_body_cb;
    config.rx_request       = _server_rx_request_cb;
    config.rx_response      = _server_rx_response_cb;
    config.rx_header        = _server_rx_header_cb;
    config.rx_headers_done  = _server_rx_headers_done_cb;
    config.rx_body          = _server_rx_body_cb;
    config.rx_done          = _server_rx_done_cb;
    config.request_complete = _server_request_complete_cb;

    hconn->http_conn = h1_lib_connection(&config, hconn);
    return !!hconn;
}


// An HttpConnector has been created.  Create an qdr_http_connection_t for it.
// Do not create a raw connection - this is done on demand when the router
// sends a delivery over the connector.
//
static bool _setup_server_connection(qd_http_connector_t *ctor,
                                     qd_dispatch_t *qd,
                                     const qd_http_bridge_config_t *bconfig)
{
    qdr_http1_connection_t *hconn = new_qdr_http1_connection_t();

    ZERO(hconn);
    hconn->type = HTTP1_CONN_SERVER;
    hconn->qd_server = qd->server;
    hconn->handler_context.handler = &_handle_connection_events;
    hconn->handler_context.context = hconn;

    hconn->cfg.host = qd_strdup(bconfig->host);
    hconn->cfg.port = qd_strdup(bconfig->port);
    hconn->cfg.address = qd_strdup(bconfig->address);
    hconn->cfg.host_port = qd_strdup(bconfig->host_port);

    if (!_setup_http1_lib(hconn)) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
               "Failed to initialize HTTP/1.x library - connector failed.");
        qdr_http1_connection_free(hconn);
        return false;
    }

    // for waking up proton when we need to create the corresponding raw connection
    hconn->server.activate_timer = qd_timer(qdr_http1_adaptor->core->qd, _on_activate, hconn);

    // create router endpoints for this connection

    qdr_connection_info_t *info = qdr_connection_info(false, //bool             is_encrypted,
                                                      false, //bool             is_authenticated,
                                                      true,  //bool             opened,
                                                      "",   //char            *sasl_mechanisms,
                                                      QD_OUTGOING, //qd_direction_t   dir,
                                                      hconn->cfg.host_port,    //const char      *host,
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
                                            false,  // incoming
                                            QDR_ROLE_NORMAL,
                                            1,      // cost
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

    // simulate a server subscription for its service address
    qdr_terminus_t *source = qdr_terminus(0);
    qdr_terminus_set_address(source, hconn->cfg.address);
    hconn->out_link = qdr_link_first_attach(hconn->qdr_conn,
                                            QD_OUTGOING,
                                            source,           //qdr_terminus_t   *source,
                                            qdr_terminus(0),  //qdr_terminus_t   *target,
                                            "http1.server.out", //const char       *name,
                                            0,                //const char       *terminus_addr,
                                            false,
                                            0,      // initial delivery
                                            &(hconn->out_link_id));
    qdr_link_set_context(hconn->out_link, hconn);

    // simulate an anonymous link for responses from the server
    hconn->in_link = qdr_link_first_attach(hconn->qdr_conn,
                                           QD_INCOMING,
                                           qdr_terminus(0),  //qdr_terminus_t   *source,
                                           qdr_terminus(0),  //qdr_terminus_t   *target
                                           "http1.server.in",  //const char       *name,
                                           0,                //const char       *terminus_addr,
                                           false,
                                           NULL,
                                           &(hconn->in_link_id));
    qdr_link_set_context(hconn->in_link, hconn);

    // The raw connection will be brought up on demand (when a request arrives
    // from the router)
    return true;
}


// Management Agent API - Create
//
qd_http_connector_t *qd_http1_configure_connector(qd_dispatch_t *qd, const qd_http_bridge_config_t *config, qd_entity_t *entity)
{
    qd_http_connector_t *c = qd_http_connector(qd->server);
    if (!c) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR, "Unable to create http connector: no memory");
        return 0;
    }

    DEQ_ITEM_INIT(c);
    if (_setup_server_connection(c, qd, config)) {
        DEQ_INSERT_TAIL(qdr_http1_adaptor->connectors, c);
        return c;
    } else {
        qd_http_connector_decref(c);
        c = 0;
    }

    return c;
}


// Management Agent API - Delete
//
void qd_http1_delete_connector(qd_dispatch_t *qd, qd_http_connector_t *ct)
{
    if (ct) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Deleted HttpConnector for %s, %s:%s", ct->config.address, ct->config.host, ct->config.port);
        DEQ_REMOVE(qdr_http1_adaptor->connectors, ct);
        qd_http_connector_decref(ct);

        // TODO(kgiusti): do we now close all related connections?
    }
}




////////////////////////////////////////////////////////
// Raw Connector Events
////////////////////////////////////////////////////////


// A timer callback to process the server qdr_connection_t prior to having
// established a raw connection to the server.  The raw connection is brought
// up on demand when output buffers are generated.
//
static void _on_activate(void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] Activating server connection", hconn->conn_id);
    while (qdr_connection_process(hconn->qdr_conn)) {}
}


// Proton Raw Connection Events
//
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log_source_t *log = qdr_http1_adaptor->log;

    qd_log(log, QD_LOG_DEBUG, "RAW CONNECTION EVENT %s\n", pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        if (!DEQ_IS_EMPTY(hconn->requests)) {
        }
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
        // @TODO(kgiusti): handle server close on EOM
        // force the HTTP1 library to complete the current
        // incoming request or cancel half processed requests
        //h1_lib_connection_close(hconn->http_conn);
        //hconn->http1_conn = 0;
        //fixme();  // when ok to re-open
        if (!DEQ_IS_EMPTY(hconn->requests)) {
            // still more incoming requests
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
                   "[C%"PRIu64"] Reconnecting to HTTP/1.x server: %s", hconn->conn_id, hconn->cfg.host_port);
            hconn->raw_conn = pn_raw_connection();
            pn_raw_connection_set_context(hconn->raw_conn, hconn);
            pn_proactor_raw_connect(qd_server_proactor(hconn->qd_server), hconn->raw_conn, hconn->cfg.host_port);
        }
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





//////////////////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
//////////////////////////////////////////////////////////////////////


// Encoder has a buffer list to send to the server
//
static void _server_tx_msg_headers_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *blist, unsigned int len)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qdr_http1_connection_t *hconn = hreq->hconn;

    if (!hconn->raw_conn) {
        // Need to establish connection to the server
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"] Connecting to HTTP/1.x server: %s", hconn->conn_id, hconn->cfg.host_port);
        hconn->raw_conn = pn_raw_connection();
        pn_raw_connection_set_context(hconn->raw_conn, hconn);
        pn_proactor_raw_connect(qd_server_proactor(hconn->qd_server), hconn->raw_conn, hconn->cfg.host_port);
    }
    qdr_http1_write_buffer_list(hreq, blist);
}


// Encoder has body data to send to the server
//
static void _server_tx_msg_body_cb(h1_lib_request_state_t *hrs, qd_message_body_data_t *body_data)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qdr_http1_write_body_data(hreq, body_data);
}


// Server will not be sending us HTTP requests
//
static int _server_rx_request_cb(h1_lib_request_state_t *hrs,
                                 const char *method,
                                 const char *target,
                                 uint32_t version_major,
                                 uint32_t version_minor)
{
    return HTTP1_STATUS_BAD_REQ;
}


// called when decoding an HTTP response from the server.
//
static int _server_rx_response_cb(h1_lib_request_state_t *hrs,
                                  int status_code,
                                  const char *reason_phrase,
                                  uint32_t version_major,
                                  uint32_t version_minor)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    assert(resp && resp == DEQ_HEAD(resp->hconn->requests));  // expected to be in-order
    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response: status=%d phrase=%s version=%"PRIi32".%"PRIi32,
               resp->hconn->conn_id, status_code, reason_phrase ? reason_phrase : "<NONE>",
               version_major, version_minor);

    resp->app_props = qd_compose(QD_PERFORMATIVE_APPLICATION_PROPERTIES, 0);
    qd_compose_start_map(resp->app_props);
    {
        char version[64];
        snprintf(version, 64, "%"PRIi32".%"PRIi32, version_major, version_minor);
        qd_compose_insert_symbol(resp->app_props, RESPONSE_HEADER_KEY);
        qd_compose_insert_string(resp->app_props, version);

        qd_compose_insert_symbol(resp->app_props, STATUS_HEADER_KEY);
        qd_compose_insert_int(resp->app_props, (int32_t)status_code);

        if (reason_phrase) {
            qd_compose_insert_symbol(resp->app_props, REASON_HEADER_KEY);
            qd_compose_insert_string(resp->app_props, reason_phrase);
        }
    }

    return 0;
}


// called for each decoded HTTP header.
//
static int _server_rx_header_cb(h1_lib_request_state_t *hrs, const char *key, const char *value)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response header: key='%s' value='%s'", key, value);

    if (strcasecmp(key, "connection") == 0) {
        // We need to filter this header out after we check for the "close"
        // flag.  @TODO(kgiusti): does case matter?  Need to ensure "close" is
        // not part of a larger value
        resp->connection_close_flag = !!strstr(value, "close");
    } else {
        // lump everything else directly into application properties
        qd_compose_insert_symbol(resp->app_props, key);
        qd_compose_insert_string(resp->app_props, value);
    }

    return 0;
}


// called after the last header is decoded, before decoding any body data.
//
static int _server_rx_headers_done_cb(h1_lib_request_state_t *hrs, bool has_body)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response headers done.",
               resp->hconn->conn_id);

    resp->in_msg = qd_message();

    qd_composed_field_t *props = qd_compose(QD_PERFORMATIVE_PROPERTIES, 0);
    qd_compose_start_list(props);

    qd_compose_insert_null(props);     // message-id
    qd_compose_insert_null(props);     // user-id
    qd_compose_insert_string(props, resp->response_addr); // to
    // subject:
    qd_compose_insert_string(props, h1_lib_request_state_method(hrs));
    qd_compose_insert_null(props);   // reply-to
    qd_compose_insert_ulong(props, resp->msg_id);  // correlation-id
    qd_compose_end_list(props);

    qd_compose_end_map(resp->app_props);

    qd_message_compose_3(resp->in_msg, props, resp->app_props, !has_body);
    qd_compose_free(props);
    qd_compose_free(resp->app_props);
    resp->app_props = 0;

    if (resp->hconn->in_link_credit > 0) {
        resp->hconn->in_link_credit -= 1;
        resp->response_dlv = qdr_link_deliver(resp->hconn->in_link, resp->in_msg, 0, false, 0, 0, 0, 0);
        qdr_delivery_set_context(resp->response_dlv, (void*) resp);
        qdr_delivery_incref(resp->response_dlv, "referenced by HTTP1 adaptor");
        resp->in_msg = 0;
    }

    return 0;
}


// Called with decoded body data.  This may be called multiple times as body
// data becomes available.
//
static int _server_rx_body_cb(h1_lib_request_state_t *hrs, qd_buffer_list_t *body, size_t offset, size_t len,
                              bool more)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qd_message_t *msg = resp->in_msg ? resp->in_msg : qdr_delivery_message(resp->response_dlv);

    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response body received len=%zu.", len);

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

    if (resp->response_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, resp->response_dlv, false);

    return 0;
}

// Called at the completion of response decoding.
//
static void _server_rx_done_cb(h1_lib_request_state_t *hrs)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    qd_message_t *msg = resp->in_msg ? resp->in_msg : qdr_delivery_message(resp->response_dlv);

    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response receive complete.",
               resp->hconn->conn_id);

    if (!qd_message_receive_complete(msg)) {
        qd_message_set_receive_complete(msg);
        if (resp->response_dlv) {
            qdr_delivery_continue(qdr_http1_adaptor->core, resp->response_dlv, false);
        }
    }

    // handle pending dispo update
    if (resp->in_dispo)
        _handle_response_disposition_update(resp);
}


// called at the completion of a full Request/Response exchange.  The hrs will
// be deleted on return from this call.  Any hrs related state must be
// released before returning from this callback.
//
static void _server_request_complete_cb(h1_lib_request_state_t *hrs, bool cancelled)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);
    if (hreq) {
        qdr_http1_connection_t *hconn = hreq->hconn;

        hreq->lib_rs = 0;
        hreq->completed = true;

        // Safe to settle the request_dlv
        if (hreq->request_dlv) {
            if (hconn->out_link) {
                // can set the disposition and settle the request delivery now
                // that the response message has been completed.
                qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                  hreq->request_dlv,
                                                  hreq->out_dispo,
                                                  true,   // settled
                                                  0,      // error
                                                  0,      // dispo data
                                                  false);
            }
            qdr_delivery_set_context(hreq->request_dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 adaptor request settled");
            hreq->request_dlv = 0;
        }

        _check_request_complete(hreq);
    }
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


// credit has been granted - responses may now be sent to the
// router core.
//
void qdr_http1_server_link_flow(qdr_http1_adaptor_t    *adaptor,
                                qdr_http1_connection_t *hconn,
                                qdr_link_t             *link,
                                int                     credit)
{
    assert(link == hconn->in_link);   // router only grants flow on incoming link

    hconn->in_link_credit += credit;
    if (hconn->in_link_credit > 0) {

        if (hconn->raw_conn)
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);

        // is the current response message blocked by lack of credit?

        qdr_http1_request_t *resp = DEQ_HEAD(hconn->requests);
        if (resp && resp->in_msg) {
            assert(!resp->response_dlv);
            hconn->in_link_credit -= 1;
            resp->response_dlv = qdr_link_deliver(hconn->in_link, resp->in_msg, 0, false, 0, 0, 0, 0);
            qdr_delivery_set_context(resp->response_dlv, (void*) resp);
            qdr_delivery_incref(resp->response_dlv, "referenced by HTTP1 adaptor");
            resp->in_msg = 0;
        }
    }

    // adjust the flow on the request link so we can accept as many requests
    // as we're allowed responses.
    hconn->out_link_credit += credit;
    qdr_link_flow(adaptor->core, link, credit, false);  // will activate conn
}


// Handle disposition/settlement update for the outstanding HTTP response.
// Regardless of error now is when the response delivery can be
// settled
//
void qdr_http1_server_delivery_update(qdr_http1_adaptor_t    *adaptor,
                                      qdr_http1_connection_t *hconn,
                                      qdr_http1_request_t    *hreq,
                                      qdr_delivery_t         *dlv,
                                      uint64_t                disp,
                                      bool                    settled)
{
    qd_message_t *msg = qdr_delivery_message(dlv);
    assert(dlv == hreq->response_dlv);

    if (!hreq->in_dispo && disp && disp != PN_RECEIVED) {
        // terminal disposition - safe to settle response
        // delivery if done sending...
        hreq->in_dispo = disp;
        if (qd_message_receive_complete(msg)) {
            if (hconn->in_link) {
                qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                                  dlv,
                                                  0, // remote dispo
                                                  true,   // settled
                                                  0,      // error
                                                  0,      // dispo data
                                                  false);
            }
            qdr_delivery_set_context(dlv, 0);
            qdr_delivery_decref(qdr_http1_adaptor->core, dlv, "HTTP1 adaptor response sent");
            hreq->response_dlv = 0;
            _check_request_complete(hreq);
        }
    }
}


//
// Request message forwarding
//


// Start a new request to the server.  msg has been validated to at least
// application properties depth.  Returns 0 on success.
//
static uint64_t _send_request_headers(qdr_http1_request_t *hreq, qd_message_t *msg)
{
    // start encoding HTTP request.  Need method, target and version

    char *method_str = 0;
    char *target_str = 0;
    qd_parsed_field_t *app_props = 0;
    uint32_t major = 1;
    uint32_t minor = 1;
    uint64_t outcome = 0;

    assert(!hreq->lib_rs);

    // method is passed in the SUBJECT field
    qd_iterator_t *method_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);
    if (!method_iter) {
        return PN_REJECTED;
    }

    method_str = (char*) qd_iterator_copy(method_iter);
    qd_iterator_free(method_iter);
    if (!method_str) {
        return PN_REJECTED;
    }

    // target, version info and other headers are in the app properties
    qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);
    if (!app_props_iter) {
        outcome = PN_REJECTED;
        goto exit;
    }

    app_props = qd_parse(app_props_iter);
    qd_iterator_free(app_props_iter);
    if (!app_props) {
        outcome = PN_REJECTED;
        goto exit;
    }

    qd_parsed_field_t *ref = qd_parse_value_by_key(app_props, TARGET_HEADER_KEY);
    target_str = (char*) qd_iterator_copy(qd_parse_raw(ref));
    if (!target_str) {
        outcome = PN_REJECTED;
        goto exit;
    }


    // Pull the version info from the app properties (e.g. "1.1")
    ref = qd_parse_value_by_key(app_props, REQUEST_HEADER_KEY);
    if (ref) {  // optional
        char *version_str = (char*) qd_iterator_copy(qd_parse_raw(ref));
        if (version_str)
            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
        free(version_str);
    }

    // done copying and converting!

    hreq->lib_rs = h1_lib_tx_request(hreq->hconn->http_conn, method_str, target_str, major, minor);
    if (hreq->lib_rs) {
        h1_lib_request_state_set_context(hreq->lib_rs, (void*) hreq);

        // now send all headers in app properties
        qd_parsed_field_t *key = qd_field_first_child(app_props);
        bool ok = true;
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

                ok = !h1_lib_tx_add_header(hreq->lib_rs, header_key, header_value);

                free(header_key);
                free(header_value);
            }

            key = qd_field_next_child(value);
        }
    }

exit:

    free(method_str);
    free(target_str);
    qd_parse_free(app_props);

    return outcome;
}


static uint64_t _decode_request_message(qdr_http1_request_t *hreq)
{
    qdr_http1_connection_t *hconn = hreq->hconn;
    qd_message_t *msg = qdr_delivery_message(hreq->request_dlv);
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
    assert(hreq == DEQ_HEAD(hconn->requests));

    if (!hreq->headers_sent) {
        uint64_t outcome = _send_request_headers(hreq, msg);
        if (outcome) {
            // @TODO(kgiusti): need to properly clean up request state here
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, hconn->out_link_id);
            qdr_link_flow(qdr_http1_adaptor->core, hconn->out_link, 1, false);
            return outcome;
        }
        hreq->headers_sent = true;
    }

    qd_message_body_data_t *body_data = 0;

    while (true) {
        switch (qd_message_next_body_data(msg, &body_data)) {
        case QD_MESSAGE_BODY_DATA_OK: {
            if (h1_lib_tx_body(hreq->lib_rs, body_data)) {
                qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                       "[C%"PRIu64"][L%"PRIu64"] body data encode failed",
                       hconn->conn_id, hconn->out_link_id);
                return PN_REJECTED;
            }
            break;
        }

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


// The I/O thread wants to send this delivery out the link. This is either the
// start of a new incoming HTTP request or the continuation of an existing one.
// Note: returning a non-zero value will cause the delivery to be settled!
//
uint64_t qdr_http1_server_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                       qdr_http1_connection_t *hconn,
                                       qdr_link_t             *link,
                                       qdr_delivery_t         *delivery,
                                       bool                    settled)
{
    qd_message_t *msg = qdr_delivery_message(delivery);
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) qdr_delivery_get_context(delivery);
    qd_message_depth_status_t status = qd_message_check_depth(msg, QD_DEPTH_BODY);
    uint64_t outcome = 0;

    switch (status) {
    case QD_MESSAGE_DEPTH_INCOMPLETE:
        return 0;

    case QD_MESSAGE_DEPTH_INVALID: {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, link->identity);
        if (!hreq) {
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            return PN_REJECTED;
        }
        outcome = PN_REJECTED;
        break;
    }

    case QD_MESSAGE_DEPTH_OK: {
        if (!hreq) {
            //
            // A new incoming request message
            //
            uint64_t msg_id = 0;
            char *reply_to = 0;
            {
                bool ok = false;
                qd_iterator_t *msg_id_itr = qd_message_field_iterator(msg, QD_FIELD_MESSAGE_ID);  // ulong
                if (msg_id_itr) {
                    qd_parsed_field_t *msg_id_pf = qd_parse(msg_id_itr);
                    if (msg_id_pf) {
                        msg_id = qd_parse_as_ulong(msg_id_pf);
                        ok = qd_parse_ok(msg_id_pf);
                        qd_parse_free(msg_id_pf);
                    }
                    qd_iterator_free(msg_id_itr);
                }
                if (!ok) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                           "[C%"PRIu64"][L%"PRIu64"] Rejecting message missing id.", hconn->conn_id, link->identity);
                    qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
                    return PN_REJECTED;
                }

                qd_iterator_t *reply_to_itr = qd_message_field_iterator(msg, QD_FIELD_REPLY_TO);
                reply_to = (char*) qd_iterator_copy(reply_to_itr);
                free(reply_to_itr);
                if (!reply_to) {
                    qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                           "[C%"PRIu64"][L%"PRIu64"] Rejecting message no reply-to.", hconn->conn_id, link->identity);
                    qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
                    return PN_REJECTED;
                }
            }

            hreq = new_qdr_http1_request_t();
            ZERO(hreq);
            hreq->hconn = hconn;
            hreq->msg_id = msg_id;
            hreq->response_addr = reply_to;
            DEQ_INIT(hreq->out_data);
            hreq->request_dlv = delivery;
            qdr_delivery_set_context(delivery, (void*) hreq);
            qdr_delivery_incref(delivery, "referenced by HTTP1 adaptor");
            DEQ_INSERT_TAIL(hconn->requests, hreq);
        }

        // service requests in order
        if (hreq == DEQ_HEAD(hconn->requests)) {
            outcome = _decode_request_message(hreq);
        }
        break;
    }
    default:
        break;
    }

    if (outcome && hreq->out_dispo == 0) {
        // delivered
        hreq->out_dispo = outcome;
        qd_message_set_send_complete(msg);
        qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);

        if (outcome == PN_ACCEPTED) {
            qdr_delivery_remote_state_updated(qdr_http1_adaptor->core,
                                              hreq->request_dlv,
                                              outcome,
                                              false,   // settled
                                              0,      // error
                                              0,      // dispo data
                                              false);
            h1_lib_tx_done(hreq->lib_rs);
            return 0;
        }

        // error

        qdr_delivery_set_context(hreq->request_dlv, 0);
        qdr_delivery_decref(qdr_http1_adaptor->core, hreq->request_dlv, "HTTP1 adaptor invalid request");
        hreq->request_dlv = 0;
        qdr_http1_request_free(hreq);
        return outcome;
    }

    // non-error outcome. Accept and settle after server delivers response
    return 0;
}



