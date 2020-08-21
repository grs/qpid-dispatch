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

//#include <stdio.h>
//#include <inttypes.h>

// callbacks from HTTP1 decoder (incoming HTTP requests)
//
static void _server_tx_msg_headers_cb(h1_lib_connection_t *lib_hconn, qd_buffer_list_t *blist, unsigned int len);
static void _server_tx_msg_body_cb(h1_lib_connection_t *lib_hconn, qd_message_body_data_t *body_data);
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
static void _server_request_complete_cb(h1_lib_request_state_t *hrs);

// Proactor event handler
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context);


////////////////////////////////////////////////////////
// HTTP/1.x Server Connector
////////////////////////////////////////////////////////


// Process the connection to generate outgoing messages and events
//
static void _process_connection(qdr_http1_connection_t *hconn)
{
    while (qdr_connection_process(hconn->qdr_conn)) {}

    // @TODO(kgiusti): close handling
    if (hconn->close_connection) {
        qdr_connection_set_context(hconn->qdr_conn, 0);
        qdr_connection_closed(hconn->qdr_conn);
        //qdr_http1_connection_free(hconn);
    }
}


// A timer callback to process the server qdr_connection_t prior to having
// established a raw connection to the server.  The raw connection is brought
// up on demand when output buffers are generated.
//
static void _on_activate(void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] Activating server connection", hconn->conn_id);
    _process_connection(hconn);
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

    DEQ_INIT(hconn->out_data);

    hconn->cfg.host = qd_strdup(bconfig->host);
    hconn->cfg.port = qd_strdup(bconfig->port);
    hconn->cfg.address = qd_strdup(bconfig->address);
    hconn->cfg.host_port = qd_strdup(bconfig->host_port);

    // The raw connection will be brought up on demand (when a request arrives
    // from the router)

    // configure the HTTP/1.x library

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
    if (!hconn->http_conn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR,
               "Failed to initialize HTTP/1.x library - connector failed.");
        //qdr_http1_connection_free(hconn);
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
    }

    qd_http_connector_decref(c);
    return 0;
}


// Management Agent API - Delete
//
void qd_http1_delete_connector(qd_dispatch_t *qd, qd_http_connector_t *ct)
{
    if (ct) {
        // TODO(kgiusti) need to close the pseudo-connection used for dispatching
        // deliveries out to live connnections:
        qd_log(qdr_http1_adaptor->log, QD_LOG_INFO, "Deleted HttpConnector for %s, %s:%s", ct->config.address, ct->config.host, ct->config.port);
        //close_egress_dispatcher((qdr_http1_connection_t*) ct->dispatcher);
        DEQ_REMOVE(qdr_http1_adaptor->connectors, ct);
        qd_http_connector_decref(ct);
    }
}




////////////////////////////////////////////////////////
// Raw Connector Events
////////////////////////////////////////////////////////



// Proton Raw Connection Events
//
static void _handle_connection_events(pn_event_t *e, qd_server_t *qd_server, void *context)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) context;
    qd_log_source_t *log = qdr_http1_adaptor->log;

    qd_log(log, QD_LOG_DEBUG, "RAW CONNECTION EVENT %s\n", pn_event_type_name(pn_event_type(e)));

    switch (pn_event_type(e)) {

    case PN_RAW_CONNECTION_CONNECTED: {
        _process_connection(hconn);
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
        // @TODO(kgiusti): cleanly terminate current response message
        _process_connection(hconn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_WRITE_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need write buffers", hconn->conn_id);
        _process_connection(hconn);
        break;
    }
    case PN_RAW_CONNECTION_NEED_READ_BUFFERS: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Need read buffers", hconn->conn_id);
        if (hconn->in_link_credit > 0 && !pn_raw_connection_is_read_closed(hconn->raw_conn))
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);
        break;
    }
    case PN_RAW_CONNECTION_WAKE: {
        qd_log(log, QD_LOG_DEBUG, "[C%i] Wake-up", hconn->conn_id);
        // see if recv buffers can be granted (in credit)
        // see if out buffers can be sent
        // see if in link credit > 0 and outgoing responses without deliveries
        _process_connection(hconn);
        break;
    }
    case PN_RAW_CONNECTION_READ: {
        // HTTP data has arrived from the server
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





//////////////////////////////////////////////////////////////////////
// HTTP/1.x Encoder/Decoder Callbacks
//////////////////////////////////////////////////////////////////////


static void _server_tx_msg_headers_cb(h1_lib_connection_t *hc, qd_buffer_list_t *blist, unsigned int len)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) h1_lib_connection_get_context(hc);

    qdr_http1_write_buffer_list(hconn, blist);
    if (!hconn->raw_conn) {
        // Need to establish connection to the server
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"] Connecting to HTTP/1.x server: %s", hconn->conn_id, hconn->cfg.host_port);
        hconn->raw_conn = pn_raw_connection();
        pn_raw_connection_set_context(hconn->raw_conn, hconn);
        pn_proactor_raw_connect(qd_server_proactor(hconn->qd_server), hconn->raw_conn, hconn->cfg.host_port);

    }
}


static void _server_tx_msg_body_cb(h1_lib_connection_t *hc, qd_message_body_data_t *body_data)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) h1_lib_connection_get_context(hc);
    qdr_http1_write_body_data(hconn, body_data);
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
    assert(resp);  // should have been created when tx_request was called!
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

    resp->response_msg = qd_message();

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

    qd_message_compose_3(resp->response_msg, props, resp->app_props, !has_body);

    if (resp->hconn->in_link_credit > 0) {
        resp->response_dlv = qdr_link_deliver(resp->hconn->in_link, resp->response_msg, 0, false, 0, 0, 0, 0);
        qdr_delivery_set_context(resp->response_dlv, (void*) resp);
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
    qd_message_extend(resp->response_msg, field);
    qd_compose_free(field);

    //
    // Notify the router that more data is ready to be pushed out on the delivery
    //
    if (!more)
        qd_message_set_receive_complete(resp->response_msg);

    if (resp->response_dlv)
        qdr_delivery_continue(qdr_http1_adaptor->core, resp->response_dlv, false);

    return 0;
}

// Called at the completion of message decoding.  This indicates the message
// has been completely decoded.  No further calls will occur for this message.
//
static void _server_rx_done_cb(h1_lib_request_state_t *hrs)
{
    qdr_http1_request_t *resp = (qdr_http1_request_t*) h1_lib_request_state_get_context(hrs);

    if (resp->hconn->trace)
        qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
               "[C%"PRIu64"] HTTP response receive complete.",
               resp->hconn->conn_id);

    if (!qd_message_receive_complete(resp->response_msg)) {
        qd_message_set_receive_complete(resp->response_msg);
        if (resp->response_dlv) {
            resp->hconn->in_link_credit -= 1;
            qdr_delivery_continue(qdr_http1_adaptor->core, resp->response_dlv, false);
        }
    }
}


// called at the completion of a full Request/Response exchange.  The hrs will
// be deleted on return from this call.  Any hrs related state must be
// released before returning from this callback.
//
static void _server_request_complete_cb(h1_lib_request_state_t *hrs)
{
}


//////////////////////////////////////////////////////////////////////
// Router Protocol Adapter Callbacks
//////////////////////////////////////////////////////////////////////


void qdr_http1_server_link_flow(qdr_http1_adaptor_t    *adaptor,
                                qdr_http1_connection_t *hconn,
                                qdr_link_t             *link,
                                int                     credit)
{
    assert(link == hconn->in_link);   // router only grants flow on incoming link

    int old = hconn->in_link_credit;
    hconn->in_link_credit += credit;

    if (hconn->in_link_credit > 0) {

        if (hconn->raw_conn && !pn_raw_connection_is_read_closed(hconn->raw_conn))
            qda_raw_conn_grant_read_buffers(hconn->raw_conn);

        if (old == 0) {
            qd_log(adaptor->log, QD_LOG_DEBUG,
                   "[C%"PRIu64"][L%"PRIu64"] link unblocked", hconn->conn_id, link->identity);

            // check for pending response messages that can now be delivered to
            // the router
            while (hconn->in_link_credit > 0) {
                qdr_http1_request_t *resp = DEQ_HEAD(hconn->requests);
                if (!resp)
                    break;

                if (resp->response_msg && !resp->response_dlv) {
                    // message pending delivery to router
                    resp->response_dlv = qdr_link_deliver(hconn->in_link, resp->response_msg, 0, false, 0, 0, 0, 0);
                    qdr_delivery_set_context(resp->response_dlv, (void*) resp);

                    if (qd_message_receive_complete(resp->response_msg)) {
                        DEQ_REMOVE_HEAD(hconn->requests);
                        // @TODO(kgiusti): settle? free?
                        hconn->in_link_credit -= 1;
                    } else
                        break;
                } else
                    break;
            }
        }
    }

    // adjust the flow on the response link so we can accept as many responses
    // as we're allowed requests.  Add extra to account for multiple
    // "non-terminal" (code 1xx) responses for any one request.
    credit *= 2;
    hconn->out_link_credit += credit;
    qdr_link_flow(adaptor->core, link, credit, false);  // will activate conn
}


//
// Response message forwarding
//


// Start a new request to the server.  msg has been validated to at least
// application properties depth.
//
static qdr_http1_request_t *_send_request_headers(qdr_http1_connection_t *hconn,
                                                  qdr_delivery_t *delivery,
                                                  qd_message_t *msg)
{
    qdr_http1_request_t *req = 0;
    qd_parsed_field_t *msg_id_pf = 0;
    qd_parsed_field_t *app_props = 0;
    char *method_str = 0;
    char *reply_to_str = 0;
    char *target_str = 0;

    qd_iterator_t *msg_id_iter = qd_message_field_iterator(msg, QD_FIELD_MESSAGE_ID);  // ulong
    qd_iterator_t *method_iter = qd_message_field_iterator(msg, QD_FIELD_SUBJECT);  // string: method
    qd_iterator_t *reply_to_iter = qd_message_field_iterator(msg, QD_FIELD_REPLY_TO);     // string
    qd_iterator_t *app_props_iter = qd_message_field_iterator(msg, QD_FIELD_APPLICATION_PROPERTIES);

    if (!msg_id_iter) goto exit;

    msg_id_pf = qd_parse(msg_id_iter);
    if (!msg_id_pf) goto exit;

    uint64_t msg_id = qd_parse_as_ulong(msg_id_pf);
    if (!qd_parse_ok(msg_id_pf)) goto exit;

    method_str = (char*) qd_iterator_copy(method_iter);
    if (!method_str) goto exit;

    reply_to_str = (char*) qd_iterator_copy(reply_to_iter);
    if (!reply_to_str) goto exit;

    app_props = qd_parse(app_props_iter);
    if (!app_props || !qd_parse_is_map(app_props)) goto exit;

    qd_parsed_field_t *target_pf = qd_parse_value_by_key(app_props, TARGET_HEADER_KEY);
    target_str = (char*) qd_iterator_copy(qd_parse_raw(target_pf));
    if (!target_str) goto exit;

    // Pull the version info from the app properties (e.g. "1.1")
    uint32_t major = 1;
    uint32_t minor = 1;
    qd_parsed_field_t *tmp = qd_parse_value_by_key(app_props, REQUEST_HEADER_KEY);
    if (tmp) {  // optional
        char *version_str = (char*) qd_iterator_copy(qd_parse_raw(tmp));
        if (version_str)
            sscanf(version_str, "%"SCNu32".%"SCNu32, &major, &minor);
        free(version_str);
    }

    // done copying and converting!

    h1_lib_request_state_t *lib_rs = h1_lib_tx_request(hconn->http_conn, method_str, target_str, major, minor);
    if (lib_rs) {
        req = new_qdr_http1_request_t();
        ZERO(req);
        req->hconn = hconn;
        req->msg_id = msg_id;
        req->response_addr = reply_to_str;
        reply_to_str = 0;
        req->lib_rs = lib_rs;
        h1_lib_request_state_set_context(lib_rs, (void*) req);

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

                ok = !h1_lib_tx_add_header(req->lib_rs, header_key, header_value);

                free(header_key);
                free(header_value);
            }

            key = qd_field_next_child(value);
        }

        if (ok) {
            DEQ_INSERT_TAIL(hconn->requests, req);
        } else {
            // @TODO(kgiusti) -- send back Bad Request AMQP message!
        }
    }

exit:

    qd_iterator_free(msg_id_iter);
    qd_iterator_free(method_iter);
    qd_iterator_free(reply_to_iter);
    qd_iterator_free(app_props_iter);
    qd_parse_free(msg_id_pf);
    qd_parse_free(app_props);
    free(method_str);
    free(reply_to_str);
    free(target_str);

    return req;
}


// helper routine to extract request body data from AMQP message and write it
// out to the server.  Expected that message has been validated to BODY depth.
//
static uint64_t _send_request_body(qdr_http1_request_t *req)
{
    qd_message_body_data_t        *body_data = 0;

    while (true) {
        switch (qd_message_next_body_data(req->request_msg, &body_data)) {
        case QD_MESSAGE_BODY_DATA_OK: {
            if (h1_lib_tx_body(req->lib_rs, body_data)) {
                // @TODO(kgiusti) handle error
            }
            break;
        }

        case QD_MESSAGE_BODY_DATA_NO_MORE:
            //
            // We have already handled the last body-data segment for this delivery.
            // Complete the "sending" of this delivery and replenish credit.
            //
            qd_message_set_send_complete(req->request_msg);
            qdr_link_flow(qdr_http1_adaptor->core, req->hconn->out_link, 1, false);
            return PN_ACCEPTED; // This will cause the delivery to be settled

        case QD_MESSAGE_BODY_DATA_INCOMPLETE:
            //
            // A new segment has not completely arrived yet.  Check again later.
            //
            return 0;

        case QD_MESSAGE_BODY_DATA_INVALID:
        case QD_MESSAGE_BODY_DATA_NOT_DATA:
            qdr_link_flow(qdr_http1_adaptor->core, req->hconn->out_link, 1, false);
            // @TODO(kgiusti): how to recover from this?
            return PN_REJECTED;
        }
    }
}


// The I/O thread wants to send this delivery out the link
//
uint64_t qdr_http1_server_link_deliver(qdr_http1_adaptor_t    *adaptor,
                                       qdr_http1_connection_t *hconn,
                                       qdr_link_t             *link,
                                       qdr_delivery_t         *delivery,
                                       bool                    settled)
{
    qd_message_t *msg = qdr_delivery_message(delivery);

    qd_message_depth_status_t status = qd_message_check_depth(msg, QD_DEPTH_BODY);
    if (status == QD_MESSAGE_DEPTH_INCOMPLETE)
        return 0;

    if (status == QD_MESSAGE_DEPTH_INVALID) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
               "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, link->identity);

        qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
        // TODO(kgiusti): send Bad Request
        return PN_REJECTED;
    }

    assert(status == QD_MESSAGE_DEPTH_OK);

    qdr_http1_request_t *req = (qdr_http1_request_t*) qdr_delivery_get_context(delivery);
    if (!req) {
        // new incoming request message
        req = _send_request_headers(hconn, delivery, msg);
        if (!req) {
            qd_log(qdr_http1_adaptor->log, QD_LOG_WARNING,
                   "[C%"PRIu64"][L%"PRIu64"] Rejecting malformed message.", hconn->conn_id, link->identity);
            qdr_link_flow(qdr_http1_adaptor->core, link, 1, false);
            // @TODO(kgiusti) deal with this
            return PN_REJECTED;
        }
        req->headers_sent = true;

        assert(!req->request_dlv && !req->request_msg);
        req->request_dlv = delivery;
        req->request_msg = msg;
        qdr_delivery_set_context(delivery, req);
    }

    return _send_request_body(req);
}


// Handle disposition/settlement update for the outstanding incoming HTTP message
//
void qdr_http1_server_delivery_update(qdr_http1_adaptor_t *adaptor,
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



