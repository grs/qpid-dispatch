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

#include <stdio.h>
#include <inttypes.h>

#define RAW_BUFFER_BATCH  16


////////////////////////////////////////
// TODO:
// * Deal with serializing the responses in the case of pipelined requests
//


/*
  HTTP/1.x <--> AMQP message mapping as defined by OASIS HTTP Over AMQP draft v1.0

  Projected Mode
  --------------

  Message Properties Section:

  HTTP Message                  AMQP Message Properties
  ------------                  -----------------------
  Request Method                subject
  Response Status               subject
  Request Target                to
  Content-Type                  content-type
  Content-Encoding              content-encoding
  Date                          creation-time
  From                          user-id
  Expires                       absolute-expiry-time

  Application Properties Section:

  HTTP Message                  AMQP Message App Properties
  ------------                  ---------------------------
  Request Version               "http:request": "<version|1.1 default>"
  Response Version              "http:response": "<version|1.1 default>"
  Response Reason               "http:reason": <string>
  *                             "<lowercase(key)>" <string>

  Notes:
   - Message App Properties Keys that start with ":" are reserved by the
  adaptor for meta-data.
   - OASIS insists the following headers MUST NOT be carried over:
       - TE
       - Trailer
       - Transfer-Encoding
       - Content-Length
       - Via
       - Connection
       - Upgrade

       however the adaptor does not modify the body encoding in any way so
       Content-Length and Tranfer-Encoding ARE going to be sent exactly as they
       are received.
   - The Connection header is a PITA.  Not only must it be parsed to determine
     if the sender is requesting that the TCP connection is closed when the
     response is complete, but it also *may* specify additional headers that
     must be filtered out.
 */



// A list of buffers containing data that starts at offset octets into
// the head buffer
//
typedef struct buffer_chain_t {
    DEQ_LINKS(struct buffer_chain_t);
    qd_buffer_list_t blist;
    size_t           offset;
} buffer_chain_t;
DEQ_DECLARE(buffer_chain_t, buffer_chain_list_t);
ALLOC_DECLARE(buffer_chain_t);
ALLOC_DEFINE(buffer_chain_t);

ALLOC_DEFINE(qdr_http1_request_t);
ALLOC_DEFINE(qdr_http1_out_data_t);
ALLOC_DEFINE(qdr_http1_connection_t);


qdr_http1_adaptor_t *qdr_http1_adaptor;


//
// Raw Connection Write Buffer Management
//


void qdr_http1_write_flush(qdr_http1_connection_t *hconn)
{
    pn_raw_buffer_t buffers[RAW_BUFFER_BATCH];
    size_t count = pn_raw_connection_write_buffers_capacity(hconn->raw_conn);

    while (count > 0 && hconn->write_ptr) {

        qdr_http1_out_data_t *od     = hconn->write_ptr;
        qd_buffer_t          *wbuf   = 0;
        int                   od_len = MIN(count,
                                           (od->buffer_count - od->next_buffer));
        assert(od_len);  // error: no data @ write_ptr?

        // send the out_data as a series of writes to proactor

        while (od_len) {

            size_t limit = MIN(RAW_BUFFER_BATCH, od_len);
            int written = 0;

            if (od->body_data) {  // buffers stored in qd_message_t

                written = qd_message_body_data_buffers(od->body_data, buffers, od->next_buffer, limit);
                for (int i = 0; i < written; ++i)
                    buffers[i].context = (uintptr_t)od;

            } else {   // list of buffers in od->raw_buffers

                // advance to next buffer to send in od
                if (!wbuf) {
                    wbuf = DEQ_HEAD(od->raw_buffers);
                    for (int i = 0; i < od->next_buffer; ++i)
                        wbuf = DEQ_NEXT(wbuf);
                }

                pn_raw_buffer_t *rdisc = &buffers[0];
                while (limit--) {
                    rdisc->context  = (uintptr_t)od;
                    rdisc->bytes    = (char*) qd_buffer_base(wbuf);
                    rdisc->size     = qd_buffer_size(wbuf);
                    rdisc->offset   = 0;
                    ++rdisc;
                    wbuf = DEQ_NEXT(wbuf);
                    written += 1;
                }
            }

            written = pn_raw_connection_write_buffers(hconn->raw_conn, buffers, written);
            count -= written;
            od_len -= written;
            od->next_buffer += written;
        }

        if (od->next_buffer == od->buffer_count) {
            // move to next out_data
            hconn->write_ptr = DEQ_NEXT(od);
            wbuf = 0;
        }
    }
}


void qdr_http1_write_buffer_list(qdr_http1_connection_t *hconn, qd_buffer_list_t *blist)
{
    qdr_http1_out_data_t *od = new_qdr_http1_out_data_t();
    ZERO(od);
    od->raw_buffers = *blist;
    od->buffer_count = (int) DEQ_SIZE(*blist);
    DEQ_INIT(*blist);

    DEQ_INSERT_TAIL(hconn->out_data, od);
    if (!hconn->write_ptr)
        hconn->write_ptr = od;

    if (hconn->raw_conn && !pn_raw_connection_is_write_closed(hconn->raw_conn))
        qdr_http1_write_flush(hconn);
}


void qdr_http1_write_body_data(qdr_http1_connection_t *hconn, qd_message_body_data_t *body_data)
{
    qdr_http1_out_data_t *od = new_qdr_http1_out_data_t();
    ZERO(od);
    od->body_data = body_data;
    od->buffer_count = qd_message_body_data_buffer_count(body_data);

    DEQ_INSERT_TAIL(hconn->out_data, od);
    if (!hconn->write_ptr)
        hconn->write_ptr = od;

    if (hconn->raw_conn && !pn_raw_connection_is_write_closed(hconn->raw_conn))
        qdr_http1_write_flush(hconn);
}


void qdr_http1_free_written_buffers(qdr_http1_connection_t *hconn)
{
    pn_raw_buffer_t buffers[RAW_BUFFER_BATCH];
    size_t count;
    while ((count = pn_raw_connection_take_written_buffers(hconn->raw_conn, buffers, RAW_BUFFER_BATCH)) != 0) {
        for (size_t i = 0; i < count; ++i) {
            qdr_http1_out_data_t *od = (qdr_http1_out_data_t*) buffers[i].context;
            assert(od);

            // Note: according to proton devs the order in which write buffers
            // are released are NOT guaranteed to be in the same order in which
            // they were written!

            od->free_count += 1;
            if (od->free_count == od->buffer_count) {
                assert(od != hconn->write_ptr);  // error: w.p. not advanced?
                DEQ_REMOVE(hconn->out_data, od);
                if (od->body_data)
                    qd_message_body_data_release(od->body_data);
                else
                    qd_buffer_list_free_buffers(&od->raw_buffers);
                free_qdr_http1_out_data_t(od);
            }
        }
    }
}


//
// Protocol Adaptor Callbacks
//


// Invoked by the core thread to wake an I/O thread for the connection
//
static void router_connection_activate_CT(void *context, qdr_connection_t *conn)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_connection_get_context(conn);
    if (!hconn) return;

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] Connection activate", hconn->conn_id);

    if (hconn->raw_conn) {
        pn_raw_connection_wake(hconn->raw_conn);

    } else if (hconn->type == HTTP1_CONN_SERVER) {
        // An HTTP server may choose to close the raw connection at any time.
        // Therefore the raw connection is created on demand. Unfortunately
        // there is a chicken/egg dilemma here: how do we wake a connection
        // when it does not exist?  In this case we have to activate the I/O
        // thread via a (gasp!) 0 duration timer.
        qd_timer_schedule(hconn->server.activate_timer, 0);

    } else {
        qd_log(qdr_http1_adaptor->log, QD_LOG_ERROR, "[C%i] Cannot activate connection", hconn->conn_id);
    }
}


static void router_link_first_attach(void               *context,
                                     qdr_connection_t   *conn,
                                     qdr_link_t         *link,
                                     qdr_terminus_t     *source,
                                     qdr_terminus_t     *target,
                                     qd_session_class_t  ssn_class)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_connection_get_context(conn);
    if (hconn)
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "[C%"PRIu64"] Link first attach", hconn->conn_id);
}


static void router_link_second_attach(void          *context,
                                     qdr_link_t     *link,
                                     qdr_terminus_t *source,
                                     qdr_terminus_t *target)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (!hconn) return;

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
           "[C%"PRIu64"][L%"PRIu64"] Link second attach", hconn->conn_id, link->identity);

    if (hconn->type == HTTP1_CONN_CLIENT) {
        if (link == hconn->out_link) {
            // this is the reply-to link for the client
            qd_iterator_t *reply_iter = qdr_terminus_get_address(source);
            hconn->client.reply_to_addr = (char*) qd_iterator_copy(reply_iter);
        }
    }
}


static void router_link_detach(void *context, qdr_link_t *link, qdr_error_t *error, bool first, bool close)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link detach", hconn->conn_id, link->identity);
    }
}


static void router_link_flow(void *context, qdr_link_t *link, int credit)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link flow (%d)",
               hconn->conn_id, link->identity, credit);
        if (hconn->type == HTTP1_CONN_SERVER)
            qdr_http1_server_link_flow((qdr_http1_adaptor_t*) context, hconn, link, credit);
        else
            qdr_http1_client_link_flow((qdr_http1_adaptor_t*) context, hconn, link, credit);
    }
}


static void router_link_offer(void *context, qdr_link_t *link, int delivery_count)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link offer (%d)",
               hconn->conn_id, link->identity, delivery_count);
    }
}


static void router_link_drained(void *context, qdr_link_t *link)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link drained",
               hconn->conn_id, link->identity);
    }
}


static void router_link_drain(void *context, qdr_link_t *link, bool mode)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link drain %s",
               hconn->conn_id, link->identity,
               mode ? "ON" : "OFF");
    }
}


static int router_link_push(void *context, qdr_link_t *link, int limit)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link push %d", hconn->conn_id, link->identity, limit);
        return qdr_link_process_deliveries(qdr_http1_adaptor->core, link, limit);
    }
    return 0;
}


// The I/O thread wants to send this delivery out the link
//
static uint64_t router_link_deliver(void *context, qdr_link_t *link, qdr_delivery_t *delivery, bool settled)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    uint64_t outcome = PN_RELEASED;

    if (hconn) {
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link deliver %p %s", hconn->conn_id, link->identity,
               (void*)delivery, settled ? "settled" : "unsettled");

        if (hconn->type == HTTP1_CONN_SERVER)
            outcome = qdr_http1_server_link_deliver(qdr_http1_adaptor, hconn, link, delivery, settled);
        else
            outcome = qdr_http1_client_link_deliver(qdr_http1_adaptor, hconn, link, delivery, settled);
    }

    return outcome;
}

static int router_link_get_credit(void *context, qdr_link_t *link)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_link_get_context(link);
    int credit = 0;
    if (hconn) {
        credit = (link == hconn->in_link) ? hconn->in_link_credit : hconn->out_link_credit;
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Link get credit (%d)", hconn->conn_id, link->identity, credit);
    }

    return credit;
}


// Handle disposition/settlement update for the outstanding incoming HTTP message
//
static void router_delivery_update(void *context, qdr_delivery_t *dlv, uint64_t disp, bool settled)
{
    qdr_http1_request_t *hreq = (qdr_http1_request_t*) qdr_delivery_get_context(dlv);
    if (hreq) {
        qdr_http1_connection_t *hconn = hreq->hconn;
        qdr_link_t *link = qdr_delivery_link(dlv);
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG,
               "[C%"PRIu64"][L%"PRIu64"] Delivery update %p 0x%"PRIx64" %s",
               hconn->conn_id, link->identity, (void*) dlv,
               settled ? "settled" : "unsettled");

        if (hconn->type == HTTP1_CONN_SERVER)
            qdr_http1_server_delivery_update(qdr_http1_adaptor, hconn, hreq, dlv, disp, settled);
        else
            qdr_http1_client_delivery_update(qdr_http1_adaptor, hconn, hreq, dlv, disp, settled);
    }
}

static void router_conn_close(void *context, qdr_connection_t *conn, qdr_error_t *error)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_connection_get_context(conn);
    if (hconn) {
        if (hconn->trace)
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"] HTTP/1.x closing connection", hconn->conn_id);

        hconn->close_connection = true;
        if (hconn->raw_conn)
            pn_raw_connection_wake(hconn->raw_conn);
    }
}


static void router_conn_trace(void *context, qdr_connection_t *conn, bool trace)
{
    qdr_http1_connection_t *hconn = (qdr_http1_connection_t*) qdr_connection_get_context(conn);
    if (hconn) {
        hconn->trace = trace;
        if (trace)
            qd_log(qdr_http1_adaptor->log, QD_LOG_TRACE,
                   "[C%"PRIu64"] HTTP/1.x trace enabled", hconn->conn_id);
    }
}


//
// Adaptor Setup & Teardown
//


static void qd_http1_adaptor_init(qdr_core_t *core, void **adaptor_context)
{
    qdr_http1_adaptor_t *adaptor = NEW(qdr_http1_adaptor_t);

    ZERO(adaptor);
    adaptor->core    = core;
    adaptor->adaptor = qdr_protocol_adaptor(core,
                                            "http/1.x",
                                            adaptor,             // context
                                            router_connection_activate_CT,  // core thread only
                                            router_link_first_attach,
                                            router_link_second_attach,
                                            router_link_detach,
                                            router_link_flow,
                                            router_link_offer,
                                            router_link_drained,
                                            router_link_drain,
                                            router_link_push,
                                            router_link_deliver,
                                            router_link_get_credit,
                                            router_delivery_update,
                                            router_conn_close,
                                            router_conn_trace);
    adaptor->log = qd_log_source(QD_HTTP_LOG_SOURCE);
    DEQ_INIT(adaptor->listeners);
    DEQ_INIT(adaptor->connectors);
    *adaptor_context = adaptor;

    qdr_http1_adaptor = adaptor;
}


static void qd_http1_adaptor_final(void *adaptor_context)
{
    qdr_http1_adaptor_t *adaptor = (qdr_http1_adaptor_t*) adaptor_context;
    qdr_protocol_adaptor_free(adaptor->core, adaptor->adaptor);
    // @TODO(kgiusti) clean up
    free(adaptor);
    qdr_http1_adaptor =  NULL;
}

/**
 * Declare the adaptor so that it will self-register on process startup.
 */
QDR_CORE_ADAPTOR_DECLARE("http1.x-adaptor", qd_http1_adaptor_init, qd_http1_adaptor_final)

