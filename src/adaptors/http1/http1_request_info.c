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
#include <qpid/dispatch/server.h>
#include "qpid/dispatch/ctools.h"
#include "qpid/dispatch/protocol_adaptor.h"
#include "dispatch_private.h"
#include <inttypes.h>

typedef struct qdr_http_request_info_t  qdr_http_request_info_t;

struct qdr_http_request_info_t {
    DEQ_LINKS(qdr_http_request_info_t);

    char     *key;
    char     *address;
    char     *host;
    char     *site;
    bool      ingress;
    uint64_t  requests;
    uint64_t  bytes_in;
    uint64_t  bytes_out;
    uint64_t  max_latency;
};
DEQ_DECLARE(qdr_http_request_info_t, qdr_http_request_info_list_t);

#define QDR_HTTP_REQUEST_INFO_NAME                   0
#define QDR_HTTP_REQUEST_INFO_IDENTITY               1
#define QDR_HTTP_REQUEST_INFO_ADDRESS                2
#define QDR_HTTP_REQUEST_INFO_HOST                   3
#define QDR_HTTP_REQUEST_INFO_SITE                   4
#define QDR_HTTP_REQUEST_INFO_DIRECTION              5
#define QDR_HTTP_REQUEST_INFO_REQUESTS               6
#define QDR_HTTP_REQUEST_INFO_BYTES_IN               7
#define QDR_HTTP_REQUEST_INFO_BYTES_OUT              8
#define QDR_HTTP_REQUEST_INFO_MAX_LATENCY            9


const char * const QDR_HTTP_REQUEST_INFO_DIRECTION_IN  = "in";
const char * const QDR_HTTP_REQUEST_INFO_DIRECTION_OUT = "out";

const char *qdr_http_request_info_columns[] =
    {"name",
     "identity",
     "address",
     "host",
     "site",
     "direction",
     "requests",
     "bytesIn",
     "bytesOut",
     "maxLatency",
     0};

const char *HTTP_REQUEST_INFO_TYPE = "org.apache.qpid.dispatch.httpRequestInfo";

typedef struct {
    qdr_http_request_info_list_t records;
} http_request_info_records_t;

static http_request_info_records_t* request_info = 0;

static http_request_info_records_t *_get_request_info()
{
    if (!request_info) {
        request_info = NEW(http_request_info_records_t);
        DEQ_INIT(request_info->records);
    }
    return request_info;
}

static void insert_column(qdr_core_t *core, qdr_http_request_info_t *record, int col, qd_composed_field_t *body)
{
    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Insert column %i for %p", col, (void*) record);

    if (!record)
        return;

    switch(col) {
    case QDR_HTTP_REQUEST_INFO_NAME:
        qd_compose_insert_string(body, record->key);
        break;

    case QDR_HTTP_REQUEST_INFO_IDENTITY: {
        qd_compose_insert_string(body, record->key);
        break;
    }

    case QDR_HTTP_REQUEST_INFO_ADDRESS:
        qd_compose_insert_string(body, record->address);
        break;

    case QDR_HTTP_REQUEST_INFO_HOST:
        qd_compose_insert_string(body, record->host);
        break;

    case QDR_HTTP_REQUEST_INFO_SITE:
        qd_compose_insert_string(body, record->site);
        break;

    case QDR_HTTP_REQUEST_INFO_DIRECTION:
        if (record->ingress)
            qd_compose_insert_string(body, QDR_HTTP_REQUEST_INFO_DIRECTION_IN);
        else
            qd_compose_insert_string(body, QDR_HTTP_REQUEST_INFO_DIRECTION_OUT);
        break;

    case QDR_HTTP_REQUEST_INFO_REQUESTS:
        qd_compose_insert_uint(body, record->requests);
        break;

    case QDR_HTTP_REQUEST_INFO_BYTES_IN:
        qd_compose_insert_uint(body, record->bytes_in);
        break;

    case QDR_HTTP_REQUEST_INFO_BYTES_OUT:
        qd_compose_insert_uint(body, record->bytes_out);
        break;

    case QDR_HTTP_REQUEST_INFO_MAX_LATENCY:
        qd_compose_insert_uint(body, record->max_latency);
        break;

    }
}


static void write_list(qdr_core_t *core, qdr_query_t *query,  qdr_http_request_info_t *record)
{
    qd_composed_field_t *body = query->body;

    qd_compose_start_list(body);

    if (record) {
        int i = 0;
        while (query->columns[i] >= 0) {
            insert_column(core, record, query->columns[i], body);
            i++;
        }
    }
    qd_compose_end_list(body);
}

static void write_map(qdr_core_t           *core,
                      qdr_http_request_info_t *record,
                      qd_composed_field_t  *body,
                      const char           *qdr_connection_columns[])
{
    qd_compose_start_map(body);

    for(int i = 0; i < QDR_HTTP_REQUEST_INFO_COLUMN_COUNT; i++) {
        qd_compose_insert_string(body, qdr_connection_columns[i]);
        insert_column(core, record, i, body);
    }

    qd_compose_end_map(body);
}

static void advance(qdr_query_t *query, qdr_http_request_info_t *record)
{
    if (record) {
        query->next_offset++;
        record = DEQ_NEXT(record);
        query->more = !!record;
    }
    else {
        query->more = false;
    }
}

static qdr_http_request_info_t *find_by_identity(qdr_core_t *core, qd_iterator_t *identity)
{
    if (!identity)
        return 0;

    qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records);
    while (record) {
        // Convert the passed in identity to a char*
        if (qd_iterator_equal(identity, (const unsigned char*) record->key))
            break;
        record = DEQ_NEXT(record);
    }

    return record;

}

void qdra_http_request_info_get_first_CT(qdr_core_t *core, qdr_query_t *query, int offset)
{
    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "query for first http request info (%i)", offset);
    query->status = QD_AMQP_OK;

    if (offset >= DEQ_SIZE(_get_request_info()->records)) {
        query->more = false;
        qdr_agent_enqueue_response_CT(core, query);
        return;
    }

    qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records);
    for (int i = 0; i < offset && record; i++)
        record = DEQ_NEXT(record);
    assert(record);

    if (record) {
        write_list(core, query, record);
        query->next_offset = offset;
        advance(query, record);
    } else {
        query->more = false;
    }

    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_http_request_info_get_next_CT(qdr_core_t *core, qdr_query_t *query)
{
    qdr_http_request_info_t *record = 0;

    if (query->next_offset < DEQ_SIZE(_get_request_info()->records)) {
        record = DEQ_HEAD(_get_request_info()->records);
        for (int i = 0; i < query->next_offset && record; i++)
            record = DEQ_NEXT(record);
    }

    if (record) {
        write_list(core, query, record);
        advance(query, record);
    } else {
        query->more = false;
    }
    qdr_agent_enqueue_response_CT(core, query);
}

void qdra_http_request_info_get_CT(qdr_core_t          *core,
                               qd_iterator_t       *name,
                               qd_iterator_t       *identity,
                               qdr_query_t         *query,
                               const char          *qdr_http_request_info_columns[])
{
    qdr_http_request_info_t *record = 0;

    if (!identity) {
        query->status = QD_AMQP_BAD_REQUEST;
        query->status.description = "Name not supported. Identity required";
        qd_log(core->agent_log, QD_LOG_ERROR, "Error performing READ of %s: %s", HTTP_REQUEST_INFO_TYPE, query->status.description);
    } else {
        record = find_by_identity(core, identity);

        if (record == 0) {
            query->status = QD_AMQP_NOT_FOUND;
        } else {
            write_map(core, record, query->body, qdr_http_request_info_columns);
            query->status = QD_AMQP_OK;
        }
    }
    qdr_agent_enqueue_response_CT(core, query);
}

static void _free_qdr_http_request_info(qdr_http_request_info_t* record)
{
    if (record->key) {
        free(record->key);
    }
    if (record->address) {
        free(record->address);
    }
    if (record->host) {
        free(record->host);
    }
    if (record->site) {
        free(record->site);
    }
    free(record);
}

static bool _update_qdr_http_request_info(qdr_http_request_info_t* record, qdr_http_request_info_t* additions)
{
    if (strcmp(record->key, additions->key) == 0) {
        record->requests += additions->requests;
        record->bytes_in += additions->bytes_in;
        record->bytes_out += additions->bytes_out;
        if (additions->max_latency > record->max_latency) {
            record->max_latency = additions->max_latency;
        }
        return true;
    } else {
        return false;
    }
}

static void _add_http_request_info_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_http_request_info_t *update = (qdr_http_request_info_t*) action->args.general.context_1;
    bool updated = false;
    for (qdr_http_request_info_t *record = DEQ_HEAD(_get_request_info()->records); record && !updated; record = DEQ_NEXT(record)) {
        if (_update_qdr_http_request_info(record, update)) {
            updated = true;
            _free_qdr_http_request_info(update);
            qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Updated http request info %s", record->key);
        }
    }
    if (!updated) {
        DEQ_INSERT_TAIL(_get_request_info()->records, update);
        qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Added http request info %s (%i)", update->key, DEQ_SIZE(_get_request_info()->records));
    }
}

static void _add_http_request_info(qdr_core_t *core, qdr_http_request_info_t* record)
{
    qdr_action_t *action = qdr_action(_add_http_request_info_CT, "add_http_request_info");
    action->args.general.context_1 = record;
    qdr_action_enqueue(core, action);
}

/*

static void _del_http_request_info_CT(qdr_core_t *core, qdr_action_t *action, bool discard)
{
    qdr_http_request_info_t *record = (qdr_http_request_info_t*) action->args.general.context_1;
    DEQ_REMOVE(_get_request_info()->records, record);
    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Removed http request info %s (%i)", record->address, DEQ_SIZE(_get_request_info()->records));
    _free_qdr_http_request_info(record);
}

static void _del_http_request_info(qdr_core_t *core, qdr_http_request_info_t* record)
{
    qdr_action_t *action = qdr_action(_del_http_request_info_CT, "delete_http_request_info");
    action->args.general.context_1 = record;
    qdr_action_enqueue(core, action);
}
*/

static qdr_http_request_info_t* _new_qdr_http_request_info_t()
{
    qdr_http_request_info_t* record = NEW(qdr_http_request_info_t);
    ZERO(record);
    return record;
}

static char *_record_key(char *host, char* site, bool ingress)
{
    size_t hostlen = strlen(host);
    char *key = malloc(hostlen + strlen(site) + 4);
    size_t i = 0;
    key[i++] = ingress ? 'i' : 'o';
    key[i++] = '_';
    strcpy(key+i, host);
    i += hostlen;
    key[i++] = '@';
    strcpy(key+i, site);
    return key;
}

void qdr_http1_record_client_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_t *request, qdr_http1_response_msg_t *response)
{
    qdr_http_request_info_t* record = _new_qdr_http_request_info_t();
    record->ingress = true;
    record->address = qd_strdup(request->hconn->cfg.address);
    record->host = qd_strdup(request->hconn->client.client_ip_addr);
    record->site = request->site;
    record->key = _record_key(record->host, request->hconn->cfg.site ? qd_strdup(request->hconn->cfg.site) : 0, true);
    record->requests = 1;
    record->bytes_in = request->in_http1_octets;
    record->bytes_out = request->out_http1_octets;
    record->max_latency = request->stop && request->start ? request->stop - request->start : 0;

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Adding client http request info %s", record->key);
    _add_http_request_info(adaptor->core, record);
}

void qdr_http1_record_server_request_info(qdr_http1_adaptor_t *adaptor, qdr_http1_request_t *request, int status, const char *reason)
{
    qdr_http_request_info_t* record = _new_qdr_http_request_info_t();
    record->ingress = false;
    record->address = qd_strdup(request->hconn->cfg.address);
    record->host = qd_strdup(request->hconn->cfg.host_port);
    record->site = request->site;
    record->key = _record_key(record->host, request->hconn->cfg.site ? qd_strdup(request->hconn->cfg.site) : 0, false);
    record->requests = 1;
    record->bytes_in = request->in_http1_octets;
    record->bytes_out = request->out_http1_octets;
    record->max_latency = request->stop && request->start ? request->stop - request->start : 0;

    qd_log(qdr_http1_adaptor->log, QD_LOG_DEBUG, "Adding server http request info %s", record->key);
    _add_http_request_info(adaptor->core, record);
}
