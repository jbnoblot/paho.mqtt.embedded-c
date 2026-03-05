/*******************************************************************************
 * Copyright (c) 2014, 2023 IBM Corp., Ian Craggs and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *   Allan Stockdill-Mander/Ian Craggs - initial API and implementation and/or initial documentation
 *   Ian Craggs - fix for #96 - check rem_len in readPacket
 *   Ian Craggs - add ability to set message handler separately #6
 *******************************************************************************/
#if defined(MQTTV5)
#include "V5/MQTTV5Client.h"
#else
#include "MQTTClient.h"
#endif

#include <stdio.h>
#include <string.h>

static void NewMessageData(MessageData *md, MQTTString *aTopicName, MQTTMessage *aMessage)
{
    md->topicName = aTopicName;
    md->message = aMessage;
}

static int getNextPacketId(MQTTClient *c)
{
    return c->next_packetid = (c->next_packetid == MAX_PACKET_ID) ? 1 : c->next_packetid + 1;
}

static int sendPacket(MQTTClient *c, int32_t length, const Timer *timer)
{
    int rc = MQTTCLIENT_FAILURE;
    int sent = 0;

    while (sent < length)
    {
        rc = c->ipstack->mqttwrite(c->ipstack, &c->buf[sent], length - sent, TimerLeftMS(timer));
        if (rc < 0) /* there was an error writing the data */
            break;
        sent += rc;
        if (TimerIsExpired(timer)) /* only check expiry after at least one attempt to write */
            break;
    }
    if (sent == length)
    {
        TimerCountdown(&c->last_sent, c->keepAliveInterval); /* record the fact that we have successfully sent the packet */
        rc = MQTTCLIENT_SUCCESS;
    }
    else
        rc = MQTTCLIENT_FAILURE;
    return rc;
}

#if defined(MQTTV5)
void MQTTV5ClientInit(MQTTClient *client, Network *network, unsigned int command_timeout_ms,
                      unsigned char *sendbuf, size_t sendbuf_size, unsigned char *readbuf, size_t readbuf_size,
                      MQTTProperties *recvProperties, bool truncateRecvProperties)
#else
void MQTTClientInit(MQTTClient *c, Network *network, unsigned int command_timeout_ms,
                    unsigned char *sendbuf, size_t sendbuf_size, unsigned char *readbuf, size_t readbuf_size)
#endif
{
    c->ipstack = network;

    for (int i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        c->messageHandlers[i].topicFilter = 0;
    }
    c->command_timeout_ms = command_timeout_ms;
    c->buf = sendbuf;
    c->buf_size = sendbuf_size;
    c->readbuf = readbuf;
    c->readbuf_size = readbuf_size;
    c->isconnected = 0;
#if defined(MQTTV5)
    c->cleanstart = 0;
    c->recvProperties = recvProperties;
    c->truncateRecvProperties = truncateRecvProperties;
#else
    c->cleansession = 0;
#endif
    c->ping_outstanding = 0;
    c->defaultMessageHandler = NULL;
    c->next_packetid = 1;
    TimerInit(&c->last_sent);
    TimerInit(&c->last_received);
    TimerInit(&c->pingresp_timer);
#if defined(MQTT_TASK)
    MutexInit(&c->mutex);
#endif
}

static int decodePacket(MQTTClient *c, int *value, int timeout)
{
    unsigned char i;
    int multiplier = 1;
    int32_t len = 0;
    const int MAX_NO_OF_REMAINING_LENGTH_BYTES = 4;

    *value = 0;
    do
    {
        int rc = MQTTPACKET_READ_ERROR;

        if (++len > MAX_NO_OF_REMAINING_LENGTH_BYTES)
        {
            rc = MQTTPACKET_READ_ERROR; /* bad data */
            goto exit;
        }
        rc = c->ipstack->mqttread(c->ipstack, &i, 1, timeout);
        if (rc != 1)
        {
            goto exit;
        }
        *value += (i & 127) * multiplier;
        multiplier *= 128;
    } while ((i & 128) != 0);
exit:
    return len;
}

static int readPacket(MQTTClient *c, const Timer *timer)
{
    unsigned char header;
    int32_t len = 0;
    int rem_len = 0;

    /* 1. read the header byte.  This has the packet type in it */
    int rc = c->ipstack->mqttread(c->ipstack, c->readbuf, 1, TimerLeftMS(timer));
    if (rc != 1)
        goto exit;

    len = 1;
    /* 2. read the remaining length.  This is variable in itself */
    decodePacket(c, &rem_len, TimerLeftMS(timer));
    len += MQTTPacket_encode(c->readbuf + 1, rem_len); /* put the original remaining length back into the buffer */

    if (rem_len > (c->readbuf_size - len))
    {
        rc = MQTTCLIENT_BUFFER_OVERFLOW;
        goto exit;
    }

    /* 3. read the rest of the buffer using a callback to supply the rest of the data */
    if (rem_len > 0 && (rc = c->ipstack->mqttread(c->ipstack, c->readbuf + len, rem_len, TimerLeftMS(timer)) != rem_len))
    {
        rc = 0;
        goto exit;
    }

    header = c->readbuf[0];
    rc = (header & MQTT_HEADER_TYPE_MASK) >> MQTT_HEADER_TYPE_SHIFT;
    if (c->keepAliveInterval > 0)
    {
        TimerCountdown(&c->last_received, c->keepAliveInterval); // record the fact that we have successfully received a packet
    }
exit:
    return rc;
}

// assume topic filter and name is in correct format
// # can only be at end
// + and # can only be next to separator
static char isTopicMatched(const char *topicFilter, const MQTTString *topicName)
{
    const char *curf = topicFilter;
    const char *curn = topicName->lenstring.data;
    const char *curn_end = curn + topicName->lenstring.len;

    while (*curf && curn < curn_end)
    {
        if (*curn == '/' && *curf != '/')
            break;
        if (*curf != '+' && *curf != '#' && *curf != *curn)
            break;
        if (*curf == '+')
        { // skip until we meet the next separator, or end of string
            const char *nextpos = curn + 1;
            while (nextpos < curn_end && *nextpos != '/')
                nextpos = ++curn + 1;
        }
        else if (*curf == '#')
            curn = curn_end - 1; // skip until end of string
        curf++;
        curn++;
    };

    return (curn == curn_end) && (*curf == '\0' || *curf == '#');
}

int deliverMessage(MQTTClient *c, MQTTString *topicName, MQTTMessage *message)
{
    int rc = MQTTCLIENT_FAILURE;

    // we have to find the right message handler - indexed by topic
    for (int i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if (c->messageHandlers[i].topicFilter != 0 && (MQTTPacket_equals(topicName, (char *)c->messageHandlers[i].topicFilter) ||
                                                       isTopicMatched((char *)c->messageHandlers[i].topicFilter, topicName)))
        {
            if (c->messageHandlers[i].fp != NULL)
            {
                MessageData md;
                NewMessageData(&md, topicName, message);
                c->messageHandlers[i].fp(&md);
                rc = MQTTCLIENT_SUCCESS;
            }
        }
    }

    if (rc == MQTTCLIENT_FAILURE && c->defaultMessageHandler != NULL)
    {
        MessageData md;
        NewMessageData(&md, topicName, message);
        c->defaultMessageHandler(&md);
        rc = MQTTCLIENT_SUCCESS;
    }

    return rc;
}

int keepalive(MQTTClient *c)
{
    int rc = MQTTCLIENT_SUCCESS;

    if (c->keepAliveInterval == 0)
        goto exit;

    // If we are waiting for a ping response, check if it has been too long
    if (c->ping_outstanding)
    {
        if (TimerIsExpired(&c->pingresp_timer))
        {
            rc = MQTTCLIENT_FAILURE; /* PINGRESP not received in keepalive interval */
            goto exit;
        }
    }
    else
    {
        // If we have not sent or received anything in the timeout period,
        // send out a ping request
        if (TimerIsExpired(&c->last_sent) || TimerIsExpired(&c->last_received))
        {
            Timer timer;

            TimerInit(&timer);
            TimerCountdownMS(&timer, 1000);
            int32_t len = MQTTSerialize_pingreq(c->buf, c->buf_size);
            if (len > 0 && (rc = sendPacket(c, len, &timer)) == MQTTCLIENT_SUCCESS)
            {
                c->ping_outstanding = 1;
                TimerCountdown(&c->pingresp_timer, c->keepAliveInterval);
            }
        }
    }

exit:
    return rc;
}

void MQTTCleanSession(MQTTClient *c)
{
    for (int i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        c->messageHandlers[i].topicFilter = NULL;
    }
}

void MQTTCloseSession(MQTTClient *c)
{
    c->ping_outstanding = 0;
    c->isconnected = 0;

#if defined(MQTTV5)
    // TODO: Session cleanup happens only if Clean Start = 1 and Session Expiry is 0.
    if (c->cleanstart)
    {
        MQTTCleanSession(c);
    }
#else
    if (c->cleansession)
    {
        MQTTCleanSession(c);
    }
#endif
}

int cycle(MQTTClient *c, const Timer *timer)
{
    int32_t len = 0;
    int rc = MQTTCLIENT_SUCCESS;

    int packet_type = readPacket(c, timer); /* read the socket, see what work is due */

    switch (packet_type)
    {
    default:
        /* no more data to read, unrecoverable. Or read packet fails due to unexpected network error */
        rc = packet_type;
        goto exit;
    case 0: /* timed out reading packet */
        break;
    case CONNACK:
    case PUBACK:
    case SUBACK:
    case UNSUBACK:
        break;
    case PUBLISH:
    {
        MQTTString topicName;
        MQTTMessage msg;

        unsigned char intQoS;
        msg.payloadlen = 0; /* this is a size_t, but deserialize publish sets this as int */
#if defined(MQTTV5)
        rc = MQTTV5Deserialize_publish(&msg.dup, &intQoS, &msg.retained, &msg.id, &topicName,
                                       c->recvProperties, (unsigned char **)&msg.payload, (int *)&msg.payloadlen, c->readbuf, c->readbuf_size);
#else
        rc = MQTTDeserialize_publish(&msg.dup, &intQoS, &msg.retained, &msg.id, &topicName,
                                     (unsigned char **)&msg.payload, (int *)&msg.payloadlen, c->readbuf, c->readbuf_size);
#endif
        if (rc != 1)
        {
            goto exit;
        }
        msg.qos = (enum MQTTQoS)intQoS;
        rc = deliverMessage(c, &topicName, &msg);
        if (msg.qos != MQTTQOS_0)
        {
#if defined(MQTTV5)
            int reasonCode = MQTTREASONCODE_SUCCESS;
            if (rc == MQTTCLIENT_FAILURE)
            {
                reasonCode = MQTTREASONCODE_NO_MATCHING_SUBSCRIBERS;
            }
#endif
            if (msg.qos == MQTTQOS_1)
            {
#if defined(MQTTV5)
                len = MQTTV5Serialize_ack(c->buf, c->buf_size, PUBACK, 0, msg.id, reasonCode, NULL);
#else
                len = MQTTSerialize_ack(c->buf, c->buf_size, PUBACK, 0, msg.id);
#endif
            }
            else if (msg.qos == MQTTQOS_2)
            {
#if defined(MQTTV5)
                len = MQTTV5Serialize_ack(c->buf, c->buf_size, PUBREC, 0, msg.id, reasonCode, NULL);
#else
                len = MQTTSerialize_ack(c->buf, c->buf_size, PUBREC, 0, msg.id);
#endif
            }
            if (len <= 0)
            {
                rc = MQTTCLIENT_FAILURE;
            }
            else
            {
                rc = sendPacket(c, len, timer);
            }
            if (rc == MQTTCLIENT_FAILURE)
            {
                goto exit; // there was a problem
            }
        }
        break;
    }
    case PUBREC:
    case PUBREL:
    {
        unsigned short mypacketid;
        unsigned char dup;
        unsigned char type;
#if defined(MQTTV5)
        unsigned char reasonCode;
        rc = MQTTV5Deserialize_ack(&type, &dup, &mypacketid, &reasonCode, c->recvProperties, c->readbuf, c->readbuf_size);
#else
        rc = MQTTDeserialize_ack(&type, &dup, &mypacketid, c->readbuf, c->readbuf_size);
#endif
        if (rc != 1)
        {
            goto exit; // there was a problem
        }
#if defined(MQTTV5)
        len = MQTTV5Serialize_ack(c->buf, c->buf_size, (packet_type == PUBREC) ? PUBREL : PUBCOMP, 0, mypacketid, MQTTREASONCODE_SUCCESS, NULL);
#else
        len = MQTTSerialize_ack(c->buf, c->buf_size, (packet_type == PUBREC) ? PUBREL : PUBCOMP, 0, mypacketid);
#endif
        if (len <= 0)
        {
            rc = MQTTCLIENT_FAILURE;
            goto exit; // there was a problem
        }
        rc = sendPacket(c, len, timer);
        if (rc != MQTTCLIENT_SUCCESS) // send the PUBREL packet
            rc = MQTTCLIENT_FAILURE;  // there was a problem
        break;
    }
    case PUBCOMP:
        break;
    case PINGRESP:
        c->ping_outstanding = 0;
        break;
#if defined(MQTTV5)
    case AUTH:
        unsigned short mypacketid;
        unsigned char dup;
        unsigned char type;
        unsigned char reasonCode;
        rc = MQTTV5Deserialize_ack(&type, &dup, &mypacketid, &reasonCode, c->recvProperties, c->readbuf, c->readbuf_size);
        c->authHandler(c->recvProperties, reasonCode, mypacketid);
        break;
    case DISCONNECT:
        unsigned short mypacketid;
        unsigned char dup;
        unsigned char type;
        unsigned char reasonCode;
        rc = MQTTV5Deserialize_ack(&type, &dup, &mypacketid, &reasonCode, c->recvProperties, c->readbuf, c->readbuf_size);
        // TODO: implement DISCONNECTv5 and callback to expose reason code and properties.
        c->disconnectHandler(c->recvProperties, reasonCode, mypacketid);
        break;
#endif
    }

    if (keepalive(c) != MQTTCLIENT_SUCCESS)
    {
        // check only keepalive MQTTCLIENT_FAILURE status so that previous MQTTCLIENT_FAILURE status can be considered as FAULT
        rc = MQTTCLIENT_FAILURE;
    }

exit:
    if (rc == MQTTCLIENT_SUCCESS)
    {
        rc = packet_type;
    }
    else if (c->isconnected)
    {
        MQTTCloseSession(c);
    }
    return rc;
}

int MQTTYield(MQTTClient *c, int timeout_ms)
{
    int rc = MQTTCLIENT_SUCCESS;
    Timer timer;

    TimerInit(&timer);
    TimerCountdownMS(&timer, timeout_ms);

    do
    {
        rc = cycle(c, &timer);
        if (rc < 0)
        {
            if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
            break;
        }
    } while (!TimerIsExpired(&timer));

    return rc;
}

int MQTTIsConnected(const MQTTClient *client)
{
    return client->isconnected;
}

void MQTTRun(void *parm)
{
    Timer timer;
    MQTTClient *c = (MQTTClient *)parm;

    TimerInit(&timer);

    while (1)
    {
#if defined(MQTT_TASK)
        MutexLock(&c->mutex);
#endif
        TimerCountdownMS(&timer, 500); /* Don't wait too long if no traffic is incoming */
        cycle(c, &timer);
#if defined(MQTT_TASK)
        MutexUnlock(&c->mutex);
#endif
    }
}

#if defined(MQTT_TASK)
int MQTTStartTask(MQTTClient *client)
{
    return ThreadStart(&client->thread, &MQTTRun, client);
}
#endif

int waitfor(MQTTClient *c, int packet_type, const Timer *timer)
{
    int rc = MQTTCLIENT_FAILURE;

    do
    {
        if (TimerIsExpired(timer))
        {
            break; // we timed out
        }
        rc = cycle(c, timer);
    } while (rc != packet_type && rc >= 0);

    return rc;
}
#if defined(MQTTV5)
int MQTTV5ConnectWithResults(MQTTClient *c, MQTTPacket_connectData *options,
                             MQTTProperties *connectProperties, MQTTProperties *willProperties, MQTTConnackData *data)
#else
int MQTTConnectWithResults(MQTTClient *c, MQTTPacket_connectData *options, MQTTConnackData *data)
#endif
{
    Timer connect_timer;
    int rc = MQTTCLIENT_FAILURE;
    MQTTPacket_connectData default_options = MQTTPacket_connectData_initializer;
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (c->isconnected) /* don't send connect packet again if we are already connected */
    {
        goto exit;
    }

    TimerInit(&connect_timer);
    TimerCountdownMS(&connect_timer, c->command_timeout_ms);

    if (options == 0)
    {
        options = &default_options; /* set default options if none were supplied */
    }
    c->keepAliveInterval = options->keepAliveInterval;
#if defined(MQTTV5)
    c->cleanstart = options->cleanstart;
#else
    c->cleansession = options->cleansession;
#endif
    TimerCountdown(&c->last_received, c->keepAliveInterval);
    if ((len = MQTTSerialize_connect(c->buf, c->buf_size, options)) <= 0)
        goto exit;
    if ((rc = sendPacket(c, len, &connect_timer)) != MQTTCLIENT_SUCCESS) // send the connect packet
    {
        goto exit; // there was a problem
    }
    // this will be a blocking call, wait for the connack
    if (waitfor(c, CONNACK, &connect_timer) == CONNACK)
    {
#if defined(MQTTV5)
        data->reasonCode = MQTTREASONCODE_SUCCESS;
#else
        data->rc = 0;
#endif

        data->sessionPresent = 0;

#if defined(MQTTV5)
        rc = MQTTV5Deserialize_connack(c->recvProperties, &data->sessionPresent, (unsigned char *)(&data->reasonCode),
                                       c->readbuf, c->readbuf_size) if (rc == 1)
        {
            rc = (int)data->reasonCode;
        }
#else
        if (MQTTDeserialize_connack(&data->sessionPresent, &data->rc, c->readbuf, c->readbuf_size) == 1)
        {
            rc = data->rc;
        }
#endif
        else
        {
            if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
        }
    }
    else
        rc = MQTTCLIENT_FAILURE;
exit:
    if (rc == MQTTCLIENT_SUCCESS)
    {
        c->isconnected = 1;
        c->ping_outstanding = 0;
    }

#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif

    return rc;
}

#if defined(MQTTV5)
int MQTTV5Connect(MQTTClient *client, MQTTPacket_connectData *options,
                  MQTTProperties *connectProperties, MQTTProperties *willProperties)
{
    MQTTConnackData data;
    return MQTTV5ConnectWithResults(client, options, connectProperties, willProperties, &data);
}
#else
int MQTTConnect(MQTTClient *c, MQTTPacket_connectData *options)
{
    MQTTConnackData data;
    return MQTTConnectWithResults(c, options, &data);
}
#endif

#if defined(MQTTV5)
int MQTTV5SetMessageHandler(MQTTClient *c, const char *topicFilter, messageHandler messageHandler)
#else
int MQTTSetMessageHandler(MQTTClient *c, const char *topicFilter, messageHandler messageHandler)
#endif
{
    int rc = MQTTCLIENT_FAILURE;
    int i = -1;

    /* first check for an existing matching slot */
    for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
    {
        if (c->messageHandlers[i].topicFilter != NULL && strcmp(c->messageHandlers[i].topicFilter, topicFilter) == 0)
        {
            if (messageHandler == NULL) /* remove existing */
            {
                c->messageHandlers[i].topicFilter = NULL;
                c->messageHandlers[i].fp = NULL;
            }
            rc = MQTTCLIENT_SUCCESS; /* return i when adding new subscription */
            break;
        }
    }
    /* if no existing, look for empty slot (unless we are removing) */
    if (messageHandler != NULL)
    {
        if (rc == MQTTCLIENT_FAILURE)
        {
            for (i = 0; i < MAX_MESSAGE_HANDLERS; ++i)
            {
                if (c->messageHandlers[i].topicFilter == NULL)
                {
                    rc = MQTTCLIENT_SUCCESS;
                    break;
                }
            }
        }
        if (i < MAX_MESSAGE_HANDLERS)
        {
            c->messageHandlers[i].topicFilter = topicFilter;
            c->messageHandlers[i].fp = messageHandler;
        }
    }
    return rc;
}

#if defined(MQTTV5)
int MQTTV5SubscribeWithResults(MQTTClient *c, const char *topicFilter, enum MQTTQoS requestedQoS,
                               MQTTProperties *properties, MQTTSubscribe_options options, messageHandler messageHandler, MQTTSubackData *data)
#else
int MQTTSubscribeWithResults(MQTTClient *c, const char *topicFilter, enum MQTTQoS requestedQoS,
                             messageHandler messageHandler, MQTTSubackData *data)
#endif
{
    int rc = MQTTCLIENT_FAILURE;
    Timer timer;
    int32_t len = 0;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicFilter;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (!c->isconnected)
        goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    unsigned char _qos = requestedQoS;
    len = MQTTSerialize_subscribe(c->buf, c->buf_size, 0, getNextPacketId(c), 1, &topic, &_qos);
    if (len <= 0)
    {
        goto exit;
    }
    rc = sendPacket(c, len, &timer);
    if (rc != MQTTCLIENT_SUCCESS)
    {              // send the subscribe packet
        goto exit; // there was a problem
    }
    if (waitfor(c, SUBACK, &timer) == SUBACK) // wait for suback
    {
        int count = 0;
        unsigned short mypacketid;
        unsigned char grantedQoS = MQTTQOS_0;

#if defined(MQTTV5)
        int retval = MQTTV5Deserialize_suback(&mypacketid, properties, 1, &count, &grantedQoS, c->readbuf, c->readbuf_size);
        if (retval == 1)
        {
            data->reasonCode = grantedQoS;
            if (data->reasonCode != MQTTREASONCODE_UNSPECIFIED_ERROR && data->reasonCode != MQTTREASONCODE_IMPLEMENTATION_SPECIFIC_ERROR)
            {
                rc = MQTTV5SetMessageHandler(c, topicFilter, messageHandler);
            }
        }
        // TODO: V5 deserialization and QoS adapter.
#else
        int retval = MQTTDeserialize_suback(&mypacketid, 1, &count, &grantedQoS, c->readbuf, c->readbuf_size);
        data->grantedQoS = grantedQoS;
        if (retval == 1)
        {
            if (data->grantedQoS != 0x80)
            {
                rc = MQTTSetMessageHandler(c, topicFilter, messageHandler);
            }
        }
#endif /* MQTTV5 */
    }
    else
        rc = MQTTCLIENT_FAILURE;

exit:
    if (rc == MQTTCLIENT_FAILURE)
        MQTTCloseSession(c);
#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}

#if defined(MQTTV5)
int MQTTV5Subscribe(MQTTClient *client, const char *topicFilter, enum MQTTQoS requestedQoS,
                    MQTTProperties *properties, MQTTSubscribe_options options, messageHandler messageHandler)
{
    MQTTSubackData data;
    return MQTTSubscribeWithResults(c, topicFilter, requestedQoS, messageHandler, &data);
}
#else
int MQTTSubscribe(MQTTClient *c, const char *topicFilter, enum MQTTQoS requestedQoS,
                  messageHandler messageHandler)
{
    MQTTSubackData data;
    return MQTTSubscribeWithResults(c, topicFilter, requestedQoS, messageHandler, &data);
}
#endif

#if defined(MQTTV5)
int MQTTV5Unsubscribe(MQTTClient *c, const char *topicFilter, MQTTProperties *properties)
#else
int MQTTUnsubscribe(MQTTClient *c, const char *topicFilter)
#endif
{
    int rc = MQTTCLIENT_FAILURE;
    Timer timer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicFilter;
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (!c->isconnected)
        goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

#if defined(MQTTV5)
    len = MQTTV5Serialize_unsubscribe(c->buf, c->buf_size, 0, getNextPacketId(c), properties, 1, &topic);
#else
    len = MQTTSerialize_unsubscribe(c->buf, c->buf_size, 0, getNextPacketId(c), 1, &topic);
#endif
    if (len <= 0)
        goto exit;
    if ((rc = sendPacket(c, len, &timer)) != MQTTCLIENT_SUCCESS) // send the subscribe packet
        goto exit;                                               // there was a problem

    if (waitfor(c, UNSUBACK, &timer) == UNSUBACK)
    {
        unsigned short mypacketid; // should be the same as the packetid above
#if defined(MQTTV5)
        int unsubcount = 0;
        unsigned char reasonCode = -1;
        len = MQTTV5Deserialize_unsuback(&mypacketid, properties, 1, &unsubcount, &reasonCode, c->readbuf, c->readbuf_size);
#else
        len = MQTTDeserialize_unsuback(&mypacketid, c->readbuf, c->readbuf_size);
#endif
        if (len == 1)
        {
#if defined(MQTTV5)
            MQTTV5SetMessageHandler(c, topicFilter, NULL);
#else
            MQTTSetMessageHandler(c, topicFilter, NULL);
#endif
            /* remove the subscription message handler associated with this topic, if there is one */
        }
    }
    else
        rc = MQTTCLIENT_FAILURE;

exit:
    if (rc == MQTTCLIENT_FAILURE)
        MQTTCloseSession(c);
#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}

#if defined(MQTTV5)
int MQTTV5Publish(MQTTClient *c, const char *topicName, MQTTMessage *message,
                  MQTTProperties *properties)
#else
int MQTTPublish(MQTTClient *c, const char *topicName, MQTTMessage *message)
#endif
{
    int rc = MQTTCLIENT_FAILURE;
    Timer timer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicName;
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (!c->isconnected)
        goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    if (message->qos == MQTTQOS_1 || message->qos == MQTTQOS_2)
    {
        message->id = getNextPacketId(c);
    }
#if defined(MQTTV5)
    len = MQTTV5Serialize_publish(c->buf, c->buf_size, 0, message->qos, message->retained,
                                  message->id, &topic, &message->properties, message->payload, message->payloadlen);
#else

    len = MQTTSerialize_publish(c->buf, c->buf_size, 0, message->qos, message->retained, message->id,
                                &topic, message->payload, message->payloadlen);
#endif
    if (len <= 0)
    {
        goto exit;
    }
    rc = sendPacket(c, len, &timer); // send the subscribe packet
    if (rc != MQTTCLIENT_SUCCESS)
    {
        goto exit; // there was a problem
    }
    if (message->qos == MQTTQOS_1)
    {
        if (waitfor(c, PUBACK, &timer) == PUBACK)
        {
            unsigned short mypacketid;
            unsigned char dup, type;
#if defined(MQTTV5)
            unsigned char reasonCode;
            rc = MQTTV5Deserialize_ack(&type, &dup, &mypacketid, &reasonCode, c->recvProperties, c->readbuf, c->readbuf_size);
            if (rc == 1)
            {
                if (reasonCode != MQTTREASONCODE_SUCCESS)
                {
                    rc = MQTTCLIENT_FAILURE;
                }
            }
            else if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
#else
            if (MQTTDeserialize_ack(&type, &dup, &mypacketid, c->readbuf, c->readbuf_size) != 1)
                rc = MQTTCLIENT_FAILURE;
#endif
        }
        else
            rc = MQTTCLIENT_FAILURE;
    }
    else if (message->qos == MQTTQOS_2)
    {
        if (waitfor(c, PUBCOMP, &timer) == PUBCOMP)
        {
            unsigned short mypacketid;
            unsigned char dup, type;
#if defined(MQTTV5)
            unsigned char reasonCode;
            rc = MQTTV5Deserialize_ack(&type, &dup, &mypacketid, &reasonCode, c->recvProperties, c->readbuf, c->readbuf_size);
            if (rc == 1)
            {
                if (reasonCode != MQTTREASONCODE_SUCCESS)
                {
                    rc = MQTTCLIENT_FAILURE;
                }
            }
            else if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
#else
            if (MQTTDeserialize_ack(&type, &dup, &mypacketid, c->readbuf, c->readbuf_size) != 1)
                rc = MQTTCLIENT_FAILURE;
#endif
        }
        else
            rc = MQTTCLIENT_FAILURE;
    }

exit:
    if (rc == MQTTCLIENT_FAILURE || rc == MQTTCLIENT_BUFFER_OVERFLOW)
    {
        MQTTCloseSession(c);
    }
#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}

/**
 * @brief MQTT Disconnect - send an MQTTv5 disconnect packet and close the connection.
 *
 * @param client The `MQTTClient` object to use.
 * @param reasonCode The MQTTv5 reason code.
 * @param properties The MQTTv5 disconnect properties.
 * @return An #MQTTClientReturnCode indicating success or failure.
 */
#if defined(MQTTV5)
int MQTTV5Disconnect(MQTTClient *c, unsigned char reasonCode, const MQTTProperties *properties)
{
    // TODO: implement MQTTv5 disconnect and callback to expose reason code and properties.
    int rc = MQTTCLIENT_FAILURE;
    Timer timer; // we might wait for incomplete incoming publishes to complete
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    len = MQTTV5Serialize_disconnect(c->buf, c->buf_size, reasonCode, properties);
    if (len > 0)
    {
        rc = sendPacket(c, len, &timer); // send the disconnect packet
    }
    MQTTCloseSession(c);

#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}
#else
int MQTTDisconnect(MQTTClient *c)
{
    int rc = MQTTCLIENT_FAILURE;
    Timer timer; // we might wait for incomplete incoming publishes to complete
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    len = MQTTSerialize_disconnect(c->buf, c->buf_size);
    if (len > 0)
        rc = sendPacket(c, len, &timer); // send the disconnect packet
    MQTTCloseSession(c);

#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}
#endif

#if defined(MQTTV5)
int MQTTV5PublishWithResults(MQTTClient *c, const char *topicName, MQTTMessage *message,
                             MQTTProperties *properties, MQTTPubDoneData *ack)
{
    int rc = MQTTCLIENT_FAILURE;
    Timer timer;
    MQTTString topic = MQTTString_initializer;
    topic.cstring = (char *)topicName;
    int32_t len = 0;

#if defined(MQTT_TASK)
    MutexLock(&c->mutex);
#endif
    if (!c->isconnected)
        goto exit;

    TimerInit(&timer);
    TimerCountdownMS(&timer, c->command_timeout_ms);

    if (message->qos == MQTTQOS_1 || message->qos == MQTTQOS_2)
    {
        message->id = getNextPacketId(c);
    }
    len = MQTTV5Serialize_publish(c->buf, c->buf_size, 0, message->qos, message->retained,
                                  message->id, &topic, &message->properties, message->payload, message->payloadlen);
    if (len <= 0)
    {
        goto exit;
    }
    rc = sendPacket(c, len, &timer); // send the subscribe packet
    if (rc != MQTTCLIENT_SUCCESS)
    {
        goto exit; // there was a problem
    }
    if (message->qos == MQTTQOS_1)
    {
        if (waitfor(c, PUBACK, &timer) == PUBACK)
        {
            unsigned short mypacketid;
            unsigned char type;
            rc = MQTTV5Deserialize_ack(&type, &ack->dup, &mypacketid, &ack->reasonCode, ack->properties, c->readbuf, c->readbuf_size);
            if (rc == 1)
            {
                if (ack->reasonCode != MQTTREASONCODE_SUCCESS)
                {
                    rc = MQTTCLIENT_FAILURE;
                }
            }
            else if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
        }
        else
            rc = MQTTCLIENT_FAILURE;
    }
    else if (message->qos == MQTTQOS_2)
    {
        if (waitfor(c, PUBCOMP, &timer) == PUBCOMP)
        {
            unsigned short mypacketid;
            unsigned char type;
            rc = MQTTV5Deserialize_ack(&type, &ack->dup, &mypacketid, &ack->reasonCode, ack->properties, c->readbuf, c->readbuf_size);
            if (rc == 1)
            {
                if (ack->reasonCode != MQTTREASONCODE_SUCCESS)
                {
                    rc = MQTTCLIENT_FAILURE;
                }
            }
            else if (rc == MQTTCLIENT_BUFFER_OVERFLOW)
            {
                rc = MQTTCLIENT_BUFFER_OVERFLOW;
            }
            else
            {
                rc = MQTTCLIENT_FAILURE;
            }
        }
        else
            rc = MQTTCLIENT_FAILURE;
    }

exit:
    if (rc == MQTTCLIENT_FAILURE || rc == MQTTCLIENT_BUFFER_OVERFLOW)
    {
        MQTTCloseSession(c);
    }
#if defined(MQTT_TASK)
    MutexUnlock(&c->mutex);
#endif
    return rc;
}

int MQTTV5Auth(MQTTClient *client, unsigned char reasonCode, MQTTProperties *properties)
{
    // TODO: implement MQTTv5 Auth and callback to expose reason code and properties.
    /*
        Header header;
        int rc = MQTTCLIENT_FAILURE;
        char *buf = NULL;
        char *ptr = NULL;
        size_t props_len = 0;
        size_t buflen = 0;

        // 1. On prépare le header (Type 15 = AUTH)
        header.byte = 0;
        header.bits.type = AUTH;

        // 2. Calcul de la taille nécessaire
        // Taille = Reason Code (1 octet) + Longueur des propriétés (VBI) + Données des propriétés
        if (properties) {
            props_len = MQTTProperties_len(properties);
        }

        buflen = 1 + props_len; // 1 octet pour le reasonCode + le bloc propriétés

        // 3. Allocation (ou buffer statique si tu as viré malloc)
        if ((buf = malloc(buflen)) == NULL)
            return MQTTCLIENT_FAILURE;

        ptr = buf;

        // 4. Sérialisation
        writeChar(&ptr, reasonCode);
        if (properties) {
            MQTTProperties_write(&ptr, properties);
        }

        // 5. Envoi via la fonction de base de la lib
        // MQTTPacket_send va ajouter le Fixed Header et le Remaining Length automatiquement
        rc = MQTTPacket_send(&client->net, header, buf, buflen, 1, client->MQTTVersion);

        if (rc != TCPSOCKET_INTERRUPTED)
            free(buf); // On libère si l'envoi est terminé

        return (rc == TCPSOCKET_COMPLETE) ? MQTTCLIENT_SUCCESS : rc;
        */
}
int MQTTV5SetAuthHandler(MQTTClient *client, controlHandler authHandler)
{
    int rc = MQTTCLIENT_FAILURE;

    if (client != NULL)
    {
        client->authHandler = authHandler;
        rc = MQTTCLIENT_SUCCESS;
    }

    return rc;
}

int MQTTV5SetDisconnectHandler(MQTTClient *client, controlHandler disconnectHandler)
{
    int rc = MQTTCLIENT_FAILURE;

    if (client != NULL)
    {
        client->disconnectHandler = disconnectHandler;
        rc = MQTTCLIENT_SUCCESS;
    }

    return rc;
}
#endif