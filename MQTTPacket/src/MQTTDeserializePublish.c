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
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *    Ian Craggs - MQTT V5 implementation
 *******************************************************************************/

#if defined(MQTTV5)
#include "MQTTV5Packet.h"
#else
#include "MQTTPacket.h"
#endif
#include "StackTrace.h"
#include <string.h>

#define min(a, b) ((a < b) ? 1 : 0)

/**
 * Deserializes the supplied (wire) buffer into publish data
 * @param dup returned integer - the MQTT dup flag
 * @param qos returned integer - the MQTT QoS value
 * @param retained returned integer - the MQTT retained flag
 * @param packetid returned integer - the MQTT packet identifier
 * @param topicName returned MQTTString - the MQTT topic in the publish
 * @param payload returned byte buffer - the MQTT publish payload
 * @param payloadlen returned integer - the length of the MQTT payload
 * @param buf the raw buffer data, of the correct length determined by the remaining length field
 * @param buflen the length in bytes of the data in the supplied buffer
 * @return error code. 1 is success
 */
#if defined(MQTTV5)
int MQTTV5Deserialize_publish(unsigned char *dup, unsigned char *qos, unsigned char *retained, unsigned short *packetid, MQTTString *topicName,
								  MQTTProperties *properties, unsigned char **payload, int32_t *payloadlen, unsigned char *buf, size_t buflen)
#else
int MQTTDeserialize_publish(unsigned char *dup, unsigned char *qos, unsigned char *retained, unsigned short *packetid, MQTTString *topicName,
								unsigned char **payload, int32_t *payloadlen, unsigned char *buf, size_t buflen)
#endif
{
	unsigned char header;
	unsigned char *curdata = buf;
	const unsigned char *enddata = NULL;
	int rc = 0;
	uint32_t mylen = 0;

	FUNC_ENTRY;
	header = readChar(&curdata);
	if ((header & MQTT_HEADER_TYPE_MASK) >> MQTT_HEADER_TYPE_SHIFT != PUBLISH)
	{
		goto exit;
	}
	*dup = (header & MQTT_HEADER_DUP_MASK) != 0;
	*qos = (header & MQTT_HEADER_QOS_MASK) >> MQTT_HEADER_QOS_SHIFT;
	*retained = (header & MQTT_HEADER_RETAIN_MASK) != 0;

	rc = MQTTPacket_decodeBuf(curdata, &mylen);
	curdata += rc; /* read remaining length */
	enddata = curdata + mylen;

	if (!readMQTTLenString(topicName, &curdata, enddata) || enddata - curdata < 0)
	{ /* do we have enough data to read the protocol version byte? */
		goto exit;
	}
	if (*qos > 0)
	{
		*packetid = readInt(&curdata);
	}

#if defined(MQTTV5)
	if (properties)
	{
		rc = MQTTProperties_read(properties, &curdata, enddata);
		if (rc < 0) {
		  goto exit;
		}
	}
#endif

	*payloadlen = enddata - curdata;
	*payload = curdata;
	rc = 1;
exit:
	FUNC_EXIT_RC(rc);
	return (int)rc;
}

/**
 * Deserializes the supplied (wire) buffer into an ack
 * @param packettype returned integer - the MQTT packet type
 * @param dup returned integer - the MQTT dup flag
 * @param packetid returned integer - the MQTT packet identifier
 * @param buf the raw buffer data, of the correct length determined by the remaining length field
 * @param buflen the length in bytes of the data in the supplied buffer
 * @return error code.  1 is success, 0 is failure
 */
#if defined(MQTTV5)
int MQTTV5Deserialize_ack(unsigned char *packettype, unsigned char *dup, unsigned short *packetid,
							  unsigned char *reasonCode, MQTTProperties *properties, unsigned char *buf, size_t buflen)
#else
int MQTTDeserialize_ack(unsigned char *packettype, unsigned char *dup, unsigned short *packetid, unsigned char *buf, size_t buflen)
#endif
{
	unsigned char header = 0;
	unsigned char *curdata = buf;
	unsigned char *enddata = NULL;
	int rc = 0;
	uint32_t mylen = 0;

	FUNC_ENTRY;
	header = readChar(&curdata);
	*dup = (header & MQTT_HEADER_DUP_MASK) != 0;
	*packettype = (header & MQTT_HEADER_TYPE_MASK) >> MQTT_HEADER_TYPE_SHIFT;

	curdata += (rc = MQTTPacket_decodeBuf(curdata, &mylen)); /* read remaining length */
	enddata = curdata + mylen;

	if (enddata - curdata < 2)
	{
		goto exit;
	}
	*packetid = readInt(&curdata);

#if defined(MQTTV5)
	if (reasonCode)
	{
		if (enddata == curdata) /* no reason code, or properties */
		{
			*reasonCode = 0;
		}
		else
		{
			*reasonCode = (unsigned char)readChar(&curdata);
		}
	}

	if (properties)
	{
		if (enddata == curdata)
		{
			properties->length = properties->count = 0; /* signal that no properties were received */
		}
		else if (!MQTTProperties_read(properties, &curdata, enddata))
		{
			goto exit;
		}
	}
#endif

	rc = 1;
exit:
	FUNC_EXIT_RC(rc);
	return (int)rc;
}
