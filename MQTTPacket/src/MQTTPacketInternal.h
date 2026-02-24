/*******************************************************************************
 * Copyright (c) 2023 Microsoft Corporation. All rights reserved.
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
 *******************************************************************************/

#ifndef MQTTPACKETINTERNAL_H_
#define MQTTPACKETINTERNAL_H_

#include <stdint.h>
#include <stddef.h>

#if defined(__cplusplus) /* If this is a C++ compiler, use C linkage */
extern "C" {
#endif

#if defined(_WIN32) && defined(BUILDING_LIB)
#define DLLImport __declspec(dllimport)
#define DLLExport __declspec(dllexport)
#elif defined(__linux__) || defined(__APPLE__) && defined(BUILDING_LIB)
#define DLLImport extern
#define DLLExport __attribute__((visibility("default")))
#else
#define DLLImport
#define DLLExport
#endif

enum errors
{
	MQTTPACKET_BAD = -4,
	MQTTPACKET_BUFFER_TOO_SHORT = -2,
	MQTTPACKET_READ_ERROR = -1,
	MQTTPACKET_READ_COMPLETE
};

enum msgTypes
{
	CONNECT = 1, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL,
	PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK,
	PINGREQ, PINGRESP, DISCONNECT, AUTH
};

// Masques
#define MQTT_HEADER_TYPE_MASK   0xF0
#define MQTT_HEADER_DUP_MASK    0x08
#define MQTT_HEADER_QOS_MASK    0x06
#define MQTT_HEADER_RETAIN_MASK 0x01

// Décalages (pour ramener les valeurs à 0)
#define MQTT_HEADER_TYPE_SHIFT		4
#define MQTT_HEADER_DUP_SHIFT		3 //TODO NOT SURE
#define MQTT_HEADER_RETAIN_SHIFT	0
#define MQTT_HEADER_QOS_SHIFT		1

/**
 * Bitfields for the MQTT header byte.
 */
typedef union
{
	unsigned char byte;	                /**< the whole byte */
#if defined(REVERSED)
	struct
	{
		unsigned int type : 4;			/**< message type nibble */
		unsigned int dup : 1;				/**< DUP flag bit */
		unsigned int qos : 2;				/**< QoS value, 0, 1 or 2 */
		unsigned int retain : 1;		/**< retained flag bit */
	} bits;
#else
	struct
	{
		unsigned int retain : 1;		/**< retained flag bit */
		unsigned int qos : 2;				/**< QoS value, 0, 1 or 2 */
		unsigned int dup : 1;				/**< DUP flag bit */
		unsigned int type : 4;			/**< message type nibble */
	} bits;
#endif
} MQTTHeader;

typedef struct
{
	size_t len;
	char* data;
} MQTTLenString;

typedef struct
{
	char* cstring;
	MQTTLenString lenstring;
} MQTTString;

#define MQTTString_initializer {NULL, {0, NULL}}

int MQTTstrlen(const MQTTString* mqttstring);

int MQTTPacket_VBIlen(uint32_t rem_len);
uint32_t MQTTPacket_len(uint32_t rem_len);
int MQTTPacket_decode(int (*getcharfn)(unsigned char*, int), uint32_t* value);
int MQTTPacket_decodeBuf(unsigned char* buf, uint32_t* value);

int readInt(unsigned char** pptr);
char readChar(unsigned char** pptr);
void writeChar(unsigned char** pptr, char c);
void writeInt(unsigned char** pptr, int anInt);
void writeUInt16(unsigned char** pptr, uint16_t anInt);
int readMQTTLenString(MQTTString* mqttstring, unsigned char** pptr, const unsigned char* enddata);
void writeCString(unsigned char** pptr, const char* string);
void writeMQTTString(unsigned char** pptr, const MQTTString* mqttstring);

#define MQTTPacket_encode_internal MQTTPacket_encode

#ifdef __cplusplus /* If this is a C++ compiler, use C linkage */
}
#endif


#endif /* MQTTPACKETINTERNAL_H_ */
