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

#ifndef MQTTCONNECTINTERNAL_H_
#define MQTTCONNECTINTERNAL_H_

#include <stdint.h>
#if !defined(DLLImport)
#define DLLImport
#endif
#if !defined(DLLExport)
#define DLLExport
#endif

#define MQTT_CONNECT_CLEAN_START_MASK  0x02 // Bit 1
#define MQTT_CONNECT_WILL_FLAG_MASK    0x04 // Bit 2
#define MQTT_CONNECT_WILL_QOS_MASK     0x18 // Bits 3 et 4 (00011000 en binaire)
#define MQTT_CONNECT_WILL_RETAIN_MASK  0x20 // Bit 5
#define MQTT_CONNECT_PASSWORD_MASK     0x40 // Bit 6
#define MQTT_CONNECT_USERNAME_MASK     0x80 // Bit 7

#define MQTT_CONNACK_SESSION_PRESENT_MASK 0x01
typedef union
{
	unsigned char all; /**< all connack flags */
#if defined(REVERSED)
	struct
	{
		unsigned int reserved : 7;		 /**< unused */
		unsigned int sessionpresent : 1; /**< session present flag */
	} bits;
#else
	struct
	{
		unsigned int sessionpresent : 1; /**< session present flag */
		unsigned int reserved : 7;		 /**< unused */
	} bits;
#endif
} MQTTConnackFlags; /**< connack flags byte */

#endif /* MQTTCONNECTINTERNAL_H_ */
