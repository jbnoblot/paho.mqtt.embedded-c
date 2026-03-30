/*******************************************************************************
 * Copyright (c) 2017, 2023 IBM Corp.
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
 *******************************************************************************/

#include "MQTTV5Packet.h"

#include <string.h>

/**
 * Writes an integer as 4 bytes to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param anInt the integer to write
 */
void writeInt4(unsigned char** pptr, int anInt)
{
	**pptr = (unsigned char)(anInt / 16777216);
	(*pptr)++;
	anInt %= 16777216;
	**pptr = (unsigned char)(anInt / 65536);
	(*pptr)++;
	anInt %= 65536;
	**pptr = (unsigned char)(anInt / 256);
	(*pptr)++;
	**pptr = (unsigned char)(anInt % 256);
	(*pptr)++;
}


/**
 * Calculates an integer from 4 bytes read from the input buffer
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the integer value calculated
 */
int readInt4(unsigned char** pptr)
{
	unsigned char* ptr = *pptr;
	int value = 16777216*(*ptr) + 65536*(*(ptr+1)) + 256*(*(ptr+2)) + (*(ptr+3));
	*pptr += 4;
	return value;
}

void writeInt42(unsigned char** pptr, int anInt)
{
    unsigned char* p = *pptr;
    p[0] = (unsigned char)((anInt >> 24) & 0xFF);
    p[1] = (unsigned char)((anInt >> 16) & 0xFF);
    p[2] = (unsigned char)((anInt >> 8)  & 0xFF);
    p[3] = (unsigned char)(anInt         & 0xFF);
    *pptr = p + 4;
}

void writeInt43(unsigned char** pptr, int anInt)
{
    // On inverse l'ordre des octets d'un coup (0xAABBCCDD -> 0xDDCCBBAA)
    uint32_t swapped = __builtin_bswap32((uint32_t)anInt);
    
    // On copie les 4 octets d'un bloc
    memcpy(*pptr, &swapped, 4);
    *pptr += 4;
}

int readInt42(unsigned char** pptr)
{
    unsigned char* p = *pptr;
    int value = (p[0] << 24) | (p[1] << 16) | (p[2] << 8) | p[3];
    *pptr = p + 4;
    return value;
}


void writeMQTTLenString(unsigned char** pptr, MQTTLenString lenstring)
{
  writeInt(pptr, lenstring.len);
  memcpy(*pptr, lenstring.data, lenstring.len);
  *pptr += lenstring.len;
}


int MQTTLenStringRead(MQTTLenString* lenstring, unsigned char** pptr, const unsigned char* const enddata)
{
	int len = -1;

	/* the first two bytes are the length of the string */
	if (enddata - (*pptr) > 1) /* enough length to read the integer? */
	{
		lenstring->len = readInt(pptr); /* increments pptr to point past length */
		if (&(*pptr)[lenstring->len] <= enddata)
		{
			lenstring->data = (char*)*pptr;
			*pptr += lenstring->len;
			len = 2 + lenstring->len;
		}
	}
	return len;
}
