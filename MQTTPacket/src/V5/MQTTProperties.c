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
#include <stdio.h>
#include <stdlib.h>

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

struct nameToType
{
  MQTTPropertyCodes name;
  MQTTPropertyTypes type;
} namesToTypes[] =
{
  {MQTTPROPERTY_CODE_PAYLOAD_FORMAT_INDICATOR, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_MESSAGE_EXPIRY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_CONTENT_TYPE, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_RESPONSE_TOPIC, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_CORRELATION_DATA, MQTTPROPERTY_TYPE_BINARY_DATA},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIER, MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_ASSIGNED_CLIENT_IDENTIFIER, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_SERVER_KEEP_ALIVE, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_AUTHENTICATION_METHOD, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_AUTHENTICATION_DATA, MQTTPROPERTY_TYPE_BINARY_DATA},
  {MQTTPROPERTY_CODE_REQUEST_PROBLEM_INFORMATION, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_WILL_DELAY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_REQUEST_RESPONSE_INFORMATION, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_RESPONSE_INFORMATION, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_SERVER_REFERENCE, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_REASON_STRING, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_RECEIVE_MAXIMUM, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS_MAXIMUM, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_MAXIMUM_QOS, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_RETAIN_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_USER_PROPERTY, MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR},
  {MQTTPROPERTY_CODE_MAXIMUM_PACKET_SIZE, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_WILDCARD_SUBSCRIPTION_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIER_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_SHARED_SUBSCRIPTION_AVAILABLE, MQTTPROPERTY_TYPE_BYTE}
};


int MQTTProperty_getType(MQTTPropertyCodes identifier)
{
  int rc = -1;

  for (int i = 0; i < ARRAY_SIZE(namesToTypes); ++i)
  {
    if (namesToTypes[i].name == identifier)
    {
      rc = namesToTypes[i].type;
      break;
    }
  }
  return rc;
}


int MQTTProperties_len(const MQTTProperties* props)
{
  /* properties length is an mbi */
  return (props == NULL) ? 1 : props->length + MQTTPacket_VBIlen(props->length);
}


int MQTTProperties_add(MQTTProperties* props, const MQTTProperty* prop)
{
  int rc = 0;
  int type;

  if (props->count >= props->max_count)
    rc = -1;  /* max number of properties already in structure */
  else if ((type = MQTTProperty_getType(prop->identifier)) < 0)
    rc = -2;
  else
  {
    int len = 0;

    props->array[props->count++] = *prop;
    /* calculate length */
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        len = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        len = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        len = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        if (prop->value.integer4 >= 0 && prop->value.integer4 <= 127)
          len = 1;
        else if (prop->value.integer4 >= 128 && prop->value.integer4 <= 16383)
          len = 2;
        else if (prop->value.integer4 >= 16384 && prop->value.integer4 < 2097151)
          len = 3;
        else if (prop->value.integer4 >= 2097152 && prop->value.integer4 < 268435455)
          len = 4;
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
        len = 2 + prop->value.data.len;
        break;
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        len = 2 + prop->value.string_pair.key.len;
        len += 2 + prop->value.string_pair.val.len;
        break;
    }
    props->length += len + 1; /* add identifier byte */
  }

  return rc;
}


int MQTTProperty_write(unsigned char** pptr, const MQTTProperty* prop)
{
  int rc = -1;
  int type = MQTTProperty_getType(prop->identifier);

  if (type >= MQTTPROPERTY_TYPE_BYTE && type <= MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
  {
    writeChar(pptr, prop->identifier);
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        writeChar(pptr, prop->value.byte);
        rc = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        writeInt(pptr, prop->value.integer2);
        rc = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        writeInt4(pptr, prop->value.integer4);
        rc = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        rc = MQTTPacket_encode(*pptr, prop->value.integer4);
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
        writeMQTTLenString(pptr, prop->value.data);
        rc = prop->value.data.len + 2; /* include length field */
        break;
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        writeMQTTLenString(pptr, prop->value.string_pair.key);
        writeMQTTLenString(pptr, prop->value.string_pair.val);
        rc = prop->value.string_pair.key.len + prop->value.string_pair.val.len + 4; /* include length fields */
        break;
    }
  }
  if (rc < 0) {
    return -1; /* error */
  }
  return rc + 1; /* include identifier byte */
}


/**
 * write the supplied properties into a packet buffer
 * @param pptr pointer to the buffer - move the pointer as we add data
 * @param remlength the max length of the buffer
 * @return whether the write succeeded or not, number of bytes written or < 0
 */
int MQTTProperties_write(unsigned char** pptr, const MQTTProperties* properties)
{
  int rc = -1;
  int total_written = 0;
  int prop_data_len = (properties == NULL) ? 0 : properties->length;

  /* write the entire property list length first */
  total_written = MQTTPacket_encode(*pptr, prop_data_len);
  *pptr += total_written;

  /* 2. Si pas de propriétés, on s'arrête là. On a fini d'écrire le 0x00. */
  if (properties == NULL || properties->count == 0)
  {
    return total_written;
  }

  for (int i = 0; i < properties->count; ++i)
  {
    rc = MQTTProperty_write(pptr, &properties->array[i]);
    if (rc < 0) {
      return rc;
    }
    
    total_written += rc;
  }

  return total_written;
}


int MQTTProperty_read(MQTTProperty* prop, unsigned char** pptr, const unsigned char* enddata)
{
  int type = -1;
  int len = -1;

  prop->identifier = (MQTTPropertyCodes)readChar(pptr);
  type = MQTTProperty_getType(prop->identifier);
  if (type >= MQTTPROPERTY_TYPE_BYTE && type <= MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
  {
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        prop->value.byte = readChar(pptr);
        len = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        prop->value.integer2 = readInt(pptr);
        len = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        prop->value.integer4 = readInt4(pptr);
        len = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        len = MQTTPacket_decodeBuf(*pptr, &prop->value.integer4);
        *pptr += len;
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
        len = MQTTLenStringRead(&prop->value.data, pptr, enddata);
        break;
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        len = MQTTLenStringRead(&prop->value.string_pair.key, pptr, enddata);
        len += MQTTLenStringRead(&prop->value.string_pair.val, pptr, enddata);
        break;
    }
  }
  return len + 1; /* 1 byte for identifier */
}

/**
 * read the properties from a packet buffer into the supplied structure, up to the length specified in the packet
 * @param properties the structure into which the properties will be read
 * @param pptr pointer to the buffer - move the pointer as we read data
 * @param enddata pointer to the end of the buffer, for security checking
 * @return -1 fail, -2 properties overflow but no truncation, 1 success (even if some properties were ignored due to overflow)
 */
int MQTTProperties_read(MQTTProperties* properties, unsigned char** pptr, const unsigned char* enddata)
{
    uint32_t remlength = 0;

    // 1. Décodage sécurisé de la longueur des propriétés
    if (enddata - (*pptr) < 1) {
      return -1; 
    }
    int rc = MQTTPacket_decodeBuf(*pptr, &remlength);
    if (rc <= 0) {
      return -1; 
    }
    *pptr += rc;

    unsigned char* expected_end = *pptr + remlength;
    // Vérification de sécurité : le bloc de propriétés dépasse-t-il le buffer ?
    if (expected_end > enddata) {
      printf("--- CRASH DEBUG ---\n");
      printf("remlength lue : %u octets\n", remlength);
      printf("Octets lus (rc): %d\n", rc);
      printf("Espace restant  : %ld octets\n", (long)(enddata - *pptr));
      printf("-------------------\n");
      return -1;
    }

        /* 2. CAS A : L'utilisateur a passé NULL (il veut juste "sauter") */
    if (properties == NULL) {
        *pptr = expected_end; // On saute directement à la fin
        return 1; // Succès (mais rien stocké)
    }
    
    properties->count = 0;
    properties->length = remlength;

    // 2. Lecture des propriétés
    while (*pptr < expected_end)
    {
        if (properties->count < properties->max_count)
        {
            // On a de la place, on stocke
            MQTTProperty_read(&properties->array[properties->count], pptr, expected_end);
            properties->count++;
        }
        else if(properties->truncateProperties)
        {
            break;
        } 
        else 
        {
            // PLUS DE PLACE et pas de truncation : on arrête la lecture, c'est une erreur
            return -2;
        }
    }

    // 3. recalage forcé (sécurité ultime)
    *pptr = expected_end; 

    return 1; // Succès, même si on en a ignoré certaines
}
