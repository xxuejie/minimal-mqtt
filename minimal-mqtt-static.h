#ifndef MMQTT_STATIC_H_
#define MMQTT_STATIC_H_

#include "minimal-mqtt.h"

typedef mmqtt_status_t (*mmqtt_s_puller)(struct mmqtt_connection*);
typedef mmqtt_status_t (*mmqtt_s_pusher)(struct mmqtt_connection*, mmqtt_ssize_t);

#define MMQTT_MESSAGE_TYPE_CONNECT 0x1
#define MMQTT_MESSAGE_TYPE_CONNACK 0x2
#define MMQTT_MESSAGE_TYPE_PUBLISH 0x3
#define MMQTT_MESSAGE_TYPE_PUBACK 0x4
#define MMQTT_MESSAGE_TYPE_SUBSCRIBE 0x8
#define MMQTT_MESSAGE_TYPE_SUBACK 0x9
#define MMQTT_MESSAGE_TYPE_UNSUBSCRIBE 0x10
#define MMQTT_MESSAGE_TYPE_UNSUBACK 0x11
#define MMQTT_MESSAGE_TYPE_PINGREQ 0x12
#define MMQTT_MESSAGE_TYPE_PINGRESP 0x13
#define MMQTT_MESSAGE_TYPE_DISCONNECT 0x14

#define MMQTT_PACK_MESSAGE_TYPE(type_) ((type_) << 4)
#define MMQTT_UNPACK_MESSAGE_TYPE(type_) (((type_) >> 4) & 0xF)

mmqtt_status_t
mmqtt_s_encode_buffer(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                      const uint8_t *buffer, uint16_t length)
{
  struct mmqtt_stream stream;
  mmqtt_status_t status;
  mmqtt_ssize_t current;
  uint16_t consumed_length;

  mmqtt_stream_init(&stream, length);
  while ((status = mmqtt_connection_add_write_stream(conn, &stream)) == MMQTT_STATUS_NOT_PUSHABLE) {
    status = puller(conn);
    if (status != MMQTT_STATUS_OK) { return status; }
  }
  if (status != MMQTT_STATUS_OK) { return status; }

  consumed_length = 0;
  while (consumed_length < length) {
    current = mmqtt_stream_push(&stream, buffer + consumed_length,
                                length - consumed_length);
    if (current >= 0) {
      consumed_length += current;
    } else if (current == MMQTT_STATUS_NOT_PUSHABLE) {
      status = puller(conn);
      if (status != MMQTT_STATUS_OK) { return status; }
    }
  }
  while (mmqtt_connection_in_write_stream(conn, &stream) == MMQTT_STATUS_OK) {
    status = puller(conn);
    if (status != MMQTT_STATUS_OK) { return status; }
  }
  return MMQTT_STATUS_OK;
}

mmqtt_status_t
mmqtt_s_decode_buffer(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                      uint8_t *buffer, uint16_t buffer_length,
                      uint16_t consume_length)
{
  struct mmqtt_stream stream;
  mmqtt_status_t status;
  mmqtt_ssize_t current;
  uint16_t length;
  uint8_t temp;

  if (mmqtt_connection_read_stream_length(conn) > 0) {
    return MMQTT_STATUS_INVALID_STATE;
  }

  if (consume_length < buffer_length) { consume_length = buffer_length; }
  mmqtt_stream_init(&stream, consume_length);
  /* Connection won't remove read stream automatically, pusher won't help here */
  status = mmqtt_connection_add_read_stream(conn, &stream);
  if (status != MMQTT_STATUS_OK) { return status; }

  length = 0;
  while (length < buffer_length) {
    current = mmqtt_stream_pull(&stream, buffer + length,
                                buffer_length - length);
    if (current >= 0) {
      length += current;
    } else if (current == MMQTT_STATUS_NOT_PULLABLE) {
      status = pusher(conn, buffer_length - length);
      if (status != MMQTT_STATUS_OK) {
        mmqtt_connection_release_read_stream(conn, &stream);
        return status;
      }
    }
  }
  while (length < consume_length) {
    current = mmqtt_stream_pull(&stream, &temp, 1);
    if (current >= 0) {
      length += current;
    } else if (current == MMQTT_STATUS_NOT_PULLABLE) {
      status = pusher(conn, consume_length - length);
      if (status != MMQTT_STATUS_OK) {
        mmqtt_connection_release_read_stream(conn, &stream);
        return status;
      }
    }
  }
  mmqtt_connection_release_read_stream(conn, &stream);
  return MMQTT_STATUS_OK;
}

/* NOTE: for smaller buffer, we can also use mmqtt_s_decode_buffer with
 * an empty buffer and 0 as buffer length. Here this function is for the
 * case where we are skipping a whole packet altogether, which could be
 * larger than 65535 in theory.
 */
mmqtt_status_t
mmqtt_s_skip_buffer(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                    uint32_t length)
{
  mmqtt_status_t status;
  while (length > 65535) {
    status = mmqtt_s_decode_buffer(conn, pusher, NULL, 0, 65535);
    if (status != MMQTT_STATUS_OK) {
      return status;
    }
    length -= 65535;
  }
  if (length > 0) {
    return mmqtt_s_decode_buffer(conn, pusher, NULL, 0, (uint16_t) length);
  } else {
    return MMQTT_STATUS_OK;
  }
}

mmqtt_status_t
mmqtt_s_encode_fixed_header(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                            uint8_t flag, uint32_t length)
{
  mmqtt_status_t status;
  mmqtt_ssize_t i;
  uint8_t c, buffer[4];

  buffer[0] = flag;
  status = mmqtt_s_encode_buffer(conn, puller, buffer, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  i = 0;
  do {
    c = (uint8_t) length % 128;
    length /= 128;
    if (length > 0) { c |= 0x80; }
    buffer[i++] = c;
  } while (length > 0);

  return mmqtt_s_encode_buffer(conn, puller, buffer, i);
}

mmqtt_status_t
mmqtt_s_decode_fixed_header(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                            uint8_t *flag, uint32_t *length)
{
  mmqtt_status_t status;
  uint8_t c;
  uint32_t l, factor;

  status = mmqtt_s_decode_buffer(conn, pusher, &c, 1, 1);
  if (status != MMQTT_STATUS_OK) { return status; }
  if (flag) { *flag = c; }

  l = 0;
  factor = 1;
  do {
    status = mmqtt_s_decode_buffer(conn, pusher, &c, 1, 1);
    if (status != MMQTT_STATUS_OK) { return status; }
    l += (c & 0x7F) * factor;
    factor *= 128;
  } while ((c & 0x80) != 0);
  if (length) { *length = l; }

  return MMQTT_STATUS_OK;
}

void mmqtt_s_pack_uint16_(uint16_t length, uint8_t *buffer)
{
  buffer[0] = (length >> 8) & 0xFF;
  buffer[1] = length & 0xFF;
}

uint16_t mmqtt_s_unpack_uint16_(uint8_t *buffer)
{
  return ((uint16_t) (buffer[0] << 8)) | ((uint16_t) buffer[1]);
}

/* We don't case uint16_t* directly into uint8_t* because there might be
 * endianness differences
 */
mmqtt_status_t
mmqtt_s_encode_uint16(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                      uint16_t val)
{
  uint8_t buffer[2];

  mmqtt_s_pack_uint16_(val, buffer);
  return mmqtt_s_encode_buffer(conn, puller, buffer, 2);
}

mmqtt_status_t
mmqtt_s_decode_uint16(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                      uint16_t *out_val)
{
  mmqtt_status_t status;
  uint8_t buffer[2];

  status = mmqtt_s_decode_buffer(conn, pusher, buffer, 2, 2);
  if (status != MMQTT_STATUS_OK) { return status; }

  if (out_val) { *out_val = mmqtt_s_unpack_uint16_(buffer); }
  return MMQTT_STATUS_OK;
}

mmqtt_status_t
mmqtt_s_encode_string(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                      const uint8_t *str, uint16_t length)
{
  mmqtt_status_t status;
  uint8_t buffer[2];

  mmqtt_s_pack_uint16_(length, buffer);
  status = mmqtt_s_encode_buffer(conn, puller, buffer, 2);
  if (status != MMQTT_STATUS_OK) { return status; }

  return mmqtt_s_encode_buffer(conn, puller, str, length);
}

mmqtt_status_t
mmqtt_s_decode_string(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                      uint8_t *str, uint16_t max_length,
                      uint16_t *out_length, uint16_t *out_true_length) {
  mmqtt_status_t status;
  uint8_t buffer[2];
  uint16_t true_length, length;

  status = mmqtt_s_decode_buffer(conn, pusher, buffer, 2, 2);
  if (status != MMQTT_STATUS_OK) { return status; }
  true_length = mmqtt_s_unpack_uint16_(buffer);

  length = min(true_length, max_length);
  status = mmqtt_s_decode_buffer(conn, pusher, str, length, true_length);
  if (status != MMQTT_STATUS_OK) { return status; }

  if (out_length) { *out_length = length; }
  if (out_true_length) { *out_true_length = true_length; }
  return MMQTT_STATUS_OK;
}

/* p stands for packet here */
struct mmqtt_p_connect_header {
  uint8_t *name;
  uint16_t name_length;
  uint16_t name_max_length;
  uint8_t protocol_version;
  uint8_t flags;
  uint16_t keepalive;
};

mmqtt_status_t
mmqtt_s_encode_connect_header(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                              const struct mmqtt_p_connect_header *header)
{
  mmqtt_status_t status;
  uint8_t buffer[2];

  mmqtt_s_pack_uint16_(header->name_length, buffer);
  status = mmqtt_s_encode_buffer(conn, puller, buffer, 2);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_encode_buffer(conn, puller, header->name, header->name_length);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_encode_buffer(conn, puller, &header->protocol_version, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_encode_buffer(conn, puller, &header->flags, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  mmqtt_s_pack_uint16_(header->keepalive, buffer);
  return mmqtt_s_encode_buffer(conn, puller, buffer, 2);
}

/* We would rewrite name_length in header to contain returned name length */
mmqtt_status_t
mmqtt_s_decode_connect_header(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                              struct mmqtt_p_connect_header *header)
{
  mmqtt_status_t status;
  uint8_t buffer[2];
  uint16_t length;

  status = mmqtt_s_decode_buffer(conn, pusher, buffer, 2, 2);
  if (status != MMQTT_STATUS_OK) { return status; }
  length = mmqtt_s_unpack_uint16_(buffer);

  header->name_length = min(header->name_max_length, length);
  status = mmqtt_s_decode_buffer(conn, pusher, header->name,
                                 header->name_length, length);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_decode_buffer(conn, pusher, &header->protocol_version, 1, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_decode_buffer(conn, pusher, &header->flags, 1, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  status = mmqtt_s_decode_buffer(conn, pusher, buffer, 2, 2);
  if (status != MMQTT_STATUS_OK) { return status; }
  header->keepalive = mmqtt_s_unpack_uint16_(buffer);

  return MMQTT_STATUS_OK;
}

struct mmqtt_p_connack_header {
  uint8_t reserved;
  uint8_t return_code;
};

mmqtt_status_t
mmqtt_s_encode_connack_header(struct mmqtt_connection *conn, mmqtt_s_puller puller,
                              const struct mmqtt_p_connack_header *header)
{
  mmqtt_status_t status;

  status = mmqtt_s_encode_buffer(conn, puller, &header->reserved, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  return mmqtt_s_encode_buffer(conn, puller, &header->return_code, 1);
}

mmqtt_status_t
mmqtt_s_decode_connack_header(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                              struct mmqtt_p_connack_header *header)
{
  mmqtt_status_t status;

  status = mmqtt_s_decode_buffer(conn, pusher, &header->reserved, 1, 1);
  if (status != MMQTT_STATUS_OK) { return status; }

  return mmqtt_s_decode_buffer(conn, pusher, &header->reserved, 1, 1);
}

/* Works for both subscribe and unsubscribe */
struct mmqtt_p_subscribe_decode_context {
  uint32_t length;
  uint8_t has_qos;
};

struct mmqtt_p_subscribe_payload_line {
  uint8_t *topic;
  uint16_t topic_length;
  uint16_t topic_max_length;
  uint8_t qos;
};

void
mmqtt_s_subscribe_decode_context_init(struct mmqtt_p_subscribe_decode_context *context,
                                      uint32_t length)
{
  context->length = length;
  context->has_qos = 1;
}

void
mmqtt_s_unsubscribe_decode_context_init(struct mmqtt_p_subscribe_decode_context *context,
                                        uint32_t length)
{
  context->length = length;
  context->has_qos = 0;
}

mmqtt_status_t
mmqtt_s_decode_subscribe_payload(struct mmqtt_connection *conn, mmqtt_s_pusher pusher,
                                      struct mmqtt_p_subscribe_decode_context *context,
                                      struct mmqtt_p_subscribe_payload_line *line)
{
  mmqtt_status_t status;
  uint16_t string_true_length;

  if (context->length <= 0) { return MMQTT_STATUS_DONE; }
  status = mmqtt_s_decode_string(conn, pusher, line->topic, line->topic_max_length,
                                 &line->topic_length, &string_true_length);
  if (status != MMQTT_STATUS_OK) { return status; }

  context->length -= string_true_length;
  context->length -= 2;

  if (context->has_qos != 0) {
    status = mmqtt_s_decode_buffer(conn, pusher, &line->qos, 1, 1);
    if (status != MMQTT_STATUS_OK) { return status; }
    context->length -= 1;
  } else {
    line->qos = 0;
  }
  return MMQTT_STATUS_OK;
}

uint32_t
mmqtt_s_string_encoded_length(uint16_t string_length)
{
  return string_length + 2;
}

uint32_t
mmqtt_s_connect_header_encoded_length(const struct mmqtt_p_connect_header *header)
{
  return mmqtt_s_string_encoded_length(header->name_length) + 4;
}

#endif  /* MMQTT_STATIC_H_ */
