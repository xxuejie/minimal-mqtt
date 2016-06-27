#ifndef MMQTT_H_
#define MMQTT_H_

#define MMQTT_STREAM_MAX_LENGTH 8
#define MMQTT_QUEUE_MAX_LENGTH 2

/* mmqtt_ssize_t must be able to hold MMQTT_STREAM_MAX_LENGTH as well as
 * MMQTT_QUEUE_MAX_LENGTH, we might add macro-based detection in the future.
 */
typedef int8_t mmqtt_ssize_t;
typedef int8_t mmqtt_status_t;

#define MMQTT_STATUS_OK 0
#define MMQTT_STATUS_UNKNOWN -1
#define MMQTT_STATUS_DONE -2
#define MMQTT_STATUS_NOT_PULLABLE -3
#define MMQTT_STATUS_NOT_PUSHABLE -4
#define MMQTT_STATUS_NOT_EXPECTED -5
#define MMQTT_STATUS_BROKEN_CONNECTION -6
#define MMQTT_STATUS_INVALID_STATE -7

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

struct mmqtt_ring_buffer_ {
  mmqtt_ssize_t start;
  mmqtt_ssize_t length;
  mmqtt_ssize_t capacity;
};

struct mmqtt_stream {
  struct mmqtt_ring_buffer_ ring_buffer;
  uint8_t data[MMQTT_STREAM_MAX_LENGTH];
  size_t left;
};

struct mmqtt_queue_ {
  struct mmqtt_ring_buffer_ ring_buffer;
  struct mmqtt_stream* streams[MMQTT_QUEUE_MAX_LENGTH];
};

#define MMQTT_QUEUE_LENGTH(queue) ((queue)->ring_buffer.length)
#define MMQTT_QUEUE_INDEX(queue, i) ((queue)->streams[((queue)->ring_buffer.start + (i)) % ((queue)->ring_buffer.capacity)])

struct mmqtt_connection {
  void *connection;
  struct mmqtt_queue_ read_queue;
  struct mmqtt_queue_ write_queue;
};

static inline void
mmqtt_ring_buffer_init_(struct mmqtt_ring_buffer_ *buffer, mmqtt_ssize_t capacity)
{
  buffer->start = 0;
  buffer->length = 0;
  buffer->capacity = capacity;
}

static inline mmqtt_ssize_t
mmqtt_ring_buffer_pullable_(const struct mmqtt_ring_buffer_ *buffer)
{
  return min(buffer->start + buffer->length, buffer->capacity) - buffer->start;
}

static inline void
mmqtt_ring_buffer_pull_(struct mmqtt_ring_buffer_ *buffer, mmqtt_ssize_t length)
{
  buffer->start = (buffer->start + length) % buffer->capacity;
  buffer->length -= length;
}

static inline mmqtt_ssize_t
mmqtt_ring_buffer_pushable_(const struct mmqtt_ring_buffer_ *buffer)
{
	if (buffer->start + buffer->length > buffer->capacity) {
		return buffer->capacity - buffer->length;
	}
	return buffer->capacity - buffer->length - buffer->start;
}

static inline void
mmqtt_ring_buffer_push_(struct mmqtt_ring_buffer_ *buffer, mmqtt_ssize_t length)
{
  buffer->length += length;
}

static void
mmqtt_stream_init(struct mmqtt_stream *stream, size_t total_size)
{
  mmqtt_ring_buffer_init_(&stream->ring_buffer, MMQTT_STREAM_MAX_LENGTH);
  stream->left = total_size;
}

static mmqtt_status_t
mmqtt_stream_running(struct mmqtt_stream *stream)
{
  if (stream->left > 0 || stream->ring_buffer.length > 0) {
    return MMQTT_STATUS_OK;
  } else {
    return MMQTT_STATUS_DONE;
  }
}

static mmqtt_ssize_t
mmqtt_stream_pull(struct mmqtt_stream *stream, uint8_t* out, mmqtt_ssize_t max_length)
{
  mmqtt_ssize_t pullable, i;
  pullable = mmqtt_ring_buffer_pullable_(&stream->ring_buffer);
  if (pullable <= 0) {
    if (stream->left == 0) {
      return MMQTT_STATUS_DONE;
    } else {
      return MMQTT_STATUS_NOT_PULLABLE;
    }
  }
  pullable = min(pullable, max_length);

  if (out) {
    for (i = 0; i < pullable; i++) {
      out[i] = stream->data[stream->ring_buffer.start + i];
    }
  }
  mmqtt_ring_buffer_pull_(&stream->ring_buffer, pullable);
  return pullable;
}

static mmqtt_status_t
mmqtt_stream_external_pullable(struct mmqtt_stream *stream,
                               const uint8_t** data, mmqtt_ssize_t* max_length)
{
  mmqtt_ssize_t pullable;
  pullable = mmqtt_ring_buffer_pullable_(&stream->ring_buffer);
  if (pullable <= 0) {
    if (stream->left == 0) {
      return MMQTT_STATUS_DONE;
    } else {
      return MMQTT_STATUS_NOT_PULLABLE;
    }
  }
  pullable = mmqtt_ring_buffer_pullable_(&stream->ring_buffer);
  if (pullable <= 0) { return MMQTT_STATUS_NOT_PULLABLE; }
  *data = stream->data + stream->ring_buffer.start;
  *max_length = pullable;
  return MMQTT_STATUS_OK;
}

static mmqtt_status_t
mmqtt_stream_external_pull(struct mmqtt_stream *stream, mmqtt_ssize_t length)
{
  mmqtt_ssize_t pullable;
  pullable = mmqtt_ring_buffer_pullable_(&stream->ring_buffer);
  if (pullable < length) {
    return MMQTT_STATUS_NOT_PULLABLE;
  } else {
    mmqtt_ring_buffer_pull_(&stream->ring_buffer, length);
    return MMQTT_STATUS_OK;
  }
}

static mmqtt_ssize_t
mmqtt_stream_push(struct mmqtt_stream *stream, const uint8_t* in, mmqtt_ssize_t length)
{
  mmqtt_ssize_t pushable, i;
  if (stream->left == 0) { return MMQTT_STATUS_DONE; }
  pushable = mmqtt_ring_buffer_pushable_(&stream->ring_buffer);
  if (pushable <= 0) { return MMQTT_STATUS_NOT_PUSHABLE; }
  pushable = min(stream->left, min(pushable, length));

  for (i = 0; i < pushable; i++) {
    stream->data[stream->ring_buffer.start + i] = in[i];
  }
  mmqtt_ring_buffer_push_(&stream->ring_buffer, pushable);
  if (stream->left > 0) { stream->left -= pushable; }
  return pushable;
}

static mmqtt_status_t
mmqtt_stream_external_pushable(struct mmqtt_stream *stream,
                               uint8_t** data, mmqtt_ssize_t* max_length)
{
  mmqtt_ssize_t pushable;
  if (stream->left == 0) { return MMQTT_STATUS_DONE; }
  pushable = mmqtt_ring_buffer_pushable_(&stream->ring_buffer);
  if (pushable <= 0) { return MMQTT_STATUS_NOT_PUSHABLE; }
  pushable = min(pushable, stream->left);
  *data = stream->data + stream->ring_buffer.start;
  *max_length = pushable;
  return MMQTT_STATUS_OK;
}

static mmqtt_status_t
mmqtt_stream_external_push(struct mmqtt_stream *stream, mmqtt_ssize_t length)
{
  mmqtt_ssize_t pushable;
  if (stream->left == 0) { return MMQTT_STATUS_DONE; }
  pushable = mmqtt_ring_buffer_pushable_(&stream->ring_buffer);
  if (pushable <= 0) { return MMQTT_STATUS_NOT_PUSHABLE; }
  pushable = min(pushable, stream->left);
  if (pushable < length) {
    return MMQTT_STATUS_NOT_PUSHABLE;
  } else {
    mmqtt_ring_buffer_push_(&stream->ring_buffer, pushable);
    return MMQTT_STATUS_OK;
  }
}

static void
mmqtt_queue_init_(struct mmqtt_queue_ *queue)
{
  mmqtt_ring_buffer_init_(&queue->ring_buffer, MMQTT_QUEUE_MAX_LENGTH);
}

static mmqtt_status_t
mmqtt_queue_add_(struct mmqtt_queue_ *queue, struct mmqtt_stream *stream)
{
  if (mmqtt_ring_buffer_pushable_(&queue->ring_buffer) <= 0) {
    return MMQTT_STATUS_NOT_PUSHABLE;
  }
  queue->streams[queue->ring_buffer.start] = stream;
  mmqtt_ring_buffer_push_(&queue->ring_buffer, 1);
  return MMQTT_STATUS_OK;
}

#define MMQTT_QUEUE_AVAILABLE(queue) ((queue)->ring_buffer.length > 0)
#define MMQTT_QUEUE_FIRST(queue) ((queue)->streams[(queue)->ring_buffer.start])

static void
mmqtt_connection_init(struct mmqtt_connection *connection, void *conn)
{
  connection->connection = conn;
  mmqtt_queue_init_(&connection->read_queue);
  mmqtt_queue_init_(&connection->write_queue);
}

/*
 * Pulls data out of connection's write queue, the returned data is supposed
 * to be sent to a TCP socket to remote servers.
 */
static mmqtt_ssize_t
mmqtt_connection_pull(struct mmqtt_connection *connection,
                      uint8_t *out, mmqtt_ssize_t max_length)
{
  mmqtt_ssize_t length, current;
  struct mmqtt_queue_ *queue;
  length = 0;
  queue = &connection->write_queue;
  while(MMQTT_QUEUE_AVAILABLE(queue) && length < max_length) {
    current = mmqtt_stream_pull(MMQTT_QUEUE_FIRST(queue),
                                out + length,
                                max_length - length);
    /* TODO: in case current is 0, should we return non-pullable? */
    if (current >= 0) {
      length += current;
    } else if (current == MMQTT_STATUS_DONE) {
      /* Release stream from write queue since it is done */
      mmqtt_ring_buffer_pull_(&queue->ring_buffer, 1);
    } else if (current == MMQTT_STATUS_NOT_PULLABLE) {
      return (length > 0) ? length : MMQTT_STATUS_NOT_PULLABLE;
    } else {
      return MMQTT_STATUS_UNKNOWN;
    }
  }
  return (length > 0) ? length : MMQTT_STATUS_NOT_PULLABLE;
}

struct mmqtt_stream *
mmqtt_connection_pullable_stream(struct mmqtt_connection *connection)
{
  mmqtt_ssize_t i;
  struct mmqtt_queue_ *queue = &connection->write_queue;
  struct mmqtt_stream *stream = NULL;
  for (i = 0; i < MMQTT_QUEUE_LENGTH(queue); i++) {
    stream = MMQTT_QUEUE_INDEX(queue, i);
    if (mmqtt_stream_running(stream) == MMQTT_STATUS_OK) { return stream; }
  }
  return NULL;
}

/*
 * Pushes data to connection's read queue, the data should come from
 * a TCP socket to a remote machine.
 */
static mmqtt_ssize_t
mmqtt_connection_push(struct mmqtt_connection *connection,
                      const uint8_t* in, mmqtt_ssize_t length)
{
  mmqtt_ssize_t i, current, consumed_length;
  struct mmqtt_queue_ *queue;
  consumed_length = 0;
  queue = &connection->read_queue;
  while (i < MMQTT_QUEUE_LENGTH(queue) && consumed_length < length) {
    current = mmqtt_stream_push(MMQTT_QUEUE_INDEX(queue, i),
                                in + consumed_length,
                                length - consumed_length);
    if (current >= 0) {
      consumed_length += current;
    } else if (current == MMQTT_STATUS_DONE) {
      i++;
    } else if (current == MMQTT_STATUS_NOT_PUSHABLE) {
      return (consumed_length > 0) ? consumed_length : MMQTT_STATUS_NOT_PUSHABLE;
    } else {
      return MMQTT_STATUS_UNKNOWN;
    }
  }
  return (consumed_length > 0) ? consumed_length : MMQTT_STATUS_NOT_PUSHABLE;
}

struct mmqtt_stream *
mmqtt_connection_pushable_stream(struct mmqtt_connection *connection)
{
  mmqtt_ssize_t i;
  struct mmqtt_queue_ *queue = &connection->read_queue;
  struct mmqtt_stream *stream = NULL;
  for (i = 0; i < MMQTT_QUEUE_LENGTH(queue); i++) {
    stream = MMQTT_QUEUE_INDEX(queue, i);
    if (mmqtt_stream_running(stream) == MMQTT_STATUS_OK) { return stream; }
  }
  return NULL;
}

static mmqtt_status_t
mmqtt_connection_add_read_stream(struct mmqtt_connection *connection,
                                 struct mmqtt_stream *stream)
{
  return mmqtt_queue_add_(&connection->read_queue, stream);
}

static mmqtt_status_t
mmqtt_connection_add_write_stream(struct mmqtt_connection *connection,
                                  struct mmqtt_stream *stream)
{
  return mmqtt_queue_add_(&connection->write_queue, stream);
}

static mmqtt_status_t
mmqtt_connection_in_read_stream(struct mmqtt_connection *connection,
                                struct mmqtt_stream *stream)
{
  mmqtt_ssize_t i;
  struct mmqtt_queue_ *queue = &connection->read_queue;
  for (i = 0; i < MMQTT_QUEUE_LENGTH(queue); i++) {
    if (MMQTT_QUEUE_INDEX(queue, i) == stream) { return MMQTT_STATUS_OK; }
  }
  return MMQTT_STATUS_NOT_EXPECTED;
}

static mmqtt_status_t
mmqtt_connection_in_write_stream(struct mmqtt_connection *connection,
                                 struct mmqtt_stream *stream)
{
  mmqtt_ssize_t i;
  struct mmqtt_queue_ *queue = &connection->write_queue;
  for (i = 0; i < MMQTT_QUEUE_LENGTH(queue); i++) {
    if (MMQTT_QUEUE_INDEX(queue, i) == stream) { return MMQTT_STATUS_OK; }
  }
  return MMQTT_STATUS_NOT_EXPECTED;
}

static mmqtt_ssize_t
mmqtt_connection_read_stream_length(struct mmqtt_connection *connection)
{
  return connection->read_queue.ring_buffer.length;
}

static mmqtt_ssize_t
mmqtt_connection_write_stream_length(struct mmqtt_connection *connection)
{
  return connection->write_queue.ring_buffer.length;
}

static mmqtt_status_t
mmqtt_connection_release_write_stream(struct mmqtt_connection *connection,
                                      struct mmqtt_stream *stream)
{
  if (!MMQTT_QUEUE_AVAILABLE(&connection->write_queue)) {
    return MMQTT_STATUS_NOT_PULLABLE;
  }
  if (stream != MMQTT_QUEUE_FIRST(&connection->write_queue)) {
    return MMQTT_STATUS_NOT_EXPECTED;
  }
  mmqtt_ring_buffer_pull_(&connection->write_queue.ring_buffer, 1);
  return MMQTT_STATUS_OK;
}

static mmqtt_status_t
mmqtt_connection_release_read_stream(struct mmqtt_connection *connection,
                                     struct mmqtt_stream *stream)
{
  if (!MMQTT_QUEUE_AVAILABLE(&connection->read_queue)) {
    return MMQTT_STATUS_NOT_PULLABLE;
  }
  if (stream != MMQTT_QUEUE_FIRST(&connection->read_queue)) {
    return MMQTT_STATUS_NOT_EXPECTED;
  }
  mmqtt_ring_buffer_pull_(&connection->read_queue.ring_buffer, 1);
  return MMQTT_STATUS_OK;
}

static mmqtt_status_t
mmqtt_connection_autorelease_write_streams(struct mmqtt_connection *connection)
{
  struct mmqtt_stream *stream;
  while (MMQTT_QUEUE_AVAILABLE(&connection->write_queue)) {
    stream = MMQTT_QUEUE_FIRST(&connection->write_queue);
    if (mmqtt_stream_running(stream) == MMQTT_STATUS_DONE) {
      mmqtt_connection_release_write_stream(connection, stream);
    } else {
      break;
    }
  }
  return MMQTT_STATUS_OK;
}

static mmqtt_status_t
mmqtt_connection_autorelease_read_streams(struct mmqtt_connection *connection)
{
  struct mmqtt_stream *stream;
  while (MMQTT_QUEUE_AVAILABLE(&connection->read_queue)) {
    stream = MMQTT_QUEUE_FIRST(&connection->read_queue);
    if (mmqtt_stream_running(stream) == MMQTT_STATUS_DONE) {
      mmqtt_connection_release_read_stream(connection, stream);
    } else {
      break;
    }
  }
  return MMQTT_STATUS_OK;
}

#ifndef MMQTT_DISABLE_STATIC

typedef mmqtt_status_t (*mmqtt_s_puller)(struct mmqtt_connection*);
typedef mmqtt_status_t (*mmqtt_s_pusher)(struct mmqtt_connection*, mmqtt_ssize_t);

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

#endif  /* MMQTT_DISABLE_STATIC */

#endif  /* MMQTT_H_ */
