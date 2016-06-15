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
  if (stream->left > 0) {
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
  mmqtt_ring_buffer_push_(&stream->ring_buffer, 1);
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

#define MMQTT_QUEUE_LENGTH(queue) ((queue)->ring_buffer.length)
#define MMQTT_QUEUE_INDEX(queue, i) ((queue)->streams[((queue)->ring_buffer.start + (i)) % ((queue)->ring_buffer.capacity)])

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

/*
 * There's no need to release a write stream, connection would detect
 * a stream is done, and release it while pulling data.
 * Read stream, on the other hand, is pulled by user manually, so connection
 * knows nothing about when it is done.
 */
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

#endif  /* MMQTT_H_ */
