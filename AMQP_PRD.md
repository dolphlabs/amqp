# amqp — RabbitMQ (AMQP 0-9-1) client for slang

**Status:** PRD. No code yet.
**Repository:** `github.com/dolphlabs/amqp`
**Import name:** `amqp`
**Package path:** `src/amqp/`

## Product goal

Give slang services a RabbitMQ client for the three jobs a broker does in
a backend: work queues (publish a task, one consumer acks it), pub/sub
(fan a message out through an exchange), and RPC (reply-to and
correlation id). Applications add one Git/tag pin in `slang.project`,
import `amqp`, and get deadlines, verified TLS, bounded memory and
explicit errors as part of the contract, the same as the `pg`, `nats` and
`mongo` clients.

AMQP 0-9-1 is the protocol RabbitMQ (3.x and 4.x) and LavinMQ speak.
AMQP 1.0, RabbitMQ Streams and MQTT are different protocols and are not
part of this package.

## Project understanding

slang compiles a package's `.sl` files into one namespace. Network
clients are written in slang over the stdlib `net` package, whose socket
waits park the calling task instead of blocking a worker thread; `net`
already has deadline-bounded TCP and verified TLS. The stdlib `nats`
client is the closest design: one reader task per connection hands each
incoming message to its subscription's channel, and a flusher task turns
bursts of publishes into a few large writes.

AMQP 0-9-1 in the parts this package needs:

- **Connection.** The client sends the protocol header `AMQP\x00\x00\x09\x01`;
  the server answers `connection.start` (mechanisms, locales, server
  properties), the client `start-ok` (SASL PLAIN credentials, client
  properties and capabilities), then `tune`/`tune-ok` (channel-max,
  frame-max, heartbeat) and `open`/`open-ok` (virtual host).
- **Frames.** Type (method, content header, content body, heartbeat),
  channel number, length, payload, and a 0xCE end byte. A message is a
  `basic.publish` or `basic.deliver` method frame, a content header frame
  (body size and properties), and body frames no larger than frame-max.
  A channel's content frames must be contiguous on the wire.
- **Channels** multiplex independent sessions over one connection. Most
  methods are synchronous (a `*-ok` reply on the same channel); `deliver`,
  `return`, `ack`/`nack` (confirms), `cancel` and `close` arrive
  unprompted.
- **Field tables** (headers, arguments) are typed values: strings, signed
  and unsigned integers of several widths, booleans, floats, decimals,
  timestamps, nested tables, arrays, byte arrays and void.
- **Errors** close a channel (`channel.close` with a reply code: 404 not
  found, 406 precondition failed, 403 access refused) or the whole
  connection (`connection.close`: 320 forced, 501 frame error, 541
  internal error, ...).

Nothing in slang speaks AMQP today. There is no dynamic value type in the
stdlib (slang enums carry no payload), so field tables need their own
small value model.

## Goals

- A standalone package that applications pin and lock, with no files
  copied into the compiler repository.
- Correct framing and field-table encoding, with strict bounds on frame
  size, message size, table nesting and element count, so a hostile or
  broken broker cannot make the client allocate without limit.
- A connection handshake with SASL PLAIN and EXTERNAL (client
  certificates), `amqp://` and `amqps://` URIs, negotiated tuning and
  heartbeats, and deadlines on every blocking call.
- Channels with topology (exchanges, queues, bindings), QoS, publishing
  with properties, publisher confirms and mandatory returns, consuming
  with ack/nack/reject, `basic.get`, and cancellation.
- Delivery to consumers through slang channels, bounded by the prefetch
  window, so a slow consumer applies backpressure through the broker
  instead of buffering without limit in the client.
- Clear errors carrying the broker's reply code and text, and a
  connection or channel that reports why it closed.
- Tests against scripted fake brokers for malformed and hostile input,
  and a live suite against RabbitMQ in CI.
- Throughput, latency and memory measured against Go's `amqp091-go` on
  the same machine, with the gaps recorded.

## Deliberate limits

- **No automatic reconnection or topology recovery in v1.** A dropped
  connection closes its channels and consumers with an error the
  application sees; it reconnects and redeclares. Go's `amqp091-go` takes
  the same position; recovery (redeclare exchanges, queues, bindings and
  consumers) is a follow-up once the core is stable.
- **No transactions** (`tx.select`); publisher confirms are the supported
  way to know a publish arrived.
- **SASL PLAIN and EXTERNAL only.** AMQPLAIN and RabbitMQ's other
  mechanisms are not offered.
- **Not AMQP 1.0, Streams or MQTT.**
- **Field tables cover the types RabbitMQ writes** (the 0-9-1 errata
  set); a broker sending a type outside it is a connection error.

## Proposed architecture

- `frame.sl`: frame reader and writer over `net`, with frame-max
  enforcement and the end-byte check.
- `table.sl`: the `Value`/`Table` model and its codec. `Value` is a
  `gc struct` with a kind and one field per kind, as `mongo`'s BSON
  `Value` is; constructors (`amqp.str_value`, `amqp.int_value`, ...) and
  accessors keep callers away from the kind field.
- `methods.sl`: encode and decode for the methods of the connection,
  channel, exchange, queue, basic and confirm classes, generated by hand
  from the spec's XML and checked against byte vectors.
- `conn.sl`: URI parsing, the handshake, tuning, a reader task that
  dispatches frames to channels, a writer guarded by a mutex (one
  message's frames are written together), a heartbeat task, and close.
- `channel.sl`: per-channel state; synchronous calls send a method and
  wait for the matching reply on a slang channel, with a deadline;
  unprompted methods go to the consumer, confirm and return queues.
- `consume.sl`, `publish.sl`: the public API for the two directions.

Sketch of the API:

```slang
import "amqp";

let cr = amqp.dial("amqps://user:pass@mq.internal/prod", amqp.options(), deadline);
guard let conn = cr else let e = err_of(cr) { panic(e); }
let chr = amqp.channel(conn, deadline);
guard let ch = chr else let e = err_of(chr) { panic(e); }

amqp.queue_declare(ch, "jobs", amqp.durable(), deadline);
amqp.qos(ch, 32, deadline);

let msg = amqp.publishing(b"{\"id\":1}");
msg.content_type = "application/json";
msg.delivery_mode = amqp.PERSISTENT;
amqp.confirm_select(ch, deadline);
let seq = amqp.publish(ch, "", "jobs", msg, deadline);       // result[int, str]
amqp.wait_confirm(ch, seq, deadline);                         // result[bool, str]: acked or nacked

let sr = amqp.consume(ch, "jobs", amqp.consume_options(), deadline);
guard let sub = sr else let e = err_of(sr) { panic(e); }
let dr = amqp.next(sub, deadline);                            // result[opt[Delivery], str]
// ... handle, then
amqp.ack(ch, d.delivery_tag, false);
```

## Delivery milestones

1. **Wire.** Frames, field tables and method codecs, with unit tests from
   byte vectors (spec examples and captured RabbitMQ traffic). No network.
2. **Connection.** URI parsing, handshake, tuning, heartbeats, graceful
   and forced close, TLS and EXTERNAL; tested against a scripted fake
   broker that also sends malformed and oversized frames.
3. **Channels and topology.** Open and close, exchange/queue declare,
   bind, unbind, purge and delete, QoS, and reply-code errors.
4. **Publishing.** Properties, body splitting at frame-max, publisher
   confirms, mandatory returns, `connection.blocked`/`unblocked`.
5. **Consuming.** `basic.consume`, deliveries to a bounded slang channel,
   ack/nack/reject (single and multiple), `basic.get`, cancel, and the
   broker's consumer-cancel notification.
6. **Live suite.** RabbitMQ 4.x in Docker, as the stdlib's NATS and MySQL
   suites run in CI: work queue, pub/sub, RPC, confirms under load, a
   broker restart mid-consume, TLS, and interoperation with a Go
   `amqp091-go` publisher and consumer.
7. **Benchmark.** Publish throughput (with and without confirms), consume
   throughput with acks, end-to-end p50/p99 latency and RSS, against Go's
   `amqp091-go`, ABBA on one machine.

## Security and compatibility requirements

- TLS verifies the chain and host name by default; turning verification
  off is an explicit option, never a fallback.
- Credentials never appear in errors or logs; a URI is echoed with its
  password removed.
- Every size the broker controls is bounded: frame-max (the negotiated
  value, capped at a client maximum), a message's total body size,
  field-table depth and entry count, and the number of unacknowledged
  deliveries buffered per consumer (the prefetch window).
- A missed heartbeat window closes the connection with an error rather
  than leaving calls waiting.
- Supported brokers: RabbitMQ 3.13 and 4.x; LavinMQ on a best-effort
  basis.

## Validation and release gates

- Unit tests for every codec, including truncated and malformed input.
- Fake-broker tests for the handshake, errors, closes and hostile frames.
- The live suite green in CI on Linux x86-64 and arm64.
- Benchmark results recorded against `amqp091-go`, with any gap
  explained.
- A clean consumer install from a Git tag pin before the first release
  tag.

## Decisions needed before implementation

1. **Delivery API:** a pull call (`amqp.next(sub, deadline)`, as `nats`
   does) rather than exposing the slang channel. Recommended, because it
   keeps the buffering policy inside the package.
2. **Reconnection** stays out of v1 (above). Confirm.
3. **Minimum broker:** RabbitMQ 3.13 (the oldest still supported
   upstream). Confirm.
