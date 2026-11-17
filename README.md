# amqp

A RabbitMQ (AMQP 0-9-1) client for slang: work queues, pub/sub and RPC
over the stdlib `net` package, with deadlines, verified TLS and bounded
memory. The plan is [AMQP_PRD.md](AMQP_PRD.md).

**Status:** milestone 2 of 7. The wire layer (frames, field tables,
methods, content headers) and the connection: `amqp://` and `amqps://`
URLs, the handshake with PLAIN or EXTERNAL login, negotiated limits,
heartbeats both ways, `connection.blocked`, and closing from either side.
Channels, publishing and consuming come next.

```slang
import "amqp";

let cr = amqp.dial("amqps://app:secret@mq.internal/prod", amqp.options(), deadline);
guard let conn = cr else let e = err_of(cr) { panic(e); }   // "amqp: 403 ACCESS_REFUSED - ..."
amqp.close(conn, deadline);
```

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/amqp`), `tests/fake` (the connection
against scripted brokers: refused logins and vhosts, another protocol,
forced closes, malformed and oversized frames, heartbeats, blocking), and
`tests/wire`: 422 frames
the package must decode, render and encode back byte for byte, or refuse.
`tests/wire/gen.py` writes them with Python's `struct` to the 0-9-1 spec
and RabbitMQ's table types, and checks that writer against pika 1.3.2
wherever pika reads the types correctly.
