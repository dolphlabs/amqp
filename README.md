# amqp

A RabbitMQ (AMQP 0-9-1) client for slang: work queues, pub/sub and RPC
over the stdlib `net` package, with deadlines, verified TLS and bounded
memory. The plan is [AMQP_PRD.md](AMQP_PRD.md).

**Status:** milestone 3 of 7: the wire layer, the connection (URLs,
PLAIN or EXTERNAL login, negotiated limits, heartbeats, blocking, closing
from either side), and channels with topology: exchanges and queues
declared, bound, purged and deleted, and QoS. Publishing and consuming
come next.

```slang
import "amqp";

let cr = amqp.dial("amqps://app:secret@mq.internal/prod", amqp.options(), deadline);
guard let conn = cr else let e = err_of(cr) { panic(e); }   // "amqp: 403 ACCESS_REFUSED - ..."
let chr = amqp.channel(conn, deadline);
guard let ch = chr else let e = err_of(chr) { panic(e); }
amqp.exchange_declare(ch, "events", "topic", amqp.exchange_options(), deadline);
let qr = amqp.queue_declare(ch, "jobs", amqp.durable(), deadline);    // result[Queue, str]
amqp.queue_bind(ch, "jobs", "events", "order.*", amqp.table(), deadline);
amqp.qos(ch, 32, deadline);
amqp.close(conn, deadline);
```

A broker error (404 NOT_FOUND, 406 PRECONDITION_FAILED, ...) closes the
channel it happened on, not the connection: the call returns the broker's
reason, and so does every later call on that channel.

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/amqp`), `tests/fake` (the connection
against scripted brokers: refused logins and vhosts, another protocol,
forced closes, malformed and oversized frames, heartbeats, blocking, and
every channel and topology call, broker errors and timeouts), and
`tests/wire`: 422 frames
the package must decode, render and encode back byte for byte, or refuse.
`tests/wire/gen.py` writes them with Python's `struct` to the 0-9-1 spec
and RabbitMQ's table types, and checks that writer against pika 1.3.2
wherever pika reads the types correctly.
