# amqp

A RabbitMQ (AMQP 0-9-1) client for slang: work queues, pub/sub and RPC
over the stdlib `net` package, with deadlines, verified TLS and bounded
memory. The plan is [AMQP_PRD.md](AMQP_PRD.md).

**Status:** milestone 1 of 7, the wire layer: frames, field tables, the
methods of the connection, channel, exchange, queue, basic and confirm
classes, and content headers (message properties). There is no
connection yet, so the package cannot talk to a broker.

## Tests

    make test SLANGC=/path/to/slangc

runs the unit tests (`slangc test src/amqp`) and `tests/wire`: 422 frames
the package must decode, render and encode back byte for byte, or refuse.
`tests/wire/gen.py` writes them with Python's `struct` to the 0-9-1 spec
and RabbitMQ's table types, and checks that writer against pika 1.3.2
wherever pika reads the types correctly.
