SLANGC ?= slangc

.PHONY: test
test:
	SLANGC="$(SLANGC)" ./tests/run.sh

# The client against RabbitMQ in Docker (TLS, a Go amqp091-go peer);
# needs docker, openssl and go.
.PHONY: live
live:
	SLANGC="$(SLANGC)" sh ./tests/live/run.sh
