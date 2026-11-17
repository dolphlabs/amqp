#!/bin/sh
# The client against a real RabbitMQ in Docker, with TLS (a throwaway CA)
# and a Go amqp091-go peer. Needs docker, openssl, go and slangc.
#
#     SLANGC=/path/to/slangc tests/live/run.sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/amqp-live.XXXXXX")
CONTAINER="amqp-live-$$"
IMAGE=rabbitmq:4.1.8
PORT=${AMQP_LIVE_PORT:-15672}
TLS_PORT=${AMQP_LIVE_TLS_PORT:-15671}
PASSWORD=$(openssl rand -hex 16)
slangc=${SLANGC:-slangc}

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

for tool in docker openssl go "$slangc"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "amqp live tests require $tool" >&2
        exit 1
    fi
done

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$TMP_DIR/ca.key" -out "$TMP_DIR/ca.crt" -days 2 \
    -subj "/CN=amqp live CA" >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$TMP_DIR/server.key" -out "$TMP_DIR/server.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' >"$TMP_DIR/ext.cnf"
openssl x509 -req -in "$TMP_DIR/server.csr" -CA "$TMP_DIR/ca.crt" -CAkey "$TMP_DIR/ca.key" -CAcreateserial \
    -out "$TMP_DIR/server.crt" -days 2 -extfile "$TMP_DIR/ext.cnf" >/dev/null 2>&1
chmod 644 "$TMP_DIR"/*.key
mkdir -p "$TMP_DIR/tls" && cp "$TMP_DIR/ca.crt" "$TMP_DIR/server.crt" "$TMP_DIR/server.key" "$TMP_DIR/tls/"
cat >"$TMP_DIR/90-test.conf" <<'CONF'
listeners.ssl.default = 5671
ssl_options.cacertfile = /tls/ca.crt
ssl_options.certfile = /tls/server.crt
ssl_options.keyfile = /tls/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
CONF

docker image inspect "$IMAGE" >/dev/null 2>&1 || docker pull -q "$IMAGE" >/dev/null
docker run -d --name "$CONTAINER" -p "$PORT:5672" -p "$TLS_PORT:5671" \
    -e RABBITMQ_DEFAULT_USER=app -e "RABBITMQ_DEFAULT_PASS=$PASSWORD" \
    -v "$TMP_DIR/tls:/tls:ro" -v "$TMP_DIR/90-test.conf:/etc/rabbitmq/conf.d/90-test.conf:ro" "$IMAGE" >/dev/null
# wait on the port from outside: a CLI tool run as root before the broker
# starts would write its Erlang cookie and stop the broker reading it
ok=0
for _i in $(seq 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then ok=1; break; fi
    sleep 1
done
[ "$ok" -eq 1 ] || { docker logs "$CONTAINER" | tail -30; exit 1; }
sleep 3

(cd "$ROOT/tests/live/peer" && go build -o "$TMP_DIR/peer" .)

expected=$(cat "$ROOT/tests/live/expected.txt")
actual=$(cd "$TMP_DIR" && \
    AMQP_URL="amqp://app:$PASSWORD@127.0.0.1:$PORT/" \
    AMQPS_URL="amqps://app:$PASSWORD@localhost:$TLS_PORT/" \
    AMQP_CA="$TMP_DIR/ca.crt" \
    AMQP_PEER="$TMP_DIR/peer" \
    AMQP_CLOSE_CMD="docker exec -u rabbitmq $CONTAINER rabbitmqctl -q close_all_connections 'test shutdown' >/dev/null" \
    "$slangc" "$ROOT/tests/live/main.sl" --run 2>/dev/null)
if [ "$actual" != "$expected" ]; then
    printf '%s\n' "$actual" >"$TMP_DIR/actual.txt"
    echo "amqp live tests: output mismatch" >&2
    diff "$ROOT/tests/live/expected.txt" "$TMP_DIR/actual.txt" >&2 || true
    exit 1
fi
printf '%s\n' "$actual"
echo "PASS live ($(printf '%s\n' "$expected" | wc -l | tr -d ' ') lines)"
