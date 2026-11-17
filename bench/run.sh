#!/bin/sh
# slang against Go's amqp091-go, against one RabbitMQ in Docker, scenario
# by scenario in ABBA rounds. Each run's rate (or latency), CPU per
# message (user + system, from /usr/bin/time) and peak RSS.
#
#     SLANGC=/path/to/slangc ROUNDS=4 bench/run.sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/amqp-bench.XXXXXX")
CONTAINER="amqp-bench-$$"
PORT=${AMQP_BENCH_PORT:-15682}
ROUNDS=${ROUNDS:-4}
N=${N:-100000}
LAT_N=${LAT_N:-5000}
SIZE=${SIZE:-256}
slangc=${SLANGC:-slangc}
URL="amqp://app:bench@127.0.0.1:$PORT/"

cleanup() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT
trap 'exit 130' HUP INT TERM

docker run -d --name "$CONTAINER" -p "$PORT:5672" -e RABBITMQ_DEFAULT_USER=app -e RABBITMQ_DEFAULT_PASS=bench \
    rabbitmq:4.1.8 >/dev/null
for _i in $(seq 120); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$PORT") 2>/dev/null; then break; fi
    sleep 1
done
sleep 3

(cd "$TMP_DIR" && "$slangc" "$ROOT/bench/slang/main.sl" -o "$TMP_DIR/slang-bench" >/dev/null 2>&1)
(cd "$ROOT/bench/go" && go build -o "$TMP_DIR/go-bench" .)

# one run: lang mode n -> "lang mode result cpu_us_per_msg rss_mb"
one() {
    bin="$TMP_DIR/$1-bench"
    /usr/bin/time -l "$bin" "$2" "$URL" "$3" "$SIZE" >"$TMP_DIR/out" 2>"$TMP_DIR/time" || {
        cat "$TMP_DIR/out" "$TMP_DIR/time" >&2; exit 1; }
    res=$(cat "$TMP_DIR/out")
    # "  1.40 real   0.26 user   0.12 sys"
    user=$(awk '/ real /{print $3}' "$TMP_DIR/time")
    sys=$(awk '/ real /{print $5}' "$TMP_DIR/time")
    rss=$(awk '/maximum resident set size/{printf "%.1f", $1/1048576}' "$TMP_DIR/time")
    cpu=$(echo "$user $sys $3" | awk '{printf "%.1f", ($1 + $2) * 1000000 / $3}')
    printf '%-5s %-8s %-30s cpu_us/msg=%-6s rss_mb=%s\n' "$1" "$2" "$res" "$cpu" "$rss"
}

for round in $(seq "$ROUNDS"); do
    if [ $((round % 2)) -eq 1 ]; then order="slang go"; else order="go slang"; fi
    for lang in $order; do
        one "$lang" publish "$N"
        one "$lang" consume "$N"
        one "$lang" confirm "$N"
        "$TMP_DIR/$lang-bench" consume "$URL" "$N" "$SIZE" >/dev/null
        one "$lang" latency "$LAT_N"
    done
done
