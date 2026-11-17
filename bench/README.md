# Benchmark: slang amqp against Go's amqp091-go

    SLANGC=/path/to/slangc ROUNDS=4 sh bench/run.sh

starts RabbitMQ 4.1.8 in Docker, builds `bench/slang` and `bench/go`
(the same four scenarios in each), and runs them in ABBA rounds. Each
run's CPU (user + system, from `/usr/bin/time -l`) and peak RSS are
recorded with its result. Scenarios, 256-byte bodies:

- **publish**: 100,000 messages, no confirms, then one round trip on the
  channel so the broker has read them all.
- **consume**: those 100,000, QoS 500, an ack each.
- **confirm**: 100,000 in confirm mode, waiting for every confirm each
  1,000.
- **latency**: 5,000 round trips, one message at a time, from a publish
  on one connection to its delivery on another.

## Results (2026-11-17)

Intel MacBook, both clients on the host, RabbitMQ in Docker Desktop on
the same machine (which was thermally throttled: read the ratios, not the
rates). Medians of 4 rounds; every run is in `results-2026-11-17.txt`.

| Scenario | slang | Go amqp091-go | slang / Go |
|---|---|---|---|
| publish, msgs/s | 33,193 | 35,186 | 0.94 |
| publish, CPU us/msg | 15.9 | 28.6 | 0.56 |
| consume + ack, msgs/s | 20,481 | 18,476 | 1.11 |
| consume, CPU us/msg | 37.8 | 54.0 | 0.70 |
| confirmed publish, msgs/s | 20,232 | 20,107 | 1.01 |
| confirmed publish, CPU us/msg | 27.7 | 36.9 | 0.75 |
| latency p50 / p99, us | 587 / 1217 | 584 / 1185 | 1.01 / 1.03 |
| peak RSS, MB | 3.9-9.0 | 10.1-11.6 | 0.4-0.8 |

Throughput is mostly the broker's: both clients publish at about the same
rate, and slang's first publish round (25k/s, a cold broker) pulls its
median down. The client's own cost shows in CPU per message, 25-44% less
than Go's, and in memory. Latency is the same within noise.
