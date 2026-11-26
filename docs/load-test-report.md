# Load test report

Generated 2026-11-26T07:57:54.451397903Z on 4 vCPU; Quarkus app (roles timers,relay; JTA transactions); PostgreSQL 16 (Testcontainers, fsync=off); broker: in-memory sink; timer workers 4, relays 2.

## A. sustained 1000/s for 60s

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 60000 |
| signal ingest | 180000 signals in 23.7 s = 7599 signals/s |
| published | 60000 / 60000 |
| publish throughput (first due -> last ack) | 999 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 1 / 3 / 5 / 40 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 18 / 30 / 35 / 105 |

## B. burst 30000 due at the same instant

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 30000 |
| signal ingest | 90000 signals in 10.5 s = 8562 signals/s |
| published | 30000 / 30000 |
| publish throughput (first due -> last ack) | 10118 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 1394 / 2673 / 2759 / 2792 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 1470 / 2742 / 2826 / 2879 |

# Load test report (Kafka)

Generated 2026-11-26T08:01:40.910077798Z on 4 vCPU; Quarkus app (roles timers,relay; JTA transactions); PostgreSQL 16 (Testcontainers, fsync=off); broker: Kafka 3.8 (Testcontainers); timer workers 4, relays 2.

## A. sustained 1000/s for 60s

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 60000 |
| signal ingest | 180000 signals in 21.9 s = 8220 signals/s |
| published | 60000 / 60000 |
| publish throughput (first due -> last ack) | 999 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 1 / 4 / 8 / 83 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 27 / 47 / 80 / 402 |

## B. burst 30000 due at the same instant

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 30000 |
| signal ingest | 90000 signals in 11.5 s = 7796 signals/s |
| published | 30000 / 30000 |
| publish throughput (first due -> last ack) | 9375 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 1546 / 2898 / 2997 / 3025 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 1645 / 2968 / 3062 / 3100 |

