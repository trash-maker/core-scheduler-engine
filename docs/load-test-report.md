# Load test report

Generated 2026-11-26T01:27:18.461760073Z on 4 vCPU; PostgreSQL 16 (Testcontainers, fsync=off); broker: in-memory sink; timer workers: 4, relays: 2, batch 200/500.

## A. sustained 1000/s for 60s

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 60000 |
| signal ingest | 180000 signals in 30.1 s = 5977 signals/s |
| published | 60000 / 60000 |
| publish throughput (first due -> last ack) | 999 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 10 / 33 / 43 / 72 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 75 / 117 / 138 / 180 |

## B. burst 30000 due at the same instant

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 30000 |
| signal ingest | 90000 signals in 14.4 s = 6255 signals/s |
| published | 30000 / 30000 |
| publish throughput (first due -> last ack) | 5719 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 2443 / 4788 / 5018 / 5105 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 2599 / 4966 / 5164 / 5245 |

# Load test report (Kafka)

Generated 2026-11-26T01:31:03.291884106Z on 4 vCPU; PostgreSQL 16 (Testcontainers, fsync=off); broker: Kafka 3.8 (Testcontainers); timer workers: 4, relays: 2, batch 200/500.

## A. sustained 1000/s for 60s

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 60000 |
| signal ingest | 180000 signals in 35.2 s = 5110 signals/s |
| published | 60000 / 60000 |
| publish throughput (first due -> last ack) | 996 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 11 / 36 / 47 / 73 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 92 / 144 / 201 / 616 |

## B. burst 30000 due at the same instant

| metric | value |
|---|---|
| notifications created (SCHEDULED) | 30000 |
| signal ingest | 90000 signals in 14.6 s = 6152 signals/s |
| published | 30000 / 30000 |
| publish throughput (first due -> last ack) | 5267 msg/s |
| timers.lag p50 / p95 / p99 / max (ms) | 2859 / 5148 / 5367 / 5393 |
| publish lag due->ack p50 / p95 / p99 / max (ms) | 3023 / 5314 / 5504 / 5550 |

