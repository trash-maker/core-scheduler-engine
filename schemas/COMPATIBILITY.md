# Message schema compatibility rule

All topics carry JSON. Schemas in this folder are the contract (JSON Schema 2020-12).

* **Rule: BACKWARD_TRANSITIVE + additive only.** Within a major version (`schema_version` = 1) producers may only
  *add optional fields*. Fields are never removed, renamed, retyped or made required; enum values are never removed.
* Consumers must **ignore unknown fields** (`additionalProperties: true`) – the engine does, actuators must too.
* A breaking change creates `schema_version: 2` and a new file (`*-v2.json`). The engine publishes v1 until every
  actuator group has been upgraded to read v2 (dual publishing is done with a second output topic if needed).
* Headers on `notifications.out`: `action`, `tenant_id`, `notification_id`, `attempt`, `schema_version`, `content-type`,
  plus W3C trace context (`traceparent`, `tracestate`). New headers may be added at any time.
