# Kafka ingestion with a PostgreSQL read model

The application publishes events to Kafka and consumes them in batches, while PostgreSQL stores the successfully processed events for authenticated API reads. We chose at-least-once delivery with a unique event ID and per-event persistence: Kafka and PostgreSQL do not share a transaction, so replay after a crash is expected and must be idempotent. Failed events are retried individually and published to a configurable dead-letter topic before their offsets are acknowledged.
