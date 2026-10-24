# Message Processing

This context accepts messages over HTTP, publishes them to Kafka, processes them in batches, and makes successfully processed messages available to authenticated clients.

## Message language

**Event**:
A versioned message describing a fact, with a type, occurrence time, and JSON payload.
_Avoid_: Record, raw message

**Processed message**:
An event whose payload has been accepted and durably stored for API retrieval.
_Avoid_: Consumed event (consumption alone does not mean processing succeeded)

**Dead-letter message**:
An event that could not be processed after the configured retries, retained with failure metadata for investigation or replay.
_Avoid_: Dropped message
