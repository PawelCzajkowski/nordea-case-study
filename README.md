# Kafka batch message service

Java 21 and Spring Boot service for publishing versioned JSON events over REST, processing Kafka records in batches, and exposing successfully processed events from PostgreSQL.

## Run locally

Docker Compose starts the application, Kafka, and PostgreSQL:

```bash
docker compose up --build
```

The API is available at `http://localhost:8080`. Provide a JWT from an OAuth2/OIDC issuer configured by `JWT_ISSUER_URI` and `JWT_JWK_SET_URI`. The token needs `messages.write` to publish and `messages.read` to list processed events. Local demo credentials for PostgreSQL are defined in Compose; do not reuse them outside development.

Publish an event with a client-generated UUID so retries can be idempotent:

```bash
curl -i -X POST http://localhost:8080/api/v1/events \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"d3d7d41a-6a9c-49d7-8f5e-4c6a2f2f0c01","type":"customer.created","schemaVersion":"1","occurredAt":"2026-10-23T12:00:00Z","payload":{"customerId":"cust-42"}}'
```

List processed events with `GET /api/messages?page=0&size=50`. Results are ordered by receive time, newest first. `GET /actuator/health` and its Kubernetes liveness/readiness groups are public; message endpoints require the respective scope.

## Configuration

Defaults are in `src/main/resources/application.yml`. Environment variables override broker endpoints, database credentials, topic names, batch size, retries/backoff, publication timeout, JWT issuer/JWK endpoint, and message retention. In Kubernetes, apply `k8s/configmap.yaml`, create the `message-service-secrets` Secret with the keys shown in `k8s/deployment.yaml`, then apply the Deployment and Service. Kafka and PostgreSQL are external to that deployment.

Build the image with `docker build -t message-service:local .`, then apply the Kubernetes files from `k8s/` to a cluster that can reach the configured services.

## Tests and coverage

Unit tests run with `mvn test`. Integration tests use Testcontainers and require Docker. Run the full verification and generate the JaCoCo HTML report with:

```bash
mvn verify
```

The report is written to `target/site/jacoco/index.html`.
