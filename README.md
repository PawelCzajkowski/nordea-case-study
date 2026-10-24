# Kafka batch message service

Java 21 and Spring Boot service for publishing versioned JSON Events over REST, processing Kafka records in batches, and exposing successfully processed messages from PostgreSQL.

## Run locally

Docker Compose starts the application, Kafka, and PostgreSQL:

```bash
docker compose up --build
```

The API is available at `http://localhost:8080`. Set `JWT_ISSUER_URI` and `JWT_JWK_SET_URI` to an OAuth2/OIDC issuer that provides tokens with the `messages.write` and `messages.read` scopes. For example, export a suitable token as `TOKEN`, then publish an Event with a client-generated UUID:

```bash
curl -i -X POST http://localhost:8080/api/v1/events \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"messageId":"d3d7d41a-6a9c-49d7-8f5e-4c6a2f2f0c01","type":"customer.created","schemaVersion":"1","occurredAt":"2026-10-23T12:00:00Z","payload":{"customerId":"cust-42"}}'
```

Publishing returns `202 Accepted` after Kafka confirms the record. Retrieve the processed Event with `GET /api/v1/messages?page=0&size=50`; results are ordered newest first. The token needs `messages.read` for retrieval. Public health and probe endpoints are available at `/actuator/health`, `/actuator/health/liveness`, and `/actuator/health/readiness`.

Compose uses `local_dev_only` as a development-only PostgreSQL password. Override it with `DATABASE_PASSWORD` if desired; never reuse the demo credential outside local development.

### Local JWT mock

For manual API testing without an OAuth2/OIDC provider, activate the `local` Spring profile. In IntelliJ's application run configuration, set **Active profiles** to `local`, or add `SPRING_PROFILES_ACTIVE=local` to the environment variables. Set `DATABASE_PASSWORD` to the password used by the local PostgreSQL instance; when using Compose, use its configured password. Alternatively, with Java 21 and `DATABASE_PASSWORD` exported:

```bash
SPRING_PROFILES_ACTIVE=local mvn spring-boot:run
```

This profile accepts the fixed bearer token `local-dev-token` as `local-developer` with the `messages.read` and `messages.write` scopes:

```bash
export TOKEN=local-dev-token
curl -i http://localhost:8080/api/v1/messages \
  -H "Authorization: Bearer $TOKEN"
```

The publishing example above works with the same token. Missing or different tokens receive `401 Unauthorized`. The local token is a mock, not a signed JWT; the mock decoder does not contact the configured issuer or JWK endpoint. Enable this profile only for local development. Without `local`, the application uses normal JWT validation. Kafka and PostgreSQL must still be running.

## Configuration

Defaults are in `src/main/resources/application.yml`. Environment variables override the Kafka broker, topic, consumer group, batch size, acknowledgement timeout, retry/DLQ settings, PostgreSQL URL and credentials, JWT issuer/JWK endpoint, and processed-message retention. `DATABASE_PASSWORD` is required and has no application default; Compose supplies it for local development, and Kubernetes reads it from the referenced Secret. Values are read at application start, so environment-specific settings can change without rebuilding the image.

PostgreSQL rounds `occurredAt` to microseconds in the read model. Identity fingerprints retain the original nanoseconds, so identical retries remain accepted while changes to the original timestamp still conflict. Kafka values that cannot be deserialized go directly to the dead-letter topic with `event: null`, the original key in `rawRecord.key`, and the original bytes encoded as Base64 in `rawRecord.value`. Failure metadata includes the source topic, partition, and offset. A Kafka tombstone has a null raw value. Other Events in the batch continue processing; the batch is acknowledged only after every outcome is durably stored or its dead-letter publication is confirmed.

## Kubernetes deployment

Kubernetes deploys only the application; Kafka and PostgreSQL must be provisioned separately and reachable from the cluster. The sample ConfigMap contains placeholder service and issuer addresses. Edit `k8s/configmap.yaml` for the target environment, then build and publish or load the image so the cluster can pull it:

```bash
docker build -t message-service:local .
```

Create the pre-existing Secret referenced by the Deployment using values from your secret manager (do not commit credentials):

```bash
kubectl create secret generic message-service-secrets \
  --from-literal=DATABASE_USERNAME='<database-user>' \
  --from-literal=DATABASE_PASSWORD='<database-password>'
```

Apply the non-secret configuration and application resources:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml -f k8s/service.yaml
kubectl rollout status deployment/message-service
```

The Deployment uses the public readiness and liveness health groups. Kafka and PostgreSQL are external dependencies, not Kubernetes workloads in this project. After changing a ConfigMap value, apply it and restart the Deployment so the new environment is loaded:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl rollout restart deployment/message-service
```

## Tests and coverage

Run unit tests with `mvn test`. Integration tests use Testcontainers and require Docker. Full verification also generates a local JaCoCo HTML report:

```bash
mvn verify
```

The report is written to `target/site/jacoco/index.html` and is not uploaded by the build.
