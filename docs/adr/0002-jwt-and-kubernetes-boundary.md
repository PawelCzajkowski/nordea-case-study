# External identity and infrastructure for Kubernetes

The REST API validates JWTs issued by an external OAuth2/OIDC provider, and authorization uses separate read and write scopes. Kubernetes manifests deploy only the application and reference externally managed Kafka and PostgreSQL services; local development uses Docker Compose for those dependencies. This keeps credentials and stateful infrastructure outside the application image while allowing the same application configuration to run in both environments.
