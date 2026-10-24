## Agent skills

### Issue tracker

Issues and specs are tracked in GitHub Issues. See `docs/agents/issue-tracker.md`.

### Domain docs

This repository uses the single-context domain-doc layout. See `docs/agents/domain.md`.

## Project requirements

- Use Java 21 for development and builds. Configure the build to target Java 21.
- Use the Spring framework for application components.
- Keep the project runnable on Kubernetes. Include and maintain the Kubernetes deployment configuration needed to build/run the application, and keep environment-specific settings outside the application artifact.
