# orders

A demo service for the [service registry POC](https://github.com/mcafeelabs/services).

The entry service. `POST /orders` charges through payments, writes to its own postgres (an isolated, dynamic output) and publishes `orders.created`. It consumes `stock.changed`, and `GET /events` lists what this copy handled.

## Files

- `service.yaml`: what orders provides and consumes. This repo is the source of truth.
- `config-values.schema.yaml`: JSON Schema for its config. Fields marked `x-ref` only accept the listed reference kinds.
- `main.go`: built on [`servicekit`](https://github.com/mcafeelabs/platform/tree/main/pkg/servicekit). The kit reads `CONFIG_FILE` and forwards the `sandbox` baggage on every HTTP and NATS hop.

## CI

1. Runs `go test` and `svcreg lint-service` (manifest and schema checked against the chart's `capabilities.yaml`).
2. Pushes the image `ghcr.io/mcafeelabs/orders`: `0.1.<run>` on main (Kargo promotes semver tags) and `pr-<n>` on same-repo PRs (for sandboxes).
3. On main, opens a PR to `mcafeelabs/services` with `service.yaml`, the schema and the new image tag for directly delivered environments. That PR auto-merges once its checks pass.

   This step needs a `SERVICES_REPO_TOKEN` secret with contents and pull-request write access to `mcafeelabs/services`. Without it, the step is skipped with a notice.

## Run locally

```sh
go test ./...
SERVICE_NAME=orders CONFIG_FILE=config.yaml go run .
```
