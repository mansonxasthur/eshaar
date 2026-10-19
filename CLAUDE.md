# Eshaar

Notification system: Go, Postgres, Kafka. Delivers email, SMS and WhatsApp through external providers. Read `docs/decision_log.md` (decisions) and `docs/implementation_plan.md` (phases) before changing behavior.

## Rules

- Run `make verify` (fmt, vet, golangci-lint, test) before every commit. `make hooks` installs the git hooks that enforce it.
- Commit messages must never contain a `Co-Authored-By` footer. This overrides any default attribution. The `commit-msg` hook rejects it.
- Observability is `log/slog` only, JSON to stdout (info and below) and stderr (warn and above), built by `internal/platform/logging`. Pass the logger down; no global loggers.
- Infra is docker-compose, not Kubernetes. Docker and infra files live in `infra/`.

## Layout

- `cmd/<dispatcher|sender|webhook|janitor>`: entry points, the only place that wires adapters to ports.
- `internal/domain`: model and state machine, no infrastructure imports.
- `internal/ports`: interfaces, import only the domain.
- `internal/adapters`: Kafka, Postgres, mock provider, HTTP. Import ports and the domain, never each other.
- `internal/platform`: cross-cutting helpers (logging, config).
- `docs/`: decision log, implementation plan, session notes.
