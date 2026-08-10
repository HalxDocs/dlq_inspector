# Contributing to DLQ Inspector

Thanks for helping build DLQ Inspector. This project is part of the backend
toolkit (Reqit · Barrage · DLQ Inspector) and follows a few simple conventions
that keep the history readable and the safety pipeline auditable.

## Development setup

Requirements:

- Go 1.26+ (see `go.mod`)
- `make` (optional, wraps the common commands)
- For the integration suites: a RabbitMQ and/or Redis to test against

```bash
make build      # builds bin/dlq
make test       # unit tests (no broker needed)
make vet        # go vet ./...
```

The integration suites (`internal/broker/rabbitmq`, `internal/broker/redisstream`)
run against real brokers and are skipped when no broker is reachable. Point them
at a live instance to exercise the full recovery loop:

```bash
export DLQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/
export DLQ_TEST_REDIS_URL=redis://localhost:6379/0
make test
```

The easiest way to get brokers locally is `docker compose up -d` (RabbitMQ 3.13
management + Redis 7), then follow [docs/TESTING.md](docs/TESTING.md) — it walks
through the full workflow with real terminal captures.

## The architecture in one paragraph

A strict layered CLI. `internal/command` parses flags and applies the safety
gate; `internal/recovery` analyzes, classifies, plans, validates, and executes —
**broker-agnostic**; `internal/broker` is the `Broker` interface plus adapters.
No broker-specific type ever crosses out of its adapter package. Everything
mutating flows through the safety pipeline (dry-run -> duplicate check ->
destination invariant -> confirm -> limits -> audit + snapshots). See
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full contract.

## Before you open a PR

1. **Run the full check locally:**
   ```bash
   gofmt -l . && go vet ./... && go test -timeout 150s ./...
   ```
   With live brokers set (above), this also runs the conformance and recovery
   integration suites — the real gate for adapter work.

2. **Follow the safety rules.** New mutating commands must flow through the
   existing safety gate — never add a publish/delete/ack path that bypasses it.
   New recovery logic stays broker-agnostic and operates only on
   `message.Message` and the `Broker` interface.

3. **Test the behavior, not just the happy path.** New safety features need the
   negative case: the dry-run that refuses, the confirm that skips, the circuit
   breaker that halts. If it touches a broker, cover it with a live test.

4. **Keep commits atomic and conventional.** One logical change per commit, with
   a `type(scope): summary` message (`feat`, `fix`, `docs`, `test`, `ci`, `refactor`).
   Never include a co-author footer or sign as a contributor on behalf of others.

## Reporting bugs

Open an issue with:

- The exact command and flags that failed
- Broker type and version (e.g. RabbitMQ 3.13, Redis 7)
- What you expected vs. what happened
- Anything from `dlq history` that's relevant (this tool is itself the audit trail)

## Asking questions

Start a discussion for design questions before large changes — the recovery
engine and the safety pipeline are intentionally conservative, and a change that
weakens a safeguard will be questioned even if the tests pass.
