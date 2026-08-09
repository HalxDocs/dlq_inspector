# DLQ Inspector

A local-first CLI for inspecting, analyzing, classifying, and **safely** recovering
failed messages from dead-letter queues.

Part of the backend toolkit: **Reqit** (build/test APIs) · **Barrage** (load testing) ·
**DLQ Inspector** (failure investigation + safe recovery).

```text
Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit
```

DLQ Inspector doesn't blindly replay a dead-letter queue. It connects to your message
infrastructure — RabbitMQ today, Redis Streams too — and reasons about which failures
share a cause, what is safe to replay, and what needs a fix first. Every mutating
operation flows through one safety pipeline, and everything it does lands in a local
audit trail. Think `htop` or `kubectl`, but for failed asynchronous work.

## Brokers

| Broker | Status |
|--------|--------|
| RabbitMQ | ✅ Production-ready — queues, DLQ inspection, recovery, conformance-tested live |
| Redis Streams | ✅ Supported — same workflow, same recovery engine, conformance-tested live |
| Kafka, SQS | Planned |

## Install

```bash
# From source (Go 1.21+)
go install github.com/HalxDocs/dlq_inspector/cmd/dlq@latest

# Or build from a checkout
make build          # produces bin/dlq
```

Or download the static binary for your platform from a
[release](https://github.com/HalxDocs/dlq_inspector/releases) — no runtime
dependencies, no cgo.

## Quickstart

Point the tool at your broker once, then work a real DLQ end to end:

```bash
# 1. Save a connection profile (URL stays in your shell's env, not the config file)
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
dlq connect rabbitmq --url-env RABBITMQ_URL --default-queue orders-dlq --profile prod
dlq profiles list

# 2. See the damage
dlq queues
dlq stats orders-dlq
dlq search orders-dlq --error timeout --since 2h
dlq inspect orders-dlq --id sha256:1a2b...

# 3. Understand the failure
dlq analyze orders-dlq
#   GROUP 1 -- Payment timeout      301 msgs (62.4%)  -> REPLAYABLE
#   GROUP 2 -- Invalid customer_id   97 msgs (20.1%)  -> REQUIRES_FIX
#   GROUP 3 -- Duplicate event       29 msgs ( 6.0%)  -> DO_NOT_REPLAY

# 4. Plan the recovery, then preview it — nothing moves without --confirm
dlq plan orders-dlq --group <group-id> --output-file recovery.json
dlq recover --plan recovery.json          # dry run: Changes made: NONE

# 5. Execute with limits, then audit
dlq recover --plan recovery.json --confirm --batch-size 25 --rate-limit 10/s
dlq history --plan <plan-id>

# 6. Changed your mind? Every confirmed recovery is snapshotted — roll it back
dlq rollback --plan <plan-id> --dry-run
dlq rollback --plan <plan-id> --confirm --reason "replay caused downstream errors"
```

Single messages can be replayed (or patched first) with the same safety gate:

```bash
dlq replay orders-dlq --id sha256:1a2b...                  # dry run
dlq replay orders-dlq --id sha256:1a2b... --confirm
dlq patch orders-dlq --id sha256:1a2b... --set customer_id=443   # diff first, then --confirm
```

## Safety model

DLQ Inspector never mutates a queue outside the shared safety pipeline:

1. **Dry-run preview** — the default for every mutating command; `Changes made: NONE`.
2. **Duplicate evidence** — prior replays, event IDs, and `x-duplicate-of` headers are
   surfaced before you confirm; DO_NOT_REPLAY messages are excluded from plans.
3. **Destination invariant** — a confirmed run refuses if the destination queue does
   not exist (a publish into a nonexistent queue can be confirmed and silently
   dropped); a large pending backlog on the destination is a dry-run warning.
4. **Explicit confirm** — batch recovery and replay require `--confirm`.
5. **Limits** — batch size, rate limit, concurrency cap, and a circuit breaker that
   pauses a run whose failure rate crosses 20% until you explicitly `--resume`.
6. **Audit + snapshots** — every dry-run and action is written to a local SQLite
   audit trail; every confirmed replay is snapshotted so `dlq rollback` can reverse it.

The classifier is honest by design: `INVESTIGATE` is the default when signals are
missing or conflict, and teams can encode their own judgment as committed YAML
[policies](docs/POLICIES.md) that override the defaults.

## Documentation

- [docs/COMMANDS.md](docs/COMMANDS.md) — full command reference
- [docs/POLICIES.md](docs/POLICIES.md) — recovery policy YAML grammar and CI usage
- [docs/PLAN.md](docs/PLAN.md) — architecture rationale and the phased build plan

## License

[MIT](LICENSE)
