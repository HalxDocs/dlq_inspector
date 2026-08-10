# DLQ Inspector — Launch Announcement Pack

Two ready-to-publish pieces: a short pitch (for the repo description, a
Show-HN-style post, or a tweet thread) and the v1.0.0 GitHub release post.

---

## 1. The short pitch

> **Don't blindly replay a dead-letter queue. Understand what failed, decide
> what's safe to recover, preview it, and only then execute — with controlled
> safeguards and a full audit trail.**
>
> DLQ Inspector is a local-first CLI for inspecting, analyzing, classifying, and
> safely recovering failed messages from dead-letter queues. RabbitMQ and Redis
> Streams today; Kafka and SQS planned. One binary, no runtime dependencies, no
> account, no web dashboard — it connects straight to your message
> infrastructure and gives you one consistent workflow:
>
> `Inspect → Analyze → Classify → Plan → Validate → Dry-run → Recover → Audit`
>
> A DLQ full of messages doesn't tell you which failures share a cause or which
> messages are safe to touch. DLQ Inspector groups failures into patterns,
> classifies each one (REPLAYABLE / REQUIRES_FIX / DO_NOT_REPLAY /
> INVESTIGATE), and never mutates a queue outside a single safety pipeline:
> dry-run by default, duplicate-evidence checks, a destination invariant,
> explicit confirm, rate limits with a circuit breaker, and a local SQLite
> audit trail with snapshots that make every confirmed recovery reversible.
>
> It completes a backend developer-tooling trio — the same philosophy as
> **Reqit** (build/test APIs) and **Barrage** (load testing):
>
> - **Reqit** → build and test APIs
> - **Barrage** → load-test systems under load
> - **DLQ Inspector** → investigate failures and recover safely
>
> ```bash
> # No Go required — one command, checksum-verified
> curl -fsSL https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.sh | bash
> dlq connect rabbitmq --url-env RABBITMQ_URL --default-queue orders-dlq
> dlq analyze orders-dlq
> dlq plan orders-dlq --group payment-timeout --output-file recovery.json
> dlq recover --plan recovery.json              # dry run: Changes made: NONE
> dlq recover --plan recovery.json --confirm --batch-size 25 --rate-limit 10/s
> dlq history --plan <plan-id>
> ```
>
> Think `htop` or `kubectl`, but for failed asynchronous work.

---

## 2. v1.0.0 release post (GitHub release body)

```
# DLQ Inspector v1.0.0

A local-first CLI for inspecting, analyzing, classifying, and **safely**
recovering failed messages from dead-letter queues.

Don't blindly replay a DLQ. DLQ Inspector connects to your message
infrastructure, reasons about which failures share a cause and which messages
are safe to touch, previews every recovery, and only executes with controlled
safeguards — writing everything to a local audit trail you can roll back.

## The workflow

    Inspect → Analyze → Classify → Plan → Validate → Dry-run → Recover → Audit

## What's in v1.0.0

### Brokers
- **RabbitMQ** — production-ready: queues, DLQ inspection, recovery, live conformance-tested
- **Redis Streams** — the same workflow and recovery engine over consumer groups, including per-group pending (PEL) visibility
- Kafka and SQS are planned; the shared `Broker` contract and conformance suite mean new adapters plug in without touching the recovery engine

### The recovery engine
- `dlq analyze` — groups failures into patterns with normalized error signatures and classifies each group (REPLAYABLE / REQUIRES_FIX / DO_NOT_REPLAY / INVESTIGATE). INVESTIGATE is the honest default when signals are missing
- `dlq plan` — exports a reviewable RecoveryPlan JSON (messages, destination, action, limits, safety checks)
- `dlq recover` — dry-run by default, then confirmed batch execution with batch size, rate limit, concurrency cap, and a circuit breaker that pauses a run whose failure rate crosses 20%
- `dlq replay` / `dlq patch` — single-message safety: dry-run, `--set` payload patching with a before/after diff, then explicit confirm
- `dlq rollback` — every confirmed recovery is snapshotted; a bad replay moves back to the DLQ with a reason

### Safety, by design
1. **Dry-run preview** — the default for every mutating command; `Changes made: NONE`
2. **Duplicate evidence** — prior replays, event IDs, and `x-duplicate-of` headers surfaced before you confirm; DO_NOT_REPLAY messages excluded from plans
3. **Destination invariant** — a confirmed run refuses if the destination queue doesn't exist; a large pending backlog is a dry-run warning
4. **Explicit confirm** — batch recovery and replay require `--confirm`
5. **Limits** — batch size, rate limit, concurrency cap, circuit breaker
6. **Audit + snapshots** — every dry-run and action lands in a local SQLite audit trail; `dlq history` is your postmortem source of truth

### Policies
- Team judgment as committed YAML: `dlq policy validate` (CI-safe) and `dlq policy apply` — rules override the classifier defaults

### Operations
- `dlq self-update` — downloads the release binary for your platform, verifies it against `checksums.txt`, and swaps it in
- JSON output everywhere (`--output json`) for scripting
- `dlq stats` / `dlq queues` — DLQ depth, age, retry distribution, pending counts

## Install

**No Go required.** One command downloads the latest release for your platform,
verifies it against `checksums.txt`, and installs it:

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.sh | bash
# Windows
irm https://raw.githubusercontent.com/HalxDocs/dlq_inspector/main/scripts/install.ps1 | iex
```

Or grab the static binary for your platform below — no runtime dependencies, no
cgo. Upgrade later with `dlq self-update --confirm`.

## Docs

- Hands-on walkthrough with real terminal captures: docs/TESTING.md
- Architecture and the Broker contract: docs/ARCHITECTURE.md
- Command reference: docs/COMMANDS.md
- Policy YAML grammar: docs/POLICIES.md

## Assets
- linux/amd64, linux/arm64 (tar.gz)
- darwin/amd64, darwin/arm64 (tar.gz)
- windows/amd64, windows/arm64 (zip)
- checksums.txt — verify any download against this

## A note on trust
DLQ Inspector never claims it can perfectly judge business-level safety. It
classifies, warns, and previews; the operator decides. The tool's own audit
trail is the record of what it changed, why, and when.
```
