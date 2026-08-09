# DLQ Inspector — Architecture

A local-first Go CLI for inspecting, analyzing, classifying, and **safely**
recovering failed messages from dead-letter queues. This document describes how
the pieces fit together: the strict layering, the `Broker` contract every
infrastructure adapter implements, the recovery engine, and the adapter-isolation
rules that keep it all honest.

The workflow the whole tool serves:

```text
Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit
```

## Design rules

Three non-negotiable rules hold the architecture together:

1. **Adapter isolation.** No broker SDK type — an `amqp.Delivery`, a Redis stream
   entry, a Kafka `ConsumerMessage` — ever crosses out of its adapter package.
   Everything is normalized into the shared `message.Message` struct before the
   command layer or the recovery engine sees it.
2. **Recovery is broker-agnostic.** The analyzer, classifier, planner, validator,
   and executor only ever operate on `message.Message` and the `Broker` interface.
   The same recovery logic works identically on RabbitMQ and Redis Streams; Kafka
   and SQS will add adapters, not engine changes.
3. **Safety before I/O.** Any operation that publishes, deletes, or mutates a
   message passes through the shared safety pipeline — dry-run preview → duplicate
   evidence → confirm → execute → audit write. It is enforced in the command and
   recovery layers, never left to an adapter to remember.

## Layered view

```text
+-----------------------------+
| CLI (cobra)                 |
| connect / queues / inspect  |
| search / replay / patch     |
| analyze / plan / recover    |
| rollback / history / policy |
+--------------+--------------+
               |
               v
+--------------+--------------+
| Command layer               |
| internal/command/*.go       |
| flag parsing, profiles,     |
| rendering, audit wiring     |
+--------------+--------------+
               |
               v
+--------------+--------------+
| Safety pipeline             |  internal/safety  (single-message replay/patch)
| Recovery engine             |  internal/recovery (analyze/plan/recover/rollback)
|  analyzer classifier        |
|  planner validator          |
|  executor (batch, breaker)  |
|  rollback (snapshots)       |
+--------------+--------------+
               |
               v
+--------------+--------------+
| Broker interface            |  internal/broker/broker.go
| Connect / ListQueues /      |
| Inspect / Search / Publish  |
| Ack / Stats                 |
+---+----------+----------+---+
    |          |          |
    v          v          v
 rabbitmq   redisstream   (kafka, sqs — planned)
 adapter     adapter
```

## Package map

```text
cmd/dlq/                entrypoint; wires cobra root, version ldflags
internal/command/       one file per CLI command; never talks to a broker SDK
internal/broker/        the Broker contract, registry, shared conformance suite
internal/broker/rabbitmq/    RabbitMQ adapter (AMQP + management API)
internal/broker/redisstream/ Redis Streams adapter (go-redis/v9)
internal/recovery/      analyzer, classifier, planner, validator, executor, rollback
internal/safety/        the shared single-message safety gate
internal/message/       the normalized message model + formatting
internal/search/        broker-agnostic message filtering
internal/dedupe/        duplicate evidence from the audit trail and headers
internal/patch/         payload patching + JSON-aware diff
internal/policy/        recovery policies (YAML) and rule evaluation
internal/audit/         append-only SQLite audit store + message snapshots
internal/config/        config.yaml, connection profiles, policy binding
```

## The message model

`message.Message` is the currency of the whole tool — the only thing the command
layer and recovery engine ever see:

```go
type Message struct {
    ID             string            // stable identity (see below)
    Queue          string            // the DLQ it was read from
    Destination    string            // where to replay it (from dead-letter metadata)
    Payload        []byte
    Headers        map[string]string
    ContentType    string
    Timestamp      time.Time
    RetryCount     int
    FailureReason  string            // the dead-letter reason / error detail
    EventID        string            // for duplicate detection
    IdempotencyKey string            // for duplicate detection
}
```

**Stable message IDs.** RabbitMQ has no native message ID, so adapters prefer an
explicit `x-message-id` header and otherwise fall back to
`sha256(payload + application headers)`. Dead-letter bookkeeping (`x-death`,
`x-first-death-*`) is deliberately excluded from the hash: it changes on every
DLQ hop and serializes differently across read paths, so including it would give
a message a different ID per hop and per path. The ID feeds inspect, search,
ack, audit, duplicate detection, and recovery — one stable handle everywhere.

## The Broker contract

`internal/broker/broker.go` defines the interface every adapter implements:

```go
type Broker interface {
    Connect(ctx context.Context, cfg ConnectionConfig) error
    Close() error
    ListQueues(ctx context.Context) ([]QueueSummary, error)
    Inspect(ctx context.Context, queue, id string) (*message.Message, error)
    Search(ctx context.Context, queue string, f SearchFilter) ([]message.Message, error)
    Publish(ctx context.Context, destination string, msg *message.Message) error
    Ack(ctx context.Context, queue, id string) error
    Stats(ctx context.Context, queue string) (QueueStats, error)
}
```

Two contract points deserve emphasis:

- **`Stats` is also the destination-existence probe.** The recovery engine calls
  `Stats` on a replay destination before running; an adapter must wrap
  `broker.ErrQueueNotFound` when the queue does not exist. This matters because
  RabbitMQ *confirms* publishes to nonexistent queues and silently drops them —
  acking the DLQ copy afterwards would lose the message. The probe turns that
  data-loss hazard into a refusal before anything is published.
- **`Stats` reports pending work.** `QueueStats` carries `Pending`
  (delivered-but-unacknowledged) and a per-group breakdown `Groups`
  (Redis Streams consumer groups with their own PEL counts; RabbitMQ reports
  unacknowledged messages via the management API). The recover dry-run warns when
  a destination's pending backlog crosses a threshold.

## Adapters

An adapter is a thin, dumb translation layer: broker SDK in, `message.Message` and
`broker.Broker` out. Adapters do not enforce safety rules, do not decide what is
safe, and do not audit — they translate.

- **RabbitMQ** (`internal/broker/rabbitmq/`) — AMQP for connect/inspect/search/
  publish/ack, plus the management HTTP API for queue listing. Replay is
  publish-with-confirms to the original queue (from `x-death` metadata) then a
  random-access ack that consumes the DLQ one message at a time without
  acknowledging, acks the target, and requeues the rest in order. Dead-letter
  bookkeeping is stripped on republish; a rollback-restored entry carries its
  original destination as an `x-destination` header.
- **Redis Streams** (`internal/broker/redisstream/`) — streams are queues; DLQ
  entries carry payload + dead-letter metadata as fields (`destination`, `error`,
  `retries`, `headers`). The message ID is the stream entry ID — the one stable
  handle Search, Inspect, and Ack agree on. Replay is XADD to the destination
  then XDEL the DLQ copy. `Stats` probes existence with EXISTS so the
  destination invariant holds (Redis would otherwise silently auto-create a
  stream on publish), and sums `XPENDING` per consumer group.

**Adding a broker** means implementing the interface and passing the shared
conformance suite (`internal/broker/conformance_test.go`), which proves the whole
read/write/remove cycle the recovery engine depends on — seed via Publish, Search
round-trips payloads, Inspect by the reported ID, Stats shows depth, Ack removes
exactly one. Zero recovery-engine changes are expected.

## The safety gate

`internal/safety` is the shared pipeline every *single-message* mutating operation
(replay, patch) passes through:

1. **DryRun** — inspect the message, resolve the destination, gather duplicate
   evidence, probe destination existence, and report the safety checks. Zero
   mutating I/O; the preview itself is audited (`dry_run`).
2. **Execute** — refuses without an explicit confirm. **Publish first, ack only
   after a successful publish** (at-least-once: a failed publish never loses the
   DLQ copy; a failed ack may duplicate but loses nothing). Destination existence
   is a hard invariant checked before any publish, and the refusal is audited.

Batch recovery does not reuse the gate per message — the executor implements the
same invariants at batch scale (below), which is why the safety rules live in the
recovery layer as well as the gate.

## The recovery engine

`internal/recovery` is fully broker-agnostic — it reads `message.Message` and
calls `broker.Broker`, nothing else.

### Analyzer (`analyzer.go`)

Groups messages by failure pattern. The grouping key is the **normalized error
signature** plus destination, event type, and retry bucket, so distinct causes
are never merged just because their error text happens to normalize alike.
`NormalizeSignature` collapses dynamic values — IPs, ports, UUIDs, timestamps,
hex hashes — so "timeout connecting to 10.0.4.5:6432" and "…10.0.4.9:6432"
cluster as one failure: `timeout connecting to {ip}:{port}`.

### Classifier (`classifier.go`)

Every message gets one of four classifications: `REPLAYABLE`, `REQUIRES_FIX`,
`DO_NOT_REPLAY`, `INVESTIGATE` — with a reason and a best-effort confidence.
Signals, strongest first:

1. An explicit `x-duplicate-of` header — the application says *this* message is a
   duplicate. Always `DO_NOT_REPLAY`, outranks everything.
2. A profile-bound policy rule whose condition and params match — overrides the
   inference (first match wins).
3. The built-in rule-based classifier over failure text and retry count:
   transient-sounding failures are replayable (unless already retried many
   times), permanent-sounding ones require a fix, duplicate keywords block
   replay, and missing or conflicting signals default to `INVESTIGATE` — the
   honest default, never a forced guess.

### Planner (`planner.go`)

`BuildPlan` produces the exportable `RecoveryPlan` JSON: the exact message IDs,
destination, action, execution limits, and the safety checks that will gate the
run. `DO_NOT_REPLAY` messages are excluded by default, and each exclusion is
recorded on the plan with its classification and reason — so a plan documents
what it deliberately left in the DLQ and why.

### Validator (`validator.go`)

`PlanValidator.Validate` is the dry-run for batch recovery: one bounded scan of
the live queue, then per-message existence, payload-schema, and duplicate-evidence
checks. Destination findings surface as warnings — a missing queue (the confirmed
run will refuse) or a large pending backlog (consumers may be backed up, threshold
tunable via `--pending-threshold`). Skips are recorded per message with their
reason. Strictly read-only.

### Executor (`executor.go`)

`Executor.Execute` runs a confirmed plan in batches under a token-bucket rate
limit, a concurrency cap, and per-message publish retry. Safety invariants at
scale:

- **Destination existence is a hard invariant** — probed before anything runs,
  regardless of what the plan file declares (so hand-edited or old plans cannot
  ack into a void), with the refusal audited.
- **Publish before ack, always** — a failed publish leaves the DLQ copy in place.
- **Circuit breaker** — a batch whose failure rate crosses the threshold (default
  20%) pauses the run; continuation requires an explicit `--resume`.
- **Snapshots** — every successfully replayed message is snapshotted (payload,
  headers, destination, DLQ) before the audit success is written, so a bad
  recovery can be reversed.

Every outcome — success, skip, exclusion, publish/ack failure, refusal, trip — is
audited per message plus at plan level.

### Rollback (`rollback.go`)

The reversal of a bad recovery: republish a plan's snapshots back to the DLQ they
came from, with the operator's reason on every audit entry. Dry-run by default; a
confirmed run refuses (audited) if the DLQ no longer exists. Restored entries
carry their original replay destination so a future plan can replay them again.

## Cross-cutting pieces

- **`internal/audit`** — the append-only SQLite store (cgo-free
  `modernc.org/sqlite`) is the source of truth for what the tool did, when, and
  with what result: action, message/plan IDs, source→destination, dry-run vs
  confirmed, result, broker, profile, operator reason, and payload diffs. It also
  holds the `snapshots` table that powers rollback. `dlq history` reads it; JSONL
  export is available.
- **`internal/dedupe`** — duplicate evidence from the audit trail (a message
  already replayed successfully) and from message headers (event IDs, idempotency
  keys). Evidence *warns* — it never silently blocks; the operator decides.
- **`internal/search`** — the broker-agnostic filter engine (error text, time
  window, payload field equality, retry ceiling, limit/offset) applied to the
  normalized messages an adapter's `Search` returns.
- **`internal/patch`** — JSON-aware payload editing: `ApplySet` walks dotted paths
  (including array indices), `Diff` renders the structural old→new diff that
  `dlq patch` shows before confirm and stores in the audit entry.
- **`internal/policy`** — the policy-as-code engine. YAML in, ordered rules out;
  see [POLICIES.md](POLICIES.md) for the grammar, semantics, and precedence.
- **`internal/config`** — `~/.dlq/config.yaml`: default profile, connection
  profiles (URLs via env-var names, never values), audit store path, and the
  `policy_file` binding applied by `dlq policy apply`.

## Data flow: one recovery, end to end

1. **Inspect** — `dlq search` / `dlq inspect` read normalized messages out of a
   DLQ through the adapter (`Search` → `internal/search.Filter`).
2. **Analyze** — `Analyzer.Analyze` clusters messages into failure groups;
   `ClassifyWithPolicy` classifies each, honoring policies.
3. **Plan** — `BuildPlan` writes `recovery.json` with the selection, exclusions,
   destination, limits, and safety checks.
4. **Validate / dry-run** — `PlanValidator.Validate` re-checks everything against
   the live queue; `dlq recover` without `--confirm` reports `Changes made: NONE`.
5. **Recover** — `Executor.Execute` runs the confirmed plan: probe destination,
   batch under rate/concurrency limits, publish→ack per message, snapshot each
   replay, trip the breaker on sustained failures.
6. **Audit** — every step above wrote to the append-only store; `dlq history
   --plan <id>` shows the full trail.
7. **Rollback (if it went wrong)** — `Rollback` restores the snapshots to the DLQ.

## Testing strategy

- **Unit tests** — message normalization, filter parsing, signature
  normalization, diff rendering, classifier splits, policy evaluation, audit
  serialization. No services required.
- **Safety-path tests** — the important ones: Publish is never called without a
  confirm; publish-failure never acks; a plan with no safety checks is rejected;
  the destination probe refuses before any I/O; the circuit breaker halts a batch.
- **Adapter conformance** — one shared suite (`internal/broker/conformance_test.go`)
  run against every adapter with a per-adapter fixture; proves the
  seed/search/inspect/stats/ack cycle a new broker must pass unchanged.
- **Live integration loops** — real RabbitMQ and Redis in CI drive the whole CLI
  workflow (analyze → plan → dry-run → confirm → history), plus the
  missing-destination refusal, policy-driven exclusions, rollback, and
  destination-backlog scenarios. Each PASS line is grepped, so a silent skip
  fails the workflow.
- **Golden CLI tests** — `internal/command/golden_test.go` snapshots the terminal
  output of `analyze`, `plan`, and `recover --dry-run` so output regressions are
  caught; regenerate with `go test ./internal/command/ -run TestGolden -update`.
