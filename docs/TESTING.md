# Testing DLQ Inspector

Two ways to test the tool:

1. **Hands-on** — run real brokers, seed a dead-letter queue with failing
   messages, and drive the full workflow (`analyze` → `plan` → `recover` →
   `history` → `rollback`) against them. This is the fastest way to *see*
   what the tool does, and every screenshot in this guide is a capture of
   that exact workflow run against real brokers.
2. **Automated** — run the unit suite plus the live-broker integration suites
   (conformance + recovery loops) with the environment variables below.

Every command in this guide was executed for real against RabbitMQ and Redis
Streams. The screenshots are unedited captures of that run; message IDs,
plan IDs, and timestamps vary from run to run, but the structure you see is
exactly what the tool prints.

---

## 1. Prerequisites

| Requirement | Check it | Notes |
|-------------|----------|-------|
| Go 1.26+ | `go version` | go.mod declares `go 1.26.5` |
| Docker (compose plugin) | `docker compose version` | Only needed for brokers; point at an existing broker to skip |
| A terminal | — | All examples assume bash; PowerShell/cmd work with the same flags |

## 2. Start the brokers

From the repo root:

```bash
docker compose up -d
docker compose ps        # wait until both report "healthy"
```

This gives you:

| Broker | Endpoint | Credentials |
|--------|----------|-------------|
| RabbitMQ (AMQP) | `amqp://guest:guest@localhost:5672/` | guest / guest |
| RabbitMQ (management UI) | http://localhost:15672/ | guest / guest |
| Redis | `redis://localhost:6379/0` | — |

Already have brokers running? Skip this step and use your URLs below.

## 3. Build the CLI and the seed helper

```bash
make build                 # produces bin/dlq (see Makefile for the ldflags)
go build ./cmd/seed        # the dev-only seed helper (not in the release binary)
```

`cmd/seed` exists only to create test data. It fills a DLQ with six
realistic failing messages so the workflow has something to work on:

| Messages | Failure | Why it matters |
|----------|---------|----------------|
| 3 | `timeout connecting to upstream payment gateway` | transient → `REPLAYABLE` |
| 2 | `invalid customer_id: must be a number` | permanent → `REQUIRES_FIX` |
| 1 | `rejected` + `x-duplicate-of` header | the application itself marks it → `DO_NOT_REPLAY` |

On RabbitMQ it publishes straight into the DLQ carrying a crafted `x-death`
header — the exact shape RabbitMQ writes when it dead-letters a message — so
destinations and failure reasons resolve precisely as they would for real
failures. It is deliberately *not* part of the shipped `dlq` binary
(goreleaser builds only `./cmd/dlq`).

## 4. RabbitMQ walkthrough

### 4.1 Connect

Save a profile once. `connect` validates the URL scheme against the broker
and persists the profile — it does **not** open a connection.

```bash
export DLQ_RABBIT_URL=amqp://guest:guest@localhost:5672/
dlq connect rabbitmq --url-env DLQ_RABBIT_URL --default-queue orders-dlq --profile dev
dlq profiles list
```

`--url-env` stores the *name* of the environment variable, never the URL
itself, so connection secrets never land in the config file (`~/.dlq/config.yaml`
by default; override with `--config <path>`). The audit store lives alongside
the config at `~/.dlq/audit.db` — see Section 9.

### 4.2 Seed the DLQ

```bash
go run ./cmd/seed --source orders --dlq orders-dlq
# Seeded 6 messages into orders-dlq (rabbitmq).
```

This declares the `orders` queue (the replay destination) and `orders-dlq`,
then publishes the six failing messages into the DLQ.

### 4.3 See the damage

```bash
dlq queues
```

![dlq queues](./screenshots/01-queues.png)

The `PENDING` column shows delivered-but-unacknowledged messages — on
RabbitMQ these are in-flight deliveries (from the management API's
`messages_unacknowledged`), on Redis they are consumer-group PEL entries.

```bash
dlq stats orders-dlq
dlq search orders-dlq --error timeout --since 2h
dlq inspect orders-dlq --id sha256:<id-from-search>
```

- `stats` summarizes one queue: depth, consumers, oldest/newest age.
- `search` filters by error text (`--error`), a time window (`--since`,
  `--until`), a payload field, retry count, or event type — and prints the
  message IDs you need for `inspect`.
- `inspect` shows the full message: headers, retry count, failure reason,
  content type, and the pretty-printed JSON payload. Note the `x-death`
  header — RabbitMQ's dead-letter bookkeeping — which is where the
  destination, retry count, and failure reason come from.

### 4.4 Understand the failure

```bash
dlq analyze orders-dlq
```

![dlq analyze](./screenshots/03-analyze.png)

This is the core of the tool. Six messages, three failure groups:

1. **`Timeout connecting to …`** (3 messages, 50%) — `REPLAYABLE`. The error
   signature is normalized: `timeout connecting to upstream payment gateway
   after 30s` collapses to `after {n}`, so all three timeouts share one group
   even though their payloads differ.
2. **`Invalid customer_id must …`** (2, 33%) — `REQUIRES_FIX`. Permanent
   failures are never blindly replayed; the payload needs correction first.
3. **`Rejected`** (1, 17%) — `DO_NOT_REPLAY`. This message carries an
   `x-duplicate-of` header set by the application itself, which outranks any
   inference.

The classifier never claims certainty it does not have: when signals are
missing or conflict, the group recommendation is `INVESTIGATE`, not an
assumed answer.

### 4.5 Plan the recovery, then preview it

```bash
dlq plan orders-dlq --output-file recovery.json
```

![dlq plan](./screenshots/04-plan.png)

The plan records the selected messages, the destination, the proposed action,
the safety checks that will run, and the exclusion (`1 excluded (left in
DLQ)` — the duplicate). Plans are plain JSON you can review, diff, or store:

```bash
dlq recover --plan recovery.json          # dry run — changes nothing
```

![dlq recover --dry-run](./screenshots/05-recover-dryrun.png)

The dry run re-validates every selected message against the live broker
(`Payload validation: 5/5 passed`), lists the safety checks run
(`schema_validated, duplicate_checked, destination_checked`), and ends with
`Changes made: NONE`. Nothing was published, acked, or deleted — the dry run
is the default, and `--confirm` is the only way past it.

### 4.6 Fix one message with a patch, then replay it

The two `invalid customer_id` messages need a fix before they can be
replayed. Grab one ID from `dlq search orders-dlq --error invalid`, then:

```bash
dlq patch orders-dlq --id sha256:<id> --set customer_id=1004
```

![dlq patch --set](./screenshots/06-patch.png)

The patch goes through the same safety pipeline as a replay: the message
exists, the destination resolves and exists, the duplicate check passes, and
the payload diff is rendered for review — `customer_id` changes from
`"not-a-number"` to `1004` — before `Changes made: NONE`. Confirm to fix and
replay it:

```bash
dlq patch orders-dlq --id sha256:<id> --set customer_id=1004 --confirm
# Patched and replayed sha256:<id> -> orders
# Audit entry written (with diff).
```

The audit entry carries the diff, so `dlq history` later shows exactly what
changed.

### 4.7 Execute the recovery, then audit

```bash
dlq recover --plan recovery.json --confirm --batch-size 2 --rate-limit 10/s
```

![dlq recover --confirm](./screenshots/07-recover-confirm.png)

`Replayed: 5` (the patched message is no longer in the DLQ), `Excluded: 1`,
`Failed: 0`. The run was throttled to batches of 2 at 10 messages/second; a
circuit breaker pauses the run if the failure rate crosses 20%, and you must
`--resume` explicitly. Every outcome lands in the local audit trail:

```bash
dlq history --plan plan_xxxxxxxxxx
```

![dlq history --plan](./screenshots/08-history.png)

The trail for the plan shows, in order: the plan write, the dry run, each
message's outcome (including the skipped duplicate with its reason), and the
completion entry — with timestamps, source queue, destination, whether it
was confirmed, and the result for every row. This is the answer to "what
did this tool actually change, and when".

### 4.8 Changed your mind? Roll it back

Every confirmed replay was snapshotted **before** it executed — payload,
headers, source DLQ, destination. Roll the whole plan back to the DLQ:

```bash
dlq rollback --plan plan_xxxxxxxxxx --dry-run
```

![dlq rollback --dry-run](./screenshots/09-rollback-dryrun.png)

```bash
dlq rollback --plan plan_xxxxxxxxxx --confirm --reason "replay caused downstream errors"
```

![dlq rollback --confirm](./screenshots/10-rollback-confirm.png)

`Restored: 5` — the messages are back in the DLQ, with the operator's reason
stamped on every audit entry.

**One consequence worth knowing:** a rolled-back message is republished fresh
(its `x-death` bookkeeping is stripped), so it no longer carries a failure
reason — `analyze` classifies it `INVESTIGATE` until it fails again for
real. Its replay destination is preserved via an `x-destination` header, so
it can still be planned, patched, and replayed without an explicit
`--destination`.

## 5. Redis Streams walkthrough

The same workflow runs unchanged over Redis Streams — a DLQ is a stream
whose entries carry `payload`, `destination`, `error`, `retries`, and
`headers` fields (the Redis equivalent of `x-death`).

```bash
export DLQ_REDIS_URL=redis://localhost:6379/0
dlq connect redisstream --url-env DLQ_REDIS_URL --default-queue orders-dlq --profile redis-dev
go run ./cmd/seed --broker redis --source orders --dlq orders-dlq
```

`dlq analyze orders-dlq` produces the same three groups (the recovery engine
is broker-agnostic — it only ever talks to the `Broker` interface). Redis
adds a visibility surface RabbitMQ does not have: in-flight work in consumer
groups.

```bash
dlq stats orders-dlq
```

![dlq stats over Redis](./screenshots/11-redis-stats.png)

`Pending: 2` means two entries were delivered to a consumer group but not yet
acknowledged — work in flight or stuck in the group's PEL. The per-group
breakdown names the group (`inspection`) and its own pending count.

```bash
dlq queues
```

![dlq queues over Redis](./screenshots/12-redis-queues.png)

The `PENDING` column shows it per stream. The seed leaves two entries
unacknowledged on purpose so this surface has something to show; acknowledge
them (`XACK`) or leave them — pending entries do not block recovery.

From `dlq plan` onward, the commands are identical to Section 4. The Redis
recovery loop (plan → dry-run → confirm → history → rollback) is exercised
live in CI on every push.

## 6. Policies (optional)

Recovery judgment can be encoded as committed YAML and checked in CI. The
gate order is: a per-message `x-duplicate-of` header outranks everything; a
matching policy rule overrides the classifier; otherwise the classifier's
inference stands.

```bash
cat > policy.yaml <<'EOF'
rules:
  - when: error contains invalid
    action: do_not_replay
EOF

dlq policy validate policy.yaml
dlq policy apply policy.yaml --profile dev
dlq analyze orders-dlq     # the invalid-customer_id group is now DO_NOT_REPLAY
```

`dlq policy validate` exits non-zero on a broken rule, so it can run in CI
next to the service the policy protects. See [POLICIES.md](POLICIES.md) for
the full grammar, semantics, and precedence.

## 7. Clean up

```bash
docker compose down          # stop the brokers
rm recovery.json policy.yaml # and any config/audit files you created
```

The walkthrough's queues/streams vanish with the containers. If you pointed
at long-lived brokers, delete the `orders` / `orders-dlq` queues yourself
(RabbitMQ: management UI or `rabbitmqadmin delete queue name=orders-dlq`;
Redis: `DEL orders orders-dlq`).

## 8. The safety model, in action

Every mutating operation flows through one gate, and you just watched each
stage of it:

| Stage | Where you saw it | What it guarantees |
|-------|------------------|--------------------|
| Dry-run preview | §4.5, §4.6, §4.8 | The default for every mutating command; `Changes made: NONE` |
| Duplicate evidence | §4.4 group 3, §4.7 skip | Prior replays, event IDs, and `x-duplicate-of` headers surface before you confirm; `DO_NOT_REPLAY` messages never enter the plan |
| Destination invariant | §4.6 safety checks | A confirmed run refuses if the destination queue does not exist (a publish into a nonexistent queue can be confirmed and silently dropped); a large pending backlog on the destination is a dry-run warning |
| Explicit confirm | §4.5, §4.7 | Batch recovery, patch replay, and rollback all require `--confirm` |
| Limits | §4.7 | Batch size, rate limit, concurrency cap, and a circuit breaker that pauses a run whose failure rate crosses 20% until you explicitly `--resume` |
| Audit + snapshots | §4.7, §4.8 | Every dry-run and action is written to the SQLite audit trail; every confirmed replay is snapshotted so `dlq rollback` can reverse it |

## 9. The audit trail

The audit store is append-only SQLite (`~/.dlq/audit.db` by default;
`audit.path` in the config file). Each entry records: timestamp, action
(`inspect | search | analyze | plan | recover | patch | rollback`), message
ID, plan ID (when applicable), source queue, destination, payload diff (for
patches), whether it was a dry run, whether it was confirmed, the result,
and the operator-provided reason (rollback).

```bash
dlq history                     # recent entries across everything
dlq history --plan plan_xxx     # one plan's full trail
dlq history --action recover    # filter by action
dlq history --output json       # machine-readable
```

Snapshots live in the same store (`snapshots` table): the point-in-time
copy of every replayed message, keyed by plan, in replay order.

## 10. Automated test suite

### Unit tests (no brokers needed)

```bash
go test -count=1 ./...
```

Covers the message model, filter parsing, error-signature normalization,
the classifier's four-way split, policy parsing, the safety gate, the audit
store, self-update's checksum verification, and the golden-file CLI snapshots
(`analyze`/`plan`/`recover --dry-run` output).

### Full suite against live brokers

Point the integration suites at real RabbitMQ and Redis:

```bash
export DLQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/
export DLQ_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672/
export DLQ_TEST_REDIS_URL=redis://localhost:6379/0
go test -count=1 ./...
```

With those set, the suites run for real: the shared adapter **conformance
suite** (the same contract exercised against RabbitMQ and Redis — a new
broker must pass it to be added), the publish/ack **write paths**, and the
end-to-end **recovery loops** (analyze → plan → dry-run → confirm → history)
for both brokers, including the missing-destination refusal, the
policy-driven exclusion, the pending-backlog warning, and snapshot-rollback.
The suites skip when the variables are absent (so the unit path stays green
offline), and CI fails the workflow if any suite silently skips:

```bash
go test -count=1 -v -run 'TestRecoveryLoop|TestConformance|TestStats|TestQueuesShowsPending' ./internal/broker/rabbitmq/ ./internal/broker/redisstream/
```

### Release pipeline

Every push also runs `goreleaser release --snapshot --clean` (the full
6-target matrix: linux/darwin/windows × amd64/arm64) and uploads the
artifacts, so release-config drift fails the workflow at commit time.

## 11. Troubleshooting

- **`Error: unknown broker "redis"`** — the CLI's Redis broker is registered
  as `redisstream` (`dlq connect redisstream …`). `cmd/seed` uses
  `--broker redis` as its own shorthand.
- **`redisstream: scan streams: ERR syntax error`** — you are on Redis 5.
  `dlq queues` uses a plain `SCAN` plus a per-key `TYPE` check so it works on
  Redis 5 and 7 (the `SCAN … TYPE` filter only exists on 6+).
- **`cannot determine replay destination (message has no dead-letter
  metadata)`** — the message has no `x-death` (e.g. it was rolled back, or it
  was seeded without one). Pass `--destination <queue>` explicitly, or re-run
  on a freshly seeded DLQ.
- **`no snapshots found for plan …`** — the plan was never executed with a
  confirmed run of this tool, or the audit store was pointed elsewhere.
  Snapshots are written only on confirmed, successful replays.
- **`dlq recover --confirm` reports `Skipped: N` with "replayed at …"** —
  the duplicate-evidence check found a prior successful replay of the same
  message ID in the audit trail (content-hash IDs are stable, so re-seeding
  identical payloads collides). Use a fresh audit store (`audit.path` in the
  config) or make the payloads unique.
- **Policy `when` with a space** — values are single tokens:
  `error contains invalid`, not `error contains invalid customer_id`.
- **`dlq stats` shows `Oldest age: n/a`** — age is reported when the broker
  exposes message timestamps (Redis entry IDs); the RabbitMQ management path
  does not, so the line reads `n/a`.
- **Broker commands say `not connected`** — check the profile URL and that
  `dlq profiles list` shows the profile you mean (the default profile is the
  first one saved; use `--profile <name>` to select another).

## 12. Screenshot gallery

All captures from this guide, in order: `docs/screenshots/01-queues.png` …
`13-redis-analyze.png`. Regenerate them by re-running the walkthrough and
rendering the transcripts (the render script is a throwaway; the PNGs are
committed as documentation).
