# Testing DLQ Inspector

Two ways to test the tool:

1. **Hands-on** — run real brokers, seed a dead-letter queue with failing
   messages, and drive the full workflow (`analyze` → `plan` → `recover` →
   `history` → `rollback`) against them. This is the fastest way to *see*
   what the tool does.
2. **Automated** — run the unit suite plus the live-broker integration suites
   (conformance + recovery loops) with the environment variables below.

Every command in this guide was verified against live brokers; the expected
output shown is real output.

---

## 1. Prerequisites

- **Go 1.26+** (`go version`).
- **Docker** with the compose plugin — or an existing RabbitMQ and/or Redis
  you can point at.

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
make build                 # produces bin/dlq (or: go build -o bin/dlq ./cmd/dlq)
go build ./cmd/seed        # the dev-only seed helper (not part of the release binary)
```

`cmd/seed` exists only to create test data: it fills a DLQ with six realistic
failing messages (three payment timeouts, two invalid `customer_id`s, one
duplicate the application itself marked as such) so the workflow below has
something to work on. It is deliberately *not* part of the shipped `dlq`
binary.

## 4. RabbitMQ walkthrough

### 4.1 Connect

Save a profile once (validation only — no connection is opened):

```bash
export DLQ_RABBIT_URL=amqp://guest:guest@localhost:5672/
dlq connect rabbitmq --url-env DLQ_RABBIT_URL --default-queue orders-dlq --profile dev
dlq profiles list
```

`--url-env` stores the *name* of the environment variable, never the URL
itself, so connection secrets stay out of the config file.

### 4.2 Seed the DLQ

```bash
go run ./cmd/seed --source orders --dlq orders-dlq
# Seeded 6 messages into orders-dlq (rabbitmq).
```

This declares the `orders` queue (the replay destination) and `orders-dlq`,
then publishes six messages into the DLQ carrying a crafted `x-death` header
— the shape RabbitMQ itself writes when it dead-letters a message, so the
tool resolves destinations and failure reasons exactly as it would for real
failures.

### 4.3 See the damage

```bash
dlq queues
dlq stats orders-dlq
dlq search orders-dlq --error timeout --since 2h
dlq inspect orders-dlq --id sha256:<id-from-search>
```

`search` prints the message IDs; `inspect` shows the full message — headers,
retry count, failure reason, pretty-printed JSON payload.

### 4.4 Understand the failure

```bash
dlq analyze orders-dlq
```

Expected output — three failure groups, each with a recommendation:

```text
6 messages analyzed in orders-dlq

GROUP 1 -- Timeout connecting to ...
3 messages - 50.0%
Recommendation: REPLAYABLE (confidence 0.80)
Signature:      timeout connecting to upstream payment gateway after {n}
Destination:    orders
Event type:     order.paid

GROUP 2 -- Invalid customer_id ...
2 messages - 33.3%
Recommendation: REQUIRES_FIX (confidence 0.80)

GROUP 3 -- Rejected ...
1 messages - 16.7%
Recommendation: DO_NOT_REPLAY (confidence 0.90)
```

Note the signature normalization: the three timeout messages share one group
even though their payloads differ, and the duplicate (marked via an
`x-duplicate-of` header by the application itself) is `DO_NOT_REPLAY`.

### 4.5 Plan the recovery, then preview it

```bash
dlq plan orders-dlq --output-file recovery.json
# Plan written: recovery.json (5 messages selected), 1 excluded (left in DLQ)
# Plan ID: plan_xxxxxxxxxx

dlq recover --plan recovery.json          # dry run — changes nothing
```

The dry run validates every selected message, surfaces the excluded
duplicate, and ends with `Changes made: NONE`. Nothing was published, acked,
or deleted.

### 4.6 Fix one message with a patch, then replay it

The two `invalid customer_id` messages need a fix before replay. Grab one ID
from `dlq search orders-dlq --error invalid`, then:

```bash
dlq patch orders-dlq --id sha256:<id> --set customer_id=1004
```

This shows a payload diff and runs the same safety checks as a replay:

```text
Payload diff:
- customer_id: "not-a-number"
+ customer_id: 1004

Changes made: NONE
```

Confirm to fix and replay it:

```bash
dlq patch orders-dlq --id sha256:<id> --set customer_id=1004 --confirm
# Patched and replayed sha256:<id> -> orders
# Audit entry written (with diff).
```

### 4.7 Execute the recovery, then audit

```bash
dlq recover --plan recovery.json --confirm --batch-size 2 --rate-limit 10/s
```

Expected summary: `Replayed: 5` (the patched message is no longer in the DLQ),
`Excluded: 1`, `Failed: 0`. Every outcome lands in the local audit trail:

```bash
dlq history --plan plan_xxxxxxxxxx
```

The trail shows the plan write, the dry run, each replay (or skip, with the
reason), and the completion entry — timestamps, destination, operator, and
result for every row.

### 4.8 Changed your mind? Roll it back

Every confirmed replay was snapshotted before it happened. Roll the whole
plan back to the DLQ:

```bash
dlq rollback --plan plan_xxxxxxxxxx --dry-run          # 5 snapshots, NONE restored
dlq rollback --plan plan_xxxxxxxxxx --confirm --reason "replay caused downstream errors"
# Restored: 5
```

One consequence worth knowing: a rolled-back message is republished fresh
(stripped of its `x-death` bookkeeping), so it no longer carries a failure
reason — `analyze` will classify it `INVESTIGATE` until it fails again for
real. Its replay destination is preserved via an `x-destination` header, so
it can still be planned and replayed.

## 5. Redis Streams walkthrough

The same workflow runs unchanged over Redis Streams — a DLQ is just a
stream whose entries carry `payload`, `destination`, `error`, `retries`, and
`headers` fields.

```bash
export DLQ_REDIS_URL=redis://localhost:6379/0
dlq connect redisstream --url-env DLQ_REDIS_URL --default-queue orders-dlq --profile redis-dev
go run ./cmd/seed --broker redis --source orders --dlq orders-dlq
```

`dlq analyze orders-dlq` produces the same three groups. Redis adds a
visibility surface RabbitMQ does not have — in-flight work in consumer
groups:

```bash
dlq stats orders-dlq
# Messages:  6
# Consumers: 1
# Pending:   2
# Consumer groups:
#   inspection  consumers: 1  pending: 2

dlq queues        # the PENDING column shows it per stream
```

The seed leaves two entries unacknowledged in a consumer group so the pending
surface has something to show; acknowledge them (`XACK`) or leave them — they
do not block recovery. From `dlq plan` onward the commands are identical to
Section 4.

## 6. Policies (optional)

Recovery judgment can be encoded as committed YAML and checked in CI:

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

See [POLICIES.md](POLICIES.md) for the full grammar.

## 7. Clean up

```bash
docker compose down          # stop the brokers
rm recovery.json policy.yaml # and any config/audit files you created
```

The walkthrough's queues/streams vanish with the containers. If you pointed
at long-lived brokers, delete the `orders` / `orders-dlq` queues (or `DEL`
the streams) yourself.

## 8. Automated test suite

### Unit tests (no brokers needed)

```bash
go test -count=1 ./...
```

### Full suite against live brokers

Point the integration suites at real RabbitMQ and Redis — the shared adapter
conformance suites, the publish/ack write paths, and the end-to-end recovery
loops (analyze → plan → dry-run → confirm → history) for both brokers:

```bash
export DLQ_TEST_AMQP_URL=amqp://guest:guest@localhost:5672/
export DLQ_TEST_MANAGEMENT_URL=http://guest:guest@localhost:15672/
export DLQ_TEST_REDIS_URL=redis://localhost:6379/0
go test -count=1 ./...
```

The suites skip when the variables are absent (so the unit path stays green
offline) and CI runs them on every push with real services, failing the
workflow if any suite silently skips. The key integration tests:

```bash
go test -count=1 -v -run 'TestRecoveryLoop|TestConformance|TestStats|TestQueuesShowsPending' ./internal/broker/rabbitmq/ ./internal/broker/redisstream/
```

## 9. Troubleshooting

- **`Error: unknown broker "redis"`** — the CLI's Redis broker is registered
  as `redisstream` (`dlq connect redisstream ...`); `cmd/seed` uses
  `--broker redis` as its own shorthand.
- **`redisstream: scan streams: ERR syntax error`** — you are on Redis 5.
  `dlq queues` uses a plain `SCAN` + per-key `TYPE` check so it works on
  Redis 5 and 7 (the `TYPE` filter option only exists on 6+).
- **`cannot determine replay destination`** — the message has no
  dead-letter metadata (e.g. it was rolled back). Pass `--destination <queue>`
  explicitly, or re-run on a freshly seeded DLQ.
- **Policy `when` with a space** — values are single tokens:
  `error contains invalid`, not `error contains invalid customer_id`.
- **`dlq stats` shows `Oldest age: n/a`** — age is reported when the broker
  exposes message timestamps (Redis entry IDs); the RabbitMQ management path
  does not, so the line reads `n/a`.
