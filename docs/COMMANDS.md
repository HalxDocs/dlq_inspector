# DLQ Inspector — Command Reference

The workflow the commands serve:

```text
Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit
```

Every command supports `--output json` (or `jsonl` where noted) for machine-readable
output, and every dry-run and confirmed action is written to the local audit trail.
Run `dlq <command> --help` for the authoritative flag list of your installed version.

## Global flags

Available on every command:

| Flag | Default | Meaning |
|------|---------|---------|
| `--config <path>` | `~/.dlq/config.yaml` | Path to the config file |
| `--profile <name>` | config's default | Connection profile to use |
| `--output <format>` | `text` | `text`, `json`, or `jsonl` |

---

## Connection & configuration

### `dlq connect <broker>`

Save a connection profile. Credentials stay out of the config file: prefer
`--url-env`, which names an environment variable holding the URL.

```bash
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/
dlq connect rabbitmq --url-env RABBITMQ_URL --default-queue orders-dlq --profile prod
```

| Flag | Meaning |
|------|---------|
| `--url <url>` | Connection URL (`amqp://` or `amqps://`) |
| `--url-env <name>` | Env var holding the URL (preferred; keeps secrets out of config) |
| `--default-queue <queue>` | Default queue to operate on |
| `--management-url <url>` | Management API base URL (defaults to the AMQP host on port 15672) |

Redis Streams: `dlq connect redisstream --url-env REDIS_URL --default-queue orders-dlq --profile dev`.

### `dlq profiles list`

List saved profiles (name, broker, default queue, URL source) without printing
secrets.

---

## Reading

### `dlq queues`

List queues on the connected broker: name, durability, auto-delete, message count,
consumer count, and pending (delivered-but-unacknowledged) work.

### `dlq stats [queue]`

Per-queue statistics. Uses the profile's `default_queue` when no queue is given.
Redis Streams additionally breaks the pending total down per consumer group:

```text
Queue:        orders-dlq
Messages:     482
Consumers:    1
Pending:      17

Consumer groups:
  workers      consumers: 1   pending: 17
```

### `dlq inspect <queue> [--id <id>]`

Inspect one failed message by its stable ID, or list the queue's messages when
`--id` is omitted. `--format pretty|raw` controls payload rendering.

### `dlq search <queue>`

Filter DLQ messages. All filters combine (AND).

```bash
dlq search orders-dlq --error timeout --since 2h --max-retries 3 --limit 100
dlq search orders-dlq --field customer_id=443
```

| Flag | Meaning |
|------|---------|
| `--error <text>` | Match failure text or payload containing this string (case-insensitive) |
| `--since <duration\|ts>` | Only messages enqueued within the last duration (e.g. `2h`) or after an RFC3339 timestamp |
| `--field <key=value>` | Require a payload field (dotted path) to equal a value; repeatable |
| `--max-retries <n>` | Only messages with retry count ≤ n |
| `--limit <n>` | Maximum number of results (default 50) |
| `--show-sensitive` | Reveal configured sensitive fields (audit-logged in a later phase) |

---

## Recovery workflow

### `dlq analyze <queue>`

Group failures by normalized signature (error text, event type, destination, retry
bucket) and classify each group:

```text
482 messages analyzed
GROUP 1 -- Payment timeout        301 msgs (62.4%)  REPLAYABLE
GROUP 2 -- Invalid customer_id     97 msgs (20.1%)  REQUIRES_FIX
GROUP 3 -- Duplicate event         29 msgs ( 6.0%)  DO_NOT_REPLAY
```

| Flag | Meaning |
|------|---------|
| `--limit <n>` | Maximum number of messages to analyze (default 1000) |

A profile-bound [policy](#dlq-policy) can override classifications.

### `dlq plan <queue>`

Build an exportable recovery plan: the selected messages, destination, action,
execution limits, and the safety checks that will gate the run. DO_NOT_REPLAY
messages are excluded and recorded on the plan with their reason.

```bash
dlq plan orders-dlq --group <group-id> --output-file recovery.json
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--group <id>` | — | Plan only this failure group (ID shown by `dlq analyze`) |
| `--destination <queue>` | — | Override the replay destination |
| `--output-file <path>` | `recovery.json` | Where to write the plan JSON |
| `--batch-size <n>` | 25 | Messages per batch during execution |
| `--rate-limit <r>` | `10/s` | Execution rate limit |
| `--concurrency <n>` | 1 | Parallel execution workers |
| `--limit <n>` | 1000 | Maximum number of messages to consider |
| `--include-do-not-replay` | false | Also select messages classified DO_NOT_REPLAY |
| `--reason <text>` | — | Operator-provided reason, recorded in the audit trail |

### `dlq recover --plan <file>`

Validate a plan against the live queue — and execute it once confirmed. Without
`--confirm` this is a **dry run**: it re-checks every planned message (existence,
payload schema, duplicate evidence), surfaces destination problems, and changes
nothing. A missing destination queue or a large pending backlog is reported as a
warning; a confirmed run refuses if the destination does not exist.

```bash
dlq recover --plan recovery.json                        # dry run
dlq recover --plan recovery.json --confirm \
  --batch-size 25 --rate-limit 10/s --reason "outage cleared"
```

| Flag | Meaning |
|------|---------|
| `--plan <file>` | Path to the recovery plan JSON (required) |
| `--dry-run` | Validate without executing (the default) |
| `--confirm` | Execute the plan |
| `--resume` | Continue a tripped run, skipping already-replayed messages |
| `--batch-size <n>` | Messages per batch (defaults to the plan's limit) |
| `--rate-limit <r>` | Execution rate limit (defaults to the plan's limit) |
| `--concurrency <n>` | Parallel workers (defaults to the plan's limit) |
| `--retry <n>` | Extra publish attempts per message before it counts as failed (default 1) |
| `--failure-threshold <r>` | Circuit-breaker failure rate 0.0–1.0 (default 0.20) |
| `--pending-threshold <n>` | Warn when the destination has at least this many pending messages (default 100) |
| `--reason <text>` | Operator-provided reason, recorded in the audit trail |

If a batch's failure rate crosses the circuit-breaker threshold the run pauses;
re-run with `--confirm --resume` to continue from where it stopped.

### `dlq replay <queue> --id <id>`

Replay a single message to its original destination (or an override). Dry-run by
default; `--confirm` (with an interactive prompt, or `--yes` to skip it) republishes
and acks the DLQ copy only after a successful publish.

```bash
dlq replay orders-dlq --id sha256:1a2b...
dlq replay orders-dlq --id sha256:1a2b... --confirm --reason "transient timeout"
```

| Flag | Meaning |
|------|---------|
| `--id <id>` | Message ID to replay |
| `--destination <queue>` | Override the replay destination |
| `--confirm` | Execute the replay (without it, this is a dry run) |
| `--yes` | Skip the interactive confirmation prompt |
| `--reason <text>` | Operator-provided reason, recorded in the audit trail |

### `dlq patch <queue> --id <id> --set <path=value>`

Edit a message's payload before replay. The old→new JSON diff is rendered first, and
`--confirm` republishes the patched payload through the same safety gate. Supports
dotted paths and array indices (`billing.address.city`, `items.0.sku`); values are
parsed as JSON when valid (`443`, `true`) or kept as strings otherwise.

```bash
dlq patch orders-dlq --id sha256:1a2b... --set customer_id=443
dlq patch orders-dlq --id sha256:1a2b... --set items.0.sku=SKU-9 --confirm
```

| Flag | Meaning |
|------|---------|
| `--id <id>` | Message ID to patch and replay |
| `--set <path=value>` | Set a dotted path to a value; repeatable |
| `--destination <queue>` | Override the replay destination |
| `--confirm` | Execute the patch and replay (without it, this is a dry run) |
| `--yes` | Skip the interactive confirmation prompt |
| `--reason <text>` | Operator-provided reason, recorded in the audit trail |

### `dlq rollback --plan <id>`

Reverse a bad recovery. Every confirmed replay is snapshotted (payload, headers,
destination, DLQ) before the DLQ copy is acked; rollback republishes those snapshots
back to the DLQ with your reason. Dry-run by default; refuses (audited) if the DLQ no
longer exists.

```bash
dlq rollback --plan plan_8e7a085962            # dry run
dlq rollback --plan plan_8e7a085962 --confirm --reason "replay caused errors"
```

| Flag | Meaning |
|------|---------|
| `--plan <id>` | Plan ID whose snapshots to restore (required) |
| `--confirm` | Restore the snapshotted messages to the DLQ |
| `--reason <text>` | Operator-provided reason, recorded in the audit trail |

### `dlq history [--plan <id>]`

List recent audit entries, or the full trail of one recovery plan: every per-message
outcome (success, skip, failure, rollback) in execution order plus the plan-level
summary and any payload diffs.

```bash
dlq history
dlq history --plan plan_8e7a085962
dlq history --output jsonl
```

| Flag | Default | Meaning |
|------|---------|---------|
| `--limit <n>` | 20 | Maximum number of entries to list |
| `--plan <id>` | — | Show the full trail for this plan ID |

---

## Governance

### `dlq policy`

Recovery policies are YAML files, committed alongside the service they protect, that
encode what is safe to replay. When a policy is loaded for a profile, its rules
override the classifier's inference for matching messages (`dlq analyze` and
`dlq plan` honor it); a per-message `x-duplicate-of` header outranks any rule.

```yaml
# policy.yaml
rules:
  - when: error == payment_timeout
    action: replay
    max_retries: 3
  - when: event_type == order.cancelled
    action: do_not_replay
```

### `dlq policy validate <policy.yaml>`

Parse and validate a policy file, reporting every problem with its rule number.
Exits non-zero on any breakage — run it in CI. `--output json` returns a
machine-readable verdict.

```bash
dlq policy validate policy.yaml          # policy.yaml: valid (2 rules)
dlq policy validate policy.yaml --output json
```

### `dlq policy apply <policy.yaml> [--profile <name>]`

Validate a policy and bind it to a profile (defaults to `default_profile`), so
analyses and plans honor its rules. A policy that does not parse is refused.

```bash
dlq policy apply policy.yaml --profile prod
```

---

## Misc

### `dlq version`

Print version, commit, and build time:

```text
dlq v1.0.0
commit: abc1234
built: 2026-08-09T00:00:00Z
go: go1.26.5
```
