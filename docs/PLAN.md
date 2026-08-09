# DLQ Inspector — Build Plan (Phased)

A local-first Go CLI for inspecting, analyzing, classifying, and **safely** recovering
failed messages from dead-letter queues. MVP broker: RabbitMQ.

Workflow the whole tool serves:
`Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit`

This document is the build order. Each **phase** is a shippable increment with an
**exit gate** (definition of done) that must pass before the next phase starts. Phases
are ordered by dependency and risk, not by spec-section order.

---

## 0. Cross-cutting rules (non-negotiables, apply from day 1)

These are cheaper to build right than to retrofit. Every phase must respect them.

1. **Adapter isolation** — no broker SDK type (`amqp.Delivery`, etc.) ever crosses
   out of its adapter package. Everything is normalized to `message.Message`. The
   command layer and recovery engine only ever see `message.Message` + `broker.Broker`.
2. **Safety before I/O** — any command that publishes, deletes, or mutates goes through
   one shared safety pipeline: `dry-run preview -> duplicate evidence -> (diff if patched)
   -> explicit confirm -> execute -> audit write`. Built once (Phase 3) as
   `internal/safety`, reused by every mutating command. Never left to an adapter.
3. **`--output json` everywhere** — every command supports machine-readable output from
   the first version of that command, not retrofitted.
4. **Audit on every state change** — dry-runs and confirmed actions both write audit
   entries. The audit store is append-only and is the source of truth.
5. **Limits are required, not optional** — multi-message operations must carry
   `--batch-size` / `--rate-limit` / `--limit`. Recover without a plan, or bulk ops
   without `--filter`/`--id`, are rejected outright.
6. **Honest classification** — `INVESTIGATE` is the default when signals conflict or
   are missing. Never force a message into `REPLAYABLE` / `DO_NOT_REPLAY` without evidence.
7. **CI-able tests** — unit tests must run with zero external services. Integration tests
   (real RabbitMQ/Redis) are dual-mode: skipped when no broker is reachable, run in CI
   where Docker exists.

---

## 1. MVP scope lock

**In (must ship before anything else):** RabbitMQ adapter · list/inspect/search ·
single replay with dry-run+confirm · failure grouping · classification ·
plan generation · dry-run bulk recovery · confirmed batch replay with limits ·
JSON output · audit history.

**Deferred (after the recovery engine feels solid end-to-end):** payload patching,
policies-as-code, second broker.

**Explicitly out:** Kafka, SQS, multi-broker fan-out, schema-aware payload validation,
a web dashboard/hosted service/Kubernetes operator — never in scope. TUI, watch mode,
notifications, Prometheus metrics, co-confirm are post-1.0 adoption features (Phase 10).
Snapshot & rollback shipped early (Phase 6 note below) because a recovery tool that
cannot reverse a bad recovery is incomplete.

---

## 2. Phase 0 — Skeleton, config, contracts (spec M0)

**Outcome:** `dlq` binary exists; config/profile system works; the `Broker` interface
and `message.Message` model are defined and frozen.

**Exit gate:**
- `go build ./...` clean; `dlq --help` and `dlq version` work.
- `dlq connect rabbitmq --url amqp://... --profile default` validates and persists a
  profile (no broker connection yet — validation only); `dlq profiles list` works.
- `~/.dlq/config.yaml` loads: `default_profile`, profiles with `broker`/`url_env`/
  `default_queue`/`require_confirm`, audit settings. Secrets never in plaintext —
  config points at env vars.
- `internal/broker/broker.go` defines the full interface
  (`Connect/Close/ListQueues/Inspect/Search/Publish/Stats`); `internal/message/message.go`
  defines the normalized struct (ID, payload, headers, timestamps, retry count, failure
  reason, queue, destination). RabbitMQ adapter exists as an empty stub.
- Makefile with `build/test/lint` targets; `go.mod` seeded.

**Packages:** `cmd/dlq`, `internal/command` (root, connect, profiles), `internal/config`,
`internal/broker` (interface + registry), `internal/message`.

**Tests:** config parse + profile round-trip; interface compile-time assertions.

**Notes / decisions to lock here:**
- **Message ID strategy (RabbitMQ has no native message ID).** Adapter prefers an
  `x-message-id` header if present, else falls back to `sha256(payload+headers)`. This
  ID feeds inspect, audit, and dedupe — lock the rule now.
- Viper only for config binding; commands get cobra. Keep adapters free of viper.

---

## 3. Phase 1 — RabbitMQ adapter: connect, list, stats (spec M1)

**Outcome:** the tool talks to a real RabbitMQ instance.

**Exit gate:**
- `dlq connect` opens a real connection against a live RabbitMQ (dual-mode test:
  skipped locally, runs in CI).
- `dlq queues` lists queues with type/state.
- `dlq stats <queue>` shows depth, consumer count, oldest/newest message age (via queue
  info / management-free AMQP where possible).
- `internal/broker/rabbitmq` implemented: `connection.go`, `adapter.go`, `mapper.go`
  (amqp → `message.Message`), including the ID rule from Phase 0.
- Conformance test harness skeleton in `internal/broker/conformance_test.go` — the suite
  every future adapter must pass.

**Tests:** mapper unit tests (header mapping, ID fallback, payload decode); connection
retry/backoff; conformance harness green against real RabbitMQ in CI.

**Notes:** DLQs reach messages via dead-letter exchange; `ListQueues` must surface
`x-dead-letter-exchange` bindings so we can find a queue's DLQ and, later, its original
destination for replay. Capture that mapping now — Phase 3 needs it.

---

## 4. Phase 2 — Inspect, search, rendering (spec M2, M3)

**Outcome:** engineers can read failed messages out of a DLQ.

**Exit gate:**
- `dlq inspect <queue> --id <id>` — pretty-JSON payload, headers, retry count,
  timestamps, failure reason; `--format pretty|json`.
- `dlq inspect <queue>` (no id) — paged summary list of messages.
- `dlq search <queue>` with `--error` (text), `--since <dur>`, `--field key=value`,
  `--retries`, `--limit`. Filtering happens broker-side where possible, in-memory
  otherwise — both behind `internal/search`.
- Sensitive-field redaction scaffolding: config-listed fields masked in output by
  default (full audit-logged `--show-sensitive` flag lands post-1.0).
- `--output json` on both commands.

**Packages:** `internal/message/format.go`, `internal/message/redact.go`,
`internal/search`, `internal/command` (inspect, search).

**Tests:** search filter parsing (durations, field paths, negation); redaction;
pretty-vs-json rendering; golden output snapshots for `inspect`/`search`.

**Gate to next phase:** `message.Message` normalization contract is now exercised by
real broker data — freeze it.

---

## 5. Phase 3 — Safety gate + single-message replay (spec M4 — core MVP finish line)

**Outcome:** one message can be replayed safely, with the shared safety pipeline in
place for everything that comes later.

**Exit gate:**
- `dlq replay <queue> --id <id> --dry-run` previews the operation and performs **zero**
  mutating I/O.
- `dlq replay <queue> --id <id> --confirm` republishes to the original destination and
  acks the DLQ copy **only after a successful publish**; writes an audit entry.
- Without `--confirm`, replay refuses to run. Interactive `[y/N]` prompt also supported.
- `internal/safety` gate exists as shared code: `dry-run -> duplicate evidence ->
  confirm -> execute -> audit`. Replay is its first consumer; recover reuses it (Phase 6).
- `internal/audit` works: append-only SQLite store (`modernc.org/sqlite`, cgo-free),
  JSONL export, `dlq history` lists recent entries.

**Packages:** `internal/safety`, `internal/audit` (logger, store), `internal/command/replay.go`.

**Tests (safety-path, the important ones):** assert `Publish` is never called without a
prior confirm; publish-failure leaves the DLQ message un-acked; audit entry shape for
dry-run vs confirmed; duplicate evidence surfaced before confirm.

**Notes:** replay = republish to original queue, then ack DLQ copy. Order matters:
publish first, ack second — a failed publish must not lose the DLQ message. At-least-once
delivery means duplicates are possible; dedupe evidence (Phase 5) exists to warn, never
to silently block.

---

## 6. Phase 4 — Failure analysis & classification (spec M6)

**Outcome:** `dlq analyze` turns a pile of messages into grouped failure patterns with
recommendations.

**Exit gate:**
- `dlq analyze <queue>` prints groups: label, signature, message count, percentage,
  recommendation (REPLAYABLE / REQUIRES_FIX / DO_NOT_REPLAY / INVESTIGATE).
- Error-signature normalization collapses dynamic values (`timeout connecting to
  10.0.4.5:6432` and `...10.0.4.9:6432` → `timeout connecting to {ip}:{port}`) before
  clustering — regex/token-based, in `internal/recovery/signature.go`.
- Grouping keys: error type + normalized message, event type, destination, payload
  shape, retry count, time window.
- `--output json` returns the structured group list.
- Classifier v1 is rule-based in code (no user policy DSL yet). INVESTIGATE is the
  honest default on missing/conflicting signals.

**Packages:** `internal/recovery` (analyzer, classifier, signature), `internal/command/analyze.go`.

**Tests:** fixture DLQs with a known failure mix → assert the exact expected
classification split; normalization unit tests (IPs, ports, IDs, timestamps, UUIDs,
hex hashes); classification confidence bounds.

---

## 7. Phase 5 — Recovery planning & dry-run validation (spec M7, M8)

**Outcome:** analysis becomes a reviewable, exportable, executable plan — and the
executor's dry-run proves it before anything runs.

**Exit gate:**
- `dlq plan <queue> --group <id>` writes `recovery.json`: plan ID, queue, message IDs,
  destination, action, `PlanLimits` (batch-size, rate-limit, concurrency), and the
  `SafetyChecks` list (`schema_validated`, `duplicate_checked`, `destination_checked`).
  Plan is reviewed/diffed as JSON before execution.
- `dlq recover --plan recovery.json --dry-run` validates: payload/schema check per
  message, duplicate evidence (event ID / idempotency-key matching in
  `internal/dedupe`), skip counts, and reports `Changes made: NONE`. **Zero mutating I/O.**
- A plan with an empty `SafetyChecks` list is rejected.
- `DO_NOT_REPLAY` messages are excluded from replay selection by default.

**Packages:** `internal/recovery` (planner, validator), `internal/dedupe`,
`internal/command` (plan, recover dry-run path).

**Tests:** plan JSON golden files; validator rejects empty safety checks; duplicate
evidence correctly flags matching event IDs/idempotency keys; dry-run proves no publish.

---

## 8. Phase 6 — Confirmed batch recovery + audit trail (spec M9, M10)

**Outcome:** plans execute safely at scale, and every action is auditable. This closes
the recovery-engine MVP.

**Exit gate:**
- `dlq recover --plan recovery.json --confirm --batch-size 25 --rate-limit 10/s`
  executes with: token-bucket rate limiting (`golang.org/x/time/rate`), concurrency cap,
  per-message retry, and a summary (selected / replayed / skipped / failed / new DLQ
  entries / duration).
- **Circuit breaker:** failure rate inside a running batch crossing the threshold
  (default 20%) auto-pauses remaining batches; continuation requires explicit
  re-confirmation.
- `dlq history --plan <plan-id>` shows the full trail for one recovery (plan ID,
  message IDs, source→destination, action, reason, operator, result).
- Audit entries written per message (success / failed / skipped) plus the plan-level
  summary.
- **Destination invariant:** the executor refuses to run (before any publish or ack,
  audited as `refused`) when the plan's destination queue does not exist — publishing
  into a nonexistent queue can be confirmed and silently dropped, which would lose the
  DLQ copy. The dry-run surfaces the same finding as a `Destination warning`.
- **Snapshot & rollback (built):** every confirmed replay is snapshotted (payload,
  content type, headers, DLQ, destination) in the audit store before the DLQ copy is
  acked. `dlq rollback --plan <id>` restores them to the DLQ with the operator's reason
  — dry-run by default, refuses (audited) if the DLQ has vanished, and restores carry
  the original replay destination (`x-destination` header / destination field) so a
  future plan can replay them again.

**Packages:** `internal/recovery/executor.go` (+ `rollback.go`), `internal/command/recover.go`
(+ `rollback.go`), `history.go`, `internal/audit` (plan queries, snapshots table).

**Tests:** circuit breaker halts a batch when injected failures cross threshold and
requires re-confirm; rate limiter throttles; executor handles publish-failure without
acking; history query returns the full plan trail.

> **Milestone gate:** nothing after this phase starts until `analyze -> plan ->
> recover` feels genuinely solid end-to-end. This is the v0.1 release candidate.

---

## 9. Phase 7 — Payload patching with diff (spec M5, deferred)

**Outcome:** fix messages before replaying them.

**Exit gate (done):**
- `dlq patch <queue> --id <id> --set customer_id=443` renders an old→new payload diff
  (`internal/patch/diff`) and, with `--confirm`, replays the patched message. The
  dry-run prints the diff + safety checks + `Changes made: NONE`; the diff is written
  to the audit entry (`payload_diff` column, with a migration for older stores).
- Patched replay flows through the **same** safety pipeline as unpatched replay
  (dry-run → diff → confirm → execute → audit) via the shared `safety` gate, which
  carries the payload override, diff, and audit action.
- `--set` supports dotted field paths (`billing.address.city`, `items.0.sku`) and JSON
  values (`443`, `true`, `["a","b"]`); non-JSON values become strings. A `--set` that
  produces no change is refused.

**Packages:** `internal/patch` (patch, diff), `internal/command/patch.go`.

**Tests:** diff rendering (added/removed/changed paths), patch application on nested
objects and arrays, patched-replay safety path (publish patched payload, ack only
after publish, refusal on missing destination), diff persisted in audit and rendered
by `dlq history`.

---

## 10. Phase 8 — Recovery policies (policy-as-code) (spec M12)

**Outcome:** teams encode "what is safe to replay" instead of the classifier hard-coding
assumptions.

**Exit gate (done):**
- `policy.yaml` parsing (`gopkg.in/yaml.v3`): `when: <condition>` (field op value on
  `error` / `event_type` / `destination` / `retries`) + `action:
  replay|do_not_replay|require_fix` + params (`max_retries`,
  `require_idempotency_key`, which gate whether the rule applies at all). Rules are
  ordered — first match wins.
- `dlq policy validate policy.yaml` is CI-friendly: reports every breakage with its
  rule number, exits non-zero, `--output json` for machine-readable verdicts.
- `dlq policy apply policy.yaml --profile production` validates and binds a policy to
  a profile (absolute path in `policy_file`).
- When a policy is loaded, its rule action **overrides** the classifier default for
  matching messages (policy gate inside `ClassifyWithPolicy`, honored by `dlq analyze`
  and `dlq plan`); a per-message `x-duplicate-of` header outranks any policy rule.

**Packages:** `internal/policy` (policy, rules), `internal/command/policy.go`.

**Tests:** rule evaluation against a `message.Message` (text/retries ops, first-match
order, param gates); broken/malformed policy rejected with clear errors; override
behavior when policy and classifier disagree; the live recovery loop drives a
policy-marked DO_NOT_REPLAY message through analyze -> plan -> dry-run -> confirm
and leaves it in the DLQ.

---

## 11. Phase 9 — Redis Streams adapter (spec M11)

**Outcome:** a second broker behind the same interface, proving broker-agnosticism.

**Exit gate (done):**
- `internal/broker/redisstream` implements the full `Broker` interface over Redis
  Streams (`go-redis/v9`): streams are queues, entries carry payload + dead-letter
  metadata (destination, error, retries, headers) as fields, replay is XADD to the
  destination then XDEL the DLQ copy. Stats probes existence with EXISTS and wraps
  `broker.ErrQueueNotFound`, so the destination-existence invariant holds.
- The shared conformance suite (`internal/broker/conformance_test.go`) passes green
  against real Redis — and was extended to cover the real contract (Publish/Search/
  Inspect/Stats/Ack over a per-adapter fixture), so it now enforces the cycle the
  recovery engine depends on for every adapter.
- `analyze / plan / recover` work against Redis Streams **with zero changes** to
  analyzer/classifier/planner/validator/executor code, proven by the live
  `TestRecoveryLoopOverRedis` (analyze -> plan -> dry-run -> confirm -> history,
  including the DO_NOT_REPLAY exclusion) gated in CI alongside the RabbitMQ loops.

**Tests:** conformance suite (RabbitMQ + Redis); recovery-engine integration over
both adapters.

**Notes:** the tool owns the DLQ outright — no consumer groups are involved in the
inspect/replay flow. Kafka and SQS follow later under the same rule: implement the
interface + pass conformance, no engine changes.

---

## 12. Phase 10 — Release & post-1.0 adoption features (spec M13, M14)

**Exit gate (release):**
- goreleaser single static binary (cgo-free — `modernc.org/sqlite` is load-bearing),
  README, `docs/ARCHITECTURE.md`, `docs/COMMANDS.md`, `docs/POLICIES.md`.
- Golden-file CLI test suite in CI (analyze/plan/recover --dry-run snapshots).

**Done so far:** `.goreleaser.yml` (one static binary per platform, CGO_ENABLED=0,
`-trimpath`, `-X main.{version,commit,date}` ldflags) with the release build verified
locally (`dlq version` reports the injected metadata; `go version -m` shows
modernc.org/sqlite embedded); README quickstart rewritten for the shipped CLI;
`docs/COMMANDS.md` written as the full command reference; golden-file CLI suite
(`internal/command/golden_test.go` + `testdata/*.golden`, regenerated with
`go test ./internal/command/ -run TestGolden -update`) snapshotting analyze, plan,
and recover --dry-run output — run in the CI Test step against the in-memory
fixture, with volatile plan IDs/paths pinned to placeholders; `docs/POLICIES.md`
documenting the policy grammar, semantics, precedence, and CI usage (linked from
the README); `docs/ARCHITECTURE.md` capturing the layered design, the Broker
contract, the recovery engine, and the adapter-isolation rules.

**Release gate: complete.** Phase 10 release items are all done. Post-1.0 adoption
features below remain open.

**Post-1.0 adoption features (in priority order):**
1. Interactive TUI (`dlq tui`) — k9s-style browsing + one-keystroke replay; the biggest
   adoption driver.
2. Live watch mode (`dlq watch <queue> --alert-threshold 100`) — DLQ growth tailing
   mid-incident.
3. Sensitive-field redaction completion (audit-logged `--show-sensitive`).
4. ~~Snapshot & rollback~~ — **done**: the executor snapshots every replayed message and
   `dlq rollback --plan <id>` moves them back to the DLQ with a reason (see Phase 6).
5. Notifications — Slack/webhook summaries after analyze and recover.
6. Two-person confirm — `require_co_confirm` profile flag needs a second operator token.
7. Replay simulation — dry-run against a shadow/staging destination.
8. Prometheus metrics — DLQ depth, recovery success rate, circuit-breaker trips.

---

## 13. Critical path & sequencing rationale

```
Phase 0 (contracts) -> Phase 1 (connect) -> Phase 2 (read) -> Phase 3 (safe replay)
   -> Phase 4 (analyze) -> Phase 5 (plan + dry-run) -> Phase 6 (confirmed recover + audit)
   -> [v0.1 gate] -> Phase 7 (patch) -> Phase 8 (policies) -> Phase 9 (2nd broker)
   -> Phase 10 (release + adoption)
```

- **Phase 0 → 3 is the dependency spine.** The `Broker` interface, `message.Message`,
  and the safety pipeline are the three things everything else leans on; getting them
  right early is why they're phased before any analysis features.
- **Phase 4–6 are sequential by data flow:** you can't plan without grouping, and you
  can't confirm-recover without a validated plan. Do not parallelize them.
- **Phases 7–8 are independent of each other** and can be reordered or skipped without
  touching the spine; both sit cleanly on the Phase 3 safety gate.
- **Phase 9 proves the architecture** (adapter isolation + conformance suite). It's
  valuable, but it's explicitly after the v0.1 gate.
- **Build the safety gate before anything that mutates.** The spec's own warning:
  "Command → BOOM → 50,000 jobs replayed" is the failure mode Phase 3 exists to prevent.

---

## 14. Spec milestone cross-reference

| Phase | Spec milestones | Delivered |
|-------|-----------------|-----------|
| 0 | M0 | Skeleton, config, contracts |
| 1 | M1 | Connect + list + stats |
| 2 | M2, M3 | Inspect + search |
| 3 | M4 | Safe single replay (core MVP finish) |
| 4 | M6 | Analyze + classify |
| 5 | M7, M8 | Plan + dry-run validation |
| 6 | M9, M10 | Confirmed batch recover + audit |
| 7 | M5 | Patch + diff |
| 8 | M12 | Policies |
| 9 | M11 | Redis Streams adapter |
| 10 | M13, M14 | Release + TUI/watch/notifications |

## 15. Environment constraints

- **No Docker on the current machine** → all integration tests must be dual-mode
  (skip when no broker reachable, run in CI). Unit + safety-path + golden tests are
  the local gate; integration is the CI gate.
- **Go 1.26.5** available; target cgo-free builds for cross-compilation
  (`modernc.org/sqlite`).
- Suggested session cadence: one phase per work session for Phases 0–3; Phases 4–6
  may each take more than one session. Reassess the plan after Phase 6's gate.
