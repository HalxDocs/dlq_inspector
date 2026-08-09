# DLQ Inspector — Recovery Policies

Recovery policies encode *what is safe to replay* for your service, instead of
leaving that judgment to the classifier's built-in assumptions. A policy is a plain
YAML file — commit it next to the service it protects, the way you'd commit a
Dockerfile or a CI workflow — and bind it to a connection profile. `dlq analyze`
and `dlq plan` then honor its rules.

```yaml
# policy.yaml
rules:
  - when: error contains payment_timeout
    action: replay
    params:
      max_retries: 3

  - when: event_type == order.cancelled
    action: do_not_replay

  - when: error == invalid_token
    action: require_fix
    params:
      require_idempotency_key: true
```

## How a policy is used

1. **Write** the policy as YAML next to the service.
2. **Validate** it in CI (`dlq policy validate policy.yaml`) so a broken rule never
   reaches production.
3. **Bind** it to a profile (`dlq policy apply policy.yaml --profile prod`).
4. **Analyze and plan** — matching messages are classified by the policy instead of
   the built-in classifier.

## YAML grammar

A policy is an ordered list of rules. Each rule is:

```yaml
rules:
  - when: <field> <op> <value>     # required — the condition
    action: <replay|require_fix|do_not_replay>   # required — the decision
    params: { ... }               # optional — gates that must also hold
```

### Fields and operators

The condition is a single `<field> <op> <value>` expression. The value may be
quoted (`error == "connection reset"`), which matters when it contains spaces.

| Field | Meaning | Valid operators |
|-------|---------|-----------------|
| `error` | The message's dead-letter/failure reason | `==`, `!=`, `contains` |
| `event_type` | The application event type (see below) | `==`, `!=`, `contains` |
| `destination` | The replay destination queue | `==`, `!=`, `contains` |
| `retries` | The message's retry count | `==`, `!=`, `>`, `>=`, `<`, `<=` |

Text operators are case-insensitive: `==` / `!=` are exact matches, `contains` is a
substring match. The `retries` value must be an integer.

`event_type` is read from the common header conventions, first match wins:
`x-event-type`, `event_type`, `event-type`, `ce-type`.

### Actions

| Action | Effect on classification |
|--------|--------------------------|
| `replay` | The message is treated as **REPLAYABLE** |
| `require_fix` | The message is treated as **REQUIRES_FIX** |
| `do_not_replay` | The message is treated as **DO_NOT_REPLAY** and excluded from plans (unless `--include-do-not-replay`) |

### Params — conditional gates

A rule only applies when its params are satisfied. A rule whose params are not
satisfied does not match, and evaluation falls through to the next rule.

| Param | Meaning |
|-------|---------|
| `max_retries: N` | The rule does **not** apply to messages with more than N retries |
| `require_idempotency_key: true` | The rule only applies to messages that carry an idempotency key |

Example — "timeouts are replayable, but only if we have not already hammered the
message three times":

```yaml
rules:
  - when: error contains timeout
    action: replay
    params:
      max_retries: 3
```

At 4 retries the rule's gate is unsatisfied, so the message falls through to the
built-in classifier (which, for a timeout at high retries, is no longer blind
optimism).

## Semantics

- **First match wins.** Rules are evaluated in order; the first rule whose
  condition matches *and* whose params are satisfied decides the message.
- **A matching rule overrides the classifier.** The classification, the rule's
  action, is reported with the rule named in the reason and a fixed confidence of
  0.90.
- **No match → classifier inference stands.** An unmatched message keeps whatever
  the built-in classifier decided.

## Precedence

Three layers decide a message's classification, strongest signal first:

1. **`x-duplicate-of` header (per-message, always wins).** If the application
   itself marked *this* message as a duplicate, nothing — not even a policy rule —
   can force a replay. A general rule cannot know that a specific message is a
   duplicate; only the application can. The message is DO_NOT_REPLAY.
2. **Policy rules (profile-bound, first match wins).** The first rule whose
   condition and params match overrides the classifier.
3. **The built-in classifier.** Conservative, code-defined rules over failure
   text, retry count, and duplicate keywords; `INVESTIGATE` when signals conflict
   or are missing.

## Validation

`dlq policy validate` parses and validates the whole file and reports **every**
problem in one pass, each prefixed with its rule number:

```text
$ dlq policy validate policy.yaml
policy.yaml: valid (3 rules)
```

```text
$ dlq policy validate broken.yaml
broken.yaml: rule 2: when "event_type == order.cancelled": op "==" is not valid for retries (want ==, !=, >, >=, <, <=); rule 3: unknown action "replay_now" (want replay, require_fix, or do_not_replay)
```

A broken file exits non-zero and is rejected **wholesale** — a policy is either
fully valid or not loaded at all, so partial rules never half-apply.

Rejected policies include: empty `rules`, a missing `when`, an unknown field or
operator, a non-integer `retries` value, an unknown action, an unknown param, a
non-integer or negative `max_retries`, and a non-boolean `require_idempotency_key`.

### CI usage

Gate every commit on the policy that protects the service:

```yaml
# .github/workflows/ci.yml (example)
- name: Validate recovery policy
  run: dlq policy validate policies/orders.yaml
```

Machine-readable verdicts for richer CI output:

```text
$ dlq policy validate policies/orders.yaml --output json
{ "valid": true, "rules": 3 }
```

## Binding a policy to a profile

`dlq policy apply` validates the file, then records its **absolute path** in the
profile's `policy_file`. The file itself stays where it lives — typically committed
with the service.

```bash
dlq policy apply policies/orders.yaml --profile prod
# Policy applied to profile "prod": /home/you/services/orders/policies/orders.yaml
# dlq analyze and dlq plan will honor it for this profile.
```

The profile defaults to the config's `default_profile`. A profile referencing a
missing or broken policy fails **loudly** at analyze/plan time — the tool never
silently runs without the rules the operator believes are in force.

## Worked example

```yaml
# A payments service: transient failures replay (a few times), cancellations and
# duplicates never do, and anything the classifier calls "validation" needs a
# human fix — unless it already carries an idempotency key, in which case a
# single replay is safe.
rules:
  - when: event_type == payment.completed
    action: do_not_replay

  - when: error contains timeout
    action: replay
    params:
      max_retries: 3

  - when: error contains validation
    action: require_fix

  - when: error == duplicate
    action: require_fix
    params:
      require_idempotency_key: true
```
