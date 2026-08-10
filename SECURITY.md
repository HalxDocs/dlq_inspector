# Security Policy

DLQ Inspector is a local-first developer tool that connects to your message
infrastructure and, with explicit confirmation, mutates it. Safety is the core
promise of the product — so security and safety issues are taken seriously.

## Reporting a vulnerability

**Please do not open a public issue for a vulnerability.** Report it privately:

- GitHub private vulnerability reporting:
  https://github.com/HalxDocs/dlq_inspector/security/advisories/new
- Or email the maintainers (address in the GitHub profile) with the subject
  prefix `[dlq-inspector-security]`

Include, if possible:

- The affected version or commit
- A minimal reproduction (broker type/version, config, exact commands)
- Impact: what could an attacker achieve, and what's the blast radius?
- Whether you've already observed exploitation

You will receive an acknowledgement within 3 business days, and a first
assessment of the report within 7 business days. We follow coordinated
disclosure: we'll work with you on a fix and release timeline, and credit you in
the release notes unless you prefer to remain anonymous.

## Scope

In scope:

- The `dlq` binary and its dependencies (adapter code, recovery engine,
  audit store, self-update path)
- The safety pipeline: dry-run guarantees, duplicate-evidence checks,
  destination invariants, confirmation gates, rate limits, circuit breaker
- The audit trail's integrity (append-only, unforgeable records)
- Release integrity: `checksums.txt` verification in `dlq self-update`

Out of scope (report upstream instead):

- The brokers themselves (RabbitMQ, Redis) and their SDKs
- GitHub Actions infrastructure

## Safety-relevant behaviors we care about

These are treated as security-grade bugs even though they are not classic
vulnerabilities:

- A mutation executing without passing the full safety gate (dry-run,
  duplicate check, destination check, confirm, limits)
- The audit trail being silently truncated, modified, or written with the
  wrong `DryRun`/`Confirmed` flags
- `dlq self-update` installing a binary that failed checksum verification,
  or resolving an unexpected repository/asset
- Sensitive payload fields leaking into logs or export output

## Supported versions

| Version | Supported |
|---------|-----------|
| Latest release (v1.0.x) | ✅ |
| Older releases | ❌ — upgrade via `dlq self-update` |

Security fixes ship in the next release; backports are not provided.
