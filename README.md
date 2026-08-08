# DLQ Inspector

A local-first CLI for inspecting, analyzing, classifying, and **safely** recovering
failed messages from dead-letter queues.

Part of the backend toolkit: **Reqit** (build/test APIs) · **Barrage** (load testing) ·
**DLQ Inspector** (failure investigation + safe recovery).

Core workflow:

```
Inspect -> Analyze -> Classify -> Plan -> Validate -> Dry-run -> Recover -> Audit
```

## Status

Under construction — Phase 0 (skeleton, config, contracts) in progress.
See [docs/PLAN.md](docs/PLAN.md) for the phased build plan.

## MVP scope

- Broker: RabbitMQ (first of many — Redis Streams, Kafka, SQS later)
- Inspect / search / filter DLQ messages
- Safe single-message replay (dry-run + confirm + audit)
- Failure grouping + recovery classification
- Recovery plans, dry-run and confirmed batch recovery with limits
- Local audit history

## Safety

DLQ Inspector never mutates a queue without going through the shared safety pipeline:
dry-run preview → duplicate evidence → explicit confirm → execute → audit write.
Multi-message operations require explicit limits; bulk recoveries require a plan.

## License

[MIT](LICENSE)
