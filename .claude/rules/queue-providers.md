---
paths:
    - "src/queue-providers/**/*.ts"
    - "tests/queue-providers/**/*.ts"
---

# Queue provider rules

- Every provider extends `BaseQueue` and keeps its method signatures. A new capability goes on `BaseQueue` first (with a no-op default) so `nStorm` never needs to know which provider it has.
- Every queue name and every Redis key must include `settings.cloudName`, built with `this.queueName()`, so two topologies sharing one Redis cannot see each other's messages. `clear()` may only touch keys belonging to this topology — never a bare wildcard, and prefer `SCAN` over `KEYS`.
- Every Redis connection, worker, timer and poll loop a provider opens must be tracked on the instance and released by `close(timeoutMs)`, which must be safe to call twice.
- `close(timeoutMs)` drains (ADR-0004): stop taking messages, wait for those in progress with `this.waitUntil()` for at most `timeoutMs`, hand back to the backend anything still held, then disconnect. After close the provider must not reconnect. Open nothing in the constructor. No orphaned connections.
- Honour the settings you are given: `redis.host/port/password/user/prefix`, `replay`, `replayLimit`, `replayTime` (use `this.maxAttempts`). If a provider cannot support a setting (e.g. no replay), say so in its class doc comment.
- A consumer's `done` callback may arrive late, twice, or never. Wrap it in `once()` before doing any accounting.
- `emit()` is `async`: it resolves when the backend has accepted the message and rejects when it has not. Never swallow an enqueue error.
- State the delivery guarantee (at-most-once / at-least-once) in the provider's class doc comment and keep it true when changing ack or pop logic.
- Gate informational logging behind `settings.debug`; errors are always logged.
- Do not add `process.exit()` or new `process.on(signal)` handlers — this is a library and the host application owns the process lifecycle.
