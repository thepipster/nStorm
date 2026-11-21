---
name: add-queue-provider
description: Add a new queue provider (backend) to nStorm, or wire an existing one into the topology. Use when asked to support a new queue/broker such as SQS, RabbitMQ, NATS or Redis Streams.
argument-hint: <ProviderName>
---

Add a queue provider named `$ARGUMENTS`. Follow `.claude/rules/queue-providers.md` throughout.

1. **Implement** `src/queue-providers/<Name>Queue.ts` extending `BaseQueue`. Use `SimpleQueue.ts` as the structural template for a network-backed provider, `AsyncQueue.ts` for an in-process one. Implement `registerBlock`, `emit`, `process`, `clear` and `close` (and `start` if needed). Name queues with `this.queueName()`, cap deliveries with `this.maxAttempts`, and wrap consumer `done` callbacks in `once()`. Document the delivery guarantee and which settings are honoured in the class doc comment.
2. **Register** it in three places:
    - the `queue` union in `NStormSettings` (`src/BaseBolt.ts`)
    - the `switch` in the `nStorm` constructor (`src/nStorm.ts`)
    - the exports in `src/index.ts`
3. **Test** in `tests/queue-providers/<Name>Queue.test.ts`, mocking the client library. Cover at least: emit to an unregistered block rejects; a backend write failure rejects; a failed message is replayed up to `replayLimit` and not at all with `replay: false`; `close()` releases everything and stops delivery; a message emitted reaches the `process` callback; concurrency limit is respected; `done(err)` is handled; `clear()` only touches this topology's data. Add a `should use <Name>Queue for queue: "<name>"` case to `tests/nStorm.test.ts`.
4. **Document** it in `docs/guide/queue-providers.md` (comparison table and its own section), the `queue` row of `docs/reference/options.md`, and the provider table in the README. Run `npm run docs:build`.
5. **Dependencies**: a new client library must work from CommonJS. Tell the user what was added to `package.json` and why.
6. Run the `verify` skill.
