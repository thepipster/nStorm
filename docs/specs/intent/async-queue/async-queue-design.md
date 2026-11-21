# Async queue — design

Source: `src/queue-providers/AsyncQueue.ts`. Requirements: [async-queue-specs.md](async-queue-specs.md). Also bound by the [queue contract](../queue-contract/queue-contract-specs.md).

An in-process provider: one [`async.queue`](https://caolan.github.io/async/v3/docs.html#queue) per block, held in memory. It needs no Redis and ignores the `redis` settings. It is for tests, development and single-process jobs; nothing is shared between processes and everything queued is lost when the process exits.

## Data model

Each queued task is `{ message, attempt }`, with `attempt` starting at 1. Messages are passed by reference, not serialised, so a consumer that mutates a message mutates the object the emitter still holds, and a non-JSON value survives here but would not on a Redis provider.

A block's queue is created by `process()`, not `registerBlock()`. `emit()` to a block that has no queue rejects.

## Delivery

`async.queue` hands tasks to the consumer in the order they were pushed, at most `limit` at a time. `emit()` resolves as soon as the task is pushed, not when it has been processed, matching the other providers.

A consumer that throws synchronously is treated as having failed the message.

## Failure and replay

When a task fails with attempts left, a timer waits `replayTime` and pushes it again with `attempt` incremented. When none are left it is logged and dropped. Timers are tracked so they can be cancelled.

## Clearing and closing

`clear()` cancels pending replays and removes queued tasks. `close()` does the same and discards the queues, so a later `emit()` rejects as for an unregistered block.

## Open Questions

- Messages are not serialised, so behaviour can differ from the Redis providers (shared references, non-JSON values). Should the async provider clone through JSON to match?
- The queue is unbounded and `emit()` applies no back-pressure, so a fast source can exhaust memory.
- `close()` abandons in-progress messages rather than waiting for them.
