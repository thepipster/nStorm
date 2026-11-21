# Queue contract — design

Source: `src/queue-providers/BaseQueue.ts`. Requirements: [queue-contract-specs.md](queue-contract-specs.md).

`BaseQueue` is the interface between the topology and whatever carries messages. The topology and blocks only ever call these methods, so they never need to know which provider is in use. This node states what **every** provider must guarantee; what an individual provider adds (durability, sharing between processes) belongs in that provider's own node.

## Methods

| Method                          | Called by          | Contract                                                                                 |
| ------------------------------- | ------------------ | ---------------------------------------------------------------------------------------- |
| `registerBlock(block)`          | topology, at setup | Make the block's queue known. Called for every block before any `process()`.             |
| `process(blockName, limit, cb)` | block, at setup    | Start delivering the block's messages to `cb(message, done)`, at most `limit` at a time. |
| `start()`                       | topology, at start | Optional hook.                                                                           |
| `emit(blockName, message)`      | block              | Enqueue for that block. Resolves when accepted, rejects when not.                        |
| `clear()`                       | topology `reset()` | Delete this topology's queued messages.                                                  |
| `close()`                       | topology `stop()`  | Stop delivering; release every connection, worker and timer.                             |

## Shared helpers

- `queueName(blockName)` returns `${cloudName}-${blockName}`. Providers build every queue name and Redis key from it, which is what keeps topologies sharing a backend apart.
- `maxAttempts` is the total number of deliveries allowed: `replayLimit`, or 1 when `replay` is off. Providers use it rather than reading the settings directly.
- `once(done)` wraps a completion callback so only its first call counts. A provider wraps `done` before doing any accounting, so a block that calls it twice cannot release two concurrency slots or schedule two retries.

## Failure and replay

A message fails when its consumer calls `done(err)` or throws. A failed message is delivered again after `replayTime` milliseconds until it has been delivered `maxAttempts` times, after which the provider logs it and gives up. How durable the wait is differs by provider and is not part of this contract (see ADR-0002 for SimpleQueue).

## Resources and lifecycle

A provider opens nothing in its constructor; connections are made at setup or first use. Everything it opens is tracked on the instance and released by `close()`. A provider never registers process signal handlers or exits the process (ADR-0001).

## Proven against

All provider behaviour is unit-tested against mocked `bullmq` and `ioredis`. On 2026-11-21 the three providers were also run end to end against a real local Redis for filtering, replay after failure and clean exit after `stop()`. Connection loss, concurrent workers and crash recovery have not been exercised against a real backend.

## Open Questions

- `QUEUE-CLOSE-002` (closing twice is harmless) is tested only for the BullMQ provider.
- `QUEUE-SIGNAL-001` and `QUEUE-CONNECT-001` are not tested for the async provider, where they hold trivially.
- `QUEUE-EMIT-002` has no async-provider test; an in-memory push cannot be refused.
- Nothing bounds the number of messages a provider will hold, and `emit()` exerts no back-pressure.
- The contract does not say whether `close()` waits for in-progress messages. BullMQ does; the simple and async providers abandon them.
