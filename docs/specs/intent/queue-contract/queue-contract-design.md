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
| `close(timeoutMs)`              | topology `stop()`  | Stop taking messages, wait for those in progress up to the timeout, release everything.  |

## Shared helpers

- `queueName(blockName)` returns `${cloudName}-${blockName}`. Providers build every queue name and Redis key from it, which is what keeps topologies sharing a backend apart.
- `maxAttempts` is the total number of deliveries allowed: `replayLimit`, or 1 when `replay` is off. Providers use it rather than reading the settings directly.
- `once(done)` wraps a completion callback so only its first call counts. A provider wraps `done` before doing any accounting, so a block that calls it twice cannot release two concurrency slots or schedule two retries.

## Failure and replay

A message fails when its consumer calls `done(err)` or throws. A failed message is delivered again after `replayTime` milliseconds until it has been delivered `maxAttempts` times, after which the provider logs it and gives up. How durable the wait is differs by provider and is not part of this contract (see ADR-0002 for SimpleQueue).

## Resources and lifecycle

A provider opens nothing in its constructor; connections are made at setup or first use. Everything it opens is tracked on the instance and released by `close()`.

`close(timeoutMs)` drains: stop taking messages, wait for the ones in progress for at most `timeoutMs` (default 30 seconds, `DEFAULT_CLOSE_TIMEOUT`), then release everything regardless. Before disconnecting, a provider returns to its backend whatever it still holds. It is idempotent, and a provider is not used again after it (ADR-0004). `waitUntil()` is the shared helper for the wait. A provider never registers process signal handlers or exits the process (ADR-0001).

## Proven against

All provider behaviour is unit-tested against mocked `bullmq` and `ioredis`. On 2026-11-21 the three providers were also run end to end against a real local Redis for filtering, replay after failure, the processing timeout, draining a message in progress on `stop()`, and clean process exit afterwards. Connection loss, concurrent workers, crash recovery and a drain that times out have not been exercised against a real backend.

## Open Questions

- `QUEUE-SIGNAL-001` and `QUEUE-CONNECT-001` are not tested for the async provider, where they hold trivially.
- `QUEUE-EMIT-002` has no async-provider test; an in-memory push cannot be refused.
- Nothing bounds the number of messages a provider will hold, and `emit()` exerts no back-pressure.
- Messages queued but not yet started are left alone by a drain. Whether a provider should offer "finish the queue" as well as "finish what is in progress" is undecided.
