# ADR-0004: Stopping drains in-progress messages, bounded by a timeout

## Status

Accepted (2026-11-21)

## Context

ADR-0001 made shutdown an explicit `stop()`. As first written, `stop()` closed the queue provider immediately. On the simple and async providers that abandoned every message a block was working on, and on the simple provider it also cancelled the timers holding messages that were waiting to be replayed, so those were lost too. A planned stop is by far the most common way a worker ends (every deploy sends `SIGTERM`), so the library was losing messages in the ordinary case, not only in a crash.

The opposite extreme, waiting for work to finish with no limit, is what BullMQ's `worker.close()` does. A block that never calls `done` would then hang shutdown forever, and orchestrators kill a process that overruns its grace period anyway.

## Decision

`nStorm.stop({ timeout })` and `BaseQueue.close(timeoutMs)` drain:

1. Stop taking new messages.
2. Wait for messages in progress to finish, for at most the timeout (default 30 seconds).
3. Release every connection, worker and timer, whether or not everything finished.

Each provider also returns to its backend whatever it can still reach before disconnecting. The simple provider pushes messages waiting for replay straight back to Redis, pushes back at once any message that fails during the drain, and hands back a message that a blocking read returns after closing began. BullMQ workers still busy at the timeout are force-closed, and BullMQ redelivers their jobs.

`stop()` is idempotent and a stopped topology cannot be restarted.

This refines ADR-0002 rather than superseding it: the simple provider is still at-most-once for a message in progress when the process _dies_, or when a drain times out. What changes is that a clean stop no longer loses anything it had time to finish or return.

## Consequences

Good:

- A normal deploy no longer drops messages on any provider.
- The behaviour is the same contract on all three providers.
- Shutdown time is bounded and the bound is the caller's choice.

Bad:

- `stop()` can now take as long as the timeout, so a host must choose one shorter than its own kill deadline. The 30 second default matches Kubernetes' default grace period, which leaves no margin for a host that does other cleanup after `stop()`.
- On the simple provider, shutdown always waits for the blocking read in flight to return, up to one second, even when nothing is in progress.
- Messages already queued but not started are not processed during the drain. On the async provider, which has no backend, they are lost.
- A message that fails during the drain is put back without waiting `replayTime`.

## Alternatives considered

- **Close immediately** (the previous behaviour). Simple and fast, but loses work on every deploy.
- **Wait without limit.** No loss, but one stuck block makes the process unkillable except by force, which then loses everything.
- **Disconnect the blocking connection to abort its read at once.** Saves up to a second, but Redis may already have popped a message whose reply is then lost with the socket.
- **Also finish everything already queued before stopping.** Unbounded for a busy queue, and pointless for Redis-backed providers, where another worker will take it.
