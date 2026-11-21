# ADR-0002: SimpleQueue stays at-most-once for in-flight messages

## Status

Accepted (2026-11-21)

## Context

`SimpleQueue` takes a message with `BLPOP`, which removes it from Redis before the block has processed it. In 4.0 that meant a message was lost if its block failed for any reason. Fixing the common case (the block reports failure) is cheap: re-queue the message with an attempt counter. Fixing the rare case (the process dies while a message is in progress, or while it waits out `replayTime` before a retry) is not. It needs a per-worker "processing" list, a stable worker identity that survives restarts, and a reaper that can tell a dead worker's messages from a live one's. Recovering naively at startup would steal messages that another live worker is still processing.

BullMQ already provides all of that and is the default provider.

## Decision

`SimpleQueue` replays a message whose block calls `done(err)`, throws or rejects, honouring `replay`, `replayLimit` and `replayTime`. It does not protect a message that is in progress, or waiting in memory to be replayed, when the process dies: that message is lost. This is stated in the class comment and in the README's provider table, which point to BullMQ for durable delivery.

## Consequences

Good:

- `SimpleQueue` stays small and cheap, which is its reason to exist next to BullMQ.
- Ordinary failures are now retried, so the `replay*` settings mean the same thing on every provider.

Bad:

- A crash or a kill during processing still loses up to `limit` messages per block, plus any waiting to be replayed.
- The replay delay is held in a timer in the worker's memory, so the window of exposure grows with `replayTime`.

## Alternatives considered

- **`BLMOVE` to a per-worker processing list with crash recovery.** The correct design, but it is most of a job queue and duplicates what BullMQ does better.
- **Re-queue failures immediately, with no in-memory delay.** Narrows the crash window but ignores `replayTime` and spins hot on a message that keeps failing.
- **Remove `SimpleQueue`.** Considered; kept because a low-overhead Redis option is useful where occasional loss is acceptable.
