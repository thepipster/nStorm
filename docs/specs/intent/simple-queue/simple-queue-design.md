# Simple queue — design

Source: `src/queue-providers/SimpleQueue.ts`. Requirements: [simple-queue-specs.md](simple-queue-specs.md). Also bound by the [queue contract](../queue-contract/queue-contract-specs.md).

A thin Redis list per block: `RPUSH` to enqueue, `BLPOP` to consume. It exists as a lower-overhead alternative to BullMQ where occasional loss is acceptable.

## Data model

Each block has one Redis list at `[<redis.prefix>:]<cloudName>-<blockName>:sq-jobs`. Each item is a JSON envelope:

```json
{ "data": { "…the message…" }, "attempt": 1 }
```

`attempt` counts deliveries, starting at 1. An item with no envelope (a bare message, as 4.0 and earlier wrote) is read as attempt 1. An item that is not valid JSON is logged and dropped; it cannot be delivered, and leaving it would block the queue.

## Consuming

Each block gets a dedicated blocking connection, because a connection inside `BLPOP` can do nothing else, and one poll loop:

1. If `limit` messages are in progress, wait until one finishes (the loop is woken by the completion, not by polling).
2. `BLPOP` with a one-second timeout, so the loop notices `close()` promptly.
3. Parse the envelope and hand the message to the consumer.

A read error is logged and retried after a second.

## Failure and replay

When a consumer fails a message that has attempts left, a timer waits `replayTime` and then pushes the envelope back with `attempt` incremented. When none are left, the message is logged and dropped.

The message is out of Redis from the moment it is popped until it is pushed back. A process that dies in that window loses it (ADR-0002).

## Connections and closing

One shared connection for writes, opened on first use, plus one blocking connection per consuming block. `close()` stops the loops, cancels pending replay timers and disconnects every connection; disconnecting aborts any `BLPOP` in flight.

## Clearing

`clear()` deletes the keys of the blocks registered with this instance in one `DEL`. It never scans or pattern-matches the keyspace: a wildcard would reach other topologies and other applications sharing the Redis.

## Open Questions

- In-flight and waiting-to-replay messages are lost on a crash (ADR-0002). Messages cancelled by `close()` while waiting to be replayed are lost too; should `close()` push them back first?
- `close()` does not wait for in-progress messages.
- A message that exhausts `replayLimit` is only logged. There is no dead-letter list.
- `SIMPLE-ENVELOPE-002` exists for upgrades from 4.0, but 4.0 used different key names, so those messages are never read. Is the fallback still worth keeping?
- A user message that itself has the shape `{ data, attempt: <number> }` and was written bare would be misread as an envelope.
