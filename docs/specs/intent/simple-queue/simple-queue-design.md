# Simple queue — design

Source: `src/queue-providers/SimpleQueue.ts`. Requirements: [simple-queue-specs.md](simple-queue-specs.md). Also bound by the [queue contract](../queue-contract/queue-contract-specs.md).

A thin Redis list per block: `RPUSH` to enqueue, `BLPOP` to consume. It exists as a lower-overhead alternative to BullMQ where occasional loss is acceptable.

## Data model

Each block has one Redis list at `[<redis.prefix>:]<cloudName>-<blockName>:sq-jobs`. Each item is a JSON envelope:

```json
{ "data": { "…the message…" }, "attempt": 1 }
```

`attempt` counts deliveries, starting at 1. An item that is not valid JSON, or is not an envelope, is logged and dropped; it cannot be delivered, and leaving it would block the queue.

## Consuming

Each block gets a dedicated blocking connection, because a connection inside `BLPOP` can do nothing else, and one poll loop:

1. If `limit` messages are in progress, wait until one finishes (the loop is woken by the completion, not by polling).
2. `BLPOP` with a one-second timeout, so the loop notices `close()` promptly.
3. Parse the envelope and hand the message to the consumer.

A read error is logged and retried after a second.

## Failure and replay

When a consumer fails a message that has attempts left, a timer waits `replayTime` and then pushes the envelope back with `attempt` incremented. When none are left, the message is logged and dropped.

The message is out of Redis from the moment it is popped until it is pushed back. A process that dies in that window loses it (ADR-0002). A clean close does not: see below.

## Connections and closing

One shared connection for writes, opened on first use, plus one blocking connection per consuming block.

`close(timeoutMs)` works in this order (ADR-0004):

1. Mark every poll loop inactive and wait for the loops to exit. A `BLPOP` already in flight is allowed to return, which takes at most its one-second timeout, because aborting it by disconnecting could lose a message Redis had already popped. If it does return a message, that message is pushed back to the _front_ of the list, undelivered.
2. Push every message that is waiting out its replay delay back to Redis now, with its attempt incremented.
3. Wait up to `timeoutMs` for messages in progress. One that fails during this wait is pushed back at once rather than after `replayTime`.
4. Disconnect everything. Messages still in progress are lost and a warning says how many.

After close the instance refuses to reconnect; a late `emit()` or `clear()` rejects instead of opening a connection nobody will close.

## Clearing

`clear()` deletes the keys of the blocks registered with this instance in one `DEL`. It never scans or pattern-matches the keyspace: a wildcard would reach other topologies and other applications sharing the Redis.

## Open Questions

- Messages in progress are still lost on a crash, or when a drain times out (ADR-0002).
- Closing always waits for the blocking read in flight, up to one second, even when idle.
- A message that exhausts `replayLimit` is only logged. There is no dead-letter list.
- A replayed message goes to the back of the queue, so order is not preserved across a failure.
