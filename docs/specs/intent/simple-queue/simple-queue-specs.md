# Simple queue — requirements

Design: [simple-queue-design.md](simple-queue-design.md). Source: `src/queue-providers/SimpleQueue.ts`.

These are in addition to the [queue contract](../queue-contract/queue-contract-specs.md).

## Keys

### SIMPLE-KEY-001

Where a Redis key prefix is configured, the simple queue shall begin every key it uses with that prefix.

### SIMPLE-CLEAR-001

When asked to clear, the simple queue shall delete the keys of its registered blocks without scanning the keyspace.

## Envelope

### SIMPLE-ENVELOPE-001

When a message is emitted, the simple queue shall store it together with a delivery count of 1.

### SIMPLE-ENVELOPE-002

When a queued item carries no delivery count, the simple queue shall treat it as a message on its first delivery.

### SIMPLE-ENVELOPE-003

If a queued item cannot be parsed, then the simple queue shall discard it and continue with the next item.

## Consuming

### SIMPLE-CONCURRENCY-001

While a block has as many messages in progress as its limit, the simple queue shall not take another message for that block.

## Replay

### SIMPLE-REPLAY-001

When a message fails and may be delivered again, the simple queue shall put it back on its queue with the delivery count increased by one.

## Redis

### SIMPLE-REDIS-001

Where a Redis username or password is configured, the simple queue shall connect with it.
