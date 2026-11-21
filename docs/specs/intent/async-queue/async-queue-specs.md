# Async queue — requirements

Design: [async-queue-design.md](async-queue-design.md). Source: `src/queue-providers/AsyncQueue.ts`.

These are in addition to the [queue contract](../queue-contract/queue-contract-specs.md).

## Delivery

### ASYNC-DELIVERY-001

The async queue shall deliver a block's messages to its consumer in the order they were emitted.

### ASYNC-DELIVERY-002

If a consumer throws, then the async queue shall treat the message as failed.

## Concurrency

### ASYNC-CONCURRENCY-001

While a block has as many messages in progress as its limit, the async queue shall not deliver another message to that block.

## Closing

### ASYNC-CLOSE-001

When closed, the async queue shall cancel every pending replay and reject later emits.
