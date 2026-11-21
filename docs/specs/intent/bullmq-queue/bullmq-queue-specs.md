# BullMQ queue — requirements

Design: [bullmq-queue-design.md](bullmq-queue-design.md). Source: `src/queue-providers/BullMQQueue.ts`.

These are in addition to the [queue contract](../queue-contract/queue-contract-specs.md).

## Delivery

### BULLMQ-DELIVERY-001

When a block is registered, the BullMQ queue shall create a queue whose jobs are attempted the permitted number of times with a fixed backoff of `replayTime` milliseconds.

### BULLMQ-DELIVERY-002

When a consumer signals success, the BullMQ queue shall complete the job.

### BULLMQ-DELIVERY-003

If a consumer signals failure or throws, then the BullMQ queue shall fail the job with that error.

## Workers

### BULLMQ-WORKER-001

When a block starts consuming, the BullMQ queue shall create a worker whose concurrency equals the block's limit.

## Redis

### BULLMQ-REDIS-001

Where a Redis username or key prefix is configured, the BullMQ queue shall pass it to BullMQ.

## Debug

### BULLMQ-DEBUG-001

Where debug is on, the BullMQ queue shall listen for and log job completion and failure events.
