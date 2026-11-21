# BullMQ queue — design

Source: `src/queue-providers/BullMQQueue.ts`. Requirements: [bullmq-queue-specs.md](bullmq-queue-specs.md). Also bound by the [queue contract](../queue-contract/queue-contract-specs.md).

The default provider, and the only durable one. It maps the contract onto [BullMQ](https://docs.bullmq.io/): one BullMQ `Queue` and one `Worker` per block.

## Mapping

| Contract                   | BullMQ                                                                                               |
| -------------------------- | ---------------------------------------------------------------------------------------------------- |
| `registerBlock`            | `new Queue(queueName, …)` with `defaultJobOptions`: `attempts`, fixed `backoff`, `removeOnComplete`. |
| `emit`                     | `queue.add(blockName, message)`. Retry options come from the queue defaults, not per job.            |
| `process(name, limit, cb)` | `new Worker(queueName, processor, { concurrency: limit })`.                                          |
| `clear`                    | `queue.obliterate({ force: true })` for each registered queue.                                       |
| `close`                    | Close workers, then queue-event listeners, then queues.                                              |

The worker's processor returns a promise that resolves when the consumer calls `done()` and rejects when it calls `done(err)` or throws. BullMQ then owns retry, backoff and redelivery after a worker dies, which is what makes this provider at-least-once.

Failed jobs are kept (the most recent 100) for inspection; completed jobs are removed.

## Connections

The provider holds no Redis connection of its own. It passes connection _options_ to BullMQ, which opens what it needs per `Queue`, `Worker` and `QueueEvents`. `redis.user` is passed as the Redis username and `redis.prefix` as BullMQ's key prefix. `maxRetriesPerRequest: null` is required by BullMQ for workers.

Nothing is opened until `registerBlock`, so constructing a topology never touches Redis.

## Closing

Workers are closed first so that in-progress jobs finish before their queues go away. The instance's lists are emptied before closing, which makes a second `close()` a no-op.

## Debug

With `debug` on, a `QueueEvents` listener per block logs job completion and failure. It is off by default because each listener holds an extra Redis connection.

## Open Questions

- `removeOnFail: 100` and `removeOnComplete: true` are fixed. Should they be settings?
- A message that exhausts `replayLimit` stays in BullMQ's failed set; nothing surfaces it to the application.
- Each block opens at least two Redis connections (queue and worker). A large topology could share them.
- Stalled-job detection and redelivery after a worker crash rely on BullMQ defaults and have not been exercised in a test.
