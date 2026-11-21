# Queue providers

The queue provider is what carries messages between blocks. Choose one with the `queue` option:

```ts
const cloud = new nStorm({ queue: "bullmq" }); // "bullmq" | "simple" | "async"
```

An unrecognised value throws when the topology is created.

## Comparison

|                                           | `bullmq` (default) | `simple` | `async`  |
| ----------------------------------------- | ------------------ | -------- | -------- |
| Needs Redis                               | Yes                | Yes      | No       |
| Shared between processes                  | Yes                | Yes      | No       |
| Replays a message whose block failed      | Yes                | Yes      | Yes      |
| Message in progress when the process dies | Redelivered        | **Lost** | **Lost** |
| Queued messages when the process dies     | Kept               | Kept     | **Lost** |
| Overhead per message                      | Highest            | Low      | Lowest   |

## bullmq

The default, built on [BullMQ](https://docs.bullmq.io/). A message stays in Redis until its block acknowledges it, so it survives a failed block, a crashed worker and a restart. Choose this unless you have a reason not to.

Each block uses its own BullMQ queue and worker. The most recent 100 failed jobs per block are kept in Redis, where BullMQ's tools and dashboards can show them; completed jobs are removed.

With `debug: true`, job completions and failures are logged, at the cost of one extra Redis connection per block.

## simple

A plain Redis list per block. It does much less work per message than BullMQ and so is faster, with one trade-off: a worker removes a message from Redis the moment it picks it up. If that process is killed before it finishes, the message is gone. The same applies to a failed message during the short wait before it is replayed.

A clean [`stop()`](./shutdown) does not lose messages; a crash or a forced kill can.

Choose it when throughput matters more than the occasional lost message, such as metrics or other data where the next message supersedes the last.

## async

Everything is held in memory, using [`async.queue`](https://caolan.github.io/async/v3/docs.html#queue). No Redis is needed, nothing is shared with other processes, and nothing survives the process ending.

Choose it for tests, for development, and for self-contained jobs that run in one process.

Messages are passed by reference rather than serialised. A block that modifies a message modifies the same object other blocks may hold, and values that would not survive JSON, such as a `Date`, arrive intact. Code that relies on either will behave differently on the Redis providers.

## Redis settings

The `bullmq` and `simple` providers read these:

```ts
const cloud = new nStorm({
    redis: {
        host: "redis.internal",
        port: 6379,
        user: "worker", // Redis ACL username
        password: process.env.REDIS_PASSWORD,
        prefix: "myapp", // prepended to every key nStorm writes
    },
});
```

No connection is opened when the topology is created. Connections are made when it is set up, and all of them are closed by `stop()`.

### Keys

Knowing what nStorm writes is useful if you share a Redis server or need to inspect a queue.

| Provider | Key pattern                                |
| -------- | ------------------------------------------ |
| `bullmq` | `<prefix or "bull">:<cloudName>-<block>:*` |
| `simple` | `[<prefix>:]<cloudName>-<block>:sq-jobs`   |

`cloud.reset()` deletes only the keys belonging to that topology's blocks.
