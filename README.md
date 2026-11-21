# nStorm

A simple real-time processing system for Node.js, inspired by [Storm](https://storm.apache.org/) and [SimpleStorm](https://www.npmjs.org/package/simplestorm).

## Overview

nStorm lets you write small processing blocks and chain them together into a topology. Messages travel between blocks over a queue, so you can scale out by running the same topology on more servers.

The high-level objectives are:

1. Simple processing blocks that are easy to chain together.
2. Scale by adding workers: every process running the same topology against the same Redis shares the work.
3. Automatic replay of messages. If a block fails on a message, it is delivered again.

## Installation

```sh
npm install nstorm
```

Requires Node.js 20 or later. The default queue provider needs [Redis](https://redis.io/); the in-memory `async` provider does not.

## Usage

```ts
import { nStorm, BaseBolt, Logger } from "nstorm";
```

Storm has two kinds of block: _spouts_, which are data sources, and _bolts_, which process data. nStorm has a single kind, `BaseBolt`, which can have a `start` method (a source), a `process` method (a processor), or both.

### Creating a data source

Extend `BaseBolt` and override `start()`, which is called once when the topology starts. Send messages with `this.emit(data)`, where `data` is a plain object that can be serialised as JSON.

Override `stop()` to clean up when the topology is stopped.

```ts
/**
 * Demo spout that randomly creates 'coins', either heads or tails
 */
class CoinSpout extends BaseBolt {
    private timer?: NodeJS.Timeout;

    async start(): Promise<void> {
        this.timer = setInterval(() => {
            const toss = Math.random() < 0.5 ? "heads" : "tails";
            void this.emit({ coin: toss, time: Date.now() });
        }, 1000);
    }

    async stop(): Promise<void> {
        clearInterval(this.timer);
    }
}
```

Every block's `start()` is called at the same time, so one that runs for a long time does not delay the others.

### Creating a processing block

Extend `BaseBolt` and override `process(message, done)`. `message` is whatever was emitted by a block this one takes input from.

```ts
class HeadsBolt extends BaseBolt {
    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        Logger.info("Heads ", message.coin);

        // Pass data along to any blocks listening to this one
        await this.emit(message);

        // Acknowledge
        done();
    }
}
```

Call `done()` once the message has been handled. A message counts as **failed**, and is replayed, if you call `done(err)`, if `process` throws, or if the promise it returns rejects. Only the first outcome counts, so a second call to `done` is ignored.

A block's concurrency slot is held until `done` is called, so a `process` that never calls it will eventually stall the block. To guard against that, set `processTimeout`:

```ts
const cloud = new nStorm({ processTimeout: 60_000 });
```

A message not finished within that time fails and is replayed. nStorm cannot cancel the work already under way, so the first attempt may still be running when the replay starts. Only use a timeout with blocks that are safe to run twice for the same message.

### Emitting

`this.emit(message)` sends the message to every block subscribed to this one. It returns a promise that resolves once the queue has accepted the message, and rejects if it could not.

Awaiting it is optional. If you do, you find out about a failed send and can react to it. If you do not, the failure is still logged and will not crash the process.

### Setting up a topology

Once you have your blocks, wire them up:

```ts
const cloud = new nStorm({
    redis: {
        port: 6379,
        host: "127.0.0.1",
    },
});

async function main(): Promise<void> {
    cloud.addBlock("coinTossSpout", new CoinSpout());
    cloud.addBlock("headsBolt", new HeadsBolt()).input("coinTossSpout", {
        filter: { coin: "heads" },
    });

    await cloud.start();
}

main().catch((err) => Logger.error(err));
```

| Method                          | Description                                                                                                             |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `addBlock(name, block, limit?)` | Add a block. Names must be unique. Returns the block so you can chain `.input()`. Must be called before setup.          |
| `setupTopology()`               | Connect the blocks and create their queues. May only be called once. Optional: `start()` does it if you have not.       |
| `reset()`                       | Delete any messages still queued for this topology. Call it between `setupTopology()` and `start()` for a clean start.  |
| `start()`                       | Start consuming and call every block's `start()`. If one of them fails, the topology is stopped and the error rethrown. |
| `stop({ timeout? })`            | Call every block's `stop()`, let messages in progress finish, then close all connections. See below.                    |

### Shutting down

nStorm does not install any signal handlers or exit the process for you. Call `stop()` from your own handler:

```ts
process.once("SIGTERM", () => {
    cloud
        .stop({ timeout: 20_000 })
        .catch((err) => Logger.error(err))
        .finally(() => process.exit(0));
});
```

`stop()` shuts down gracefully:

1. Every block's `stop()` is called, so sources stop emitting.
2. No new messages are taken.
3. Messages already in progress get up to `timeout` milliseconds to finish. The default is 30 seconds; choose a value shorter than the time your platform allows before it kills the process.
4. All connections are closed.

Messages that were queued but not started stay in Redis for the next worker. With the `simple` provider, messages waiting to be replayed are put back in Redis first, so a clean stop loses nothing unless the timeout expires. With the `async` provider nothing survives the process, so anything not started is discarded.

`stop()` is safe to call more than once, or before `start()`. A stopped topology cannot be started again; create a new one.

### Parallelism

By default a block processes one message at a time _per worker process_. Pass a limit as the third argument to `addBlock` to raise it:

```ts
cloud.addBlock("headsBolt", headsBolt, 5).input("coinTossSpout");
```

Here up to 5 messages are processed by `headsBolt` at once. Further messages wait in the queue until one finishes, i.e. until `done()` is called.

### Chaining inputs

A block can take input from several others:

```ts
cloud
    .addBlock("resultsBolt", resultsBolt)
    .input("tailsBolt")
    .input("headsBolt");
```

A block may only list a given source once; a repeated `.input()` for the same source is ignored.

### Filtering

An input can be filtered, so that only matching messages are delivered:

```ts
cloud.addBlock("headsBolt", headsBolt).input("coinTossSpout", {
    filter: { coin: "heads" },
});
```

A message is delivered only if **every** key in the filter is present on the message and strictly equal (`===`) to the filter's value. Here `headsBolt` receives `{ coin: "heads" }` but not `{ coin: "tails" }`, and not a message with no `coin` key at all.

### Options

Pass these to `new nStorm(options)`. All are optional.

| Option           | Default        | Description                                                                                      |
| ---------------- | -------------- | ------------------------------------------------------------------------------------------------ |
| `cloudName`      | `"stormcloud"` | Name of the topology. Namespaces its queues, so several topologies can share one Redis.          |
| `queue`          | `"bullmq"`     | Queue provider: `"bullmq"`, `"simple"` or `"async"`. Anything else throws.                       |
| `redis.host`     | `"127.0.0.1"`  | Redis host.                                                                                      |
| `redis.port`     | `6379`         | Redis port.                                                                                      |
| `redis.user`     |                | Redis ACL username.                                                                              |
| `redis.password` |                | Redis password.                                                                                  |
| `redis.prefix`   |                | Prefix for every Redis key nStorm writes.                                                        |
| `replay`         | `true`         | Deliver a failed message again. With `false`, each message is attempted once.                    |
| `replayLimit`    | `3`            | Total number of times a message is delivered before it is given up on.                           |
| `replayTime`     | `300`          | Milliseconds to wait before delivering a failed message again.                                   |
| `processTimeout` | `0`            | Fail a message whose block has not finished it within this many milliseconds. `0` waits forever. |
| `debug`          | `false`        | Log nStorm's internals.                                                                          |

### Queue providers

| Provider | Needs Redis | Shared between processes | If a block fails on a message | If the process dies mid-message |
| -------- | ----------- | ------------------------ | ----------------------------- | ------------------------------- |
| `bullmq` | Yes         | Yes                      | Replayed                      | Redelivered                     |
| `simple` | Yes         | Yes                      | Replayed                      | **Lost**                        |
| `async`  | No          | No                       | Replayed                      | **Lost**, along with the queue  |

- **`bullmq`** (default) uses [BullMQ](https://docs.bullmq.io/). The most robust choice: a message stays in Redis until its block acknowledges it.
- **`simple`** is a thin Redis list (`RPUSH`/`BLPOP`). Less overhead than BullMQ, but a message is removed from Redis as soon as a worker picks it up, so one that is in progress, or waiting to be replayed, when the process dies is gone.
- **`async`** keeps everything in memory using [`async.queue`](https://caolan.github.io/async/v3/docs.html#queue). Good for tests, development and single-process jobs.

No Redis connection is opened when you construct `nStorm`; connections are made when the topology is set up and used.

### Adding workers

With the `bullmq` or `simple` provider, run the same topology (same `cloudName`, same block names, same Redis) in as many processes or servers as you like. Each message is handled by one of them.

Note that every process also runs every block's `start()`, so a source block will emit in each of them. If you want a single source, give the other processes a topology whose source block has no `start()`.

### Logging

`Logger` is a small wrapper around [winston](https://github.com/winstonjs/winston) that nStorm uses internally and exports for convenience. It writes to the console with `debug`, `info` (alias `log`), `warn` and `error` (alias `fatal`) methods.

```ts
Logger.setLevel("warn"); // "error" | "warn" | "info" | "debug"
Logger.stackTraces = false; // don't append the calling file and line
```

The default level is `info`, or the value of the `NSTORM_LOG_LEVEL` environment variable. The level is shared by the whole process, and nStorm never changes it.

## Upgrading from 4.0

- **Filters are stricter.** Every key in a filter must now be present and equal. Previously only the first key found was checked, and a message missing the key was let through.
- **Shutdown is yours.** nStorm no longer handles `SIGINT`/`SIGTERM` or calls `process.exit()`. Call `cloud.stop()` yourself. It waits (30 seconds by default) for messages in progress to finish.
- **Failures in an `async process()` are now caught** and fail the message, rather than surfacing as an unhandled rejection.
- **`emit()` returns a promise**, on blocks and on queue providers. A custom queue provider's `emit(blockName, message)` should return a promise, and no longer receives the settings as a third argument. Providers can also implement `close()`.
- **`setupTopology()` and `start()` may only be called once**, and blocks cannot be added after setup.
- **An unknown `queue` value throws** instead of quietly using the `async` provider.
- **`simple` provider keys changed** from `<block>:sq-jobs` to `<cloudName>-<block>:sq-jobs`, and messages are stored in a wrapper that tracks attempts. Messages left in the old keys are not picked up, and messages in the old format are discarded; drain them before upgrading. `reset()` now deletes only this topology's keys.
- **The `simple` and `async` providers now replay** failed messages, honouring `replay`, `replayLimit` and `replayTime`.
- **The default log level is `info`**, was `debug`. The `debug` option does not affect the log level.
- **New `processTimeout` option**, off by default.
- **Removed** because they did nothing: the `metaLogging` option, `BaseBolt.ack()` and `BaseBolt.threads`.
- A block that does not override `process()` now acknowledges messages instead of leaving them pending.

## Development

```sh
npm test            # run the tests (no Redis needed)
npm run check       # formatting, lint, typecheck and tests
npm run build       # compile to dist/
npx tsx example/test.ts   # in-memory example
npm run example     # BullMQ example, needs Redis on 127.0.0.1:6379
```

## Contributing

Contributions are welcome. Please [file issues](https://github.com/thepipster/nStorm/issues) and submit [pull requests](https://github.com/thepipster/nStorm/pulls), or contact me at mike@arsenicsoup.com.

## License

The MIT License (MIT)

Copyright (c) 2014 Mike Pritchard

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
