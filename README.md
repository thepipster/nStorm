# nStorm

A simple real-time processing system for Node.js, inspired by [Storm](https://storm.apache.org/) and [SimpleStorm](https://www.npmjs.org/package/simplestorm).

**[Documentation](https://thepipster.github.io/nStorm/)** · [Getting started](https://thepipster.github.io/nStorm/guide/getting-started) · [API reference](https://thepipster.github.io/nStorm/reference/nstorm) · [Upgrading from 4.0](https://thepipster.github.io/nStorm/guide/upgrading)

## Overview

nStorm lets you write small processing blocks and chain them together into a topology. Messages travel between blocks over a queue, so you can scale out by running the same topology on more servers.

- **Small blocks, wired together.** A block is a class with a `start` method (a source), a `process` method (a processor), or both.
- **Replay on failure.** A message whose block fails is delivered again, up to a limit you set.
- **Scale by adding workers.** Every process running the same topology against the same Redis shares the work.
- **Pluggable queues.** BullMQ for durability, a lightweight Redis list for throughput, or in-memory for tests.
- **Graceful shutdown.** Stopping lets messages in progress finish. The library never takes over your process.

## Installation

```sh
npm install nstorm
```

Requires Node.js 20 or later. The default queue provider needs [Redis](https://redis.io/); the in-memory `async` provider does not.

## Example

```ts
import { nStorm, BaseBolt, Logger } from "nstorm";

// A source: emits a coin toss every second
class CoinSpout extends BaseBolt {
    private timer?: NodeJS.Timeout;

    async start(): Promise<void> {
        this.timer = setInterval(() => {
            const coin = Math.random() < 0.5 ? "heads" : "tails";
            void this.emit({ coin, time: Date.now() });
        }, 1000);
    }

    async stop(): Promise<void> {
        clearInterval(this.timer);
    }
}

// A processor: receives messages and acknowledges them
class HeadsBolt extends BaseBolt {
    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        Logger.info("Heads ", message.coin);
        done();
    }
}

const cloud = new nStorm({
    redis: { host: "127.0.0.1", port: 6379 },
});

cloud.addBlock("coins", new CoinSpout());
cloud.addBlock("heads", new HeadsBolt()).input("coins", {
    filter: { coin: "heads" },
});

cloud.start().catch((err) => Logger.error(err));

// nStorm leaves process signals to you
process.once("SIGTERM", () => {
    cloud.stop().finally(() => process.exit(0));
});
```

## Queue providers

Choose one with the `queue` option.

| Provider           | Needs Redis | Shared between processes | If a block fails on a message | If the process dies mid-message |
| ------------------ | ----------- | ------------------------ | ----------------------------- | ------------------------------- |
| `bullmq` (default) | Yes         | Yes                      | Replayed                      | Redelivered                     |
| `simple`           | Yes         | Yes                      | Replayed                      | **Lost**                        |
| `async`            | No          | No                       | Replayed                      | **Lost**, along with the queue  |

## Documentation

The full documentation is at **<https://thepipster.github.io/nStorm/>**:

- [Writing blocks](https://thepipster.github.io/nStorm/guide/blocks) and [wiring them together](https://thepipster.github.io/nStorm/guide/topologies), including filters and parallelism
- [Failures and replay](https://thepipster.github.io/nStorm/guide/failures)
- [Shutting down](https://thepipster.github.io/nStorm/guide/shutdown)
- [Queue providers](https://thepipster.github.io/nStorm/guide/queue-providers) and [scaling out](https://thepipster.github.io/nStorm/guide/scaling)
- [All options](https://thepipster.github.io/nStorm/reference/options)
- [Upgrading from 4.0](https://thepipster.github.io/nStorm/guide/upgrading), which lists the breaking changes in version 5

## Development

```sh
npm test                  # run the tests (no Redis needed)
npm run check             # formatting, lint, typecheck and tests
npm run build             # compile to dist/
npm run docs:dev          # preview the documentation site locally
npx tsx example/test.ts   # in-memory example
npm run example           # BullMQ example, needs Redis on 127.0.0.1:6379
```

The documentation site is built from [`docs/`](docs/) and published to GitHub Pages when `master` changes.

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
