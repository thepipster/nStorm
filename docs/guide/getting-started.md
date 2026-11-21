# Getting started

nStorm lets you write small processing blocks and chain them together into a _topology_. Messages travel between blocks over a queue, so you can scale out by running the same topology in more processes.

## Install

```sh
npm install nstorm
```

nStorm needs Node.js 20 or later.

The default queue provider needs a [Redis](https://redis.io/) server. If you just want to try things out, the in-memory `async` provider used below needs nothing else installed. See [Queue providers](./queue-providers) for the differences.

## A first topology

This topology tosses a coin once a second and logs the heads.

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
        Logger.info("Got", message.coin);
        done();
    }
}

const cloud = new nStorm({ queue: "async" });

cloud.addBlock("coins", new CoinSpout());
cloud.addBlock("heads", new HeadsBolt()).input("coins", {
    filter: { coin: "heads" },
});

await cloud.start();

// nStorm leaves process signals to you
process.once("SIGINT", () => {
    cloud.stop().finally(() => process.exit(0));
});
```

What is happening:

1. `CoinSpout` has a `start()` method, so it acts as a source. It calls `this.emit()` to send a message.
2. `HeadsBolt` has a `process()` method, so it receives messages. It calls `done()` when it has finished with each one.
3. `addBlock` gives each block a name, and `.input("coins", …)` subscribes `heads` to what `coins` emits, keeping only messages where `coin` is `"heads"`.
4. `start()` connects everything and calls each block's `start()`.
5. `stop()` shuts the topology down cleanly.

## Using Redis

To run the same topology on the default, durable provider, drop the `queue` option and point it at your Redis:

```ts
const cloud = new nStorm({
    redis: { host: "127.0.0.1", port: 6379 },
});
```

Nothing else changes.

## Next steps

- [Concepts](./concepts) explains blocks, topologies and messages.
- [Writing blocks](./blocks) covers sources, processors and emitting in detail.
- [Failures and replay](./failures) explains what happens when a block fails.
- [Options](../reference/options) lists every setting.
