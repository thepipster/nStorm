# Writing blocks

Every block extends `BaseBolt` and overrides one or more of its methods.

## Sources

Override `start()`, which is called once when the topology starts, and send messages with `this.emit()`. Override `stop()` to clean up when the topology stops.

```ts
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
```

Every block's `start()` is called at the same time, so a source whose `start()` runs for a long time does not delay the others.

`start()` is also a good place for any set-up a block needs, such as opening a database connection, whether or not the block emits anything.

::: warning
If any block's `start()` throws or rejects, the whole topology is stopped and `cloud.start()` rejects with that error.
:::

## Processors

Override `process(message, done)`. `message` is whatever was emitted by a block this one takes input from.

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

Call `done()` when the message has been handled. Until you do, the message counts as in progress and occupies one of the block's [concurrency slots](./topologies#parallelism).

To report a failure, call `done(err)`, throw, or let the returned promise reject. All three do the same thing. See [Failures and replay](./failures).

Only the first outcome counts. If you call `done()` twice, or call it and then throw, everything after the first is ignored.

## Emitting

`this.emit(message)` sends the message to every block subscribed to this one. A block with no subscribers can still call it; nothing happens.

It returns a promise that resolves once the queue has accepted the message, and rejects if it could not, for example because Redis is unreachable.

Awaiting it is optional:

- **If you await it**, a failed send throws in your code. Inside `process()`, that fails the incoming message, which is then replayed. This is usually what you want in a processor.
- **If you do not**, the failure is logged and nothing else happens. It will not crash the process. This suits a source emitting from a timer.

```ts
await this.emit(message); // find out if it failed
void this.emit(message); // deliberately not waiting
```

Messages should be plain, JSON-compatible objects. See [Concepts](./concepts#messages).

## Helpers

`this.delay(ms)` returns a promise that resolves after the given time, for use in `start()` or `process()`.

`this.name` is the name the block was given in `addBlock`.

## A block that does both

A block can have `start()` and `process()` together. This one starts a counter and reports it for each message:

```ts
class Counter extends BaseBolt {
    private seen = 0;
    private timer?: NodeJS.Timeout;

    async start(): Promise<void> {
        this.timer = setInterval(() => {
            void this.emit({ seen: this.seen });
        }, 10_000);
    }

    async stop(): Promise<void> {
        clearInterval(this.timer);
    }

    async process(_message: unknown, done: () => void): Promise<void> {
        this.seen++;
        done();
    }
}
```

Bear in mind that state held in a block, like `seen` here, belongs to one process. If you [run several workers](./scaling), each has its own.
