# Writing a queue provider

If none of the built-in providers fits, you can write your own by extending `BaseQueue`, for example to carry messages over SQS, RabbitMQ or NATS.

## Using a custom provider

Assign an instance to the topology's `queue` property before adding blocks:

```ts
import { nStorm } from "nstorm";
import { MyQueue } from "./MyQueue";

const cloud = new nStorm({ cloudName: "orders" });
cloud.queue = new MyQueue(cloud.settings);

cloud.addBlock(/* … */);
await cloud.start();
```

The provider that the `queue` option selected is created first and then replaced. None of the built-in providers opens a connection until the topology is set up, so the one being replaced costs nothing.

## The methods

```ts
import { BaseQueue, QueueCallback } from "nstorm";

export class MyQueue extends BaseQueue {
    async registerBlock(block: { name: string }): Promise<void> {}

    process(blockName: string, limit: number, callback?: QueueCallback): void {}

    async emit(
        blockName: string,
        message: Record<string, unknown>,
    ): Promise<void> {}

    async clear(): Promise<void> {}

    async close(timeoutMs?: number): Promise<void> {}
}
```

| Method          | Called                    | What it must do                                                                                |
| --------------- | ------------------------- | ---------------------------------------------------------------------------------------------- |
| `registerBlock` | Once per block, at set-up | Create or look up the block's queue. All blocks are registered before any `process` call.      |
| `process`       | Once per block, at set-up | Start delivering the block's messages to `callback(message, done)`, at most `limit` at a time. |
| `start`         | When the topology starts  | Optional hook.                                                                                 |
| `emit`          | Whenever a block emits    | Put the message on the named block's queue. Resolve when accepted; reject if it was not.       |
| `clear`         | `cloud.reset()`           | Delete this topology's queued messages, and nothing else.                                      |
| `close`         | `cloud.stop()`            | Stop, drain and disconnect. See below.                                                         |

## What every provider must guarantee

These are the rules the built-in providers follow. The topology and blocks rely on them.

**Names include the cloud name.** Build every queue name from `this.queueName(blockName)`, which returns `<cloudName>-<blockName>`. That keeps topologies sharing a backend apart.

**`emit` rejects when the message was not accepted.** Never swallow a failed send. A block may be awaiting the result.

**`done` counts once.** A block may call `done` twice. Wrap it before doing any accounting:

```ts
import { once } from "nstorm";

const finish = once((err) => {
    // release the slot, acknowledge or fail the message
});
callback(message, finish);
```

**Failures are replayed.** When `done` receives an error, deliver the message again after `this.settings.replayTime` milliseconds, until it has been delivered `this.maxAttempts` times. `maxAttempts` already accounts for the `replay` and `replayLimit` options.

**`callback` may throw.** Treat a synchronous throw as a failure.

**Open nothing in the constructor.** Connect at set-up or on first use, so that creating a topology has no side effects.

**`close(timeoutMs)` drains.** Stop taking messages, wait up to `timeoutMs` for those in progress, hand back to the backend anything you still hold, then release every connection and timer. It must be safe to call twice. `this.waitUntil(isDone, timeoutMs)` is provided for the wait.

**Leave the process alone.** No signal handlers and no `process.exit()`.

## Inherited helpers

| Member                          | Purpose                                                                  |
| ------------------------------- | ------------------------------------------------------------------------ |
| `this.settings`                 | The topology's [options](../reference/options), with defaults filled in. |
| `this.queueName(blockName)`     | The queue name for a block.                                              |
| `this.maxAttempts`              | Total deliveries allowed per message.                                    |
| `this.waitUntil(fn, timeoutMs)` | Resolves `true` when `fn()` holds, or `false` at the timeout.            |

## Going further

The complete, testable list of what a provider must do is in the [queue contract requirements](../specs/intent/queue-contract/queue-contract-specs), and the reasoning is in the [design notes](../specs/intent/queue-contract/queue-contract-design). The three built-in providers in [`src/queue-providers`](https://github.com/thepipster/nStorm/tree/master/src/queue-providers) are short and make good templates: `AsyncQueue` for an in-process provider, `SimpleQueue` for a networked one.
