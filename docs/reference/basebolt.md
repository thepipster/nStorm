# BaseBolt

The base class for every block.

```ts
import { BaseBolt } from "nstorm";

class MyBlock extends BaseBolt {
    // override start(), process() and/or stop()
}
```

## Methods to override

### process

```ts
process(message: unknown, done: (err?: Error | null) => void): Promise<void>
```

Called for each message delivered to the block. Call `done()` when finished, or `done(err)` to fail the message. Throwing, or returning a promise that rejects, also fails it.

Only the first outcome counts. The default implementation acknowledges the message.

You may give `message` a more specific type in your override, such as `Record<string, unknown>` or an interface of your own.

See [Writing blocks](../guide/blocks#processors) and [Failures and replay](../guide/failures).

### start

```ts
start(): Promise<void>
```

Called once when the topology starts. Use it to begin emitting, or for set-up. If it rejects, the topology is stopped. The default does nothing.

### stop

```ts
stop(): Promise<void>
```

Called when the topology is stopped, before messages in progress have drained. Use it to clear timers and stop emitting. The default does nothing.

## Methods to call

### emit

```ts
emit(message: Record<string, unknown>): Promise<void>
```

Sends a message to every block subscribed to this one whose filter it passes.

The promise resolves when every such block's queue has accepted the message, and rejects if any could not. It is safe not to await it: a failure is always logged and never causes an unhandled rejection.

Rejects if the block has not been added to a topology.

### input

```ts
input(sourceName: string, options?: InputOptions): this
```

Subscribes this block to the block named `sourceName`. Returns the block, so calls can be chained. Call it before the topology is set up.

```ts
interface InputOptions {
    filter?: Record<string, unknown>;
}
```

With a `filter`, a message is delivered only if every key in the filter is present on the message and strictly equal to the filter's value. See [Filtering](../guide/topologies#filtering).

A second call naming the same source is ignored.

### delay

```ts
delay(ms: number): Promise<string>
```

Resolves after `ms` milliseconds.

## Properties

| Property      | Type             | Description                                                   |
| ------------- | ---------------- | ------------------------------------------------------------- |
| `name`        | `string`         | The name given in `addBlock`. Empty until the block is added. |
| `limit`       | `number`         | Maximum messages handled at once, per process.                |
| `context`     | `nStorm \| null` | The topology the block belongs to.                            |
| `debug`       | `boolean`        | The topology's `debug` option.                                |
| `inputs`      | `InputEntry[]`   | The inputs declared with `.input()`.                          |
| `subscribers` | `Subscriber[]`   | The blocks listening to this one. Filled in at set-up.        |

Treat the properties as read-only.
