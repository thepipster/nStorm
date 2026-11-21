# BaseQueue

The base class for queue providers. You only need it to [write a provider of your own](../guide/custom-providers).

```ts
import { BaseQueue, AsyncQueue, BullMQQueue, SimpleQueue } from "nstorm";
```

The three built-in providers are exported too. They are normally selected with the [`queue` option](./options) rather than constructed directly.

## Constructor

```ts
new BaseQueue(settings: NStormSettings)
```

A provider must not open connections in its constructor.

## Methods to implement

### registerBlock

```ts
registerBlock(block: { name: string }): Promise<void>
```

Called once per block when the topology is set up, before any call to `process`.

### process

```ts
process(blockName: string, concurrency: number, callback?: QueueCallback): void
```

Begin delivering the block's messages to `callback`, with at most `concurrency` in progress at once.

```ts
type QueueCallback = (job: unknown, done: DoneCallback) => void;
type DoneCallback = (err?: Error | null) => void;
```

### emit

```ts
emit(blockName: string, message: Record<string, unknown>): Promise<void>
```

Queue a message for the named block. Resolves when the backend has accepted it; rejects if it has not, or if the block has no queue.

### clear

```ts
clear(): Promise<void>
```

Delete the messages queued for this topology's blocks, and nothing else.

### close

```ts
close(timeoutMs?: number): Promise<void>
```

Stop taking messages, wait up to `timeoutMs` (default `30000`) for those in progress, then release every connection, worker and timer. Must be safe to call more than once.

### start

```ts
start(): Promise<void>
```

Optional hook called when the topology starts.

## Protected helpers

| Member                         | Description                                                                       |
| ------------------------------ | --------------------------------------------------------------------------------- |
| `settings`                     | The topology's settings.                                                          |
| `queueName(blockName)`         | Returns `<cloudName>-<blockName>`. Use it for every queue name and backend key.   |
| `maxAttempts`                  | Total deliveries allowed per message: `replayLimit`, or `1` when `replay` is off. |
| `waitUntil(isDone, timeoutMs)` | Resolves `true` once `isDone()` holds, or `false` when the timeout passes.        |

## once

```ts
import { once } from "nstorm";

once(fn: DoneCallback): DoneCallback
```

Wraps a completion callback so that only its first call has any effect. Providers use it so that a block calling `done` twice cannot release two slots.
