# nStorm

The topology, also called the cloud. It owns the blocks and the queue provider.

```ts
import { nStorm } from "nstorm";
```

## Constructor

```ts
new nStorm(options?)
```

`options` is an optional object; every field has a default. See [Options](./options).

Throws if `options.queue` is not a known provider. Creating a topology opens no connections.

## Methods

### addBlock

```ts
addBlock(name: string, block: BaseBolt, limit?: number): BaseBolt
```

Adds a block to the topology and returns it, so that [`.input()`](./basebolt#input) can be chained.

| Parameter | Description                                                           |
| --------- | --------------------------------------------------------------------- |
| `name`    | Unique name for the block within this topology.                       |
| `block`   | An instance of a class extending `BaseBolt`.                          |
| `limit`   | Maximum messages the block handles at once, per process. Default `1`. |

Throws if the name is already in use, or if the topology has already been set up.

### start

```ts
start(): Promise<void>
```

Starts the topology: sets it up if that has not been done, begins delivering messages, and calls every block's `start()` at the same time. Resolves when all of them have returned.

Rejects if an input names a block that does not exist, if it has already been called, or if the topology has been stopped. If a block's `start()` rejects, the topology is stopped and that error is rethrown.

### stop

```ts
stop(options?: { timeout?: number }): Promise<void>
```

Stops the topology gracefully. Calls every block's `stop()`, stops taking messages, waits up to `timeout` milliseconds (default `30000`) for messages in progress, then closes all connections.

Safe to call more than once and before `start()`; every call returns the same promise. A stopped topology cannot be started again. See [Shutting down](../guide/shutdown).

### setupTopology

```ts
setupTopology(): Promise<void>
```

Connects the blocks according to their inputs and creates their queues. Optional, since `start()` does it; call it yourself when you need to do something, usually `reset()`, between set-up and start.

Rejects if an input names a block that does not exist, in which case nothing is connected, or if it has already been called.

### reset

```ts
reset(): Promise<void>
```

Deletes any messages still queued for this topology's blocks. Call it after `setupTopology()`; before that the queues are not known and nothing is deleted.

## Properties

| Property   | Type             | Description                                                                                                   |
| ---------- | ---------------- | ------------------------------------------------------------------------------------------------------------- |
| `settings` | `NStormSettings` | The options in effect, with defaults filled in.                                                               |
| `blocks`   | `BaseBolt[]`     | The blocks added so far.                                                                                      |
| `queue`    | `BaseQueue`      | The queue provider. May be replaced before set-up; see [Writing a queue provider](../guide/custom-providers). |
