# Wiring blocks together

## Adding blocks

Create an `nStorm` instance and add blocks to it by name. Names must be unique within the topology.

```ts
const cloud = new nStorm();

cloud.addBlock("coins", new CoinSpout());
cloud.addBlock("heads", new HeadsBolt()).input("coins");

await cloud.start();
```

`addBlock` returns the block, so `.input()` can be chained onto it. All blocks must be added before the topology is set up or started.

Blocks can be added in any order. A block may name an input that has not been added yet, as long as it exists by the time the topology starts. If it does not, `start()` rejects and names the missing block.

## Inputs

`.input(name)` subscribes a block to everything the named block emits. A block can take input from several others:

```ts
cloud.addBlock("results", new ResultsBolt()).input("heads").input("tails");
```

Several blocks can also take input from the same source. Each receives its own copy of every message.

A block may only list a given source once. A second `.input()` naming the same source is ignored, including its filter.

## Filtering

An input can carry a filter, so that only matching messages are delivered:

```ts
cloud.addBlock("heads", new HeadsBolt()).input("coins", {
    filter: { coin: "heads" },
});
```

A message is delivered only if **every** key in the filter is present on the message and strictly equal (`===`) to the filter's value.

| Filter                          | Message                           | Delivered? |
| ------------------------------- | --------------------------------- | ---------- |
| `{ coin: "heads" }`             | `{ coin: "heads", n: 1 }`         | Yes        |
| `{ coin: "heads" }`             | `{ coin: "tails" }`               | No         |
| `{ coin: "heads" }`             | `{ n: 1 }`                        | No         |
| `{ coin: "heads", side: "up" }` | `{ coin: "heads", side: "down" }` | No         |

Because the comparison is strict equality, filters work on simple values such as strings, numbers and booleans. A filter value that is an object or array never matches.

Filtering happens when the message is emitted, so a filtered-out message is never queued.

## Parallelism

By default a block handles one message at a time in each worker process. Pass a limit as the third argument to `addBlock` to raise it:

```ts
cloud.addBlock("heads", new HeadsBolt(), 5).input("coins");
```

Here up to 5 messages are in progress in `heads` at once. Further messages wait in its queue until one finishes, that is, until `done()` is called.

The limit is per process. Three workers with a limit of 5 handle up to 15 messages at once between them.

## Setting up and starting

`cloud.start()` does everything needed. If you want to do something between connecting the blocks and starting them, call `setupTopology()` yourself first. The usual reason is `reset()`, which deletes any messages left in this topology's queues from a previous run:

```ts
await cloud.setupTopology();
await cloud.reset();
await cloud.start();
```

`reset()` only knows about the queues once the topology has been set up, so calling it earlier clears nothing.

`setupTopology()` and `start()` can each be called only once.

## Several topologies

Each topology has a `cloudName`, which namespaces its queues. Give each topology that shares a Redis server its own name:

```ts
const orders = new nStorm({ cloudName: "orders" });
const emails = new nStorm({ cloudName: "emails" });
```

Two topologies with the same `cloudName` and the same block names share queues. That is exactly how [scaling out](./scaling) works, so only do it on purpose.
