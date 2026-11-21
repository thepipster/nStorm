# Concepts

## Blocks

A **block** is a unit of work: a class that extends `BaseBolt`.

Apache Storm, which inspired nStorm, has two kinds of component: _spouts_, which are data sources, and _bolts_, which process data. nStorm has one kind. A block is a source if it has a `start()` method, a processor if it has a `process()` method, and it can be both.

| A block that…                 | Implements  | Storm would call it |
| ----------------------------- | ----------- | ------------------- |
| produces messages             | `start()`   | a spout             |
| receives and handles messages | `process()` | a bolt              |
| does both                     | both        |                     |

## Messages

A **message** is a plain object. On the Redis-backed providers it is serialised as JSON between blocks, so it should contain only JSON-compatible values: no functions, class instances, `Date` objects or `undefined`.

## Topology

A **topology** is a set of named blocks and the connections between them. You build one with an `nStorm` instance, which this documentation also calls the _cloud_.

A connection is declared on the receiving block: "this block takes input from that one". When a block emits a message, every block that takes input from it receives a copy, subject to that connection's filter.

```
                 ┌──▶ heads ──┐
   coins ────────┤            ├──▶ results
                 └──▶ tails ──┘
```

## Queues

Blocks do not call each other directly. Each block has its own **queue**; emitting a message puts it on the queue of every subscribed block, and each block takes messages off its own queue at its own pace.

That gap is what makes the rest possible:

- A slow block does not hold up the block feeding it. Messages wait in the queue.
- A message whose block fails can be put back and tried again.
- Several processes can take messages from the same queue, which is how a topology scales out.

What carries the queues is the **queue provider**. nStorm ships three, and you choose one with the `queue` option. See [Queue providers](./queue-providers).

## Lifecycle

A topology goes through these steps, once and in order:

1. **Build**: add blocks and declare their inputs.
2. **Set up**: the connections are validated and each block's queue is created. `start()` does this for you.
3. **Start**: blocks begin receiving messages and every block's `start()` is called.
4. **Stop**: sources are told to stop, messages in progress are allowed to finish, and connections are closed.

A stopped topology cannot be started again; create a new one.
