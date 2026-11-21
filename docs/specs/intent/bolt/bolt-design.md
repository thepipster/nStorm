# Bolt — design

Source: `src/BaseBolt.ts`. Requirements: [bolt-specs.md](bolt-specs.md).

`BaseBolt` is the base class for every block in a topology. A block may be a source (overrides `start()`), a processor (overrides `process()`), or both.

## Data model

| Field         | Meaning                                                                                |
| ------------- | -------------------------------------------------------------------------------------- |
| `name`        | Unique name within the topology, set by `nStorm.addBlock()`.                           |
| `limit`       | Maximum messages this block processes at once per worker process. Default 1.           |
| `inputs`      | Blocks this one listens to, each with optional `InputOptions`. Declared by `.input()`. |
| `subscribers` | Blocks listening to this one. Derived from every block's `inputs` by the topology.     |
| `context`     | The owning `nStorm`, or `null` until the block is added to one.                        |

`inputs` is what the user declares; `subscribers` is its inverse and is rebuilt by the topology at setup. A block never edits its own `subscribers`.

## Emitting

`emit(message)` selects the subscribers whose filter the message passes and hands the message to the queue provider once per selected subscriber, concurrently.

A filter passes when every key it lists is an own property of the message and strictly equal to the filter's value. No filter passes everything. Comparison is `===`, so nested objects never match; filters are for scalar routing keys.

The returned promise rejects on failure, with a logging `catch` attached internally so that ignoring it cannot crash the process (ADR-0003).

## Processing

`_init()` registers a consumer with the queue provider. For each message it calls the block's `process(message, done)` and translates the outcome into a single completion:

- `done()` — success.
- `done(err)`, a synchronous throw, or a rejected promise — failure with that error. A non-`Error` rejection value is wrapped in an `Error`.

`done` is wrapped in `once()`, so whichever outcome arrives first is the only one the provider sees. A `process()` that calls `done()` and then throws has succeeded; the later error is logged and otherwise ignored.

### Timeout

With the `processTimeout` setting above zero, a timer starts with each message and fails it if the block has not completed it in time. This frees the concurrency slot and lets the message be replayed.

It cannot cancel the block's work. JavaScript offers no way to stop a running async function, so the original `process()` carries on, and its eventual `done` is ignored. A replay can therefore run while the first attempt is still going. That is why the timeout is off by default and blocks that use it must tolerate a message being handled twice.

The base `process()` acknowledges immediately, so a source-only block that is sent a message by mistake does not hold a concurrency slot forever.

## Lifecycle hooks

`start()` and `stop()` are no-ops for subclasses to override. `stop()` is where a source clears its timers (ADR-0001).

## Open Questions

- With `processTimeout` off (the default), a `process()` that never completes still holds its slot forever. Should a generous default replace "off" in a later major version?
- The timeout is one value for the whole topology. A per-block override would suit topologies that mix fast and slow blocks.
- `emit()` logs a failure even when the caller also handles the rejection, so it can be reported twice.
- If one subscriber's queue rejects, the others may already have accepted the message. A caller that retries the emit will duplicate it for those subscribers.
