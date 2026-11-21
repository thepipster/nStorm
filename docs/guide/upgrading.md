# Upgrading from 4.0

Version 5 fixes a number of bugs in ways that change behaviour. Go through this list before upgrading.

## Things you must act on

### Shut down yourself

nStorm no longer handles `SIGINT` or `SIGTERM`, and no longer calls `process.exit()`. A program that relied on it to exit on Ctrl-C will now keep running. Add a handler that calls `cloud.stop()`:

```ts
process.once("SIGTERM", () => {
    cloud.stop().finally(() => process.exit(0));
});
```

`stop()` waits, 30 seconds by default, for messages in progress to finish. See [Shutting down](./shutdown).

### Check your filters

A filter now requires **every** key to be present on the message and equal. In 4.0 only the first key found was compared, and a message that lacked the key altogether was let through.

```ts
.input("coins", { filter: { coin: "heads" } })
```

| Message             | 4.0           | 5.0           |
| ------------------- | ------------- | ------------- |
| `{ coin: "heads" }` | Delivered     | Delivered     |
| `{ coin: "tails" }` | Not delivered | Not delivered |
| `{ other: 1 }`      | Delivered     | Not delivered |

Also check the syntax. The filter goes under a `filter` key. Passing `{ coin: "heads" }` directly as the second argument to `.input()`, as older documentation showed, has no effect.

### Drain `simple` queues first

The `simple` provider's Redis keys changed from `<block>:sq-jobs` to `<cloudName>-<block>:sq-jobs`, and messages are now stored in a wrapper that records the attempt count. Messages left under the old keys will not be picked up, and items in the old format are discarded. Let the old version empty its queues before deploying the new one.

### Remove `metaLogging`, `ack()` and `threads`

The `metaLogging` option, `BaseBolt.ack()` and `BaseBolt.threads` did nothing and have been removed. TypeScript will flag any use.

### Fix an invalid `queue` value

An unrecognised `queue` option now throws. Previously it silently selected the in-memory provider.

## Things that behave differently

- **Errors in an async `process()` are caught.** A `process()` that throws or rejects now fails the message, which is then replayed. Previously it surfaced as an unhandled rejection and could end the process.
- **`done` only counts once.** Extra calls are ignored.
- **`emit()` returns a promise.** It rejects if the message could not be queued. Existing calls that ignore the result keep working and cannot crash the process. See [Emitting](./blocks#emitting).
- **`simple` and `async` now replay failed messages**, honouring `replay`, `replayLimit` and `replayTime`. Previously only BullMQ did.
- **`reset()` on the `simple` provider** deletes only that topology's keys. It used to delete every key ending in `:sq-jobs` on the server.
- **`setupTopology()` and `start()` can only be called once**, and blocks cannot be added after set-up. `start()` now runs set-up for you if you have not.
- **Blocks start together.** One block's long-running `start()` no longer delays the others. If any `start()` fails, the topology is stopped.
- **A block with no `process()` acknowledges messages** instead of leaving them pending for ever.
- **No Redis connection is made when a topology is created**, only when it is set up.
- **The default log level is `info`**, not `debug`. The `debug` option no longer changes it. See [Logging](./logging).
- **Node.js 20 or later is required.**

## New in 5.0

- [`cloud.stop()`](./shutdown) and a `stop()` hook on blocks.
- The [`processTimeout`](./failures#processing-timeout) option.
- The `redis.user` and `redis.prefix` options now take effect.

## Custom queue providers

If you wrote your own provider:

- `emit(blockName, message)` should be `async` and reject on failure. It no longer receives the settings as a third argument; use `this.settings`.
- Implement `close(timeoutMs)`.
- Remove any signal handling.

See [Writing a queue provider](./custom-providers).
