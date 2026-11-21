# Topology — design

Source: `src/nStorm.ts`. Requirements: [topology-specs.md](topology-specs.md).

`nStorm` (the "cloud") owns a set of blocks and one queue provider, and moves through a one-way lifecycle.

## Settings

Options are merged over defaults one level deep (`redis` is merged separately). A key explicitly set to `undefined` is treated as absent, so it cannot mask a default. The `queue` setting selects the provider; an unrecognised value throws from the constructor rather than falling back, because a typo that silently selected the in-memory provider would lose durability without any sign.

Constructing a topology opens no connections.

## Lifecycle

```
constructed ──addBlock()*──▶ setupTopology() ──▶ start() ──▶ stop()
```

| Phase | What happens                                                                                                 |
| ----- | ------------------------------------------------------------------------------------------------------------ |
| Build | `addBlock(name, block, limit)` registers blocks. Names are unique.                                           |
| Setup | Validate every input, derive `subscribers`, register every block's queue, then start every block's consumer. |
| Start | Run setup if it has not been run, call the provider's `start()`, then call every block's `start()` together. |
| Stop  | Call every block's `stop()`, then the provider's `close(timeout)`, which drains.                             |

Setup validates all inputs before changing anything, so a topology with a bad input is left with no subscribers wired. It registers all queues before starting any consumer, so a block that emits from its consumer always finds its subscribers' queues.

Setup and start each run at most once. Running setup twice would append a second copy of every subscriber and create a second set of workers, delivering every message twice; a second call is therefore an error, as is adding a block after setup.

Blocks are started together rather than in sequence because a source block's `start()` may run for as long as it has data to emit. `start()` resolves when every block's `start()` has.

If any block's `start()` rejects, the topology stops itself before rejecting, so a failed start never leaves the other blocks running.

`stop()` waits for every block's `stop()` to settle, logs any that reject, and closes the provider regardless. Sources are stopped first so that nothing new is emitted while the provider drains: it stops taking messages, waits up to the timeout (default 30 seconds) for those in progress, then releases its connections (ADR-0004).

`stop()` may be called at any point, including before `start()`, and any number of times; every call returns the first call's promise. A stopped topology refuses to start. It installs nothing on the process; the host calls it (ADR-0001).

`debug: true` makes nStorm log its internals at the `info` level. It does not touch the logger's level, which is shared by the whole process.

## Open Questions

- A topology cannot be restarted after `stop()`; a new one must be constructed. Is a restart ever needed?
- Blocks downstream of a block that is still finishing its last message during a drain will not process what it emits; on the Redis providers that output waits for the next run.
- `reset()` before setup clears nothing, because queues are not known until setup. It does not warn.
