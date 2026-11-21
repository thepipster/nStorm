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
| Stop  | Call every block's `stop()`, then the provider's `close()`.                                                  |

Setup validates all inputs before changing anything, so a topology with a bad input is left with no subscribers wired. It registers all queues before starting any consumer, so a block that emits from its consumer always finds its subscribers' queues.

Setup and start each run at most once. Running setup twice would append a second copy of every subscriber and create a second set of workers, delivering every message twice; a second call is therefore an error, as is adding a block after setup.

Blocks are started together rather than in sequence because a source block's `start()` may run for as long as it has data to emit. `start()` resolves when every block's `start()` has.

`stop()` waits for every block's `stop()` to settle, logs any that reject, and closes the provider regardless. It installs nothing on the process; the host calls it (ADR-0001).

## Open Questions

- `stop()` is not guarded against being called before `start()` or twice. It relies on the provider's `close()` being idempotent, which only the BullMQ provider has a test for.
- A topology cannot be restarted after `stop()`. Is a restart ever needed, or is "construct a new one" enough?
- If a block's `start()` rejects, `start()` rejects but the other blocks keep running and the topology is marked started. Should it stop them?
- `reset()` before setup clears nothing, because queues are not known until setup. It does not warn.
