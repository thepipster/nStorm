# nStorm

TypeScript library (published to npm as `nstorm`) for building Storm-style processing topologies in Node: blocks ("bolts"/"spouts") are chained together and messages flow between them over a pluggable queue provider.

## Commands

- `npm run check` — everything CI runs: prettier check, eslint, typecheck, tests
- `npm test` — run the vitest suite once (no Redis needed; `bullmq` and `ioredis` are mocked)
- `npx vitest run tests/BaseBolt.test.ts` — run one test file
- `npm run typecheck` — typecheck `src/`, `tests/` and `example/` (`tsconfig.check.json`)
- `npm run lint` / `npm run format` — eslint / prettier (4 spaces, double quotes)
- `npm run build` — compile `src/` to `dist/` (CommonJS + `.d.ts`)
- `npx tsx example/test.ts` — in-memory example (`queue: "async"`), no Redis
- `npm run example` — BullMQ example, needs Redis on `127.0.0.1:6379`

## Architecture

- `src/nStorm.ts` — the topology ("cloud"). `addBlock()` registers blocks. `setupTopology()` (once only) validates inputs, turns each block's `inputs` into `subscribers` on the source block, registers every block with the queue and starts consumers. `start()` runs setup if needed, then the queue's `start()` and every block's `start()` concurrently. `stop()` calls every block's `stop()` then `queue.close()`.
- `src/BaseBolt.ts` — base class for every block, plus the shared types (`NStormSettings`, `InputOptions`, ...). `emit()` applies each subscriber's filter and hands the message to `context.queue.emit()`. `_init()` wires `process()` up as the queue consumer and turns a throw or rejection into `done(err)`.
- `src/queue-providers/` — `BaseQueue` defines the provider contract (`registerBlock`, `start`, `emit`, `process`, `clear`, `close`) and the shared helpers `queueName()`, `maxAttempts` and `once()`. Implementations: `BullMQQueue` (default, durable, at-least-once), `SimpleQueue` (Redis RPUSH/BLPOP, replays failures but loses in-flight messages on a crash), `AsyncQueue` (in-process `async.queue`).
- `src/Logger.ts` — static winston wrapper, initialised at import time.
- `src/index.ts` — the public API. Anything exported here is a semver commitment.

## Things that will trip you up

- Queue names are always `${cloudName}-${blockName}` — use `BaseQueue.queueName()`, never build one by hand.
- `emit()` returns a promise that rejects on failure but is safe to leave un-awaited: `BaseBolt.emit` attaches its own logging `catch`. Keep that property if you touch it.
- A message fails when `process()` calls `done(err)`, throws, or rejects. `done` is wrapped in `once()`, so only the first outcome counts.
- Nothing connects to Redis in a constructor. Connections open during setup / first use and every one must be released by `close()`.
- The library never touches process signals or calls `process.exit()`.
- Provider tests only prove behaviour against mocks. After changing `BullMQQueue` or `SimpleQueue`, also run a real topology against a local Redis if one is available (`redis-cli ping`), using a throwaway `cloudName`.
- User-visible behaviour changes go in the README, including its "Upgrading" section.
- Intent is documented in `docs/specs/` (design + EARS requirements per component) and decisions in `docs/adr/`. Tests and entry-point code carry `// @spec <ID>` comments that link to those requirements: when you change behaviour, update the requirement first, and keep the IDs on tests and code in step. `grep -rn "@spec BOLT-FILTER-001" src tests` walks the chain.
