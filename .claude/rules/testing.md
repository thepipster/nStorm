---
paths:
    - "tests/**/*.ts"
---

# Testing rules

- Vitest, with explicit imports: `import { describe, it, expect, vi } from 'vitest'` (globals are not enabled).
- Tests mirror `src/`: `src/queue-providers/Foo.ts` → `tests/queue-providers/Foo.test.ts`.
- Unit tests must not need a running Redis. Mock `bullmq` and `ioredis` with `vi.mock(...)` declared _before_ importing the module under test — copy the pattern at the top of `tests/queue-providers/BullMQQueue.test.ts`.
- Mock constructors with `vi.fn(function (this) { ... })`, not arrow functions, so `new` works.
- Use fake timers (`vi.useFakeTimers()`) for anything involving `delay()`, polling or backoff rather than real sleeps, and restore them in `afterEach`.
- Tests are typechecked and linted by `npm run check`, not by `npm run build`.
- Real-timer waits: prefer `await vi.waitFor(() => expect(...))` over a fixed sleep. It polls every 50ms, so any delay the test is racing against (e.g. `replayTime` before a `close()`) must be well above that.
- A mocked blocking call (`blpop`) must yield to the event loop (a short `setTimeout`) before returning `null`, or the poll loop spins forever. Always `close()` the queue in `afterEach`.
- `tests/setup.ts` silences `Logger` for every test, so that tests of failure paths do not print expected errors. Run `NSTORM_TEST_LOGS=1 npm test` to see the log output when debugging. A passing run should print no error lines; if one does, something is logging outside `Logger`.
- When fixing a bug, add a test that fails without the fix. If an existing test asserts the buggy behaviour, change the test and call that out explicitly.
