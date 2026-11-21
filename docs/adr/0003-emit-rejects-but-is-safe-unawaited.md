# ADR-0003: `emit()` rejects on failure but is safe to leave un-awaited

## Status

Accepted (2026-11-21)

## Context

Up to 4.0 `BaseBolt.emit()` returned `void` and providers only logged a failed enqueue, so a block had no way to know a message had been dropped. The obvious fix is to return a promise that rejects. But every existing block, and every example, calls `this.emit(row)` without awaiting it, typically from a timer callback. In current Node versions an unobserved rejection terminates the process, so a plain rejecting promise would turn a brief Redis outage into a crash for all existing users.

## Decision

`emit()` returns a promise that resolves once every matching subscriber's queue has accepted the message and rejects if any could not. `BaseBolt.emit` attaches its own `catch` to that promise, which logs the failure, before returning it. The promise is therefore always "handled" as far as Node is concerned, while a caller that awaits it still receives the rejection. Queue providers' `emit()` are plain `async` methods that reject; the safety net lives only in `BaseBolt`.

## Consequences

Good:

- Existing un-awaited calls keep working and cannot crash the process.
- A block that cares can `await this.emit(...)` and fail its message, which makes the upstream message replay.
- A failed send is always logged, whether or not anyone awaits it.

Bad:

- A failure is logged even when the caller also handles it, so it can appear twice in logs.
- The no-floating-promises lint rule flags un-awaited calls, so callers that deliberately do not wait must write `void this.emit(...)`.
- The behaviour is subtle. Removing the internal `catch` during a refactor would compile, pass most tests, and reintroduce the crash; a dedicated test guards it.

## Alternatives considered

- **Return a promise that always resolves, reporting failure only in the log.** Safe, but gives a block no way to react.
- **Return a plain rejecting promise.** Simplest and most conventional, but crashes existing users on the first failed send.
- **Keep `void` and add a separate `emitAsync()`.** Two ways to do the same thing, with the unsafe-to-ignore one being the one people would reach for.
