# ADR-0001: The host application owns the process lifecycle

## Status

Accepted (2026-11-21)

## Context

Up to 4.0 the BullMQ and Simple queue providers registered `SIGTERM`/`SIGINT` handlers in `start()` and called `process.exit(0)` from them. nStorm is a library that runs inside someone else's process. Its handler exited before the host's own shutdown code (closing HTTP servers, flushing logs, finishing database work) could run, two topologies in one process raced each other to exit, and there was no way for a host to shut a topology down on its own terms because no public stop method existed.

## Decision

nStorm never registers a process signal handler and never calls `process.exit()`. Shutdown is an explicit API: `nStorm.stop()` calls each block's `stop()` hook and then `BaseQueue.close()`, which every provider implements to stop consuming and release all of its connections, workers and timers. The host decides when to call it.

## Consequences

Good:

- A host can sequence its own shutdown and can run several topologies in one process.
- A topology can be stopped and the process kept alive, which also makes real-connection tests possible.
- Every provider now has to account for the resources it opens, which is what exposed the leaked connections in 4.0.

Bad:

- Breaking change. An application that relied on nStorm to exit on Ctrl-C now keeps running until it calls `stop()` itself; a few lines of handler are required in every program, including the examples.
- A host that forgets to call `stop()` leaves Redis connections open and the event loop alive.

## Alternatives considered

- **Keep the handlers but make them opt-out.** Still surprising by default, and still wrong whenever two libraries each believe they own the exit.
- **Keep the handlers but drop `process.exit()`.** Removes the worst behaviour, but a library adding listeners to a global the host owns remains a side effect nobody asked for, and the order of handlers is not something the host can control.
