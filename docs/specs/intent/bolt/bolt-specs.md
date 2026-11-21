# Bolt — requirements

Design: [bolt-design.md](bolt-design.md). Source: `src/BaseBolt.ts`.

## Filtering

### BOLT-FILTER-001

When a block emits a message, the bolt shall deliver it to a subscriber that has a filter only if every key in that filter is an own property of the message and strictly equal to the filter's value.

### BOLT-FILTER-002

When a block emits a message, the bolt shall deliver it to every subscriber that has no filter.

## Emitting

### BOLT-EMIT-001

When a block emits a message, the bolt shall return a promise that resolves once the queue of every subscriber the message is delivered to has accepted it.

### BOLT-EMIT-002

If the queue of any subscriber cannot accept an emitted message, then the bolt shall reject the returned promise with that error.

### BOLT-EMIT-003

If the caller does not observe the promise returned by `emit()`, then the bolt shall not cause an unhandled promise rejection.

### BOLT-EMIT-004

If `emit()` is called on a block that has not been added to a topology, then the bolt shall reject the returned promise.

## Processing

### BOLT-PROCESS-001

If a block's `process()` throws or returns a rejected promise, then the bolt shall fail the message with that error exactly as if `done(err)` had been called.

### BOLT-PROCESS-002

When `done` is called more than once for the same message, the bolt shall act on the first call only.

### BOLT-PROCESS-003

The base implementation of `process()` shall acknowledge the message.

### BOLT-PROCESS-004

Where a processing timeout is configured, if a block has not completed a message within it, then the bolt shall fail the message.

## Inputs

### BOLT-INPUT-001

When an input is declared more than once for the same source block, the bolt shall keep the first declaration and ignore the rest.
