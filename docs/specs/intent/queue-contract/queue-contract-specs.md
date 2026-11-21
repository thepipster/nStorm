# Queue contract — requirements

Design: [queue-contract-design.md](queue-contract-design.md). Source: `src/queue-providers/BaseQueue.ts`.

These bind every queue provider.

## Naming and isolation

### QUEUE-NAME-001

The queue provider shall include the topology's cloud name in the name of every queue and backend key it uses.

### QUEUE-CLEAR-001

When asked to clear, the queue provider shall remove only messages queued for blocks of its own topology.

## Emitting

### QUEUE-EMIT-001

If a message is emitted to a block that has no registered queue, then the queue provider shall reject the emit.

### QUEUE-EMIT-002

If the backend does not accept an emitted message, then the queue provider shall reject the emit with the backend's error.

## Replay

### QUEUE-REPLAY-001

When a message fails and has been delivered fewer times than `replayLimit`, the queue provider shall deliver it again after `replayTime` milliseconds.

### QUEUE-REPLAY-002

Where replay is turned off, the queue provider shall deliver each message once only.

## Completion

### QUEUE-DONE-001

When a consumer signals completion of the same message more than once, the queue provider shall act on the first signal only.

## Resources and lifecycle

### QUEUE-CONNECT-001

The queue provider shall not open a backend connection when it is constructed.

### QUEUE-CLOSE-001

When closed, the queue provider shall stop taking messages, wait for the messages in progress to finish, and then release every connection, worker and timer it opened.

### QUEUE-CLOSE-002

If the queue provider is closed more than once, then the later calls shall have no effect.

### QUEUE-CLOSE-003

If messages are still in progress when the close timeout expires, then the queue provider shall release its resources without waiting any longer.

### QUEUE-SIGNAL-001

The queue provider shall not register process signal handlers or terminate the process.
