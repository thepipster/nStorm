# Shutting down

nStorm does not install signal handlers and never exits the process for you. Your application decides when to stop, and calls `stop()`:

```ts
for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
        cloud
            .stop({ timeout: 20_000 })
            .catch((err) => Logger.error(err))
            .finally(() => process.exit(0));
    });
}
```

## What `stop()` does

1. Calls every block's `stop()`, so that sources stop emitting.
2. Stops taking new messages from the queues.
3. Waits for messages already in progress to finish, for at most `timeout` milliseconds.
4. Closes all connections.

The promise it returns resolves when all of that is done.

## Choosing a timeout

The default is 30 seconds. Pick a value shorter than the time your platform allows between asking a process to stop and killing it, leaving room for anything else your application does on the way out.

| Platform               | Default grace period |
| ---------------------- | -------------------- |
| Kubernetes             | 30 seconds           |
| Docker (`docker stop`) | 10 seconds           |
| systemd                | 90 seconds           |

If the timeout expires with messages still in progress, `stop()` closes the connections anyway and logs a warning. What happens to those messages depends on the provider.

## What happens to messages

| Message state when `stop()` is called | `bullmq`                      | `simple`                      | `async`   |
| ------------------------------------- | ----------------------------- | ----------------------------- | --------- |
| Queued, not started                   | Stays in Redis                | Stays in Redis                | Discarded |
| In progress, finishes within timeout  | Completed                     | Completed                     | Completed |
| In progress, still going at timeout   | Redelivered to another worker | **Lost**                      | **Lost**  |
| Failed, waiting to be replayed        | Stays in Redis                | Put back in Redis immediately | Discarded |

With the two Redis providers a clean stop loses nothing, provided messages in progress finish within the timeout. The `async` provider keeps everything in memory, so whatever has not been processed goes with the process.

## Writing `stop()` in a block

Use a block's `stop()` to undo what its `start()` set up: clear timers, close connections, stop reading from an external source.

```ts
class Poller extends BaseBolt {
    private timer?: NodeJS.Timeout;

    async start(): Promise<void> {
        this.timer = setInterval(() => this.poll(), 5000);
    }

    async stop(): Promise<void> {
        clearInterval(this.timer);
    }
}
```

A block's `stop()` is called before the queues drain, so the block may still receive and finish messages after it. Do not close something in `stop()` that `process()` needs.

If a block's `stop()` throws, the error is logged and the shutdown carries on.

## Other details

- `stop()` is safe to call more than once, and before `start()`. Every call returns the same promise.
- A stopped topology cannot be restarted. Create a new `nStorm` instance.
- With the `simple` provider, `stop()` takes up to a second even when idle, because it waits for its current read from Redis to return rather than risk dropping a message.
