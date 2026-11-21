# Failures and replay

## When a message fails

A message counts as failed when its block does any of these:

- calls `done(err)` with an error
- throws from `process()`
- returns a promise from `process()` that rejects
- does not finish within the [processing timeout](#processing-timeout), if one is set

```ts
class Saver extends BaseBolt {
    async process(message: Record<string, unknown>, done: () => void) {
        await db.save(message); // if this rejects, the message fails
        done();
    }
}
```

The error is logged, and the message is replayed.

## Replay

A failed message is delivered to the same block again after a short wait. If it keeps failing, nStorm gives up once the message has been delivered `replayLimit` times in total, logs it, and moves on.

| Option        | Default | Meaning                                                   |
| ------------- | ------- | --------------------------------------------------------- |
| `replay`      | `true`  | Whether to replay at all. With `false`, one attempt only. |
| `replayLimit` | `3`     | Total deliveries, including the first.                    |
| `replayTime`  | `300`   | Milliseconds to wait before each redelivery.              |

With the defaults, a message that always fails is attempted three times, about 300 ms apart.

Only the failing block sees the message again. Blocks it had already passed through are not re-run.

::: tip Make blocks safe to repeat
Replay means a block can see the same message more than once. If the block did part of its work before failing, that part happens again. Write blocks so that handling a message twice gives the same result as handling it once, for example by upserting rather than inserting.
:::

All three queue providers replay failed messages. They differ in what happens if the _process_ dies partway through; see [Queue providers](./queue-providers#comparison).

## Processing timeout

A block's concurrency slot is held until `done` is called. A `process()` that never calls it, perhaps because a network call hangs, silently uses up a slot. Once every slot is gone the block stops taking messages, with no error to show for it.

To guard against that, set `processTimeout`:

```ts
const cloud = new nStorm({ processTimeout: 60_000 });
```

A message not finished within that many milliseconds fails and is replayed like any other failure. The default, `0`, waits forever.

::: warning
nStorm cannot cancel work that is already under way. When the timeout fires, the original `process()` call keeps running, and its eventual `done()` is ignored. The replay can therefore start while the first attempt is still going. Only use a timeout with blocks that are safe to run twice for the same message.
:::

## Failed sends

`emit()` can also fail, if the queue cannot accept the message. See [Emitting](./blocks#emitting) for how to detect that. Awaiting `emit()` inside `process()` turns a failed send into a failed message, so the incoming message is replayed and the send is tried again.

A block that emits to several subscribers sends to each separately. If one send fails and the message is replayed, the subscribers that did receive it the first time will receive it again.

## What is not handled

- **Messages that run out of attempts** are logged and dropped. There is currently no hook or dead-letter queue for them. With the BullMQ provider the last 100 failed jobs per block remain in Redis and can be inspected with BullMQ's own tools.
- **Ordering** is not preserved across a failure: a replayed message goes behind messages that arrived after it.
