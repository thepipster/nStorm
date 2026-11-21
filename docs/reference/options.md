# Options

Passed to `new nStorm(options)`. Every option is optional. An option set to `undefined` is treated as not given.

```ts
const cloud = new nStorm({
    cloudName: "orders",
    queue: "bullmq",
    redis: { host: "redis.internal", port: 6379 },
    replayLimit: 5,
    processTimeout: 60_000,
});
```

## General

| Option      | Type      | Default        | Description                                                                                            |
| ----------- | --------- | -------------- | ------------------------------------------------------------------------------------------------------ |
| `cloudName` | `string`  | `"stormcloud"` | Name of the topology. Namespaces its queues, so several topologies can share one Redis server.         |
| `queue`     | `string`  | `"bullmq"`     | [Queue provider](../guide/queue-providers): `"bullmq"`, `"simple"` or `"async"`. Anything else throws. |
| `debug`     | `boolean` | `false`        | Log nStorm's internals. Does not change the [log level](../guide/logging).                             |

## Failure handling

| Option           | Type      | Default | Description                                                                                      |
| ---------------- | --------- | ------- | ------------------------------------------------------------------------------------------------ |
| `replay`         | `boolean` | `true`  | Deliver a failed message again. With `false`, each message is attempted once.                    |
| `replayLimit`    | `number`  | `3`     | Total number of times a message is delivered, including the first, before it is given up on.     |
| `replayTime`     | `number`  | `300`   | Milliseconds to wait before delivering a failed message again.                                   |
| `processTimeout` | `number`  | `0`     | Fail a message whose block has not finished it within this many milliseconds. `0` waits forever. |

See [Failures and replay](../guide/failures).

## Redis

Used by the `bullmq` and `simple` providers, and ignored by `async`.

| Option           | Type     | Default       | Description                               |
| ---------------- | -------- | ------------- | ----------------------------------------- |
| `redis.host`     | `string` | `"127.0.0.1"` | Redis host.                               |
| `redis.port`     | `number` | `6379`        | Redis port.                               |
| `redis.user`     | `string` |               | Redis ACL username.                       |
| `redis.password` | `string` |               | Redis password.                           |
| `redis.prefix`   | `string` |               | Prefix for every Redis key nStorm writes. |

## Types

```ts
import type { NStormSettings } from "nstorm";
```

`NStormSettings` is the full settings object with every field present, as found on `cloud.settings`. The constructor accepts a partial version of it.
