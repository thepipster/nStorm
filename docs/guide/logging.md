# Logging

nStorm exports the small logger it uses internally. You are welcome to use it in your blocks, or to ignore it and use your own.

```ts
import { Logger } from "nstorm";

Logger.info("Processed", { id: 42 });
Logger.error("Could not save;", err);
```

It writes to the console. Arguments are joined with spaces, and objects and errors are printed in full.

## Levels

| Method           | Level   |
| ---------------- | ------- |
| `Logger.error()` | `error` |
| `Logger.fatal()` | `error` |
| `Logger.warn()`  | `warn`  |
| `Logger.info()`  | `info`  |
| `Logger.log()`   | `info`  |
| `Logger.debug()` | `debug` |

The logger shows messages at its current level and above. It starts at `info`, so `debug` messages are hidden. Change the level in code:

```ts
Logger.setLevel("debug"); // "error" | "warn" | "info" | "debug"
```

or before the program starts, with an environment variable:

```sh
NSTORM_LOG_LEVEL=warn node worker.js
```

The level is shared by the whole process.

## Call sites

Each message ends with the file, line and function it was logged from:

```
[Sat Nov 21 11:15:17] info: Processed  {from line 12 of worker.ts (process)}
```

Turn that off with:

```ts
Logger.stackTraces = false;
```

## Silencing

To discard all output, including nStorm's own errors:

```ts
Logger.silent = true;
```

This is mainly useful in your tests, where blocks that fail on purpose would otherwise fill the output with expected errors.

## What nStorm logs

nStorm always logs errors: a block that fails, a send that fails, a message that has run out of attempts.

With the `debug: true` option it also logs what it is doing internally, such as queues being registered and the topology starting and stopping. Those messages are written at the `info` level, and the option does not change the logger's level.

```ts
const cloud = new nStorm({ debug: true });
```

## Limitations

The logger is global and console-only. nStorm cannot yet be given a logger of your own to write to, and its output cannot be redirected or silenced separately from your application's use of `Logger`.
