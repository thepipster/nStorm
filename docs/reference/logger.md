# Logger

A small console logger, used by nStorm and exported for convenience. All members are static.

```ts
import { Logger } from "nstorm";
```

See the [Logging guide](../guide/logging) for an overview.

## Writing

```ts
Logger.debug(...args: unknown[]): void
Logger.info(...args: unknown[]): void
Logger.log(...args: unknown[]): void   // same as info
Logger.warn(...args: unknown[]): void
Logger.error(...args: unknown[]): void
Logger.fatal(...args: unknown[]): void // same as error
```

Arguments are joined with spaces. Objects, including errors, are printed in full on their own lines.

A message below the current level costs almost nothing: its arguments are not formatted.

## setLevel

```ts
Logger.setLevel(level?: "error" | "warn" | "info" | "debug"): void
```

Sets the level for the whole process. Called with no argument, it sets `debug`.

The starting level is `info`, or the value of the `NSTORM_LOG_LEVEL` environment variable.

## Properties

| Property             | Type      | Default | Description                                                 |
| -------------------- | --------- | ------- | ----------------------------------------------------------- |
| `Logger.stackTraces` | `boolean` | `true`  | Append the calling file, line and function to each message. |
| `Logger.colorize`    | `boolean` | `true`  | Colour printed objects and the call-site suffix.            |
