# Logger — design

Source: `src/Logger.ts`. Requirements: [logger-specs.md](logger-specs.md).

A static wrapper around [winston](https://github.com/winstonjs/winston) that nStorm uses internally and exports for applications' convenience. It writes to the console only.

## Shape

`Logger` is a class of static methods over one module-level winston logger, created when the module is first imported. There are no instances: every importer shares the same level and options. That is convenient and is also the main limitation, since a host cannot give nStorm a logger of its own.

## Levels

Four levels are used: `error`, `warn`, `info`, `debug`. `log()` is an alias for `info()` and `fatal()` for `error()`; neither `log` nor `fatal` is a winston level, and writing at an unknown level makes winston print an error instead of the message.

The level starts at `info`, or the value of `NSTORM_LOG_LEVEL`. `setLevel()` changes it at run time, and a topology created with `debug: true` sets it to `debug` ([TOPO-CONFIG-003](../topology/topology-specs.md)).

## Cost

Every write checks the level first and returns immediately if the message would be discarded. Serialising arguments (`util.inspect` at unlimited depth) and capturing a stack are both expensive, and debug calls sit on message-handling paths.

## Message format

Arguments are joined with spaces; objects, including errors, are rendered with `util.inspect`. Unless `stackTraces` is turned off, the calling file, line and function are appended, found by walking the stack to the first frame outside the logger.

## Open Questions

- The logger is global and console-only. A host cannot redirect nStorm's output to its own logger or silence it per topology. Should `nStorm` accept a logger in its options?
- `debug: true` on any one topology raises the level for the whole process, and nothing lowers it again.
- An invalid `NSTORM_LOG_LEVEL` is passed straight to winston.
- The call-site lookup depends on the file being named `Logger.ts` or `Logger.js`, so a bundler that renames or inlines it would break it.
- `colorize` only affects object rendering and the call-site suffix; the level label is always coloured.
