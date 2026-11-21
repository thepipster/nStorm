# Logger — requirements

Design: [logger-design.md](logger-design.md). Source: `src/Logger.ts`.

## Levels

### LOG-LEVEL-001

The logger shall start at the `info` level.

### LOG-LEVEL-002

Where the `NSTORM_LOG_LEVEL` environment variable is set, the logger shall start at that level.

### LOG-LEVEL-003

When the level is changed, the logger shall write messages at that level and above.

### LOG-LEVEL-004

If a message is below the current level, then the logger shall not serialise its arguments or capture a call site.

## Aliases

### LOG-ALIAS-001

When `log()` is called, the logger shall write at the `info` level.

### LOG-ALIAS-002

When `fatal()` is called, the logger shall write at the `error` level.

## Format

### LOG-FORMAT-001

When a message is written, the logger shall render object and error arguments in readable form.

### LOG-TRACE-001

While call-site reporting is on, the logger shall append the calling file and line to each message.

### LOG-TRACE-002

While call-site reporting is off, the logger shall write the message without a call site.
