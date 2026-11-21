# Architecture decision records

The decision log for nStorm: why we chose one approach over another, where that choice constrains future work. Design and requirements live in [`docs/specs/`](../specs/README.md); user-facing behaviour lives in the [README](../../README.md).

Rules:

- **Append-only and numbered** (`NNNN-short-title.md`). Take the next number after the highest here.
- **An accepted ADR is never edited.** To change a decision, write a new ADR and mark the old one `Superseded by ADR-NNNN`.
- Cite an ADR from code as `(ADR-NNNN)` at the place the decision is enforced.

## Index

| ADR                                                 | Decision                                                    | Status   |
| --------------------------------------------------- | ----------------------------------------------------------- | -------- |
| [0001](0001-host-owns-process-lifecycle.md)         | The host application owns the process lifecycle             | Accepted |
| [0002](0002-simple-queue-at-most-once-in-flight.md) | SimpleQueue stays at-most-once for in-flight messages       | Accepted |
| [0003](0003-emit-rejects-but-is-safe-unawaited.md)  | `emit()` rejects on failure but is safe to leave un-awaited | Accepted |

## Template

```markdown
# ADR-NNNN: Title

## Status

Accepted (YYYY-MM-DD)

## Context

What forced a decision.

## Decision

What we decided.

## Consequences

Good and bad, honestly.

## Alternatives considered

What else was on the table and why it lost.
```
