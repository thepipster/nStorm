# Specs

nStorm's intent, kept as a linked chain:

```
HLD  →  LLDs  →  EARS  →  Tests  →  Code
```

The documents are the source of truth for what the library is meant to do; the code implements them. Decisions and their rationale live separately in [the decision records](../adr/index.md).

## Layout

Each component ("node") has a folder under `intent/` holding two files:

- `<node>-design.md` — the low-level design: data model, error handling, local decisions and an **Open Questions** section.
- `<node>-specs.md` — testable requirements in EARS syntax, each with an ID of the form `DOMAIN-SUBDOMAIN-NNN`.

There is no high-level design document yet. One will be added when a session changes component boundaries.

## Nodes

| Node                                                            | Source                               | ID prefix | Status  |
| --------------------------------------------------------------- | ------------------------------------ | --------- | ------- |
| [bolt](intent/bolt/bolt-specs.md)                               | `src/BaseBolt.ts`                    | `BOLT-`   | Written |
| [topology](intent/topology/topology-specs.md)                   | `src/nStorm.ts`                      | `TOPO-`   | Written |
| [queue-contract](intent/queue-contract/queue-contract-specs.md) | `src/queue-providers/BaseQueue.ts`   | `QUEUE-`  | Written |
| [bullmq-queue](intent/bullmq-queue/bullmq-queue-specs.md)       | `src/queue-providers/BullMQQueue.ts` | `BULLMQ-` | Written |
| [simple-queue](intent/simple-queue/simple-queue-specs.md)       | `src/queue-providers/SimpleQueue.ts` | `SIMPLE-` | Written |
| [async-queue](intent/async-queue/async-queue-specs.md)          | `src/queue-providers/AsyncQueue.ts`  | `ASYNC-`  | Written |
| [logger](intent/logger/logger-specs.md)                         | `src/Logger.ts`                      | `LOG-`    | Written |

The provider nodes hold only what each provider adds. What every provider must do is in `queue-contract`, and provider tests cite those `QUEUE-` IDs too.

## Traceability

Requirements are linked to tests and code by annotation, so each hop is one search:

```sh
grep -rn "@spec BOLT-FILTER-001" src tests
```

- A **test** cites the IDs it verifies in a `// @spec` comment directly above it.
- **Code** cites IDs at the entry point of the behaviour, not in every helper.
- Every ID should be cited by at least one test. One that is not is listed under Open Questions in its node's design file.

## Keeping it current

- When intent changes, update in order: design, requirements, tests, code.
- A change inside one node can be made freely. A change that alters what a neighbouring node must guarantee (for example, a `queue-contract` change that binds every provider) needs agreement first.
- Delete an obsolete requirement and its `@spec` citations rather than marking it; git keeps the history. A superseded _decision_ gets a new ADR instead.

## EARS shapes

```
Ubiquitous:   The <system> shall <response>.
Event:        When <trigger>, the <system> shall <response>.
State:        While <state>, the <system> shall <response>.
Unwanted:     If <condition>, then the <system> shall <response>.
Optional:     Where <feature is present>, the <system> shall <response>.
```
