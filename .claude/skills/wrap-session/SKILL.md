---
name: wrap-session
description: End-of-session consolidation — capture what this session changed into the repo's durable knowledge so the next session (human or agent) inherits it. Updates skills, rules, agents, and memory, writes ADRs for decisions, and keeps the LID spec chain (HLD → LLDs → EARS → Tests → Code) current. Use at the end of a work session, or when the user says "wrap up", "wrap the session", "end of session", "consolidate", or runs /wrap-session.
---

# Wrap session

Turn what _this session_ learned into durable, checked-in knowledge. Code and git
already record _what_ changed; this skill records the **intent, standards, and
procedures** behind it — the things that would otherwise evaporate when the context
window closes.

Run it at the end of a session, before you `git commit` the day's work (or right
after). It is **propose-then-apply**: you scan, present one consolidated plan, and
only write the durable shared files after the user confirms. Memory is the one
low-friction exception — write it directly.

## The six sinks

| Sink               | Lives in                                                  | Write when this session…                                                   |
| ------------------ | --------------------------------------------------------- | -------------------------------------------------------------------------- |
| **Memory**         | `~/.claude/projects/<proj>/memory/` (+ `MEMORY.md` index) | surfaced a durable fact, preference, or gotcha not derivable from the code |
| **Rules**          | `.claude/rules/*.md`                                      | clarified or violated a coding standard that should bind future work       |
| **Agents**         | `.claude/agents/*.md`                                     | proved an agent's guidance wrong, thin, or missing a trigger               |
| **Skills**         | `.claude/skills/<name>/SKILL.md`                          | established a repeatable multi-step procedure worth replaying              |
| **ADRs**           | `docs/adr/`                                               | made an architectural **decision** that constrains future work             |
| **LID spec chain** | `docs/specs/`                                             | changed **design** (HLD/LLD) or **behavioural requirements** (EARS)        |

nStorm is a single repo and a published library, so two existing documents are
not sinks of this skill but must already be current before you wrap:

- **`README.md`** — user-visible behaviour, options, and the "Upgrading" section.
  If the session changed behaviour and the README does not say so, fix that first.
- **`.claude/CLAUDE.md`** — commands, the architecture map, and "Things that will
  trip you up". A gotcha that every session needs goes there, not in memory.

`docs/adr/` and `docs/specs/` do not exist yet. Both are created lazily (see below).

---

## Step 0 — Gather the session's material

Before deciding anything, reconstruct what actually happened:

1. `git status` + `git diff` (and `git log master..HEAD` if the session committed) —
   the concrete changes.
2. Skim the conversation for: **decisions** ("we'll do X because Y"), **corrections**
   ("no, always do it this way"), **new procedures** (a sequence you'd repeat),
   **new requirements** ("when X, it must Y"), and **gotchas** (a non-obvious trap
   you hit).
3. Note anything you had to _ask_ the user that a future agent shouldn't have to.
4. If `docs/specs/` exists, **check every spec ID the session cited in code against
   it**: `grep -rhoE "@spec [A-Z]+-[A-Z]+-[0-9]+" src tests`, then
   `grep -rn "### <ID>" docs/specs` for each. An ID that already exists with another
   meaning is a collision: renumber the session's citations to the next free number.
   An ID with no entry gets its EARS line in this wrap.

If nothing durable emerged — a pure mechanical change, a one-off — say so and stop.
Not every session needs to write to every sink; most write to one or two.

---

## Step 1 — Triage, then present ONE plan

Sort the material into the six sinks using the table above. Then present a single
consolidated proposal — per sink, the file(s) you'd create or edit and a one-line
why — and ask for a go-ahead. Do **not** start writing shared files first.

Exception: **memory** is safe and low-friction — write it as you go, no gate needed.

---

## Memory

Follow the memory format already defined for this project (frontmatter with
`name`/`description`/`metadata.type`, one fact per file, `type` ∈
`user | feedback | project | reference`). After writing a file, add its one-line
pointer to `MEMORY.md`. Before creating, check for an existing file that already
covers it and update that instead. Don't save what the repo, git history, or
CLAUDE.md already records.

Good session-end memories: a preference the user stated, a constraint you discovered
about the environment (e.g. how this machine authenticates to GitHub or npm), a
decision's _rationale_ that isn't in the ADR, a link to a dashboard/ticket. Convert
relative dates to absolute.

---

## Rules — `.claude/rules/*.md`

A rule is a **standard that binds future code**. Each rule file starts with `paths:`
frontmatter (the globs it applies to), then a heading and a flat bullet list. Update
a rule when this session revealed the _intended_ standard, especially if existing
code had drifted from it.

- Match the voice of the existing rules (`queue-providers.md`, `public-api.md`,
  `testing.md`): one imperative bullet per standard, with the _why_ in the same
  bullet.
- Prefer editing an existing rule over adding a new file; only add a file for a
  genuinely new topic, and give it `paths:` globs so it loads for the right files.
- If the session _followed_ a rule that isn't written down yet but should be, that's
  also a reason to write it.

---

## Agents — `.claude/agents/*.md`

Update an agent when the session showed its guidance was wrong, incomplete, or
missing a dispatch trigger. Keep the frontmatter shape used by `queue-reviewer.md`
(`name`, a one-line `description` that says what it does and when to use it,
`tools`). Typical edits:

- Add a **trigger** to the description when a diff _should_ have dispatched the agent
  but the phrasing didn't cover it (e.g. a file outside `src/queue-providers/` that
  still affects delivery).
- Sharpen the checklist when the agent missed (or should have caught) a class of
  issue this session — cite the file it should read.
- Keep read-only agents read-only: `queue-reviewer` does not get edit tools, and
  `test-writer` does not modify `src/`.

---

## Skills — `.claude/skills/<name>/SKILL.md`

Write or update a skill when this session ran a **repeatable, multi-step procedure**
that a future session would replay. A skill earns its keep only if it encodes
_non-obvious sequencing or cross-file coordination_ — not a single edit.

- Format: a `<name>/SKILL.md` with `name` + `description` frontmatter. The
  description must say **what it does and when to use it** (that's what makes it
  fire). Model the body on `add-queue-provider` — numbered steps, real file paths,
  "use X as the template" pointers, ending with "run the `verify` skill".
- Prefer updating an existing skill (`verify`, `release`, `add-queue-provider`) with
  a newly-discovered step or gotcha over spawning a near-duplicate.
- A skill that publishes or pushes (like `release`) keeps
  `disable-model-invocation: true` and an explicit confirmation step.

---

## ADRs — `docs/adr/` (the decision log)

Write an ADR when the session made a **decision that shapes structure or constrains
future work** — the _why we chose this over that_ (e.g. "the library never touches
process signals", "SimpleQueue stays at-most-once for in-flight messages"). ADRs are
the decision log; they are complementary to the LID chain below (which holds design +
requirements).

- **Append-only, numbered** (`0001-short-title.md`). Next number after the highest
  existing. Accepted ADRs are never edited — supersede with a new one and mark the
  old `Superseded by ADR-NNNN`.
- Sections: `Status` (Accepted (date)), `Context`, `Decision`, `Consequences` (Good /
  Bad honestly), `Alternatives considered`. Add the new ADR to the
  `docs/adr/README.md` index.
- Cite it from code as `(ADR-NNNN)` where the decision is enforced.
- `docs/adr/` does not exist yet. Create it, with a `README.md` holding the template
  and index, **only** when a real decision warrants the first ADR — don't scaffold
  empty folders.

---

## LID spec chain — `docs/specs/` (design + requirements)

Intent is tracked as a **Linked-Intent Development** chain
(ref: https://github.com/jszmajda/lid):

```
HLD  →  LLDs  →  EARS  →  Tests  →  Code
```

Docs are the **source of truth**; code is compiled output. Each hop is _walkable in
one `grep`_. Rooted at `docs/specs/` (ADRs stay separate in `docs/adr/`):

```
docs/specs/
├── README.md               how the chain works + index of nodes
├── high-level-design.md    HLD: system-wide decisions, component boundaries
└── intent/
    └── <node>/             one folder per component
        ├── <node>-design.md   LLD: data model, error handling, decisions, Open Questions
        └── <node>-specs.md    EARS: testable requirements with semantic IDs
```

Nodes follow the architecture in `.claude/CLAUDE.md`: `topology` (`nStorm.ts`),
`bolt` (`BaseBolt.ts`), `queue-contract` (`BaseQueue.ts`), one per provider
(`bullmq-queue`, `simple-queue`, `async-queue`), and `logger`.

### What each artifact is

- **HLD** — the few architectural trade-offs and the component map. Touch it only
  when the session _shifted_ boundaries or a cross-cutting choice (e.g. what the
  provider contract guarantees).
- **LLD** (`<node>-design.md`) — per component: data models, error handling, the
  decisions local to it, and an **Open Questions** section for unresolved gaps.
- **EARS** (`<node>-specs.md`) — testable requirements in EARS syntax, each with a
  **semantic ID** `DOMAIN-SUBDOMAIN-NNN` (e.g. `BOLT-FILTER-001`).

### EARS syntax

Every requirement is one of these shapes (pick the one that fits; keep them atomic
and testable):

```
Ubiquitous:   The <system> shall <response>.
Event:        When <trigger>, the <system> shall <response>.
State:        While <state>, the <system> shall <response>.
Unwanted:     If <condition>, then the <system> shall <response>.
Optional:     Where <feature is present>, the <system> shall <response>.
Complex:      When <trigger>, while <state>, the <system> shall <response>.
```

Example node `docs/specs/intent/bolt/bolt-specs.md`:

```markdown
### BOLT-FILTER-001

When a block emits a message, the bolt shall deliver it to a subscriber only if
every key in that subscriber's filter is present in the message and equal to the
filter's value.

### BOLT-PROCESS-001

If a block's `process()` throws or returns a rejected promise, then the bolt shall
fail the message with that error exactly as if `done(err)` had been called.

### BOLT-PROCESS-002

When `done` is called more than once for the same message, the bolt shall act on
the first call only.
```

### Traceability — `@spec` annotations

The chain is enforced by annotations, not prose links:

- **Tests** cite the EARS IDs they verify: `// @spec BOLT-FILTER-001` above the test.
- **Code** cites IDs at the **entry point** of the behaviour (the topmost function/
  method that owns it, not every helper): `// @spec BOLT-PROCESS-001, BOLT-PROCESS-002`.
- Verify walkability: `grep -rn "@spec BOLT-FILTER-001" src tests` should return the
  test(s) and the code that own it. Every EARS ID touched this session should be
  cited by at least one test; flag any that isn't as an Open Question in the LLD.
- Provider requirements proven only against mocks (see `.claude/CLAUDE.md`) should
  say so in the LLD if no real-Redis run backed them this session.

### Cascade rule (how far a change ripples)

When intent changed this session, cascade **HLD → LLD → EARS → Tests → Code**:

- **Within one node** (an LLD and its downstream specs/tests/code): update freely.
- **Across node boundaries** (e.g. a `queue-contract` change that alters what every
  provider node must guarantee): pause and confirm with the user before editing the
  neighbouring node.

### Docs reflect current intent, not history

**Delete** obsolete EARS specs and stale LLD prose rather than annotating them —
git preserves history. A superseded _decision_ gets an ADR; a superseded
_requirement_ just gets removed and its `@spec` citations cleaned up.

### Lazy creation

`docs/specs/` doesn't exist yet. Create it **only** for the node(s) this session
actually touched, plus a short `README.md` that explains the chain and indexes the
nodes. Don't pre-scaffold an HLD or empty node folders for the whole library.

---

## Step 2 — Apply and report

After the user approves the plan:

1. Write memory (already allowed), then the approved rules / agents / skills / ADRs /
   spec-chain edits.
2. Re-run the traceability grep for any EARS IDs you added, and confirm indexes
   (`MEMORY.md`, `docs/adr/README.md`, `docs/specs/README.md`) list the new entries.
3. Run `npm run format` so the new markdown passes `npm run check` (Prettier covers
   `.claude/` and `docs/`).
4. Report a short summary: per sink, what was written and where, and any **Open
   Questions** you parked in an LLD for next session.

## Guardrails

- **Don't duplicate the code.** If the fact is legible from the source or git
  history, don't memorialize it — capture only the non-obvious _why_.
- **Prefer editing over adding.** A new rule/skill/spec file is a last resort; fold
  into an existing one when the topic already has a home.
- **One decision, one place.** Decision → ADR. Design → HLD/LLD. Requirement → EARS.
  Standard → rule. Procedure → skill. Fact/preference → memory. User-visible
  behaviour → README. Don't write the same thing to two sinks.
- **Respect append-only ADRs** and **delete-don't-mark specs** — they have opposite
  history rules on purpose.
