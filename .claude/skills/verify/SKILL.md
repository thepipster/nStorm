---
name: verify
description: Typecheck, test and build nStorm to confirm a change is sound. Use after editing anything under src/ or tests/, and before committing or releasing.
allowed-tools: Bash(npm run check *), Bash(npm run format *), Bash(npm test *), Bash(npx vitest *), Bash(npm run build *)
---

Run these in order and stop at the first failure:

1. `npm run check` — prettier check, eslint, typecheck of `src/`, `tests/` and `example/`, then the full vitest suite. If only formatting fails, run `npm run format` and re-run.
2. `npm run build` — only if `src/index.ts`, `package.json` or `tsconfig.json` changed; confirms `dist/index.js` and `dist/index.d.ts` are emitted.

If `BullMQQueue` or `SimpleQueue` changed and `redis-cli ping` answers, also run a small real topology against it with a throwaway `cloudName`, call `stop()`, confirm the process exits by itself, and delete any keys left behind. The unit tests mock Redis and cannot catch connection or shutdown bugs.

If a public export or an `NStormSettings` default changed, also run `npx tsx example/test.ts` for a few seconds (in-memory queue, no Redis) and confirm messages reach `ResultsBolt`, then stop it.

Report the actual result of each step. If a test fails, show the failing assertion — do not edit a test to make it pass unless the test itself is asserting wrong behaviour, and say so if you do.
