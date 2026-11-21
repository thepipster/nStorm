---
name: release
description: Cut a new nstorm npm release - version bump, verification and publish.
argument-hint: <patch|minor|major>
disable-model-invocation: true
---

Release type: `$ARGUMENTS` (if empty, propose one from the commits since the last tag and ask).

1. Confirm the working tree is clean and the branch is `master`. Stop if not.
2. Run the `verify` skill, including the build step. Stop on any failure.
3. Check the bump is honest: `git diff <last-tag>..HEAD -- src/index.ts src/BaseBolt.ts src/queue-providers/BaseQueue.ts`. Removed or renamed exports, changed signatures or changed `NStormSettings` defaults require `major`.
4. Run `npm pack --dry-run` and confirm the tarball contains `dist/index.js`, `dist/index.d.ts` and nothing from `src/`, `tests/` or `example/`.
5. `npm version $ARGUMENTS` (creates the commit and tag).
6. Show the user the version, the tarball file list and the commits included, then **wait for explicit confirmation** before running `npm publish` and `git push --follow-tags`. Publishing cannot be undone.
