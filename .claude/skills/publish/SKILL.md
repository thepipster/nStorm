---
name: publish
description: Merge a finished branch into master and publish the new nstorm version to npm. Use when a branch is ready to ship.
argument-hint: "[patch|minor|major] - only needed if the version has not been bumped yet"
disable-model-invocation: true
---

Ship the current branch: merge it into `master`, publish to npm, tag the release. Stop at the first step that fails and report it; never work around a failed check.

Publishing cannot be undone (a version number can never be reused), so steps 1-5 only prepare and step 6 waits for the user.

## 1. Preconditions

- Working tree is clean and `git fetch origin` shows the branch is not behind its remote.
- `npm whoami` succeeds. If it does not, stop and ask the user to run `npm login` - you cannot do this for them.
- If already on `master` there is nothing to merge: skip step 4.

## 2. Version

Compare `package.json`'s version with `npm view nstorm version`.

- If it is not greater, bump it on the branch: `npm version $ARGUMENTS --no-git-tag-version`, then commit `package.json` and `package-lock.json` together. If no argument was given, propose one from the commits and ask.
- Check the bump is honest: `git diff origin/master...HEAD -- src/index.ts src/BaseBolt.ts src/queue-providers/BaseQueue.ts`. Removed or renamed exports, changed signatures, changed `NStormSettings` defaults or changed delivery behaviour require a major version, and a matching entry in `docs/guide/upgrading.md`.

## 3. Verify the branch

- Run the `verify` skill, including the build step.
- `npm audit --omit=dev` must report nothing. If it does, tell the user what and let them decide; do not publish known-vulnerable runtime dependencies silently.

## 4. Merge

```sh
git checkout master
git pull --ff-only
git merge --no-ff <branch>
```

On a conflict, stop and hand back to the user. After a clean merge run `npm ci && npm run check` again on `master` - the merged result is what ships, not the branch.

## 5. Inspect the package

`npm pack --dry-run`. Confirm it contains `dist/index.js` and `dist/index.d.ts`, nothing from `src/`, `tests/`, `example/` or `.claude/`, and that the version is the one from step 2.

## 6. Confirm, then ship

Show the user: the version (and what is currently on npm), the commits being released, and the tarball file list. **Wait for an explicit go-ahead.** Then, in this order:

1. `git push origin master`
2. `npm publish` - `prepublishOnly` re-runs the checks and the build. If npm asks for a one-time password, ask the user to run this command themselves.
3. Confirm with `npm view nstorm version`.
4. Only once it is on npm: `git tag -a v<version> -m "v<version>"` and `git push origin v<version>`, so a tag always means "this was published".

If publish fails after the push, `master` simply holds an unpublished version: fix the cause and re-run from step 5. Do not bump the version to get past a failure.

## 7. Report

State the published version, the tag, and anything skipped. Offer to delete the merged branch locally and on the remote; do not delete it unasked.
