---
paths:
    - "src/**/*.ts"
---

# Library source rules

- `src/index.ts` is the published surface. Removing or renaming an export, changing a method signature on `nStorm`/`BaseBolt`/`BaseQueue`, or changing a default in `NStormSettings` is a breaking change — flag it, expect a major version bump, and add it to `docs/guide/upgrading.md`.
- New settings go in the `NStormSettings` interface _and_ the `defaults` object in the `nStorm` constructor, and get a row in `docs/reference/options.md`. Do not add a setting that nothing reads.
- `strict` is on. Do not add `any`; use `unknown` and narrow. Avoid new non-null assertions (`!`) — check and throw a clear error instead.
- Output is CommonJS for Node. Keep dependencies CJS-compatible (this is why `chalk` is pinned to v4).
- `package.json` ranges are what consumers install from; the lockfile only protects this repo. When `npm audit --omit=dev` flags a runtime dependency, raise its minimum version in `package.json` as well as updating the lockfile.
- Never leave a promise floating: `await` it, return it, or attach a `.catch` that logs.
- Log through `Logger`, not `console`.
