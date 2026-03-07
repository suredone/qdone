# Releasing qdone

## Prerequisites

- npm account with publish access to the `qdone` package
- 2FA enabled on your npm account (OTP required for publish)
- pnpm installed locally
- Node.js >= 16

## Publishing a New Version

1. **Make changes and ensure tests pass:**

   ```
   pnpm test
   ```

2. **Bump the version** in `package.json`.

3. **Prep for publish** — cleans, builds the CommonJS output, and generates `npm-shrinkwrap.json`:

   ```
   pnpm run prep-for-publish
   ```

   This must use `npm` internally for the shrinkwrap step (the script will remind you).

4. **Commit** the version bump and updated `npm-shrinkwrap.json`:

   ```
   git add package.json npm-shrinkwrap.json
   git commit -m "v2.2.0"
   ```

5. **Tag and push:**

   ```
   git tag v2.2.0
   git push && git push --tags
   ```

6. **Publish to npm** (requires OTP):

   - Stable release: `npm publish --tag latest`
   - Pre-release: `npm publish --tag next`

## Updating SureDone Consumers

After publishing, update these files in [suredone/suredone](https://github.com/suredone/suredone):

- `deploy/versions.sh` — the canonical `QDONE_VERSION` pin (this is what AMI builds actually use)
- `deploy/image/provision.sh` — the `QDONE_VERSION` fallback default (only applies when `versions.sh` is absent)
- `ui/server/package.json` — qdone dependency
- `data-models/reports/package.json` — qdone dependency

Then regenerate the lockfile:

```
pnpm install
```

This updates `pnpm-lock.yaml` to reflect the new qdone version in the above `package.json` files.

Create a single PR for all changes.

> **Warning:** `deploy/versions.sh` is sourced by `provision.sh` at startup and overrides
> its `${QDONE_VERSION:-...}` default. If you only update `provision.sh`, the version in
> `versions.sh` wins and new AMI builds will install the old version.

## npm Dist Tags

| Tag | Purpose |
|-----|---------|
| `latest` | Current stable release (default for `npm install qdone`) |
| `next` | Pre-release / development versions |

## CommonJS Build

The `commonjs/` directory is built by `pnpm run build` (called by `prep-for-publish`). All SureDone Node.js consumers import from `qdone/commonjs` — this subpath must always be included in published packages. The `files` array in `package.json` controls what's published.

## npm Access

Current maintainers: check with `npm owner ls qdone`.

To add a new maintainer:

```
npm owner add <username> qdone
```
