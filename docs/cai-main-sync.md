# CAI project ↔ `main` branch sync

The CAI Workbench project file tree should match **`origin/main`**, not ad-hoc feature branch uploads.

## 1. Update `main` on Origin

All tested feature work is merged via fast-forward from `cursor/cdp-base-iceberg-smoke-8307` (tip of the stacked PR chain):

- Monitoring, Live Events, AWC theme, SDK, Marketplace
- Lakehouse jobs, Trino OAuth probe, CDP Base smoke scripts

```bash
git checkout main
git pull origin main
git merge origin/cursor/cdp-base-iceberg-smoke-8307   # or already merged
git push origin main
git push github main   # optional personal mirror
```

## 2. Sync CAI project from `main`

```bash
export CAI_BASE=https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work
export CAI_PID=k87h-zej9-dugs-473y          # production project
export CAI_KEY=$CDSW_APIV2_KEY

git checkout main
git pull origin main

# Upload all git-tracked deploy files (+ local kafka certs)
python3 scripts/cai_project_sync.py upload

# Optional: remove stale files left from old experiments
python3 scripts/cai_project_sync.py upload --prune

# Verify remote matches manifest
python3 scripts/cai_project_sync.py verify

# Upload + recreate Applications
python3 scripts/cai_project_sync.py deploy
```

### What gets uploaded

| Source | Included |
|--------|----------|
| `git ls-files` | All tracked files except `.cursor/`, `agent-tools/`, `.venv/`, `.cache/` |
| `CAI_SYNC_EXTRA` | `config/kafka/kafka-ca.crt`, `config/kafka/oauth-ca.crt` (gitignored, must exist locally) |

### Branch guard

By default sync **refuses** unless `HEAD` is `main`. Override for experiments:

```bash
CAI_REQUIRE_BRANCH=cursor/my-feature python3 scripts/cai_project_sync.py upload --allow-branch cursor/my-feature
```

## 3. Test project (optional)

Create a new empty CAI project in the Console, then:

```bash
export CAI_PID=<new-project-id>
export CAI_REQUIRE_BRANCH=main
python3 scripts/cai_project_sync.py upload --prune
python3 scripts/cai_deploy.py
```

Use this to validate GitHub/Origin `main` before touching the production project.

## 4. GitHub mirror

Origin (`cloudera/datapulse`) is source of truth for Cloud Agent. Mirror to GitHub after merging `main`:

```bash
git push github main
git push github 'refs/heads/cursor/*:refs/heads/cursor/*'   # optional feature branches
```
