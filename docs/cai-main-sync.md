# CAI project ↔ `main` branch sync

The CAI Workbench project file tree should match **GitHub `main`**, not ad-hoc feature branch uploads.

**Git remote:** `https://github.com/will-sh/datapulse.git` (sole source of truth). We no longer push to or pull from Origin (`origin.cursor.com`).

## 1. Update `main` on GitHub

```bash
git checkout main
git pull origin main
# ... edit, commit ...
git push origin main
```

CAI Git-connected projects (e.g. `datapulse-readygo`) do **not** auto-sync: after `git push`, run **git pull** in the CAI project Console session.

## 2. Sync CAI project from `main` (API upload)

For projects without Git, or to upload gitignored files (Kafka certs):

```bash
export CAI_BASE=https://ray-ml.cldr-csk-cai-readygo.a70735.test.cldr.work
export CAI_PID=k87h-zej9-dugs-473y          # production project
export CAI_KEY=$CDSW_APIV2_KEY

git checkout main
git pull origin main

python3 scripts/cai_project_sync.py upload --prune
python3 scripts/cai_project_sync.py verify
python3 scripts/cai_project_sync.py deploy     # optional: recreate Applications
```

### What gets uploaded

| Source | Included |
|--------|----------|
| `git ls-files` | All tracked files except `.cursor/`, `agent-tools/`, `.venv/`, `.cache/` |
| `CAI_SYNC_EXTRA` | `config/kafka/kafka-ca.crt`, `config/kafka/oauth-ca.crt` (gitignored, must exist locally) |

Zero-byte files are uploaded with a trailing newline because CAI rejects empty uploads.

`upload --prune` removes stale **top-level** remote files before upload. Verification checks each manifest path via GET.

### Branch guard

By default sync **refuses** unless `HEAD` is `main`.

## 3. Git-connected test project

Create project from Git URL `https://github.com/will-sh/datapulse.git`, branch `main` (e.g. `datapulse-readygo`).

After each `git push origin main`:

```bash
git pull origin main
git log -1 --oneline   # should match GitHub main
```

Still run `cai_project_sync.py upload` once for Kafka TLS certs (gitignored).

## 4. Cloud Agent / local clone

```bash
git clone https://github.com/will-sh/datapulse.git
cd datapulse
git checkout main
```

For push from Cloud Agent, set `GITHUB_TOKEN` (see skill `github-datapulse-sync`).
