"""Build a main-dashboard-only source bundle using explicit allowlists."""
from pathlib import Path
import ast, hashlib, json, shutil, tarfile, tempfile
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/deployment'
NAME = 'bee-ridge-atspm-main-2026-10-15'
OUT.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory(prefix='bee-ridge-package-') as tmp:
    stage = Path(tmp) / NAME
    stage.mkdir()
    def copy(rel):
        src = ROOT / rel
        assert src.is_file() and not src.is_symlink(), rel
        dst = stage / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
    for rel in ['docker-compose.yml', 'backend/Dockerfile', 'backend/requirements.txt',
                'backend/server.py', 'backend/mapping_store.py', 'backend/detector_config_overlays.py',
                'backend/historical_hourly_run_dask.py', 'atspm/pyproject.toml',
                'atspm/MANIFEST.in', 'atspm/README.md', 'atspm/LICENSE',
                'atspm/BeeRidgeIntersections.csv', 'dashboard-app/Dockerfile',
                'dashboard-app/nginx.conf', 'dashboard-app/package.json',
                'dashboard-app/package-lock.json', 'dashboard-app/index.html',
                'dashboard-app/vite.config.ts', 'dashboard-app/tsconfig.json',
                'dashboard-app/tsconfig.app.json']:
        copy(rel)
    for p in (ROOT/'atspm').glob('bee_ridge_rd_at_*.csv'): copy(p.relative_to(ROOT))
    for directory, suffixes in [
        ('backend', set()),
        ('dashboard-app/src', {'.ts','.tsx','.css','.svg','.png','.jpg','.woff','.woff2'}),
        ('dashboard-app/public', {'.svg','.png','.jpg','.ico','.woff','.woff2'}),
        ('atspm/src', {'.py','.sql','.parquet'}),
        ('bottleneck_detection', {'.py'}),
        ('corridor_correlation_cliques', {'.py'}),
        ('Correlation_Feature (1)/synchrony', {'.py','.json'})]:
        for p in (ROOT/directory).rglob('*'):
            if p.is_file() and p.suffix in suffixes and not {'__pycache__','notebooks','tests'}.intersection(p.parts):
                copy(p.relative_to(ROOT))
    (stage/'.dockerignore').write_text('derived\n.git\n**/node_modules\n**/dist\n**/__pycache__\n**/*.pyc\n*.tar.gz\n')
    (stage/'.env.example').write_text('ATSPM_RAW_DIR=/app/derived/bee_ridge_atspm_feb_to_may\nATSPM_RAW_GLOB=atspm-*_filtered.csv\n')
    (stage/'DEPLOYMENT.md').write_text('''# Main Bee Ridge ATSPM dashboard — Docker handoff

This package contains only the main dashboard (port 4174), its Python API,
required ATSPM processing library, detector mappings, and runtime feature modules.
It excludes the anomaly dashboard, notebooks, presentations, reports, logs,
local environments, credentials, and large operational datasets.
The atspm library README/license and small packaged sample resources are retained
because they belong to the installed processing library.

## Prerequisites
Linux VM with Docker and Docker Compose. Building requires internet access to
container registries, npm, PyPI, and Debian package mirrors. This is a source/build
bundle, not a prebuilt image archive. Python dependencies include version ranges;
the frontend uses npm ci with its lockfile.

## Extract on the VM
From the directory containing the uploaded archive:

```bash
mkdir -p ~/bee-ridge-atspm
tar -xzf bee-ridge-atspm-main-2026-10-15.tar.gz -C ~/bee-ridge-atspm
cd ~/bee-ridge-atspm/bee-ridge-atspm-main-2026-10-15
cp .env.example .env
mkdir -p derived
```

## Supply data before startup
Data are NOT included. Preserve the following paths beneath this extracted folder:

- derived/bee_ridge_atspm_feb_to_may/atspm-*_filtered.csv — raw observations.
- derived/reference_data.sqlite — existing effective mappings/reference data.
- derived/dask_historical_<signal>_all_days_feb_to_may/ — historical outputs for
  each deployed signal; preserve folder contents and relative paths.
- Any applicable detector override files, such as
  derived/ctrl01240_atspm/detector_config_1240.csv, if used in the source deployment.

Do not transfer the entire project or unrelated datasets. The application may
write historical outputs and caches under derived, so the volume must be writable.
Changing ATSPM_RAW_DIR requires a container path beneath /app/derived.

## Build and run

```bash
docker compose config --quiet
docker compose up -d --build
docker compose ps
curl -f http://127.0.0.1:4174/api/health
```

The UI is http://127.0.0.1:4174/ on the VM. Frontend nginx forwards /api/ to the
backend on the internal Docker network; no separate API port is published.
The existing configuration publishes port 4174 on the VM interfaces. Access from
the VPN depends on the VM firewall/network policy.

For access through SSH from the Mac:

```bash
ssh -N -L 4174:127.0.0.1:4174 ufadmin@10.128.72.18
```

Then open http://127.0.0.1:4174/ on the Mac.

## Troubleshooting and stopping

```bash
docker compose logs --tail=100 backend frontend
docker compose down
```

The backend health check must succeed before the frontend starts. Check data
paths and backend logs if it remains unhealthy. docker compose down leaves the
host derived directory intact.

## Validation limits
The source bundle is checked for syntax, file inventory, and forbidden content.
Docker image builds and runtime health must be verified on the target VM when
Docker is unavailable on the packaging host. No server deployment is performed
by creating this archive.
''')
    files=sorted(p for p in stage.rglob('*') if p.is_file())
    for p in files:
        if p.suffix=='.py': ast.parse(p.read_text(),filename=str(p.relative_to(stage)))
        assert not any(x in p.relative_to(stage).parts for x in ['anomaly-dashboard','notebooks','.aws','.git','node_modules'])
        assert p.suffix not in {'.ipynb','.pptx','.log','.bak'}
    manifest={str(p.relative_to(stage)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files}
    (stage/'MANIFEST.sha256.json').write_text(json.dumps(manifest,indent=2)+'\n')
    archive=OUT/(NAME+'.tar.gz')
    with tarfile.open(archive,'w:gz') as t: t.add(stage,arcname=NAME)
    with tarfile.open(archive) as t:
        for rel,digest in manifest.items():
            assert hashlib.sha256(t.extractfile(NAME+'/'+rel).read()).hexdigest()==digest
    digest=hashlib.sha256(archive.read_bytes()).hexdigest()
    (OUT/(archive.name+'.sha256')).write_text(f'{digest}  {archive.name}\n')
    print(f'Created {archive}\n{len(manifest)} files; {archive.stat().st_size:,} bytes\nSHA256 {digest}')
