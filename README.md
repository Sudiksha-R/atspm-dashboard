# ATSPM Dashboard

This repository contains the custom dashboard, backend APIs, and historical-processing scripts built on top of Shawn Strasser's `atspm` tooling.

## What is included

- `dashboard-app/`
  - React/Vite frontend for the ATSPM dashboard UI
- `backend/`
  - local API server
  - historical hourly processing scripts
- project-level setup files like `.gitignore`

## What is intentionally not included

This GitHub version does **not** include:

- raw ATSPM CSV datasets
- detector/intersection mapping CSVs
- derived SQLite outputs
- local virtualenv / node_modules
- local clones of `atspm` and `atspm-report`

Those are kept out of version control because they are large or external.

## External dependencies used during development

This project expects local access to:

- `atspm`
- `atspm-report`

In the development workspace, those lived beside this repo:

- `./atspm`
- `./atspm-report`

## Local setup

1. Clone this repo.
2. Clone Shawn's repos beside it:
   - `git clone https://github.com/ShawnStrasser/atspm`
   - `git clone https://github.com/ShawnStrasser/atspm-report`
3. Create a Python virtualenv and install backend dependencies.
4. Install frontend dependencies inside `dashboard-app/`.
5. Place the required ATSPM CSV datasets and mapping files in the project root if you want to run the real-data workflows.

## Docker setup

The dashboard can run as two containers:

- `backend`: Python API server on container port `8000`
- `frontend`: Vite production build served by nginx on host port `4174`

Build and start everything from the repository root:

```bash
docker compose up --build
```

Then open:

```text
http://127.0.0.1:4174/
```

The compose file mounts local `./derived` into the backend at `/app/derived` so large raw CSV and SQLite outputs stay outside the Docker image. By default the backend reads:

```text
/app/derived/bee_ridge_atspm_feb_to_may
```

To use a different raw-data folder, edit `ATSPM_RAW_DIR` in `docker-compose.yml`, or override it at runtime:

```bash
ATSPM_RAW_DIR=/app/derived/bee_ridge_atspm_v3 docker compose up --build
```

The backend image expects the local `atspm/` folder to exist before building because this dashboard imports Shawn's `atspm` package and Bee Ridge detector mapping CSVs from that folder.

### Sending The Docker Bundle

To package the Docker-ready dashboard for someone else:

```bash
scripts/package_docker_bundle.sh
```

This writes:

```text
atspm-dashboard-docker-bundle.tar.gz
```

The bundle includes the main dashboard app code, Dockerfiles, Compose file, local `atspm/` source, and Bee Ridge mapping CSVs. It excludes heavyweight/runtime folders such as `derived/`, `node_modules/`, `.python_deps/`, build outputs, logs, local container caches, and zip archives. The anomaly dashboard is maintained and deployed from its own repository.

The recipient should unpack it, put the required raw/derived data under:

```text
derived/bee_ridge_atspm_feb_to_may/
```

Then run:

```bash
cp .env.example .env
docker compose up --build
```

They can open the dashboard at:

```text
http://127.0.0.1:4174/
```

### Apptainer on UF/HPC

On the RHEL cluster, rootless Podman cannot build this image unless the account has `/etc/subuid` and `/etc/subgid` ranges. Use the Apptainer runner instead:

```bash
scripts/run_apptainer_dashboard.sh
```

The first run builds:

```text
.apptainer/atspm-dashboard.sif
```

Then it starts:

- backend API: `http://127.0.0.1:8000/api/health`
- frontend: `http://127.0.0.1:4174/`

If you are connecting from a laptop, tunnel both ports:

```bash
ssh -L 4174:127.0.0.1:4174 -L 8000:127.0.0.1:8000 USER@HOST
```

The runner rebuilds the frontend with `VITE_API_BASE_URL=http://127.0.0.1:8000`, so the browser must be able to reach the tunneled backend port.

## Historical processing scripts

The backend folder includes scripts for:

- one-day hourly aggregation for a single signal
- all-day hourly aggregation for a single signal
- a Dask-based scalable version for signal-level preprocessing
