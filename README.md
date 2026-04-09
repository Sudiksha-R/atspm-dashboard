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

## Historical processing scripts

The backend folder includes scripts for:

- one-day hourly aggregation for a single signal
- all-day hourly aggregation for a single signal
- a Dask-based scalable version for signal-level preprocessing

These scripts write only derived outputs and do not modify the original source datasets.
