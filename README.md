# imgw pipeline

[![Release](https://img.shields.io/github/v/release/dp997/imgw)](https://img.shields.io/github/v/release/dp997/imgw)
[![Build status](https://img.shields.io/github/actions/workflow/status/dp997/imgw/main.yml?branch=main)](https://github.com/dp997/imgw/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/dp997/imgw/branch/main/graph/badge.svg)](https://codecov.io/gh/dp997/imgw)
[![Commit activity](https://img.shields.io/github/commit-activity/m/dp997/imgw)](https://img.shields.io/github/commit-activity/m/dp997/imgw)
[![License](https://img.shields.io/github/license/dp997/imgw)](https://img.shields.io/github/license/dp997/imgw)

## Intro
This is a project that will encompass loading, modelling and presenting data from [imgw public API](https://danepubliczne.imgw.pl/pl/apiinfo).

Goal is to try out [dlt](https://dlthub.com) and maybe some orchestration tools.

What is available for now:
- pipeline for historic data

to do:
- loading data from real-time endpoints
- modelling
- finishing with analysis/vis through a semantic layer

## Prerequisites
Project is managed with uv, so you should probably [install](https://docs.astral.sh/uv/getting-started/) it.

Clone the repository with git and go into project directory:
```
git clone https://github.com/dp997/imgw
cd imgw
```

Synchronize the environment:
```uv sync```
The command will create a new python virtual environment inside `.venv` directory.

## 1. Configuring the pipeline
The pipeline is managed with dlt (optionally dagster). The `.dlt` directory contains `config.toml` with some sane defaults for pipeline to run.
If you want to store pipeline results somewhere outside of local duckdb instance, you should configure dlt secrets:
1. In a `.dlt/secrets.toml` file:
```
[destination.datalake]
bucket_url = "s3://your_bucket"

[destination.datalake.credentials]
aws_access_key_id = "your_aws_access_key_id"
aws_secret_access_key = "your_aws_secret_access_key"
endpoint_url = "your_s3_endpoint" # if needed
```
2. Using environment variables:
Learn how to do that [here](https://dlthub.com/docs/general-usage/credentials/setup#environment-variables)

## 2. Running the pipeline
Use the provided pipeline.py script:

`uv run pipeline.py`

That will execute the script with `datalake` destination (object storage).

If you want to run the pipeline locally with duckdb output, use the `--local` flag:

### WARNING BEFORE YOU RUN LOCAL PIPELINE
This is a **lot** of data. Pipeline **will** consume a lot of memory to insert it into duckdb. The config writer defaults seem to be OK for now, but you need to monitor the process to make sure you're not going to run into out-of-memory exceptions.

`uv run pipeline.py --local`

Duckdb database will be saved in `outputs/imgw.db` by default.

## 3. Dagster
Install the Dagster adapter, then run the dev webserver:

```
uv sync --extra dagster
uv run --with=dagster-webserver dagster dev
```

To validate the Dagster definitions locally without materializing assets or
executing dlt loads (structure-only check):

```
PYTHONPATH=. uv run python -m pytest tests/orchestrators/test_dagster_definitions.py -v
```

## 4. Airflow
Airflow is an optional orchestrator extra. Install it alongside the core dependencies with:

```
uv sync --extra airflow
```

To validate the DAGs locally with a parse-only check via `DagBag` (no task
execution, no IMGW requests, no dlt loads):

```
PYTHONPATH=. uv run python -m pytest tests/orchestrators/test_airflow_dags.py -v
```

> Note: the Dagster and Airflow commands above only validate the definitions
> structurally. Configuring dlt credentials/secrets (see section 1) is still
> required to actually run any data loads.

## 5. Docker
TODO


Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
