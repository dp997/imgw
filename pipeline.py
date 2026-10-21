import argparse
import logging
import os

from imgw.common import setup_logging
from imgw.extract import (
    get_dlt_datalake_pipeline,
    get_dlt_local_pipeline,
    get_dlt_snowflake_pipeline,
    imgw_historic,
    imgw_real_time,
)


def _build_pipeline(args: argparse.Namespace, dataset_name: str):
    if args.local:
        local_kwargs = {"db_file": args.local_output} if args.local_output else {}
        return get_dlt_local_pipeline(dataset_name=dataset_name, **local_kwargs)
    if args.snowflake:
        return get_dlt_snowflake_pipeline(dataset_name=dataset_name)
    return get_dlt_datalake_pipeline()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the IMGW extract pipelines.")
    parser.add_argument("--historic", action="store_true", help="Run historic pipeline")
    parser.add_argument("--real-time", action="store_true", help="Run real-time pipeline")
    destination = parser.add_mutually_exclusive_group()
    destination.add_argument("--local", action="store_true", help="Run pipeline locally (using duckdb)")
    destination.add_argument("--snowflake", action="store_true", help="Run pipeline on snowflake")
    parser.add_argument("--verbose", action="store_true", help="Set logging level to DEBUG")
    parser.add_argument("--failed-output", help="Directory to store failed files")
    parser.add_argument("--local-output", help="Path to duckdb database where data will be loaded")
    parser.add_argument("--refresh", action="store_true", help="Refresh data (delete and reload)")
    args = parser.parse_args()

    if args.failed_output:
        os.environ["IMGW_FAILED_OUTPUT"] = args.failed_output

    setup_logging()

    logger = logging.getLogger()
    logger.setLevel("DEBUG" if args.verbose else "INFO")
    logger.info("Configuring DLT pipeline...")

    run_kwargs = {"refresh": "drop_data"} if args.refresh else {}
    run_both = not args.historic and not args.real_time
    failed = False

    if args.historic or run_both:
        logger.info("Starting historic pipeline run...")
        try:
            load_info = _build_pipeline(args, dataset_name="imgw_historic").run(imgw_historic(), **run_kwargs)
            logger.info("IMGW historic run finished. Load info:\n%s", load_info.metrics)
        except Exception:
            logger.exception("Historic pipeline run failed.")
            failed = True

    if args.real_time or run_both:
        logger.info("Starting real-time pipeline run...")
        try:
            load_info = _build_pipeline(args, dataset_name="imgw_real_time").run(imgw_real_time())
            logger.info("IMGW real-time run finished. Load info:\n%s", load_info.metrics)
        except Exception:
            logger.exception("Real-time pipeline run failed.")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
