import argparse
import logging

from imgw.common import setup_logging
from imgw.extract import get_dlt_datalake_pipeline, get_dlt_local_pipeline, imgw_historic, imgw_real_time

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--historic", action="store_true", help="Run historic pipeline")
    parser.add_argument("--local", action="store_true", help="Run pipeline locally (using duckdb)")
    parser.add_argument("--verbose", action="store_true", help="Set logging level to DEBUG")
    parser.add_argument("--failed-output", help="Directory to store failed files")
    parser.add_argument("--local-output", help="Path to duckdb database where data will be loaded")
    parser.add_argument("--refresh", action="store_true", help="Refresh data (delete and reload)")
    args = parser.parse_args()

    setup_logging()

    logger = logging.getLogger()
    logger.setLevel("DEBUG") if args.verbose else logger.setLevel("INFO")
    logger.info("Configuring DLT pipeline...")

    logger.info("Starting pipeline run...")

    run_kwargs = {}
    if args.refresh:
        run_kwargs["refresh"] = "drop_data"

    if args.historic:
        try:
            if args.local:
                load_info_imgw_historic = get_dlt_local_pipeline(dataset_name="imgw_historic").run(
                    imgw_historic(), **run_kwargs
                )
            else:
                load_info_imgw_historic = get_dlt_datalake_pipeline().run(imgw_historic(), **run_kwargs)
            logger.info("IMGW historic run finished. Load info:\n%s", load_info_imgw_historic.metrics)
        except Exception:
            logger.exception("Historic pipeline run failed.")

    try:
        if args.local:
            load_info_imgw_real_time = get_dlt_local_pipeline(dataset_name="imgw_real_time").run(imgw_real_time())
        else:
            load_info_imgw_real_time = get_dlt_datalake_pipeline().run(imgw_real_time())
        logger.info("IMGW real-time run finished. Load info:\n%s", load_info_imgw_real_time)
    except Exception:
        logger.exception("Pipeline run failed.")
