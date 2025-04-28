import argparse
import logging
import os
from typing import Optional

from imgw import get_datalake_pipeline, get_local_pipeline, imgw_historic, imgw_real_time
from imgw.utils import setup_logging


def check_directory(path: str, create: Optional[bool] = False) -> None:
    """
    Check if the directory exists, create it if it doesn't and create is True.

    Args:
    path (str): The directory path to check.
    create (Optional[bool], optional): Whether to create the directory if it doesn't exist. Defaults to False.

    Raises:
    NotADirectoryError: If the path exists but is not a directory.
    FileNotFoundError: If create is False and the directory doesn't exist.
    OSError: If there's an OS-related error while creating the directory.
    """
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise NotADirectoryError(path)
    else:
        if create:
            try:
                os.makedirs(path, exist_ok=True)
                logging.info("Created directory %s", path)
            except OSError:
                logging.exception("Failed to create directory %s", path)
                raise
        else:
            raise FileNotFoundError(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--historic", action="store_true", help="Run historic pipeline")
    parser.add_argument("--local", action="store_true", help="Run pipeline locally (using duckdb)")
    parser.add_argument("--verbose", action="store_true", help="Set logging level to DEBUG")
    parser.add_argument("--failed-output", help="Directory to store failed files")
    parser.add_argument("--local-output", help="Path to duckdb database where data will be loaded")
    args = parser.parse_args()

    setup_logging()

    logger = logging.getLogger()
    logger.setLevel("DEBUG") if args.verbose else logger.setLevel("INFO")
    logger.info("Configuring DLT pipeline...")

    failed_output_directory = args.failed_output if args.failed_output else "./.failed_files/"
    check_directory(failed_output_directory, True)

    logger.info("Starting pipeline run...")

    if args.historic:
        try:
            if args.local:
                load_info_imgw_historic = get_local_pipeline(dataset_name="imgw_historic").run(imgw_historic())
            else:
                load_info_imgw_historic = get_datalake_pipeline().run(imgw_historic())
            logger.info("IMGW historic run finished. Load info:\n%s", load_info_imgw_historic)
        except Exception:
            logger.exception("Historic pipeline run failed.")

    try:
        if args.local:
            load_info_imgw_real_time = get_local_pipeline(dataset_name="imgw_real_time").run(imgw_real_time())
        else:
            load_info_imgw_real_time = get_datalake_pipeline().run(imgw_real_time())
        logger.info("IMGW real-time run finished. Load info:\n%s", load_info_imgw_real_time)
    except Exception:
        logger.exception("Pipeline run failed.")
