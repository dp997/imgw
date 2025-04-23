import argparse
import logging

from imgw import get_datalake_pipeline, get_local_pipeline, imgw_historic
from imgw.utils import setup_logging

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--local", action="store_true", help="Run pipeline locally (using duckdb)")
    parser.add_argument("--verbose", action="store_true", help="Set logging level to DEBUG")
    args = parser.parse_args()

    setup_logging()

    logger = logging.getLogger()
    logger.setLevel("DEBUG") if args.verbose else logger.setLevel("INFO")
    logger.info("Configuring DLT pipeline...")

    logger.info("Starting pipeline run...")
    try:
        if args.local:
            load_info_imgw_historic = get_local_pipeline().run(imgw_historic())
        else:
            load_info_imgw_historic = get_datalake_pipeline().run(imgw_historic())
        logger.info("IMGW historic run finished. Load info:\n%s", load_info_imgw_historic)
    except Exception:
        logger.exception("Pipeline run failed.")
