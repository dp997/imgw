from collections.abc import Iterable

import dlt
import dlt.extract
import duckdb
from dlt.common.typing import TDataItem
from dlt.extract.resource import DltResource

from imgw.helpers.extract import ImgwCsv, fetch_zip_data, get_json_data, read_table, unzip
from imgw.helpers.scraper import find_zip_links
from imgw.schema import COLUMNS_DLT_SCHEMA
from imgw.utils import get_logger

logger = get_logger(__name__)

DEFAULT_ENDPOINTS = [
    {"endpoint_name": "synoptyczne", "api_path": "synop"},
    {"endpoint_name": "hydrologiczne", "api_path": "hydro"},
    {"endpoint_name": "meteorologiczne", "api_path": "meteo"},
    {"endpoint_name": "ostrzezenia_meteorologiczne", "api_path": "warningsmeteo"},
    {"endpoint_name": "ostrzezenia_hydrologiczne", "api_path": "warningshydro"},
]


@dlt.resource(selected=False, parallelized=True)
def zip_links(root_urls: list[str] = dlt.config.value) -> Iterable[TDataItem]:
    """
    Retrieves zip links from the provided root URLs.

    Args:
        root_urls (list[str], optional): List of root URLs to fetch zip links from. Defaults to dlt.config.value.

    Yields:
        Iterable[TDataItem]: An iterable of zip links.
    """
    for _root_url in root_urls:
        links = find_zip_links(_root_url)
        yield from links


@dlt.transformer(selected=False, parallelized=True)
def csv_files(zip_link: str) -> Iterable[TDataItem]:
    """
    Fetches and unzips CSV files from a given ZIP link.

    Args:
        zip_link (str): The URL of the ZIP file containing CSV files.

    Yields:
        Iterable[TDataItem]: An iterable of unzipped CSV files.
    """
    zip_file = fetch_zip_data(zip_link)

    unzipped_files = unzip(zip_file)

    yield from unzipped_files


@dlt.transformer(parallelized=True, write_disposition="replace")
def weather_tables(csv_file: ImgwCsv) -> Iterable[TDataItem]:
    """
    Processes weather data from an IMGW CSV file and yields extracted data with appropriate schema hints.

    Args:
    csv_file (ImgwCsv): The CSV file containing weather data.

    Yields:
    Iterable[TDataItem]: Extracted data items with schema hints based on the detected table type.

    Notes:
    If the detected table type has a corresponding schema in COLUMNS_DLT_SCHEMA, it is used to provide schema hints.
    Otherwise, no data is yielded.
    """
    table_data, table_type = read_table(csv_file)
    table_schema = COLUMNS_DLT_SCHEMA.get(table_type)
    if table_schema:
        yield dlt.extract.with_hints(table_data, dlt.extract.make_hints(table_type, columns=table_schema))
    else:
        pass


@dlt.source(name="imgw_historic")
def imgw_historic() -> list[DltResource]:
    """
    Returns a list of DltResources representing the historic weather data from IMGW.

    The returned list includes resources for zip links, CSV files, and weather tables.

    Returns:
        list[DltResource]: A list of DltResources for the historic weather data.
    """
    return [zip_links | csv_files | weather_tables]


def get_local_pipeline(
    dataset_name: str,
    db_file: str = "output/imgw.db",
) -> dlt.Pipeline:
    """
    Creates a dlt pipeline for loading data into a local DuckDB database.

    Args:
        db_file (str): The path to the DuckDB database file.

    Returns:
        dlt.Pipeline: A dlt pipeline configured to load data into the specified local DuckDB database.
    """
    db = duckdb.connect(db_file, config={"memory_limit": "2GB", "preserve_insertion_order": "false"})
    return dlt.pipeline(
        pipeline_name="imgw_pipeline_local",
        destination=dlt.destinations.duckdb(db, destination_name="local"),
        dataset_name=dataset_name,
        progress="log",
    )


def get_datalake_pipeline(dataset_name: str = "raw_data") -> dlt.Pipeline:
    """
    Returns a dlt Pipeline instance configured for the datalake.

    The pipeline is named "imgw_pipeline_datalake" and is set up to write to the "datalake" destination.
    The dataset name is set to "raw_data" and progress is logged.

    Returns:
        dlt.Pipeline: A dlt Pipeline instance.
    """

    return dlt.pipeline(
        pipeline_name="imgw_pipeline_datalake",
        destination=dlt.destinations.filesystem(destination_name="datalake"),
        dataset_name=dataset_name,
        progress="log",
    )


@dlt.source(name="imgw_real_time", max_table_nesting=0)
def imgw_real_time() -> list[DltResource]:
    """
    IMGW real-time API source function generating a list of resources based on endpoints.

    Returns:
        Iterable[DltResource]: List of resource functions.
    """
    resources = []
    for endpoint in DEFAULT_ENDPOINTS:
        res_function = dlt.resource(get_json_data, name=endpoint["endpoint_name"], write_disposition="append")(
            path=endpoint["api_path"]
        )
        resources.append(res_function)
    return resources
