from collections.abc import Iterable
from typing import Optional

import dlt
import dlt.extract
import duckdb
from dlt.common.typing import TDataItem
from dlt.extract.items import DataItemWithMeta
from dlt.extract.resource import DltResource

from imgw.helpers.extract import ImgwCsv, fetch_data, read_table, unzip
from imgw.helpers.scraper import find_zip_links
from imgw.schema import COLUMNS_DLT_SCHEMA
from imgw.utils import get_logger

logger = get_logger(__name__)


@dlt.resource(selected=False)
def zip_links(root_urls: list[str] = dlt.config.value) -> Iterable[TDataItem]:
    for _root_url in root_urls:
        links = find_zip_links(_root_url)
        yield from links


@dlt.transformer(parallelized=True, write_disposition="replace")
def get_tables(zip_link: str) -> Iterable[TDataItem]:
    zip_file = fetch_data(zip_link)

    unzipped_file = unzip(zip_file)

    def _get_table(csv_file: ImgwCsv) -> Optional[DataItemWithMeta]:
        table_data, table_type = read_table(csv_file)
        table_schema = COLUMNS_DLT_SCHEMA.get(table_type)
        if table_schema:
            return dlt.extract.with_hints(table_data, dlt.extract.make_hints(table_type, columns=table_schema))
        else:
            return None

    for csv_file in unzipped_file:
        yield _get_table(csv_file)


@dlt.source(name="imgw_historic")
def imgw_historic() -> list[DltResource]:
    return [zip_links | get_tables]


def get_local_pipeline() -> dlt.Pipeline:
    db = duckdb.connect("./output/imgw.db", config={"memory_limit": "2GB", "preserve_insertion_order": "false"})
    return dlt.pipeline(
        pipeline_name="imgw_pipeline_local",
        destination=dlt.destinations.duckdb(db, destination_name="local"),
        dataset_name="imgw_historic",
        progress="log",
    )


def get_datalake_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="imgw_pipeline_datalake",
        destination=dlt.destinations.filesystem(destination_name="datalake"),
        dataset_name="raw_data",
        progress="log",
    )
