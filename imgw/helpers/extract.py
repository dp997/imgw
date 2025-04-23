import re
import zipfile
from io import BytesIO
from typing import Optional, Union
from urllib.parse import urlparse

import pyarrow as pa
from dlt.sources.helpers import requests
from pyarrow import csv
from pydantic import BaseModel

from imgw.schema import COLUMNS_DICT
from imgw.utils import get_logger

logger = get_logger(__name__)


class ImgwCsv(BaseModel):
    filename: str
    content: bytes


class ImgwZip(BaseModel):
    filename: str
    content: bytes


def save_failed_file(file: Union[ImgwCsv, ImgwZip]) -> None:
    try:
        with open(f"./.failed_files/{file.filename}", "wb") as f:
            f.write(file.content)
    except Exception:
        logger.exception("Failed to save file '%s'", file.filename)
    return None


def read_table(file: ImgwCsv, schemas: dict = COLUMNS_DICT) -> tuple[Optional[pa.Table], str]:
    try:
        table_type_match = re.match(r"^\D+", file.filename)
        if table_type_match:
            table_type = table_type_match.group().rstrip("_")
        else:
            return None, ""
    except AttributeError:
        logger.warning("'%s' does not match any table type", file.filename)
        return None, ""
    except Exception:
        logger.exception("could not match file name")
        return None, ""

    if table_type not in schemas:
        logger.warning("Unknown table type: %s", table_type)
        return None, ""

    logger.debug("Reading file: %s", file.filename)

    table_columns = list(schemas[table_type].keys())
    buffer_reader = BytesIO(file.content)

    try:
        table = csv.read_csv(
            buffer_reader,
            csv.ReadOptions(column_names=table_columns, encoding="windows-1250"),
            csv.ParseOptions(),
            csv.ConvertOptions(column_types=schemas[table_type]),
        )
        logger.debug(table)
    except Exception:
        logger.exception("Error while parsing CSV file: %s", file.filename)
        save_failed_file(file)
        return None, ""
    else:
        return table, table_type


def unzip(zip_file: ImgwZip) -> list[ImgwCsv]:
    try:
        zip_data = BytesIO(zip_file.content)
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            files = []
            for file_info in zip_ref.infolist():
                if not file_info.is_dir():
                    with zip_ref.open(file_info.filename) as file:
                        imgw_file = ImgwCsv(filename=file_info.filename, content=file.read())
                        files.append(imgw_file)
                        return files
                else:
                    return []
    except zipfile.BadZipFile:
        logger.exception(
            "The provided bytes are not a valid zip file. Saving file %s in rejected location.", zip_file.filename
        )

        return []
    except Exception:
        logger.exception("Failed to read zip file.")
        return []
    return []


def fetch_data(url: str) -> ImgwZip:
    try:
        filename = urlparse(url).path.split("/")[-1]
        response = requests.get(url)
        print(response.status_code)
        response.raise_for_status()
    except Exception:
        logger.exception("Error fetching data for %s", url)
        return ImgwZip(filename="", content=b"")
    else:
        return ImgwZip(filename=filename, content=response.content)
