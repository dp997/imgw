import os
import re
import shutil
import subprocess
import zipfile
from collections.abc import Iterable
from io import BytesIO
from typing import Optional, Union
from urllib.parse import urlparse

import pyarrow as pa
from dlt.sources import TDataItem
from dlt.sources.helpers import requests
from pyarrow import csv
from pydantic import BaseModel, field_validator

from imgw.schema import COLUMNS_DICT
from imgw.utils import get_logger

logger = get_logger(__name__)


class FileResource(BaseModel):
    filename: str
    content: bytes


class ImgwCsv(FileResource):
    """Represents a CSV file resource."""

    @field_validator("filename")
    @classmethod
    def filename_must_be_csv(cls, v: str) -> str:
        """Validate if the filename ends with .csv."""
        if not v.lower().endswith(".csv"):
            raise ValueError("Filename must end with .csv")  # noqa: TRY003
        return v


class ImgwZip(FileResource):
    """Represents a ZIP file resource."""

    @field_validator("filename")
    @classmethod
    def filename_must_be_zip(cls, v: str) -> str:
        """Validate if the filename ends with .zip."""
        if not v.lower().endswith(".zip"):
            raise ValueError("Filename must end with .zip")  # noqa: TRY003
        return v


def save_failed_file(file: Union[ImgwCsv, ImgwZip]) -> str | None:
    """
    Saves a file to the rejected folder.

    Attempts to save the given file to the './.failed_files/' directory.
    Logs the outcome of the operation.

    Args:
        file (Union[ImgwCsv, ImgwZip]): The file to be saved.

    Returns:
        None
    """

    logger.info("Saving file: %s in rejected folder", file.filename)
    try:
        with open(f"./.failed_files/{file.filename}", "wb") as f:
            f.write(file.content)
    except Exception:
        logger.exception("Failed to save file '%s'", file.filename)
    else:
        if file.filename.endswith(".zip"):
            return f"./.failed_files/{file.filename}"
    return None


def read_table(file: ImgwCsv, schemas: dict = COLUMNS_DICT) -> tuple[Optional[pa.Table], str]:
    """
    Reads a CSV file into a PyArrow Table based on the file's table type.

    Args:
    file (ImgwCsv): The CSV file to be read.
    schemas (dict, optional): A dictionary mapping table types to their respective column schemas. Defaults to COLUMNS_DICT.

    Returns:
    tuple[Optional[pa.Table], str]: A tuple containing the read PyArrow Table and its corresponding table type.
    If the file does not match any known table type or an error occurs during parsing, returns (None, "").

    Raises:
    Logs exceptions and warnings using the logger.
    """
    try:
        table_type_match = re.match(r"^\D+", file.filename)
        if table_type_match:
            table_type = table_type_match.group().rstrip("_")
        else:
            logger.warning("'%s' does not match any table type", file.filename)
            save_failed_file(file)
            return None, ""
    except Exception:
        logger.exception("Failed while matching table type")
        save_failed_file(file)
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
    """
    Unzips the provided ImgwZip file into memory and returns a list of ImgwCsv objects.

    Args:
        zip_file (ImgwZip): The zip file to be unzipped.

    Returns:
        list[ImgwCsv]: A list of ImgwCsv objects representing the unzipped files.
        If the zip file is empty or invalid, an empty list is returned.
    """
    try:
        zip_data = BytesIO(zip_file.content)
        files = []
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                if not file_info.is_dir():
                    with zip_ref.open(file_info.filename) as file:
                        imgw_file = ImgwCsv(filename=file.name, content=file.read())
                        files.append(imgw_file)
                else:
                    return []
    except zipfile.BadZipFile:
        logger.exception("The provided bytes are not a valid zip file.")
        logger.info("Turning to alternative unzip method.")
        return unzip_alt(zip_file)
    except Exception:
        logger.exception("Failed to read zip file.")
        return []
    else:
        return files


def _unzip_file(unzip_path: str, failed_file_path: str, tmp_dir: str) -> None:
    """
    Unzips the file using the provided unzip_path.

    Args:
        unzip_path (str): The path to the unzip executable.
        failed_file_path (str): The path to the file to be unzipped.
        tmp_dir (str): The directory where the file will be unzipped.

    Raises:
        subprocess.CalledProcessError: If the unzip operation fails.
    """
    try:
        subprocess.run([unzip_path, "-d", str(tmp_dir), str(failed_file_path)], check=True)  # noqa: S603
    except subprocess.CalledProcessError:
        logger.exception("Failed to unzip file %s", failed_file_path)
        raise


def _validate_and_unzip(zip_file: ImgwZip) -> tuple[str, str]:
    """
    Saves the zip file and unzips it into a temporary directory.

    Args:
        zip_file: The zip file to be saved and unzipped.

    Returns:
        A tuple containing the path to the temporary directory where the zip file is unzipped and the path to the saved zip file.

    Raises:
        FileNotFoundError: If the 'unzip' command is not found.
    """
    failed_file_path = save_failed_file(zip_file)
    if not failed_file_path:
        return "", ""

    tmp_dir = f".tmp-{zip_file.filename}"
    unzip_path = shutil.which("unzip")
    if unzip_path is None:
        raise FileNotFoundError("unzip")

    logger.info("Unzipping file %s into %s...", failed_file_path, tmp_dir)
    _unzip_file(unzip_path, failed_file_path, tmp_dir)
    return tmp_dir, failed_file_path


def _read_csv_files(tmp_dir: str) -> list[ImgwCsv]:
    """Reads CSV files from the provided directory.

    Args:
        tmp_dir (str): The directory containing the CSV files.

    Returns:
        list[ImgwCsv]: A list of ImgwCsv objects representing the read CSV files.

    Raises:
        Exception: If an error occurs while reading a CSV file.
    """
    csv_files = [f for f in os.listdir(tmp_dir) if f.endswith(".csv")]
    imgw_files = []
    for csv_file in csv_files:
        file_path = os.path.join(tmp_dir, csv_file)
        try:
            logger.debug("Reading file %s", file_path)
            with open(file_path, "rb") as file:
                imgw_file = ImgwCsv(filename=csv_file, content=file.read())
                imgw_files.append(imgw_file)
        except Exception:
            logger.exception("Failed to read file %s", file_path)
    return imgw_files


def _cleanup(tmp_dir: str, failed_file_path: str) -> None:
    """
    Removes the temporary directory and the failed file.

    Args:
        tmp_dir (str): The path to the temporary directory to be removed.
        failed_file_path (str): The path to the failed file to be removed.

    Returns:
        None

    Notes:
        Logs exceptions if removal of temporary directory or failed file fails.
    """
    try:
        for file in os.listdir(tmp_dir):
            os.remove(os.path.join(tmp_dir, file))
        os.rmdir(tmp_dir)
    except Exception:
        logger.exception("Failed to remove temporary directory %s", tmp_dir)

    try:
        os.remove(failed_file_path)
    except Exception:
        logger.exception("Failed to remove failed file %s", failed_file_path)


### alternative (using `unzip`) extract method


def unzip_alt(zip_file: ImgwZip) -> list[ImgwCsv]:
    """
    Unzips the provided ImgwZip into temp directory, reads the CSV files, and returns them as a list of ImgwCsv objects.

    Args:
    zip_file (ImgwZip): The zip file to be unzipped.

    Returns:
    list[ImgwCsv]: A list of ImgwCsv objects representing the CSV files.
    """
    try:
        tmp_dir, failed_file_path = _validate_and_unzip(zip_file)
        if not tmp_dir:
            return []

        imgw_files = _read_csv_files(tmp_dir)
        _cleanup(tmp_dir, failed_file_path)
    except Exception:
        logger.exception("An error occurred while unzipping file %s", zip_file.filename)
        return []
    else:
        return imgw_files


def fetch_zip_data(url: str) -> ImgwZip:
    """
    Fetches zip data from a given URL.

    Args:
        url (str): The URL to fetch zip data from.

    Returns:
        ImgwZip: An ImgwZip object containing the filename and content of the fetched zip data.
                 If an error occurs during fetching, returns an ImgwZip object with empty filename and content.

    Raises:
        None
    """
    try:
        filename = urlparse(url).path.split("/")[-1]
        response = requests.get(url)
        response.raise_for_status()
    except Exception:
        logger.exception("Error fetching data for %s", url)
        return ImgwZip(filename="", content=b"")
    else:
        return ImgwZip(filename=filename, content=response.content)


def get_json_data(path: str) -> Iterable[TDataItem]:
    """
    Fetches JSON data from the specified IMGW API endpoint.

    Args:
        path (str): The API endpoint path.

    Yields:
        TDataItem: The JSON data items.

    Raises:
        requests.HTTPError: If the HTTP request returns an unsuccessful status code.
    """
    url = f"https://danepubliczne.imgw.pl/api/data/{path}"
    headers = {"Accept": "application/json"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.HTTPError as http_err:
        if http_err.response.status_code == 404:
            try:
                error_body = http_err.response.json()
                if (
                    isinstance(error_body, dict)
                    and error_body.get("status") is False
                    and error_body.get("message") == "No products were found"
                ):
                    logger.warning("No data for endpoint %s, skipping", path)
                    yield {}
                    return  # return statement to exit function
            except ValueError:
                pass
        raise  # This will be executed if the status code is not 404 or the response body doesn't match
    except requests.RequestException:
        raise
    else:
        result = response.json()
        yield result
