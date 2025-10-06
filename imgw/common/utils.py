import logging
import os
from typing import Optional


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
