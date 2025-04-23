import logging
import logging.config
import sys
from typing import Optional

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

RESET_SEQ = "\033[0m"
COLOR_SEQ = "\033[1;%dm"
BOLD_SEQ = "\033[1m"

COLORS = {
    "WARNING": YELLOW,
    "INFO": WHITE,
    "DEBUG": BLUE,
    "CRITICAL": YELLOW,
    "ERROR": RED,
}


def setup_logging() -> None:
    class ColoredFormatter(logging.Formatter):
        def __init__(self, fmt: str, datefmt: Optional[str] = None, use_color: bool = True) -> None:
            logging.Formatter.__init__(self, fmt, datefmt)
            self.use_color = use_color

        def format(self, record: logging.LogRecord) -> str:
            levelname = record.levelname
            if self.use_color and levelname in COLORS:
                levelname_color = COLOR_SEQ % (30 + COLORS[levelname]) + levelname + RESET_SEQ
                record.levelname = levelname_color
            return logging.Formatter.format(self, record)

    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "colored": {
                "()": ColoredFormatter,
                "fmt": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "use_color": True,
            },
            "verbose": {
                "format": "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "colored",
            },
            "file": {
                "class": "logging.FileHandler",
                "filename": "log.log",
                "formatter": "verbose",
            },
        },
        "root": {"handlers": ["console", "file"], "level": "INFO"},
    }
    logging.config.dictConfig(LOGGING_CONFIG)


def get_logger(module_name: str) -> logging.Logger:
    return logging.getLogger(module_name)
