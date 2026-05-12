import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import sys
from colorama import init, Fore, Style

init(autoreset=True)

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
MAX_LOG_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5


def _create_rotating_handler(filename: str, level: int, formatter: logging.Formatter):
    path = LOG_DIR / filename
    handler = RotatingFileHandler(
        path,
        maxBytes=MAX_LOG_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG":    Fore.BLUE,
        "INFO":     Fore.GREEN,
        "WARNING":  Fore.YELLOW,
        "ERROR":    Fore.RED,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, Fore.WHITE)
        message = super().format(record)
        return f"{log_color}{message}{Style.RESET_ALL}"


class CustomLogger:
    def __init__(self, module_name: str):
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            self._setup_logger(module_name)

    def _setup_logger(self, module_name: str):
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_formatter = ColoredFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.WARNING)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

        if module_name == "metrics":
            self.logger.addHandler(
                _create_rotating_handler("metrics.log", logging.INFO, file_formatter)
            )
        else:
            self.logger.addHandler(
                _create_rotating_handler("transfer.log", logging.INFO, file_formatter)
            )
            self.logger.addHandler(
                _create_rotating_handler("error.log", logging.WARNING, file_formatter)
            )

    def get_logger(self) -> logging.Logger:
        return self.logger


def get_logger(module_name: str) -> logging.Logger:
    return CustomLogger(module_name).get_logger()
