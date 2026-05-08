# logger.py
import logging
from datetime import datetime
from pathlib import Path
import sys
import schedule
import time
import threading
from typing import Dict
from colorama import init, Fore, Style

init(autoreset=True)


class CustomLogger:
    def __init__(self, module_name: str, log_dir: str = "logs"):
        """
        Initialize a logger for a specific module with automated
        date-wise log files and colored console output.
        """
        self.module_name = module_name
        self.log_dir = Path(log_dir)
        self.logger = logging.getLogger(module_name)
        self.logger.setLevel(logging.DEBUG)
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.file_handler = None

        if not self.logger.handlers:
            self._setup_logger()

        LoggerManager.register_logger(self)

    def _get_log_file_path(self) -> Path:
        """
        Generate the path for the log file based on module and current date.
        """
        date_str = datetime.now().strftime("%Y-%m-%d")
        module_dir = self.log_dir / self.module_name
        module_dir.mkdir(parents=True, exist_ok=True)
        return module_dir / f"{date_str}.log"

    def _setup_logger(self):
        """
        Set up the logger with file and console handlers.
        File handler writes detailed logs to date-wise files.
        Console handler outputs colored logs for real-time monitoring.
        """
        self.file_handler = logging.FileHandler(
            self._get_log_file_path(), encoding="utf-8"
        )
        self.file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.file_handler.setFormatter(file_formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        console_formatter = ColoredFormatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(console_handler)

    def rotate_log_file(self):
        """
        Rotate the log file for the current module to a new date-based file.
        """
        new_date = datetime.now().strftime("%Y-%m-%d")
        if new_date != self.current_date:
            self.logger.info(
                f"Rotating log file for module {self.module_name} to {new_date}"
            )
            if self.file_handler:
                self.logger.removeHandler(self.file_handler)
                self.file_handler.close()

            self.file_handler = logging.FileHandler(
                self._get_log_file_path(), encoding="utf-8"
            )
            self.file_handler.setLevel(logging.DEBUG)
            file_formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
            self.file_handler.setFormatter(file_formatter)
            self.logger.addHandler(self.file_handler)
            self.current_date = new_date
            self.logger.debug(f"Log file rotated to {self._get_log_file_path()}")

    def get_logger(self) -> logging.Logger:
        """Return the configured logger instance."""
        return self.logger


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter to add colors to console logs based on log level.
    """
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


class LoggerManager:
    _loggers: Dict[str, CustomLogger] = {}

    @classmethod
    def register_logger(cls, logger: CustomLogger):
        """Register a logger instance for scheduled rotation."""
        cls._loggers[logger.module_name] = logger

    @classmethod
    def rotate_all_loggers(cls):
        """Rotate log files for all registered loggers."""
        for module_name, logger in cls._loggers.items():
            logger.rotate_log_file()

    @classmethod
    def start_scheduler(cls):
        """Start the scheduler to rotate log files daily at midnight."""
        schedule.every().day.at("00:00").do(cls.rotate_all_loggers)

        def run_scheduler():
            while True:
                schedule.run_pending()
                time.sleep(60)

        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()


def get_logger(module_name: str) -> logging.Logger:
    """Get a logger instance for the specified module."""
    return CustomLogger(module_name).get_logger()


# Start scheduler when module is imported
LoggerManager.start_scheduler()