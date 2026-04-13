import logging
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_LOGS_DIR = _PROJECT_ROOT / "logs"
_ROOT_FILE_CONFIGURED = False


def _ensure_root_file_handler(formatter: logging.Formatter) -> None:
    """
    One file per process: all loggers propagate to root, which writes to logs/.
    Avoids duplicate lines from multiple FileHandlers on different named loggers.
    """
    global _ROOT_FILE_CONFIGURED
    if _ROOT_FILE_CONFIGURED:
        return

    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_path = _LOGS_DIR / f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    root = logging.getLogger()
    if root.level > logging.DEBUG:
        root.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _ROOT_FILE_CONFIGURED = True


def setup_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """Console on this logger; file output once on root (logs/etl_YYYYMMDD_HHMMSS.log)."""

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper()))

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    _ensure_root_file_handler(formatter)

    if not logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger
