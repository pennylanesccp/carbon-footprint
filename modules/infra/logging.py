# modules/infra/logging.py
# -*- coding: utf-8 -*-

"""
Central logging configuration for the project.

This is the single source of truth for how logging is configured.
Existing modules that imported `modules.functions._logging` will go
through a thin adapter, but the implementation lives here.

Usage (preferred for new code)
------------------------------
    from modules.infra.logging import init_logging, get_logger

    init_logging(level="INFO")
    log = get_logger(__name__)
    log.info("Hello")

Notes
-----
- By default logs go to stdout.
- Optionally, logs can also be written to a file if `log_file` is given
  or `write_output=True` (you can adapt this to your preference).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional


def init_logging(
      level: str = "INFO"
    , *
    , force: bool = True
    , write_output: bool = False
    , log_file: Optional[Path] = None
) -> None:
    """
    Configure root logging.

    Parameters
    ----------
    level : str
        Logging level name (e.g. "DEBUG", "INFO", "WARNING").
    force : bool, default True
        If True, existing handlers on the root logger are removed before
        applying the new configuration. This is useful for CLIs/tests.
    write_output : bool, default False
        If True and `log_file` is not provided, a default file under
        `logs/app.log` may be created (you can adjust this behaviour).
    log_file : Optional[Path]
        Optional explicit path to a log file. If provided, a FileHandler
        will be added in addition to the stdout handler.
    """
    # Translate level string to numeric level (fallback to INFO)
    numeric_level = getattr(logging, str(level).upper(), logging.INFO)

    root = logging.getLogger()
    if force:
        # Manually clear handlers to support older Python versions
        for handler in list(root.handlers):
            root.removeHandler(handler)

    root.setLevel(numeric_level)

    # Common formatter: [YYYY-MM-DD HH:MM:SS][LEVEL][logger.name] message
    formatter = logging.Formatter(
          fmt="[{asctime}][{levelname}][{name}] {message}"
        , datefmt="%Y-%m-%d %H:%M:%S"
        , style="{"
    )

    # Stream handler (stdout)
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    # Optional file handler
    if write_output or log_file is not None:
        if log_file is None:
            logs_dir = Path("logs")
            logs_dir.mkdir(parents=True, exist_ok=True)
            log_file = logs_dir / "app.log"

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    # Small confirmation (logger name shows this module)
    log = get_logger(__name__)
    log.info("Logging configured")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Convenience wrapper around logging.getLogger.

    New modules should use this instead of calling logging.getLogger
    directly, so if the logging backend ever changes, the impact is
    localized to this module.
    """
    return logging.getLogger(name if name is not None else __name__)
