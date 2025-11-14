# modules/infra/logging.py
# -*- coding: utf-8 -*-

"""
Central logging configuration for the project.

This is the single source of truth for how logging is configured.

Usage
-----
    from modules.infra.logging import init_logging, get_logger

    init_logging(level="INFO")
    log = get_logger(__name__)
    log.info("Hello from my module")
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
    level : str, default "INFO"
        Logging level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
    force : bool, default True
        If True, existing handlers on the root logger are removed before
        applying the new configuration. Useful for CLIs/tests.
    write_output : bool, default False
        If True and `log_file` is not provided, a default file under
        `logs/app.log` is created and logs are also written there.
    log_file : Optional[Path]
        If provided, logs are written to this file *in addition* to stdout.
    """
    # Translate string level to numeric level (fallback to INFO if invalid)
    numeric_level = getattr(logging, str(level).upper(), logging.INFO)

    root = logging.getLogger()
    if force:
        # Manually clear handlers (works across Python versions)
        for handler in list(root.handlers):
            root.removeHandler(handler)

    root.setLevel(numeric_level)

    # Common formatter:
    # [YYYY-MM-DD HH:MM:SS][LEVEL][logger.name] message
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

    log = get_logger(__name__)
    log.info("Logging configured")

    
def log_banner(log: logging.Logger, msg: str, *, char: str = "=", width: int = 60, box: bool = False) -> None:
    if not box:
        bar = char * width
        log.info(bar)
        log.info(msg)
        log.info(bar)
    else:
        inner = " " + msg + " "
        pad = max(0, width - len(inner))
        left = pad // 2
        right = pad - left
        top_bot = "═" * width
        log.info(f"╔{top_bot}╗")
        log.info(f"║{' ' * left}{inner}{' ' * right}║")
        log.info(f"╚{top_bot}╝")


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Convenience wrapper around logging.getLogger.

    New modules should use this instead of calling logging.getLogger()
    directly, so if the logging backend ever changes, only this module
    needs to be updated.
    """
    return logging.getLogger(name if name is not None else __name__)
