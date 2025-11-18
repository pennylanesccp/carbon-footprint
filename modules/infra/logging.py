# modules/infra/logging.py
# -*- coding: utf-8 -*-

"""
Central logging configuration for the project.

This is the single source of truth for how logging is configured.

Usage
-----
    from modules.infra.logging import init_logging, get_logger

    init_logging(level="INFO", write_output=True)
    log = get_logger(__name__)
    log.info("Hello from my module")

Environment
-----------
- CARBON_LOG_LEVEL, if set, overrides the `level` parameter.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# ────────────────────────────────────────────────────────────────────────────────
# Globals
# ────────────────────────────────────────────────────────────────────────────────

_DEFAULT_LOGS_DIR = Path("logs")

_current_logs_dir: Path = _DEFAULT_LOGS_DIR
_current_log_file: Optional[Path] = None


# ────────────────────────────────────────────────────────────────────────────────
# Public helpers
# ────────────────────────────────────────────────────────────────────────────────

def get_logs_dir() -> Path:
    """
    Return the directory where log files are written.

    This reflects whatever was configured in the last `init_logging()` call.
    """
    return _current_logs_dir


def get_current_log_path() -> Optional[Path]:
    """
    Return the path to the *current* log file, if any.

    - If no file handler is configured, returns None.
    - Useful for CLIs that want to print "Log file → ..." after init_logging().
    """
    global _current_log_file

    if _current_log_file is not None:
        return _current_log_file

    # Fallback: inspect root handlers (in case logging was configured elsewhere)
    root = logging.getLogger()
    for handler in root.handlers:
        if isinstance(handler, logging.FileHandler):
            try:
                fp = Path(handler.baseFilename)
                _current_log_file = fp
                return fp
            except Exception:
                continue
    return None


def init_logging(
      level: str = "INFO"
    , *
    , force: bool = True
    , write_output: bool = False
    , log_file: Optional[Path] = None
    , logs_dir: Optional[Path] = None
) -> None:
    """
    Configure root logging.

    Parameters
    ----------
    level : str, default "INFO"
        Logging level ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL").
        If environment variable CARBON_LOG_LEVEL is set, it overrides this.
    force : bool, default True
        If True, existing handlers on the root logger are removed before
        applying the new configuration. Useful for CLIs/tests.
    write_output : bool, default False
        If True and `log_file` is not provided, a per-run file is created under
        `logs/` (or the provided `logs_dir`) and logs are written there as well.
    log_file : Optional[Path]
        If provided, logs are written to this file *in addition* to stdout.
        Parent directory is created automatically.
    logs_dir : Optional[Path]
        Base directory for log files when `log_file` is not explicitly given.
        Defaults to `logs/` at repo root.
    """
    global _current_logs_dir
    global _current_log_file

    # Env override (used by child processes: CARBON_LOG_LEVEL)
    env_level = os.getenv("CARBON_LOG_LEVEL")
    if env_level:
        level = env_level

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

    _current_log_file = None  # will be set below if we add a file handler

    # Optional file handler (per-run log)
    if write_output or log_file is not None:
        if log_file is None:
            # Determine logs dir
            base_dir = Path(logs_dir) if logs_dir is not None else _DEFAULT_LOGS_DIR
            base_dir.mkdir(parents=True, exist_ok=True)

            # Script name (best-effort) + timestamp → e.g. bulk_multimodal_fuel_emissions_and_costs__20251117-174709.log
            script_name = Path(sys.argv[0] or "app").stem or "app"
            if script_name in {"-m", ""}:
                script_name = "app"
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            log_file = base_dir / f"{script_name}__{ts}.log"
        else:
            # Ensure parent exists if user passed a custom path
            log_file = Path(log_file)
            log_file.parent.mkdir(parents=True, exist_ok=True)
            base_dir = log_file.parent

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

        _current_logs_dir = base_dir
        _current_log_file = log_file.resolve()

    log = get_logger(__name__)
    log.info("Logging configured")


def log_banner(
      log: logging.Logger
    , msg: str
    , *
    , char: str = "="
    , width: int = 60
    , box: bool = False
) -> None:
    """
    Helper to print a visual banner in logs.

    - Simple mode (box=False): prints a bar, the message, and another bar.
    - Box mode: prints a Unicode box with the message centered.
    """
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


def get_logger(
    name: Optional[str] = None
) -> logging.Logger:
    """
    Convenience wrapper around logging.getLogger.

    New modules should use this instead of calling logging.getLogger()
    directly, so if the logging backend ever changes, only this module
    needs to be updated.
    """
    return logging.getLogger(name if name is not None else __name__)
