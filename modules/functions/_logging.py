# modules/functions/logging.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Iterable, Optional, Union

# Map string levels → logging levels
_LEVELS = {
    "CRITICAL": logging.CRITICAL,
    "ERROR":    logging.ERROR,
    "WARNING":  logging.WARNING,
    "INFO":     logging.INFO,
    "DEBUG":    logging.DEBUG,
    "NOTSET":   logging.NOTSET,
}

def _parse_level(level: Optional[Union[int, str]]) -> int:
    """Accept int, 'INFO', etc., or fall back to INFO."""
    if isinstance(level, int):
        return level
    if isinstance(level, str):
        return _LEVELS.get(level.upper(), logging.INFO)
    return logging.INFO


def init_logging(
    level: Optional[Union[int, str]] = None,
    *,
    stream = None,
    force: bool = False,
    quiet: Iterable[str] = (),
    write_output: bool = False,
    logs_dir: str = "logs",
) -> logging.Logger:
    """
    Configure root logging once with the format:
        [YYYY-MM-DD HH:MM:SS][LEVEL][logger.name] message

    Priority for level:
        1) 'level' argument
        2) env CARBON_LOG_LEVEL or LOG_LEVEL
        3) INFO

    Parameters
    ----------
    level : int | str | None
        Desired log level (e.g., "DEBUG", logging.INFO).
    stream : file-like | None
        Defaults to sys.stdout.
    force : bool
        If True, remove existing handlers before installing ours.
    quiet : Iterable[str]
        Logger names to quiet (set to WARNING if current level is lower).
    write_output : bool
        If True, also write logs to a file at logs/output_YYYYMMDDHHMMSS
        (path is exposed at root._carbon_fp_logfile).
    logs_dir : str
        Base directory for log files (default: "logs").

    Returns
    -------
    logging.Logger
        The root logger.
    """
    # Resolve level: arg → env → default
    env_level = os.getenv("CARBON_LOG_LEVEL") or os.getenv("LOG_LEVEL")
    lvl = _parse_level(level if level is not None else env_level)

    root = logging.getLogger()
    root.setLevel(lvl)

    # Avoid duplicate handlers unless force=True
    signature_attr = "_carbon_fp_handler"
    already_installed = any(getattr(h, signature_attr, False) for h in root.handlers)

    if force or not already_installed:
        if force:
            for h in list(root.handlers):
                root.removeHandler(h)

        # Common formatter (includes logger name)
        fmt = logging.Formatter(
            fmt="[%(asctime)s][%(levelname)s][%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Stream handler (stdout)
        sh = logging.StreamHandler(stream or sys.stdout)
        sh.setFormatter(fmt)
        setattr(sh, signature_attr, True)
        root.addHandler(sh)

        # Optional file handler
        log_file_path = None
        if write_output:
            os.makedirs(logs_dir, exist_ok=True)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            log_file_path = os.path.join(logs_dir, f"output_{ts}.log")
            fh = logging.FileHandler(log_file_path, encoding="utf-8")
            fh.setFormatter(fmt)
            setattr(fh, signature_attr, True)
            root.addHandler(fh)
        setattr(root, "_carbon_fp_logfile", log_file_path if write_output else None)

    # Quiet noisy third-party loggers if requested
    for name in quiet:
        lg = logging.getLogger(name)
        if lg.level < logging.WARNING:
            lg.setLevel(logging.WARNING)

    # Emit the init line (uses our module logger so the name appears)
    logging.getLogger(__name__).info("Logging configured")

    return root


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Get a logger by name. Usage: _log = get_logger(__name__)"""
    return logging.getLogger(name or __name__)


def set_level(level: Union[int, str], *, logger: Optional[logging.Logger] = None) -> None:
    """Change level at runtime."""
    (logger or logging.getLogger()).setLevel(_parse_level(level))


def get_current_log_path() -> Optional[str]:
    """Return the active log file path (if write_output=True), else None."""
    return getattr(logging.getLogger(), "_carbon_fp_logfile", None)

"""
────────────────────────────────────────────────────────────────────────────────
Quick logging smoke test (PowerShell)
python -c "from modules.functions.logging import init_logging, get_logger; init_logging(level='DEBUG', force=True, write_output=True); log=get_logger('modules.addressing.resolver'); log.debug('debug line'); log.info('hello world'); log.warning('heads up'); log.error('boom')"
────────────────────────────────────────────────────────────────────────────────
"""
