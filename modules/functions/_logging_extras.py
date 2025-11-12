# modules/functions/logging_extras.py
import logging

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
