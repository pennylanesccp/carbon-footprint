# modules/road/ors_common.py
from __future__ import annotations

import os as _os
import time as _time
import json as _json
import random as _random
import logging as _logging
import hashlib as _hashlib
import sqlite3 as _sqlite3
from typing import Any as _Any, Dict as _Dict, Tuple as _Tuple, Optional as _Optional
from datetime import datetime

# ────────────────────────────────────────────────────────────────────────────────
# Errors
# ────────────────────────────────────────────────────────────────────────────────
class RateLimited(Exception): ...
class NoRoute(Exception): ...
class GeocodeNotFound(Exception): ...

# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────
def _setup_logger(name: str = "cabosupernet.road.ors_client") -> _logging.Logger:
    lvl = _os.getenv("ORS_LOG_LEVEL", "INFO").upper()
    logger = _logging.getLogger(name)
    if not logger.handlers:
        handler = _logging.StreamHandler()
        handler.setFormatter(_logging.Formatter(
              "[%(asctime)s] %(levelname)s %(name)s | %(message)s"
            , datefmt="%H:%M:%S"
        ))
        logger.addHandler(handler)
    logger.setLevel(getattr(_logging, lvl, _logging.INFO))
    return logger

_log = _setup_logger()

def _short(v: _Any, maxlen: int = 420) -> str:
    try:
        s = _json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        s = str(v)
    return (s if len(s) <= maxlen else s[:maxlen] + " …")

# ────────────────────────────────────────────────────────────────────────────────
# Rate limiter
# ────────────────────────────────────────────────────────────────────────────────
class _RateLimiter:
    # ~35 calls/min is safe on free tiers. Tune if you upgraded.
    def __init__(self, max_calls=35, per_seconds=60):
        self.max_calls = max_calls
        self.per       = per_seconds
        self.ts        = []

    def wait(self):
        now = _time.time()
        self.ts = [t for t in self.ts if now - t < self.per]
        if len(self.ts) >= self.max_calls:
            sleep_s = self.per - (now - self.ts[0]) + 0.05
            if sleep_s > 0:
                _time.sleep(sleep_s)
        self.ts.append(_time.time())

_rate_limiter = _RateLimiter()

# ────────────────────────────────────────────────────────────────────────────────
# Retry-After helper
# ────────────────────────────────────────────────────────────────────────────────
def _retry_after_seconds(resp) -> float | None:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)  # seconds
    except ValueError:
        try:
            dt = datetime.strptime(ra, "%a, %d %b %Y %H:%M:%S %Z")
            return max(0.0, (dt - datetime.utcnow()).total_seconds())
        except Exception:
            return None

# ────────────────────────────────────────────────────────────────────────────────
# Cache
# ────────────────────────────────────────────────────────────────────────────────
class _Cache:
    def __init__(self, path: str, ttl_s: int):
        self._path = path
        self._ttl  = ttl_s
        self._ensure()

    def _ensure(self) -> None:
        _os.makedirs(_os.path.dirname(self._path), exist_ok=True)
        con = _sqlite3.connect(self._path)
        try:
            with con:
                con.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                          k  TEXT PRIMARY KEY
                        , v  BLOB NOT NULL
                        , ts INTEGER NOT NULL
                    )
                """)
        finally:
            con.close()

    def get(self, k: str) -> _Optional[_Dict[str, _Any]]:
        con = _sqlite3.connect(self._path)
        try:
            row = con.execute("SELECT v, ts FROM cache WHERE k = ?", (k,)).fetchone()
            if not row:
                return None
            v_raw, ts = row
            if int(_time.time()) - int(ts) > self._ttl:
                return None
            return _json.loads(v_raw)
        finally:
            con.close()

    def set(self, k: str, v: _Dict[str, _Any]) -> None:
        con = _sqlite3.connect(self._path)
        try:
            with con:
                con.execute(
                      "INSERT OR REPLACE INTO cache(k,v,ts) VALUES (?,?,?)"
                    , (k, _json.dumps(v), int(_time.time()))
                )
        finally:
            con.close()

# ────────────────────────────────────────────────────────────────────────────────
# Small utils
# ────────────────────────────────────────────────────────────────────────────────
def _sha_key(endpoint: str, payload: _Dict[str, _Any]) -> str:
    msg = endpoint + "||" + _json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return _hashlib.sha256(msg.encode("utf-8")).hexdigest()

def _extract_error_text(resp) -> str:
    try:
        j = resp.json()
        if isinstance(j, dict):
            return _short(j)
        return str(j)
    except Exception:
        return (resp.text or "")[:500]

def _parse_retry_after(headers: _Dict[str, str], default_s: float) -> float:
    ra = headers.get("Retry-After")
    if ra:
        try:
            return max(default_s, float(ra))
        except Exception:
            pass
    return default_s + _random.uniform(0.05, 0.35)

# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────
class ORSConfig:
    def __init__(
          self
        , api_key: str | None = None
        , base_url: str = "https://api.openrouteservice.org"
        , connect_timeout_s: float = 8.0
        , read_timeout_s: float = 30.0
        , max_retries: int = 3
        , backoff_s: float = 0.9
        , cache_path: str = ".cache/ors_cache.sqlite"
        , cache_ttl_s: int = 30 * 24 * 3600
        , default_country: str = "BR"
        , default_profile: str = "driving-hgv"
        , user_agent: str = "Cabosupernet-ORSClient/4.0"
        , snap_retry_on_404: bool = True
        , snap_radius_m: int = 2500
    ):
        self.api_key           = (api_key or _os.getenv("ORS_API_KEY", "")).strip()
        self.base_url          = base_url.rstrip("/")
        self.connect_timeout_s = connect_timeout_s
        self.read_timeout_s    = read_timeout_s
        self.max_retries       = max_retries
        self.backoff_s         = backoff_s
        self.cache_path        = _os.path.abspath(_os.path.expanduser(cache_path))
        self.cache_ttl_s       = cache_ttl_s
        self.default_country   = (default_country or "BR").upper()
        self.default_profile   = default_profile
        self.user_agent        = user_agent
        self.snap_retry_on_404 = bool(snap_retry_on_404)
        self.snap_radius_m     = int(snap_radius_m)
        if not self.api_key:
            raise RuntimeError("ORS_API_KEY not set. Export ORS_API_KEY or pass api_key= to ORSConfig().")

    @property
    def timeouts(self) -> _Tuple[float, float]:
        return (self.connect_timeout_s, self.read_timeout_s)
