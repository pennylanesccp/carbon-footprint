# modules/road/ors_common.py
# -*- coding: utf-8 -*-
"""
Common pieces for the ORS client stack:
- Error classes
- Standardized logging (via modules.functions.logging)
- Simple token-bucket-ish rate limiter
- Lightweight SQLite cache (keyed by endpoint+payload hash)
- Helpers for Retry-After/backoff and response error extraction
- ORSConfig (timeouts, base URL, API key, cache settings, etc.)

This module is "pure infra" — it does not perform HTTP calls; the HTTP logic
lives in modules/road/ors_client.py. Keep this module side-effect free (no
init_logging here); the entry points/scripts should call init_logging().
"""

from __future__ import annotations

import os
import time
import json
import random
import hashlib
import sqlite3
from typing import Any, Dict, Tuple, Optional
from datetime import datetime, timezone

from modules.infra.logging import get_logger

# ────────────────────────────────────────────────────────────────────────────────
# Errors
# ────────────────────────────────────────────────────────────────────────────────

class RateLimited(Exception):
    """Raised by higher-level code when a 429 was seen repeatedly."""
    ...

class NoRoute(Exception):
    """Raised when ORS reports that no route could be found."""
    ...

class GeocodeNotFound(Exception):
    """Raised when geocoding produced no acceptable results after fallbacks."""
    ...


# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────

_log = get_logger(__name__)

def _short(v: Any, maxlen: int = 420) -> str:
    """
    Safe, concise preview of a Python object. Useful in logs.
    """
    try:
        s = json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        s = str(v)
    return s if len(s) <= maxlen else (s[:maxlen] + " …")


# ────────────────────────────────────────────────────────────────────────────────
# Rate limiter
# ────────────────────────────────────────────────────────────────────────────────

class _RateLimiter:
    """
    Very simple sliding-window rate limiter.
    Default ~35 calls/min is safe for many free tiers; tune if you upgraded.
    """
    def __init__(self, max_calls: int = 35, per_seconds: float = 60.0) -> None:
        self.max_calls = int(max_calls)
        self.per = float(per_seconds)
        self.ts: list[float] = []

    def wait(self) -> None:
        """
        If window is saturated, sleep just enough to fall below the threshold.
        """
        now = time.time()
        # keep timestamps inside the current window
        self.ts = [t for t in self.ts if (now - t) < self.per]
        if len(self.ts) >= self.max_calls:
            sleep_s = self.per - (now - self.ts[0]) + 0.05
            if sleep_s > 0:
                _log.debug(
                    "rate-limit: window=%ss max_calls=%s current=%s → sleeping %.3fs",
                    self.per, self.max_calls, len(self.ts), sleep_s
                )
                time.sleep(sleep_s)
        # record this call time
        self.ts.append(time.time())

_rate_limiter = _RateLimiter()


# ────────────────────────────────────────────────────────────────────────────────
# Retry-After helper (RFC 7231)
# ────────────────────────────────────────────────────────────────────────────────

def _retry_after_seconds(resp) -> Optional[float]:
    """
    Extract Retry-After header as seconds.
    Supports delta-seconds or HTTP-date. Returns None if not present/parsable.
    """
    ra = getattr(resp, "headers", {}).get("Retry-After")
    if not ra:
        return None
    # delta-seconds
    try:
        return float(ra)
    except (ValueError, TypeError):
        pass
    # HTTP-date
    try:
        # Example: 'Wed, 21 Oct 2015 07:28:00 GMT'
        dt = datetime.strptime(ra, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0.0, (dt - now).total_seconds())
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────────────────────
# Cache (SQLite)
# ────────────────────────────────────────────────────────────────────────────────

class _Cache:
    """
    Very small key/value cache backed by SQLite.
    Keys are opaque hashes (see _sha_key), values are JSON blobs.
    TTL is enforced on read; expired entries are treated as misses.
    """
    def __init__(self, path: str, ttl_s: int) -> None:
        self._path = path
        self._ttl = int(ttl_s)
        self._ensure()

    def _ensure(self) -> None:
        os.makedirs(os.path.dirname(self._path), exist_ok=True)
        con = sqlite3.connect(self._path)
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
        _log.debug("cache: ensured db at %s (ttl_s=%s)", self._path, self._ttl)

    def get(self, k: str) -> Optional[Dict[str, Any]]:
        con = sqlite3.connect(self._path)
        try:
            row = con.execute("SELECT v, ts FROM cache WHERE k = ?", (k,)).fetchone()
            if not row:
                _log.debug("cache: MISS key=%s", k[:12])
                return None
            v_raw, ts = row
            age = int(time.time()) - int(ts)
            if age > self._ttl:
                _log.debug("cache: EXPIRED key=%s age=%ss ttl=%ss", k[:12], age, self._ttl)
                return None
            try:
                val = json.loads(v_raw)
            except Exception:
                _log.debug("cache: CORRUPT JSON key=%s", k[:12])
                return None
            _log.debug("cache: HIT key=%s age=%ss", k[:12], age)
            return val
        finally:
            con.close()

    def set(self, k: str, v: Dict[str, Any]) -> None:
        con = sqlite3.connect(self._path)
        try:
            payload = json.dumps(v, ensure_ascii=False)
            with con:
                con.execute(
                    "INSERT OR REPLACE INTO cache(k,v,ts) VALUES (?,?,?)",
                    (k, payload, int(time.time())),
                )
            _log.debug("cache: SET key=%s size=%sB", k[:12], len(payload.encode("utf-8")))
        finally:
            con.close()


# ────────────────────────────────────────────────────────────────────────────────
# Small utils
# ────────────────────────────────────────────────────────────────────────────────

def _sha_key(endpoint: str, payload: Dict[str, Any]) -> str:
    """
    Create a stable key for (endpoint, payload). Payload is JSON-dumped
    with sort_keys=True so keys order doesn't affect the hash.
    """
    msg = endpoint + "||" + json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(msg.encode("utf-8")).hexdigest()

def _extract_error_text(resp) -> str:
    """
    Best-effort extraction of a human-friendly error from a HTTP response.
    """
    try:
        j = resp.json()
        if isinstance(j, dict):
            return _short(j)
        return str(j)
    except Exception:
        try:
            return (resp.text or "")[:500]
        except Exception:
            return "<no-text>"

def _parse_retry_after(headers: Dict[str, str], default_s: float) -> float:
    """
    Read Retry-After header (if any) and return a backoff in seconds.
    Adds small jitter otherwise to avoid thundering herds.
    """
    ra = headers.get("Retry-After")
    if ra:
        try:
            sec = float(ra)
            return max(default_s, sec)
        except Exception:
            pass
    jitter = random.uniform(0.05, 0.35)
    return default_s + jitter


# ────────────────────────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────────────────────────

class ORSConfig:
    """
    Configuration bundle for the ORS client.

    Parameters
    ----------
    api_key : str | None
        If None, reads from env ORS_API_KEY.
    base_url : str
        ORS base URL (no trailing slash).
    connect_timeout_s : float
        TCP connect timeout (seconds).
    read_timeout_s : float
        Response/read timeout (seconds).
    max_retries : int
        Max HTTP retries for transient failures (5xx/429/timeout).
    backoff_s : float
        Base backoff (seconds) for retry scheduling.
    cache_path : str
        SQLite cache path (created if missing).
    cache_ttl_s : int
        Cache TTL in seconds (default 30 days).
    default_country : str
        ISO2 country for geocoding hint (default BR).
    default_profile : str
        ORS routing profile (e.g., 'driving-hgv', 'driving-car').
    user_agent : str
        Sent as User-Agent.
    snap_retry_on_404 : bool
        For snap-to-road helpers in ors_client.
    snap_radius_m : int
        Snap-to-road search radius (meters).
    """
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = "https://api.openrouteservice.org",
        connect_timeout_s: float = 8.0,
        read_timeout_s: float = 30.0,
        max_retries: int = 3,
        backoff_s: float = 0.9,
        cache_path: str = ".cache/ors_cache.sqlite",
        cache_ttl_s: int = 30 * 24 * 3600,
        default_country: str = "BR",
        default_profile: str = "driving-hgv",
        user_agent: str = "Cabosupernet-ORSClient/4.0",
        snap_retry_on_404: bool = True,
        snap_radius_m: int = 2500,
    ) -> None:
        self.api_key = (api_key or os.getenv("ORS_API_KEY", "")).strip()
        self.base_url = base_url.rstrip("/")
        self.connect_timeout_s = float(connect_timeout_s)
        self.read_timeout_s = float(read_timeout_s)
        self.max_retries = int(max_retries)
        self.backoff_s = float(backoff_s)
        self.cache_path = os.path.abspath(os.path.expanduser(cache_path))
        self.cache_ttl_s = int(cache_ttl_s)
        self.default_country = (default_country or "BR").upper()
        self.default_profile = str(default_profile)
        self.user_agent = str(user_agent)
        self.snap_retry_on_404 = bool(snap_retry_on_404)
        self.snap_radius_m = int(snap_radius_m)

        if not self.api_key:
            _log.error("ORSConfig init: ORS_API_KEY not set")
            raise RuntimeError(
                "ORS_API_KEY not set. Export ORS_API_KEY or pass api_key= to ORSConfig()."
            )

        # Log a concise, non-sensitive summary
        _log.info(
            "ORSConfig init: base_url=%s timeouts=(%.1f,%.1f)s retries=%s backoff=%.2fs "
            "cache=%s ttl=%ss country=%s profile=%s snap_404=%s radius_m=%s ua=%s",
            self.base_url,
            self.connect_timeout_s,
            self.read_timeout_s,
            self.max_retries,
            self.backoff_s,
            self.cache_path,
            self.cache_ttl_s,
            self.default_country,
            self.default_profile,
            self.snap_retry_on_404,
            self.snap_radius_m,
            self.user_agent,
        )

    @property
    def timeouts(self) -> Tuple[float, float]:
        """Return (connect_timeout_s, read_timeout_s) for requests."""
        return (self.connect_timeout_s, self.read_timeout_s)

"""
────────────────────────────────────────────────────────────────────────────────
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging, get_logger, get_current_log_path; `
from modules.road.ors_common import ORSConfig, _Cache, _sha_key, _parse_retry_after, _RateLimiter, _retry_after_seconds; `
import os, json, time; from datetime import datetime, timezone, timedelta; `
init_logging(level='INFO', force=True, write_output=True); `
log = get_logger('smoke.ors_common'); `
print('LOGFILE =', get_current_log_path()); `
# ORSConfig test (will raise if ORS_API_KEY is not set)
try: `
    cfg = ORSConfig(); `
    print('ORSConfig OK:', cfg.base_url, cfg.default_country, cfg.default_profile); `
except Exception as e: `
    print('ORSConfig error:', type(e).__name__, str(e)); `
# Cache test
c = _Cache('.cache/test_ors_common.sqlite', ttl_s=60); `
k = _sha_key('/geocode', {'q':'test'}); `
c.set(k, {'hello':'world'}); `
print('Cache get:', json.dumps(c.get(k), ensure_ascii=False)); `
# Rate limiter test (force a sleep by allowing only 2 calls/sec and making 3)
rl = _RateLimiter(max_calls=2, per_seconds=1.0); `
t0 = time.time(); rl.wait(); rl.wait(); rl.wait(); dt = time.time()-t0; `
print('RateLimiter elapsed >= 1s?', dt>=1.0, 'dt=', round(dt,3)); `
# Retry-After tests
class Resp: `
    def __init__(self, h): self.headers = h `
r1 = Resp({'Retry-After':'2'}); `
print('Retry-After seconds:', _retry_after_seconds(r1)); `
r2 = Resp({'Retry-After': (datetime.now(timezone.utc)+timedelta(seconds=3)).strftime('%a, %d %b %Y %H:%M:%S GMT')}); `
print('Retry-After http-date:', round(_retry_after_seconds(r2) or -1, 3)); "
────────────────────────────────────────────────────────────────────────────────
"""
