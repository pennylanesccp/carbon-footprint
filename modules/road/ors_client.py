# modules/road/ors_client.py
# -*- coding: utf-8 -*-
"""
Concrete ORS HTTP client:
- Composes GeocodingMixin + RoutingMixin
- Centralizes HTTP (session, retries, headers)
- Applies simple rate-limiting and optional caching
- Emits standardized, high-signal logs for observability

Notes
-----
• Keep infra knobs in ORSConfig (timeouts, retries, cache path/ttl, UA).
• Mixins call _get/_post which land here; this function does:
    - rate-limit gate
    - (optional) cache lookup/store
    - request w/ Retry adapter
    - JSON decode + error mapping (429→RateLimited, 404/422→NoRoute)
• Read-timeout strategy is "fast-then-slow" to cut latency tails:
    - First attempt uses a short read timeout (e.g., 6–8 s)
    - If that times out, one salvage attempt uses a longer read timeout
• Entry points should call init_logging() — this module only fetches the logger.
"""

from __future__ import annotations

import time as _time
import json as _json
from typing import Any as _Any, Dict as _Dict, Optional as _Optional, Iterable as _Iterable

import requests as _req
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from modules.functions._logging import get_logger
from .ors_common import (
      _rate_limiter
    , _retry_after_seconds
    , _extract_error_text
    , ORSConfig
    , NoRoute
    , RateLimited
    , _Cache
    , _sha_key
)
from .ors_mixins import GeocodingMixin, RoutingMixin

_log = get_logger(__name__)


class ORSClient(GeocodingMixin, RoutingMixin):
    """
    Backward-compatible constructor:
      - Prefer: ORSClient(cfg=ORSConfig(...))
      - Also ok: ORSClient(base_url, api_key, timeout=(5,45))
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        timeout: tuple[float, float] | None = None,
        cfg: ORSConfig | None = None,
    ):
        # ────────────────────────────────────────────────────────────────────
        # Config (tolerant to older ORSConfig without the new fast/slow fields)
        # ────────────────────────────────────────────────────────────────────
        self.cfg = cfg or ORSConfig(
              api_key=(api_key or None)
            , base_url=(base_url or "https://api.openrouteservice.org")
        )
        self.base_url = self.cfg.base_url

        # Legacy tuple support: (connect_s, read_slow_s)
        _legacy_ct, _legacy_rt_slow = (8.0, 45.0)
        if timeout is not None:
            _legacy_ct, _legacy_rt_slow = timeout

        # Pick timeouts with graceful fallback if cfg lacks newer attrs
        self.connect_timeout_s      = getattr(self.cfg, "connect_timeout_s", _legacy_ct)
        self.read_timeout_fast_s    = getattr(self.cfg, "read_timeout_fast_s", 2.0)
        self.read_timeout_slow_s    = getattr(self.cfg, "read_timeout_slow_s", _legacy_rt_slow)
        self.escalate_on_timeout    = getattr(self.cfg, "escalate_on_timeout", True)

        # Retry knobs (keep modest to avoid long stalls)
        self.max_retries_total      = getattr(self.cfg, "max_retries", 2)
        self.retry_backoff_s        = getattr(self.cfg, "backoff_s", 0.2)

        # Cache knobs
        self.cache_path             = getattr(self.cfg, "cache_path", None)
        self.cache_ttl_s            = getattr(self.cfg, "cache_ttl_s", 0)

        # HTTP headers
        self.user_agent             = getattr(self.cfg, "user_agent", "ors-client/unknown")
        self.api_key                = getattr(self.cfg, "api_key", None)

        # ────────────────────────────────────────────────────────────────────
        # HTTP session with retries (status-based; timeouts are per-request)
        # ────────────────────────────────────────────────────────────────────
        self._sess = _req.Session()
        retries = Retry(
              total=self.max_retries_total
            , connect=0                     # let connect timeout govern latency
            , read=min(1, self.max_retries_total)  # at most one re-read
            , backoff_factor=self.retry_backoff_s
            , status_forcelist=(429, 500, 502, 503, 504)
            , allowed_methods=frozenset(["GET", "POST", "HEAD", "OPTIONS"])
            , respect_retry_after_header=True
            , raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries)
        self._sess.mount("https://", adapter)
        self._sess.mount("http://", adapter)
        self._sess.headers.update(
            {
                  "Authorization": self.api_key
                , "User-Agent": self.user_agent
                , "Accept": "application/json"
            }
        )

        # SQLite cache (shared for GET/POST payloads)
        self._cache = _Cache(self.cache_path, ttl_s=self.cache_ttl_s)

        _log.debug(
            "ORSClient ready base=%s ct=%.1fs rt_fast=%.1fs rt_slow=%.1fs retries=%s cache=%s",
              self.base_url
            , self.connect_timeout_s
            , self.read_timeout_fast_s
            , self.read_timeout_slow_s
            , self.max_retries_total
            , self.cache_path
        )

    # ────────────────────────────────────────────────────────────────────────
    # Lifecycle helpers
    # ────────────────────────────────────────────────────────────────────────
    @classmethod
    def from_env(cls) -> "ORSClient":
        """Convenience ctor that pulls ORS_API_KEY from env."""
        return cls(cfg=ORSConfig())

    def close(self) -> None:
        """Explicitly close the underlying HTTP session."""
        try:
            self._sess.close()
        except Exception:
            pass

    # ────────────────────────────────────────────────────────────────────────
    # Core HTTP layer (used by GeocodingMixin / RoutingMixin)
    # ────────────────────────────────────────────────────────────────────────
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: _Optional[_Dict[str, _Any]] = None,
        json: _Optional[_Dict[str, _Any]] = None,
        timeout_profile: str = "fast_then_slow",    # "fast_only" | "slow_only" | "fast_then_slow"
        cache: bool = True,
    ) -> _Dict[str, _Any]:
        """
        Single entry point for GET/POST:
          1) rate-limit gate
          2) cache check (endpoint+payload hash key)
          3) request with retries
          4) map errors; parse JSON; cache store

        Parameters
        ----------
        method : str
            "GET" or "POST".
        path : str
            Endpoint path starting with "/".
        params / json : dict | None
            Payload (mutually exclusive by method).
        timeout_profile : str
            "fast_only" → use fast read timeout only;
            "slow_only" → use slow read timeout only;
            "fast_then_slow" → first fast, then one salvage slow attempt.
        cache : bool
            Whether to use the SQLite cache for this call.
        """
        method_u = method.upper()
        url = f"{self.base_url}{path}"

        # Build an immutable payload for cache key derivation
        payload_for_key: _Dict[str, _Any]
        if method_u == "GET":
            payload_for_key = params or {}
        else:
            payload_for_key = json or {}

        key = _sha_key(f"{method_u}:{path}", payload_for_key)

        # Cache: try fast path first
        if cache:
            cached = self._cache.get(key)
            if cached is not None:
                _log.debug("HTTP %s %s — cache HIT", method_u, path)
                return cached

        # Rate-limit gate
        _rate_limiter.wait()

        # Determine the per-attempt read timeouts
        attempt_read_timeouts: _Iterable[float]
        if timeout_profile == "fast_only":
            attempt_read_timeouts = (self.read_timeout_fast_s,)
        elif timeout_profile == "slow_only":
            attempt_read_timeouts = (self.read_timeout_slow_s,)
        else:  # "fast_then_slow"
            attempt_read_timeouts = (self.read_timeout_fast_s, self.read_timeout_slow_s) if self.escalate_on_timeout else (self.read_timeout_fast_s,)

        # Attempt loop: fast → slow
        last_exc: Exception | None = None
        for idx, rt in enumerate(attempt_read_timeouts, start=1):
            t0 = _time.time()
            try:
                resp = self._sess.request(
                      method_u
                    , url
                    , params=params if method_u == "GET" else None
                    , json=json if method_u == "POST" else None
                    , timeout=(self.connect_timeout_s, rt)
                )
            except _req.ReadTimeout as e:
                dt_ms = (_time.time() - t0) * 1000.0
                _log.warning(
                    "HTTP %s %s — ReadTimeout after %.0f ms (attempt %d, rt=%.1fs)",
                      method_u
                    , path
                    , dt_ms
                    , idx
                    , rt
                )
                last_exc = e
                continue
            except _req.RequestException as e:
                dt_ms = (_time.time() - t0) * 1000.0
                _log.error(
                    "HTTP %s %s — request exception %s after %.0f ms (attempt %d)",
                      method_u
                    , path
                    , type(e).__name__
                    , dt_ms
                    , idx
                )
                raise

            # Successful HTTP connection; evaluate response
            dt_ms = (_time.time() - t0) * 1000.0

            # 429 — respect server's backpressure
            if resp.status_code == 429:
                wait_s = _retry_after_seconds(resp) or 60.0
                _log.warning(
                    "HTTP 429 %s (%.0f ms, attempt %d) — waiting %.1fs then raising RateLimited",
                      path
                    , dt_ms
                    , idx
                    , wait_s
                )
                _time.sleep(wait_s)
                raise RateLimited(f"429 from {path}; waited {wait_s:.1f}s")

            # 2xx — parse JSON once, log size and duration, cache it
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                except ValueError:
                    txt = (resp.text or "")[:200]
                    _log.error(
                        "HTTP %s %s — invalid JSON (%.0f ms, attempt %d): %s",
                          method_u
                        , path
                        , dt_ms
                        , idx
                        , txt
                    )
                    raise

                size_b = len((_json.dumps(data, ensure_ascii=False)).encode("utf-8"))
                _log.info(
                    "HTTP %s %s — %s (%.0f ms, %s B, attempt %d, rt=%.1fs)",
                      method_u
                    , path
                    , resp.status_code
                    , dt_ms
                    , size_b
                    , idx
                    , rt
                )

                # Persist to cache (idempotent reads; deterministic posts)
                if cache:
                    try:
                        self._cache.set(key, data)
                    except Exception:
                        # Cache failures must not break the request path
                        _log.debug("cache set failed (non-fatal) for key=%s", key[:12])

                return data

            # Known "no route" family
            if resp.status_code in (404, 422):
                msg = _extract_error_text(resp)
                _log.warning(
                    "HTTP %s %s — %s (%.0f ms, attempt %d) no-route: %s",
                      method_u
                    , path
                    , resp.status_code
                    , dt_ms
                    , idx
                    , msg
                )
                raise NoRoute(f"No route for {path}: {msg}")

            # Other HTTP errors → raise immediately (adapter already retried statuses)
            msg = _extract_error_text(resp)
            _log.error(
                "HTTP %s %s — %s (%.0f ms, attempt %d) body=%s",
                  method_u
                , path
                , resp.status_code
                , dt_ms
                , idx
                , msg
            )
            resp.raise_for_status()

        # If we got here, all attempts have timed out
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"HTTP {method_u} {path} failed without response")

    # Thin wrappers used by mixins
    def _get(
        self,
        path: str,
        params: _Optional[_Dict[str, _Any]] = None,
        *,
        timeout_profile: str = "fast_then_slow",
        cache: bool = True,
    ) -> _Dict[str, _Any]:
        return self._request(
              "GET"
            , path
            , params=params
            , timeout_profile=timeout_profile
            , cache=cache
        )

    def _post(
        self,
        path: str,
        json: _Optional[_Dict[str, _Any]] = None,
        *,
        timeout_profile: str = "fast_then_slow",
        cache: bool = True,
    ) -> _Dict[str, _Any]:
        return self._request(
              "POST"
            , path
            , json=json
            , timeout_profile=timeout_profile
            , cache=cache
        )


__all__ = ["ORSClient", "ORSConfig"]


"""
────────────────────────────────────────────────────────────────────────────────
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.road.ors_common import ORSConfig; `
from modules.road.ors_client import ORSClient; `
import json; `
init_logging(level='INFO', force=True, write_output=False); `
ors = ORSClient(cfg=ORSConfig()); `
print('== geocode_text =='); `
print(json.dumps(ors.geocode_text('avenida luciano gualberto, 380', size=1), ensure_ascii=False)[:400]); print(); `
print('== geocode_structured =='); `
print(json.dumps(ors.geocode_structured(street='Av. Paulista', housenumber='1000', locality='São Paulo', region='SP', size=1), ensure_ascii=False)[:400]); print(); `
print('== route_road =='); `
print(json.dumps(ors.route_road('avenida luciano gualberto, 380', 'Curitiba, PR'), ensure_ascii=False, indent=2)); print(); `
print('== matrix_road (1x1) =='); `
print(json.dumps(ors.matrix_road(['São Paulo, SP'], ['Curitiba, PR']), ensure_ascii=False, indent=2)); "
────────────────────────────────────────────────────────────────────────────────
"""
