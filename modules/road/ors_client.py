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
• Entry points should call init_logging() — this module only fetches the logger.
"""

from __future__ import annotations

import time as _time
import json as _json
from typing import Any as _Any, Dict as _Dict, Optional as _Optional

import requests as _req
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from modules.functions.logging import get_logger
from .ors_common import (
    _rate_limiter,
    _retry_after_seconds,
    _extract_error_text,
    ORSConfig,
    NoRoute,
    RateLimited,
    _Cache,
    _sha_key,
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
        # Config
        self.cfg = cfg or ORSConfig(
            api_key=(api_key or None),
            base_url=(base_url or "https://api.openrouteservice.org"),
        )
        self.base_url = self.cfg.base_url
        self.timeout = timeout if timeout is not None else self.cfg.timeouts

        # HTTP session with retries
        self._sess = _req.Session()
        retries = Retry(
            total=self.cfg.max_retries,
            backoff_factor=self.cfg.backoff_s,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "HEAD", "OPTIONS"]),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self._sess.mount("https://", adapter)
        self._sess.mount("http://", adapter)
        self._sess.headers.update(
            {
                "Authorization": self.cfg.api_key,
                "User-Agent": self.cfg.user_agent,
                "Accept": "application/json",
            }
        )

        # SQLite cache (shared for GET/POST payloads)
        self._cache = _Cache(self.cfg.cache_path, ttl_s=self.cfg.cache_ttl_s)
        _log.debug(
            "ORSClient ready base=%s timeout=%ss cache=%s",
            self.base_url,
            self.timeout,
            self.cfg.cache_path,
        )

    # -------------------------------------------------------------------------
    # Lifecycle helpers
    # -------------------------------------------------------------------------
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

    # -------------------------------------------------------------------------
    # Core HTTP layer (used by mixins)
    # -------------------------------------------------------------------------
    def _request(self, method: str, path: str, **kwargs) -> _Dict[str, _Any]:
        """
        Single entry point for GET/POST:
          1) rate-limit gate
          2) cache check (endpoint+payload hash key)
          3) request with retries
          4) map errors; parse JSON; cache store
        """
        method_u = method.upper()
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)

        # Build an immutable payload to derive the cache key
        params = kwargs.get("params") if method_u == "GET" else None
        body = kwargs.get("json") if method_u == "POST" else None
        payload_for_key: _Dict[str, _Any] = params if params is not None else (body or {})
        key = _sha_key(f"{method_u}:{path}", payload_for_key)

        # Cache: try fast path first
        cached = self._cache.get(key)
        if cached is not None:
            _log.debug("HTTP %s %s — cache HIT", method_u, path)
            return cached

        # Rate-limit gate
        _rate_limiter.wait()

        # Do the request with timing
        t0 = _time.time()
        try:
            resp = self._sess.request(method_u, url, **kwargs)
        except _req.RequestException as e:
            _log.error("HTTP %s %s — request exception: %s", method_u, path, type(e).__name__)
            raise

        dt = (_time.time() - t0) * 1000.0  # ms

        # Map known statuses
        if resp.status_code == 429:
            wait_s = _retry_after_seconds(resp) or 60.0
            _log.warning("HTTP 429 %s (%.0f ms) — waiting %.1fs then raising RateLimited", path, dt, wait_s)
            _time.sleep(wait_s)
            raise RateLimited(f"429 from {path}; waited {wait_s:.1f}s")

        if 200 <= resp.status_code < 300:
            # Parse JSON once, log size and duration
            try:
                data = resp.json()
            except ValueError:
                txt = (resp.text or "")[:200]
                _log.error("HTTP %s %s — invalid JSON (%.0f ms): %s", method_u, path, dt, txt)
                raise

            size_b = len((_json.dumps(data, ensure_ascii=False)).encode("utf-8"))
            _log.info("HTTP %s %s — %s (%.0f ms, %s B)", method_u, path, resp.status_code, dt, size_b)

            # Persist to cache (idempotent reads; deterministic posts)
            try:
                self._cache.set(key, data)
            except Exception:
                # Cache failures must not break the request path
                _log.debug("cache set failed (non-fatal) for key=%s", key[:12])

            return data

        # Map "no route" family (common for directions)
        if resp.status_code in (404, 422):
            msg = _extract_error_text(resp)
            _log.warning("HTTP %s %s — %s (%.0f ms) no-route: %s", method_u, path, resp.status_code, dt, msg)
            raise NoRoute(f"No route for {path}: {msg}")

        # Other HTTP errors → raise
        msg = _extract_error_text(resp)
        _log.error("HTTP %s %s — %s (%.0f ms) body=%s", method_u, path, resp.status_code, dt, msg)
        resp.raise_for_status()

    def _get(self, path: str, params: _Optional[_Dict[str, _Any]] = None) -> _Dict[str, _Any]:
        """Thin GET wrapper."""
        return self._request("GET", path, params=params)

    def _post(self, path: str, json: _Optional[_Dict[str, _Any]] = None) -> _Dict[str, _Any]:
        """Thin POST wrapper."""
        return self._request("POST", path, json=json)


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
