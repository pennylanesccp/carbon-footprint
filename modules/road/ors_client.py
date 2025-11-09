# modules/road/ors_client.py
from __future__ import annotations

import requests as _req
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .ors_common import (
      _log
    , _rate_limiter
    , _retry_after_seconds
    , _extract_error_text
    , ORSConfig
    , NoRoute, RateLimited
)
from .ors_mixins import GeocodingMixin, RoutingMixin

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
        cfg: ORSConfig | None = None
    ):
        self.cfg = cfg or ORSConfig(
              api_key=(api_key or None)
            , base_url=(base_url or "https://api.openrouteservice.org")
        )
        self.base_url = self.cfg.base_url
        self.timeout  = timeout if timeout is not None else self.cfg.timeouts

        self._sess = _req.Session()
        retries = Retry(
              total=self.cfg.max_retries
            , backoff_factor=self.cfg.backoff_s
            , status_forcelist=(429, 500, 502, 503, 504)
            , allowed_methods=frozenset(["GET", "POST", "HEAD", "OPTIONS"])
            , respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retries)
        self._sess.mount("https://", adapter)
        self._sess.mount("http://",  adapter)
        self._sess.headers.update({
              "Authorization": self.cfg.api_key
            , "User-Agent":   self.cfg.user_agent
            , "Accept":       "application/json"
        })

    @classmethod
    def from_env(cls) -> "ORSClient":
        return cls(cfg=ORSConfig())

    # ────────────────────────────────────────────────────────────────────────────
    # HTTP helpers (used by mixins)
    # ────────────────────────────────────────────────────────────────────────────
    def _request(self, method: str, path: str, **kwargs):
        _rate_limiter.wait()
        url = f"{self.base_url}{path}"
        kwargs.setdefault("timeout", self.timeout)

        resp = self._sess.request(method, url, **kwargs)
        if resp.status_code == 429:
            wait_s = _retry_after_seconds(resp) or 60.0
            import time as _time
            _time.sleep(wait_s)
            raise RateLimited(f"429 from {path}; waited {wait_s:.1f}s")

        if 200 <= resp.status_code < 300:
            return resp.json()

        if resp.status_code in (404, 422):
            raise NoRoute(f"No route for {path}: {_extract_error_text(resp)}")
        resp.raise_for_status()

    def _get(self, path: str, params=None):
        return self._request("GET", path, params=params)

    def _post(self, path: str, json=None):
        return self._request("POST", path, json=json)


__all__ = ["ORSClient", "ORSConfig"]
