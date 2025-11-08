# modules/road/ors_client.py
# OpenRouteService client focused on road (default: driving-hgv),
# with caching, verbose logging, structured + text geocoding,
# and a helper for matrix routing.
#
# Env:
#   ORS_API_KEY=...
#   ORS_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR (default INFO)
#   ORS_ALLOW_VIACEP=1|0                     (resolver.py uses this; default 1)

from __future__ import annotations

import os as _os
import re as _re
import time as _time
import json as _json
import random as _random
import logging as _logging
import hashlib as _hashlib
import sqlite3 as _sqlite3
from typing import Any as _Any, Dict as _Dict, List as _List, Tuple as _Tuple, Optional as _Optional

import requests as _req

# ─────────────────────────────── logging
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

# ─────────────────────────────── cache
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

# ─────────────────────────────── utils
def _sha_key(endpoint: str, payload: _Dict[str, _Any]) -> str:
    msg = endpoint + "||" + _json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return _hashlib.sha256(msg.encode("utf-8")).hexdigest()

def _parse_retry_after(headers: _Dict[str, str], default_s: float) -> float:
    ra = headers.get("Retry-After")
    if ra:
        try:
            return max(default_s, float(ra))
        except Exception:
            pass
    return default_s + _random.uniform(0.05, 0.35)

def _extract_error_text(resp: _req.Response) -> str:
    try:
        j = resp.json()
        if isinstance(j, dict):
            return _short(j)
        return str(j)
    except Exception:
        return (resp.text or "")[:500]

# ─────────────────────────────── config
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

# ─────────────────────────────── client
class ORSClient:
    def __init__(self, cfg: ORSConfig):
        self.cfg    = cfg
        self._sess  = _req.Session()
        self._cache = _Cache(cfg.cache_path, cfg.cache_ttl_s)
        _log.debug(f"Cache DB at: {self._cache._path}")

    # 低-level POST with retry + cache + logging
    def _post(self, path: str, payload: _Dict[str, _Any]) -> _Dict[str, _Any]:
        url = f"{self.cfg.base_url}{path}"
        key = _sha_key(url, payload)
        cached = self._cache.get(key)
        if cached is not None:
            _log.debug(f"CACHE HIT POST {path} payload={_short(payload)}")
            return cached

        headers = {
              "Authorization": self.cfg.api_key
            , "Content-Type": "application/json; charset=utf-8"
            , "Accept": "application/json"
            , "User-Agent": self.cfg.user_agent
        }

        for attempt in range(1, self.cfg.max_retries + 1):
            t0 = _time.time()
            try:
                _log.debug(f"POST {path} attempt={attempt} payload={_short(payload)}")
                resp = self._sess.post(
                      url
                    , headers=headers
                    , data=_json.dumps(payload)
                    , timeout=self.cfg.timeouts
                )
                dt = int(((_time.time() - t0) * 1000))
                if resp.status_code in (429, 503):
                    wait = _parse_retry_after(resp.headers, self.cfg.backoff_s * attempt)
                    _log.warning(f"POST {path} attempt={attempt} status={resp.status_code} -> backing off {wait:.2f}s")
                    # no sleep on purpose (you preferred it snappy)
                    continue
                if not resp.ok:
                    msg = _extract_error_text(resp)
                    _log.error(f"POST {path} attempt={attempt} status={resp.status_code} in {dt}ms err={msg}")
                    resp.raise_for_status()
                data = resp.json()
                _log.debug(f"POST {path} OK status={resp.status_code} in {dt}ms resp={_short(data)}")
                self._cache.set(key, data)
                return data
            except _req.RequestException as e:
                if attempt >= self.cfg.max_retries:
                    _log.exception(f"POST {path} failed after {attempt} attempt(s).")
                    raise
                wait = self.cfg.backoff_s * attempt
                _log.warning(f"POST {path} attempt={attempt} exception={type(e).__name__} -> retry in {wait:.2f}s")
                # no sleep -> continue loop
                continue

        raise RuntimeError("Unreachable")

    def _get(self, path: str, params: _Dict[str, _Any]) -> _Dict[str, _Any]:
        url = f"{self.cfg.base_url}{path}"
        key = _sha_key(url, params)
        cached = self._cache.get(key)
        if cached is not None:
            _log.debug(f"CACHE HIT GET {path} params={_short(params)}")
            return cached

        headers = {
              "Authorization": self.cfg.api_key
            , "Accept": "application/json"
            , "User-Agent": self.cfg.user_agent
        }

        for attempt in range(1, self.cfg.max_retries + 1):
            t0 = _time.time()
            try:
                _log.debug(f"GET  {path} attempt={attempt} params={_short(params)}")
                resp = self._sess.get(
                      url
                    , headers=headers
                    , params=params
                    , timeout=self.cfg.timeouts
                )
                dt = int(((_time.time() - t0) * 1000))
                if resp.status_code in (429, 503):
                    wait = _parse_retry_after(resp.headers, self.cfg.backoff_s * attempt)
                    _log.warning(f"GET  {path} attempt={attempt} status={resp.status_code} -> backing off {wait:.2f}s")
                    continue
                if not resp.ok:
                    msg = _extract_error_text(resp)
                    _log.error(f"GET  {path} attempt={attempt} status={resp.status_code} in {dt}ms err={msg}")
                    resp.raise_for_status()
                data = resp.json()
                _log.debug(f"GET  {path} OK status={resp.status_code} in {dt}ms resp={_short(data)}")
                self._cache.set(key, data)
                return data
            except _req.RequestException as e:
                if attempt >= self.cfg.max_retries:
                    _log.exception(f"GET  {path} failed after {attempt} attempt(s).")
                    raise
                wait = self.cfg.backoff_s * attempt
                _log.warning(f"GET  {path} attempt={attempt} exception={type(e).__name__} -> retry in {wait:.2f}s")
                continue

        raise RuntimeError("Unreachable")

    # ─────────────────── geocoding (ORS endpoints)
    def geocode_text(self, text: str, size: int = 1, country: str | None = None) -> _List[_Dict[str, _Any]]:
        params = {
              "text": text
            , "size": size
            , "boundary.country": (country or self.cfg.default_country)
        }
        _log.info(f"GEOCODE text='{text}' country={params['boundary.country']} size={size}")
        return self._get("/geocode/search", params)

    def geocode_structured(
          self
        , street: str | None = None
        , housenumber: str | None = None
        , locality: str | None = None
        , region: str | None = None
        , postalcode: str | None = None
        , country: str | None = None
        , size: int = 1
    ) -> _Dict[str, _Any]:
        address = None
        if street and housenumber:
            address = f"{housenumber} {street}"
        elif street:
            address = street

        params: _Dict[str, _Any] = {
              "size": size
            , "country": (country or self.cfg.default_country)
        }
        if address:     params["address"]    = address
        if locality:    params["locality"]   = locality
        if region:      params["region"]     = region
        if postalcode:  params["postalcode"] = postalcode

        _log.info(f"GEOCODE structured params={_short(params)}")
        return self._get("/geocode/search/structured", params)

    # ─────────────────── road directions / matrix
    def _snap_to_road(self, coords_lonlat, profile: str, *, radius_m: int | None = None):
        rad = int(radius_m or self.cfg.snap_radius_m or 2500)
        payload = {
            "locations": coords_lonlat,
            "radius": rad,
        }
        _log.info(f"SNAP {profile} radius={rad} coords={_short(coords_lonlat)}")

        # Correct endpoint path is /v2/snap/{profile}
        try:
            data = self._post(f"/v2/snap/{profile}", payload)
        except _req.HTTPError as e:
            # Some ORS deployments don’t expose snap for all profiles (or at all).
            # Fall back to driving-car for snapping only (just to find nearest road).
            resp = getattr(e, "response", None)
            if resp is not None and resp.status_code in (400, 404):
                _log.warning("SNAP %s unavailable (status=%s) — falling back to driving-car", profile, getattr(resp, "status_code", "?"))
                data = self._post("/v2/snap/driving-car", payload)
            else:
                raise

        snapped = []
        for i, item in enumerate(data.get("locations", [])):
            if not isinstance(item, dict):
                continue
            loc = item.get("location") or []
            if isinstance(loc, list) and len(loc) == 2:
                snapped.append([float(loc[0]), float(loc[1])])
        return snapped or coords_lonlat

    def route_road(
          self
        , origin: _Any
        , destination: _Any
        , profile: str | None = None
        , geometry: bool = False
        , extra_info: _Optional[_List[str]] = None
        , avoid_features: _Optional[_List[str]] = None
    ) -> _Dict[str, _Any]:
        # defer resolving to addressing.resolver to avoid duplication
        from modules.addressing.resolver import resolve_point

        o = resolve_point(origin, ors=self)
        d = resolve_point(destination, ors=self)
        prof = (profile or self.cfg.default_profile)

        payload: _Dict[str, _Any] = {
              "coordinates": [[o["lon"], o["lat"]], [d["lon"], d["lat"]]]
            , "units": "m"
            , "preference": "fastest"
        }
        if extra_info:
            payload["extra_info"] = list(extra_info)
        if avoid_features:
            payload["options"] = { "avoid_features": list(avoid_features) }

        _log.info(f"ROUTE try1 {prof} coords={payload['coordinates']}")
        try:
            data = self._post(f"/v2/directions/{prof}", payload)
        except _req.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            body = _extract_error_text(e.response) if getattr(e, "response", None) else str(e)
            _log.error(f"ROUTE try1 failed status={code} msg={body}")
            if self.cfg.snap_retry_on_404 and code == 404:
                snapped = self._snap_to_road(payload["coordinates"], prof, radius_m=self.cfg.snap_radius_m)
                if snapped != payload["coordinates"]:
                    payload["coordinates"] = snapped
                    _log.info(f"ROUTE try2 after SNAP {prof} coords={payload['coordinates']}")
                    data = self._post(f"/v2/directions/{prof}", payload)
                else:
                    _log.warning("ROUTE SNAP made no change; re-raising.")
                    raise
            else:
                raise

        route = (data.get("routes") or data.get("features") or [None])[0]
        if route is None:
            raise RuntimeError("Directions response missing 'routes'.")

        summ = route.get("summary")
        if not summ and isinstance(route, dict):
            props = route.get("properties", {})
            summ = props.get("summary")

        resp: _Dict[str, _Any] = {
              "distance_m": float(summ["distance"]) if summ else None
            , "duration_s": float(summ["duration"]) if summ else None
            , "origin": o
            , "destination": d
        }
        if geometry:
            resp["geometry"] = route.get("geometry")
        # segments intentionally omitted from default response

        _log.info(
            f"ROUTE OK km={None if resp['distance_m'] is None else round(resp['distance_m']/1000,2)} "
            f"hrs={None if resp['duration_s'] is None else round(resp['duration_s']/3600,2)}"
        )
        return resp

    def matrix_road(
          self
        , origins: _List[_Any]
        , destinations: _List[_Any]
        , profile: str | None = None
    ) -> _Dict[str, _Any]:
        from modules.addressing.resolver import resolve_point

        os_ = [resolve_point(x, ors=self) for x in origins]
        ds_ = [resolve_point(x, ors=self) for x in destinations]

        coords = [[p["lon"], p["lat"]] for p in (os_ + ds_)]
        n_o = len(os_)
        sources          = list(range(0, n_o))
        destinations_idx = list(range(n_o, n_o + len(ds_)))
        prof = (profile or self.cfg.default_profile)

        payload = {
              "locations": coords
            , "sources": sources
            , "destinations": destinations_idx
            , "metrics": ["distance", "duration"]
            , "units": "m"
        }
        _log.info(f"MATRIX {prof} n_origins={len(os_)} n_destinations={len(ds_)}")
        data = self._post(f"/v2/matrix/{prof}", payload)
        return {
              "origins": os_
            , "destinations": ds_
            , "distances_m": data.get("distances")
            , "durations_s": data.get("durations")
        }
