# modules/routes_provider.py
# ────────────────────────────────────────────────────────────────────────────────
# OpenRouteService client tuned for TRUCK ROUTES (driving-hgv) by default.
# - Verbose logging
# - SQLite caching
# - CEP handling with ViaCEP fallback (optional)
# - Automatic SNAP retry when a point is off-network (404)
# - Truck profile options injected automatically (vehicle_type + restrictions)
#
# pip install requests python-dotenv
#
# Env:
#   ORS_API_KEY=...
#   ORS_LOG_LEVEL=DEBUG|INFO|WARNING|ERROR   (default INFO)
#   ORS_ALLOW_VIACEP=1|0                     (default 1)
# ────────────────────────────────────────────────────────────────────────────────

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
def _setup_logger(name: str = "cabosupernet.routes_provider") -> _logging.Logger:
    lvl = _os.getenv("ORS_LOG_LEVEL", "INFO").upper()
    logger = _logging.getLogger(name)
    if not logger.handlers:
        handler = _logging.StreamHandler()
        handler.setFormatter(_logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s | %(message)s",
            datefmt="%H:%M:%S"
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


# ─────────────────────────────── config & utils
class ORSConfig:
    """
    Defaults are tailored for heavy vehicles (container trucks).

    You can override the truck defaults here or per-call:
      - hgv_vehicle_type: "goods" | "hgv"
      - hgv_restrictions: dict with keys like
            weight (kg), axleload (kg), height/width/length (m), hazmat (bool)
      - avoid_features_default: e.g. ["ferries"]
    """
    def __init__(
          self
        , api_key: str | None = None
        , base_url: str = "https://api.openrouteservice.org"
        , connect_timeout_s: float = 8.0
        , read_timeout_s: float = 30.0
        , max_retries: int = 3
        , backoff_s: float = 0.9
        , cache_path: str = "cache/ors_cache.sqlite"
        , cache_ttl_s: int = 30 * 24 * 3600  # 30 days
        , default_country: str = "BR"
        , default_profile: str = "driving-hgv"                 # <<<<<<<<<< default = TRUCK
        , user_agent: str = "Cabosupernet-ORSClient/4.0"
        , snap_retry_on_404: bool = True
        , snap_radius_m: int = 2500
        , allow_viacep: bool | None = None
        # truck defaults (can be tuned to your TF assumptions)
        , hgv_vehicle_type: str = "goods"                      # or "hgv"
        , hgv_restrictions: _Optional[_Dict[str, _Any]] = None
        , avoid_features_default: _Optional[_List[str]] = None
    ):
        self.api_key           = (api_key or _os.getenv("ORS_API_KEY", "")).strip()
        self.base_url          = base_url.rstrip("/")
        self.connect_timeout_s = connect_timeout_s
        self.read_timeout_s    = read_timeout_s
        self.max_retries       = max_retries
        self.backoff_s         = backoff_s
        self.cache_path        = _os.path.abspath(_os.path.expanduser(cache_path))
        _os.makedirs(_os.path.dirname(self.cache_path), exist_ok=True)
        self.cache_ttl_s       = cache_ttl_s
        self.default_country   = (default_country or "BR").upper()
        self.default_profile   = default_profile
        self.user_agent        = user_agent
        self.snap_retry_on_404 = bool(snap_retry_on_404)
        self.snap_radius_m     = int(snap_radius_m)

        env_flag               = _os.getenv("ORS_ALLOW_VIACEP")
        self.allow_viacep      = (allow_viacep if allow_viacep is not None
                                  else (False if env_flag == "0" else True))

        # default heavy-vehicle params (typical BR articulated truck)
        self.hgv_vehicle_type  = hgv_vehicle_type
        self.hgv_restrictions  = hgv_restrictions or {
            "weight":   40000,   # kg (gross)
            "axleload": 10000,   # kg
            "height":   4.20,    # m
            "width":    2.60,    # m
            "length":   18.60,   # m
            "hazmat":   False
        }
        self.avoid_features_default = avoid_features_default or ["ferries"]

        if not self.api_key:
            raise RuntimeError("ORS_API_KEY not set. Export ORS_API_KEY or pass api_key= to ORSConfig().")

    @property
    def timeouts(self) -> _Tuple[float, float]:
        return (self.connect_timeout_s, self.read_timeout_s)


def _sha_key(endpoint: str, payload: _Dict[str, _Any]) -> str:
    msg = endpoint + "||" + _json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return _hashlib.sha256(msg.encode("utf-8")).hexdigest()


class _Cache:
    def __init__(self, path: str, ttl_s: int):
        self._path = path
        self._ttl  = ttl_s
        self._ensure()

    def _ensure(self) -> None:
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
                con.execute("INSERT OR REPLACE INTO cache(k,v,ts) VALUES (?,?,?)",
                            (k, _json.dumps(v), int(_time.time())))
        finally:
            con.close()


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


# ─────────────────────────────── client
class ORSClient:
    def __init__(self, cfg: ORSConfig):
        self.cfg    = cfg
        self._sess  = _req.Session()
        self._cache = _Cache(cfg.cache_path, cfg.cache_ttl_s)
        _log.debug(f"Cache DB at: {self._cache._path}")

    # ─────────────────── low-level POST/GET with retry + cache + logging
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
                resp = self._sess.post(url, headers=headers, data=_json.dumps(payload), timeout=self.cfg.timeouts)
                dt = int(((_time.time() - t0) * 1000))
                if resp.status_code in (429, 503):
                    wait = _parse_retry_after(resp.headers, self.cfg.backoff_s * attempt)
                    _log.warning(f"POST {path} attempt={attempt} status={resp.status_code} -> backing off {wait:.2f}s")
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
                _log.warning(f"POST {path} attempt={attempt} exception={type(e).__name__} -> retrying in {wait:.2f}s")

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
                resp = self._sess.get(url, headers=headers, params=params, timeout=self.cfg.timeouts)
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
                _log.warning(f"GET  {path} attempt={attempt} exception={type(e).__name__} -> retrying in {wait:.2f}s")

        raise RuntimeError("Unreachable")

    # ─────────────────── geocoding helpers
    @staticmethod
    def _is_latlon_str(text: str) -> _Optional[_Tuple[float, float]]:
        if not isinstance(text, str):
            return None
        s = text.strip()
        m = _re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$", s)
        if not m:
            return None
        lat = float(m.group(1)); lon = float(m.group(2))
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return None
        return (lat, lon)

    @staticmethod
    def _is_cep(text: str) -> _Optional[str]:
        if not isinstance(text, str):
            return None
        m = _re.match(r"^\s*(\d{5})-?(\d{3})\s*$", text)
        if not m:
            return None
        return f"{m.group(1)}{m.group(2)}"

    @staticmethod
    def _reject_centroid(lat: float, lon: float) -> bool:
        # Brazil centroid (approx) used when Pelias returns country-level feature
        return abs(lat - (-10.0)) < 0.5 and abs(lon - (-55.0)) < 0.5

    @staticmethod
    def _filter_hits(hits: _List[_Dict[str, _Any]], allowed_layers: _Optional[_List[str]] = None):
        allowed = allowed_layers or [
            "address", "street", "venue", "postalcode", "postcode",
            "neighbourhood", "locality", "localadmin", "borough", "municipality"
        ]
        out = []
        for h in hits:
            layer = (h.get("layer") or "").lower()
            lat = float(h["lat"]); lon = float(h["lon"])
            if layer == "country":
                continue
            if ORSClient._reject_centroid(lat, lon):
                continue
            if allowed and layer and layer not in allowed:
                continue
            out.append(h)
        return out

    # ─────────────────── geocoding (text)
    def geocode_text(self, text: str, size: int = 1, country: str | None = None) -> _List[_Dict[str, _Any]]:
        params = {"text": text, "size": size, "boundary.country": (country or self.cfg.default_country)}
        _log.info(f"GEOCODE text='{text}' country={params['boundary.country']} size={size}")
        data = self._get("/geocode/search", params)
        feats = data.get("features", []) or []
        out: _List[_Dict[str, _Any]] = []
        for f in feats:
            coords = f.get("geometry", {}).get("coordinates") or []
            props  = f.get("properties", {})
            if len(coords) != 2:
                continue
            out.append({
                "lon": float(coords[0]),
                "lat": float(coords[1]),
                "label": props.get("label") or props.get("name"),
                "confidence": props.get("confidence"),
                "layer": (props.get("layer") or "").lower(),
                "raw": f
            })
        _log.debug(f"GEOCODE text results={len(out)} first={_short(out[0]) if out else None}")
        return out

    # ─────────────────── geocoding (structured)
    def geocode_structured(
          self
        , street: str | None = None
        , housenumber: str | None = None
        , locality: str | None = None
        , region: str | None = None
        , postalcode: str | None = None
        , country: str | None = None
        , size: int = 1
    ) -> _List[_Dict[str, _Any]]:
        # Prefer single 'address' field when possible
        address = None
        if street and housenumber:
            address = f"{housenumber} {street}"
        elif street:
            address = street

        params: _Dict[str, _Any] = {
            "size": size,
            "country": (country or self.cfg.default_country)
        }
        if address:     params["address"]    = address
        if locality:    params["locality"]   = locality
        if region:      params["region"]     = region
        if postalcode:  params["postalcode"] = postalcode

        _log.info(f"GEOCODE structured params={_short(params)}")
        data = self._get("/geocode/search/structured", params)
        feats = data.get("features", []) or []
        out: _List[_Dict[str, _Any]] = []
        for f in feats:
            coords = f.get("geometry", {}).get("coordinates") or []
            props  = f.get("properties", {})
            if len(coords) != 2:
                continue
            out.append({
                "lon": float(coords[0]),
                "lat": float(coords[1]),
                "label": props.get("label") or props.get("name"),
                "confidence": props.get("confidence"),
                "layer": (props.get("layer") or "").lower(),
                "raw": f
            })
        _log.debug(f"GEOCODE structured results={len(out)} first={_short(out[0]) if out else None}")
        return out

    # ─────────────────── ViaCEP fallback
    def _viacep_lookup(self, cep: str) -> _Optional[_Dict[str, str]]:
        key = f"viacep::{cep}"
        cached = self._cache.get(key)
        if cached is not None:
            _log.debug(f"ViaCEP cache hit for {cep}")
            return cached

        url = f"https://viacep.com.br/ws/{cep}/json/"
        _log.info(f"ViaCEP GET {url}")
        try:
            r = self._sess.get(url, timeout=(4.0, 10.0), headers={"User-Agent": self.cfg.user_agent})
            if not r.ok:
                _log.warning(f"ViaCEP status={r.status_code} text={(r.text or '')[:120]}")
                return None
            data = r.json()
            if data.get("erro"):
                return None
            out = {
                "logradouro": data.get("logradouro") or "",
                "bairro":     data.get("bairro") or "",
                "localidade": data.get("localidade") or "",
                "uf":         data.get("uf") or "",
            }
            self._cache.set(key, out)
            return out
        except Exception as e:
            _log.warning(f"ViaCEP exception {type(e).__name__}: {e}")
            return None

    # ─────────────────── high-level resolve (address/CEP/city/coords)
    def _resolve_point(self, value: _Any) -> _Dict[str, _Any]:
        # (lat, lon) tuple/list
        if isinstance(value, (list, tuple)) and len(value) == 2:
            lat, lon = float(value[0]), float(value[1])
            out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
            _log.info(f"RESOLVE (tuple/list) -> {out}")
            return out

        # dict with lat/lon
        if isinstance(value, dict) and {"lat", "lon"}.issubset(value.keys()):
            out = {
                "lat": float(value["lat"]),
                "lon": float(value["lon"]),
                "label": value.get("label", f"{float(value['lat']):.6f},{float(value['lon']):.6f}")
            }
            _log.info(f"RESOLVE (dict lat/lon) -> {out}")
            return out

        # dict structured
        if isinstance(value, dict):
            street      = value.get("street")
            housenumber = value.get("housenumber")
            locality    = value.get("locality")
            region      = value.get("region")
            postalcode  = value.get("postalcode")
            country     = value.get("country") or self.cfg.default_country
            if any([street, housenumber, locality, region, postalcode]):
                hits = self.geocode_structured(
                    street=street, housenumber=housenumber, locality=locality,
                    region=region, postalcode=postalcode, country=country, size=1
                )
                hits = self._filter_hits(hits)
                if hits:
                    h = hits[0]
                    out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or "structured"}
                    _log.info(f"RESOLVE (structured dict) -> {out}")
                    return out
                raise ValueError(f"Structured geocoding yielded no acceptable results for: {value}")

        # string inputs
        if isinstance(value, str):
            # "lat,lon"
            maybe = self._is_latlon_str(value)
            if maybe:
                lat, lon = maybe
                out = {"lat": lat, "lon": lon, "label": f"{lat:.6f},{lon:.6f}"}
                _log.info(f"RESOLVE (lat,lon string) -> {out}")
                return out

            # CEP
            cep = self._is_cep(value)
            if cep:
                hyph = f"{cep[:5]}-{cep[5:]}"
                hits = (self.geocode_structured(postalcode=cep, country=self.cfg.default_country, size=1)
                        or self.geocode_structured(postalcode=hyph, country=self.cfg.default_country, size=1)
                        or self.geocode_text(hyph, size=1, country=self.cfg.default_country))
                hits = self._filter_hits(hits, allowed_layers=["postalcode", "postcode", "address", "street", "locality", "neighbourhood"])
                if hits:
                    h = hits[0]
                    out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or hyph}
                    _log.info(f"RESOLVE (CEP via ORS) -> {out}")
                    return out

                if self.cfg.allow_viacep:
                    via = self._viacep_lookup(cep)
                    if via and (via["localidade"] or via["uf"] or via["logradouro"]):
                        query = ", ".join([x for x in [via["logradouro"], via["bairro"], via["localidade"], via["uf"]] if x])
                        _log.info(f"RESOLVE (CEP ViaCEP->text) query='{query}'")
                        hits = self.geocode_text(query, size=1, country=self.cfg.default_country)
                        hits = self._filter_hits(hits)
                        if hits:
                            h = hits[0]
                            out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or query}
                            _log.info(f"RESOLVE (CEP via ViaCEP) -> {out}")
                            return out

                raise ValueError(f"CEP geocoding yielded no acceptable results for: {value}")

            # free text
            hits = self.geocode_text(value, size=1, country=self.cfg.default_country)
            hits = self._filter_hits(hits)
            if hits:
                h = hits[0]
                out = {"lat": h["lat"], "lon": h["lon"], "label": h.get("label") or value}
                _log.info(f"RESOLVE (text) -> {out}")
                return out
            raise ValueError(f"Geocoding yielded no acceptable results for: {value}")

        raise TypeError("Unsupported point type. Use string (address/CEP/city/'lat,lon'), dicts, or (lat,lon).")

    # ─────────────────── snap to road
    def _snap_to_road(self, coords_lonlat: _List[_List[float]], profile: str, radius_m: int | None = None):
        rad = int(radius_m or self.cfg.snap_radius_m)
        payload = {"locations": coords_lonlat, "radius": rad}
        _log.info(f"SNAP {profile} radius={rad} coords={_short(coords_lonlat)}")
        try:
            data = self._post(f"/v2/snapping/{profile}", payload)
        except Exception as e:
            _log.warning(f"SNAP failed ({type(e).__name__}); returning originals.")
            return coords_lonlat

        snapped = []
        for i, item in enumerate(data.get("locations", [])):
            if item and "location" in item and item["location"]:
                lon, lat = item["location"]
                snapped.append([float(lon), float(lat)])
            else:
                snapped.append(coords_lonlat[i])
        _log.debug(f"SNAP result={_short(snapped)}")
        return snapped

    # ─────────────────── helpers: inject truck options
    def _inject_hgv_options(self, payload: _Dict[str, _Any],
                            vehicle_type: _Optional[str],
                            restrictions: _Optional[_Dict[str, _Any]]) -> None:
        payload.setdefault("options", {})
        if vehicle_type:
            payload["options"]["vehicle_type"] = vehicle_type  # "goods" or "hgv"
        if restrictions:
            payload["options"].setdefault("profile_params", {})["restrictions"] = restrictions

    # ─────────────────── directions (single route)
    def route_road(
          self
        , origin: _Any
        , destination: _Any
        , profile: str | None = None
        , geometry: bool = False
        , extra_info: _Optional[_List[str]] = None
        , avoid_features: _Optional[_List[str]] = None
        , vehicle_type: _Optional[str] = None
        , hgv_restrictions: _Optional[_Dict[str, _Any]] = None
    ) -> _Dict[str, _Any]:
        o = self._resolve_point(origin)
        d = self._resolve_point(destination)

        prof = (profile or self.cfg.default_profile)

        payload: _Dict[str, _Any] = {
            "coordinates": [[o["lon"], o["lat"]], [d["lon"], d["lat"]]],
            "units": "m",
            "preference": "fastest"
        }
        if extra_info:
            payload["extra_info"] = list(extra_info)

        # Avoid features
        final_avoids = list(self.cfg.avoid_features_default or [])
        if avoid_features:
            final_avoids.extend([x for x in avoid_features if x not in final_avoids])
        if final_avoids:
            payload.setdefault("options", {})["avoid_features"] = final_avoids

        # Inject truck options if profile is driving-hgv
        if prof == "driving-hgv":
            self._inject_hgv_options(
                payload,
                vehicle_type or self.cfg.hgv_vehicle_type,
                hgv_restrictions or self.cfg.hgv_restrictions
            )

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
                    data = self._post(f"/v2/directions/{prof}", payload)  # may raise again
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
        if "segments" in route:
            resp["segments"] = route["segments"]
        elif "properties" in route and "segments" in route["properties"]:
            resp["segments"] = route["properties"]["segments"]
        if "extras" in route:
            resp["extras"] = route["extras"]
        elif "properties" in route and "extras" in route["properties"]:
            resp["extras"] = route["properties"]["extras"]

        _log.info(f"ROUTE OK km={None if resp['distance_m'] is None else round(resp['distance_m']/1000,2)} "
                  f"hrs={None if resp['duration_s'] is None else round(resp['duration_s']/3600,2)}")
        return resp

    # ─────────────────── matrix (batch OD)
    def matrix_road(
          self
        , origins: _List[_Any]
        , destinations: _List[_Any]
        , profile: str | None = None
    ) -> _Dict[str, _Any]:
        os_ = [self._resolve_point(x) for x in origins]
        ds_ = [self._resolve_point(x) for x in destinations]

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

        # NOTE: The public Matrix endpoint supports profile=driving-hgv,
        # but may ignore profile_params.restrictions. We *do not* inject them here.
        _log.info(f"MATRIX {prof} n_origins={len(os_)} n_destinations={len(ds_)}")
        data = self._post(f"/v2/matrix/{prof}", payload)
        _log.debug(f"MATRIX resp keys={list(data.keys())}")
        return {
              "origins": os_
            , "destinations": ds_
            , "distances_m": data.get("distances")
            , "durations_s": data.get("durations")
        }
