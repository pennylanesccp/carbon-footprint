# cabosupernet/routes_provider.py
# ────────────────────────────────────────────────────────────────────────────────
# OpenRouteService client (geocode text/structured + directions + matrix) w/ cache
#
# Dependencies:
#   pip install requests
#   # optional (for tests / local runs): pip install python-dotenv
#
# Env:
#   ORS_API_KEY=<your key>
#
# Accepted point formats:
#   • string free text  (e.g., "Porto de Santos, SP")
#   • string CEP        (e.g., "11010-913" or "11010913")
#   • string "lat,lon"  (e.g., "-23.5505,-46.6333")
#   • tuple/list (lat, lon)
#   • dict {'lat':..,'lon':..}
#   • dict structured address: {'street','housenumber','locality','region','postalcode','country'}
# Geocoding:
#   • /geocode/search               (free text)
#   • /geocode/search/structured    (structured; used for CEP or address dicts)
# Directions v2: POST /v2/directions/{profile}
# Matrix v2:     POST /v2/matrix/{profile}
# ────────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import os as _os
import re as _re
import time as _time
import json as _json
import random as _random
import hashlib as _hashlib
import sqlite3 as _sqlite3
from typing import Any as _Any, Dict as _Dict, List as _List, Tuple as _Tuple, Optional as _Optional

import requests as _req


# ─────────────────────────────── config & utils
class ORSConfig:
    def __init__(
          self
        , api_key: str | None = None
        , base_url: str = "https://api.openrouteservice.org"
        , connect_timeout_s: float = 5.0
        , read_timeout_s: float = 20.0
        , max_retries: int = 3
        , backoff_s: float = 0.75
        , cache_path: str = "cache/.ors_cache.sqlite"
        , cache_ttl_s: int = 30 * 24 * 3600  # 30 days
        , default_country: str = "BR"
        , default_profile: str = "driving-car"
        , user_agent: str = "Cabosupernet-ORSClient/1.0 (+https://example.local)"
    ):
        self.api_key             = (api_key or _os.getenv("ORS_API_KEY", "")).strip()
        self.base_url            = base_url.rstrip("/")
        self.connect_timeout_s   = connect_timeout_s
        self.read_timeout_s      = read_timeout_s
        self.max_retries         = max_retries
        self.backoff_s           = backoff_s
        self.cache_path          = cache_path
        self.cache_ttl_s         = cache_ttl_s
        self.default_country     = (default_country or "BR").upper()
        self.default_profile     = default_profile
        self.user_agent          = user_agent
        if not self.api_key:
            raise RuntimeError("ORS_API_KEY not set. Export ORS_API_KEY or pass api_key to ORSConfig().")

    @property
    def timeouts(self) -> _Tuple[float, float]:
        return (self.connect_timeout_s, self.read_timeout_s)


def _sha_key(endpoint: str, payload: _Dict[str, _Any]) -> str:
    msg = endpoint + "||" + _json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return _hashlib.sha256(msg.encode("utf-8")).hexdigest()


class _Cache:
    def __init__(self, path: str, ttl_s: int):
        # expand ~ and make absolute
        path = _os.path.abspath(_os.path.expanduser(path))
        # create parent directory if provided
        parent = _os.path.dirname(path)
        if parent:
            _os.makedirs(parent, exist_ok=True)

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
                con.execute(
                      "INSERT OR REPLACE INTO cache(k, v, ts) VALUES (?, ?, ?)"
                    , (k, _json.dumps(v), int(_time.time()))
                )
        finally:
            con.close()


def _parse_retry_after(headers: _Dict[str, str], default_s: float) -> float:
    # Honor 'Retry-After' (seconds) if present; else fall back to default with tiny jitter.
    ra = headers.get("Retry-After")
    if ra:
        try:
            return float(ra)
        except ValueError:
            pass
    return default_s + _random.uniform(0.05, 0.35)


# ─────────────────────────────── client
class ORSClient:
    def __init__(self, cfg: ORSConfig):
        self.cfg    = cfg
        self._sess  = _req.Session()
        self._cache = _Cache(cfg.cache_path, cfg.cache_ttl_s)

    # ─────────────────── low-level POST/GET with retry + cache
    def _post(self, path: str, payload: _Dict[str, _Any]) -> _Dict[str, _Any]:
        url = f"{self.cfg.base_url}{path}"
        key = _sha_key(url, payload)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        headers = {
              "Authorization": self.cfg.api_key
            , "Content-Type": "application/json; charset=utf-8"
            , "Accept": "application/json"
            , "User-Agent": self.cfg.user_agent
        }

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                resp = self._sess.post(
                      url
                    , headers=headers
                    , data=_json.dumps(payload)
                    , timeout=self.cfg.timeouts
                )
                if resp.status_code in (429, 503):
                    _time.sleep(_parse_retry_after(resp.headers, self.cfg.backoff_s * attempt))
                    continue
                resp.raise_for_status()
                data = resp.json()
                self._cache.set(key, data)
                return data
            except _req.RequestException:
                if attempt >= self.cfg.max_retries:
                    raise
                _time.sleep(self.cfg.backoff_s * attempt)

        raise RuntimeError("Unreachable")

    def _get(self, path: str, params: _Dict[str, _Any]) -> _Dict[str, _Any]:
        url = f"{self.cfg.base_url}{path}"
        key = _sha_key(url, params)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        headers = {
              "Authorization": self.cfg.api_key
            , "Accept": "application/json"
            , "User-Agent": self.cfg.user_agent
        }

        for attempt in range(1, self.cfg.max_retries + 1):
            try:
                resp = self._sess.get(
                      url
                    , headers=headers
                    , params=params
                    , timeout=self.cfg.timeouts
                )
                if resp.status_code in (429, 503):
                    _time.sleep(_parse_retry_after(resp.headers, self.cfg.backoff_s * attempt))
                    continue
                resp.raise_for_status()
                data = resp.json()
                self._cache.set(key, data)
                return data
            except _req.RequestException:
                if attempt >= self.cfg.max_retries:
                    raise
                _time.sleep(self.cfg.backoff_s * attempt)

        raise RuntimeError("Unreachable")

    # ─────────────────── geocoding helpers
    @staticmethod
    def _is_latlon_str(text: str) -> _Optional[_Tuple[float, float]]:
        """
        Accepts strings like "-23.55,-46.63" or " -23.55 , -46.63 ".
        Returns (lat, lon) or None.
        """
        if not isinstance(text, str):
            return None
        s = text.strip()
        m = _re.match(r"^\s*([+-]?\d+(?:\.\d+)?)\s*,\s*([+-]?\d+(?:\.\d+)?)\s*$", s)
        if not m:
            return None
        lat = float(m.group(1))
        lon = float(m.group(2))
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return None
        return (lat, lon)

    @staticmethod
    def _is_cep(text: str) -> _Optional[str]:
        """
        Matches Brazilian CEP: 8 digits with optional hyphen (NNNNN-NNN).
        Returns normalized CEP (########) or None.
        """
        if not isinstance(text, str):
            return None
        m = _re.match(r"^\s*(\d{5})-?(\d{3})\s*$", text)
        if not m:
            return None
        return f"{m.group(1)}{m.group(2)}"

    # ─────────────────── geocoding (text)
    def geocode_text(
          self
        , text: str
        , size: int = 1
        , country: str | None = None
    ) -> _List[_Dict[str, _Any]]:
        """
        Free-text geocoding (/geocode/search).
        Returns list of features with 'lon','lat','label','confidence'.
        """
        params = {
              "text": text
            , "size": size
            , "boundary.country": (country or self.cfg.default_country)
        }
        data = self._get("/geocode/search", params)
        feats = data.get("features", []) or []
        out: _List[_Dict[str, _Any]] = []
        for f in feats:
            coords = f.get("geometry", {}).get("coordinates", [])
            props  = f.get("properties", {})
            if len(coords) != 2:
                continue
            out.append({
                  "lon": float(coords[0])
                , "lat": float(coords[1])
                , "label": props.get("label") or props.get("name")
                , "confidence": props.get("confidence")
                , "raw": f
            })
        return out
    
    # ─────────────────── snap to road (helps when a point is off-network)
    def _snap_to_road(self, coords_lonlat, profile: str, radius_m: int = 2000):
        """
        coords_lonlat: list of [lon, lat]
        Returns a list of [lon, lat] snapped to nearest edge (keeps originals if not found).
        """
        payload = {"locations": coords_lonlat, "radius": int(radius_m)}
        path_try = [f"/v2/snap/{profile}", f"/v2/snap/{profile}/json"]  # some deployments want /json
        for path in path_try:
            try:
                data = self._post(path, payload)
                snapped = []
                for i, item in enumerate(data.get("locations", [])):
                    if item and "location" in item:
                        lon, lat = item["location"]
                        snapped.append([float(lon), float(lat)])
                    else:
                        snapped.append(coords_lonlat[i])
                return snapped
            except Exception:
                continue
        # if everything fails, just return originals
        return coords_lonlat


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
        """
        Structured geocoding (/geocode/search/structured).
        Useful for CEP-only or well-formed address parts.
        """
        # Prefer 'address' field; keep compatibility with separate street/housenumber.
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

        data = self._get("/geocode/search/structured", params)
        feats = data.get("features", []) or []
        out: _List[_Dict[str, _Any]] = []
        for f in feats:
            coords = f.get("geometry", {}).get("coordinates", [])
            props  = f.get("properties", {})
            if len(coords) != 2:
                continue
            out.append({
                  "lon": float(coords[0])
                , "lat": float(coords[1])
                , "label": props.get("label") or props.get("name")
                , "confidence": props.get("confidence")
                , "raw": f
            })
        return out

    # ─────────────────── high-level resolve (address/CEP/city/coords)
    def _resolve_point(self, value: _Any) -> _Dict[str, _Any]:
        """
        Returns a dict: {'lat': float, 'lon': float, 'label': str}
        Accepts:
          - string free text (address/city/place),
          - string CEP (NNNNN-NNN),
          - string "lat,lon",
          - tuple/list (lat, lon),
          - dict {'lat','lon'},
          - dict structured address {'street','housenumber','locality','region','postalcode','country'}.
        """
        # 1) tuple/list (lat,lon)
        if isinstance(value, (list, tuple)) and len(value) == 2:
            lat, lon = float(value[0]), float(value[1])
            return {
                  "lat": lat
                , "lon": lon
                , "label": f"{lat:.6f},{lon:.6f}"
            }

        # 2) dict with lat/lon
        if isinstance(value, dict) and {"lat", "lon"}.issubset(value.keys()):
            return {
                  "lat": float(value["lat"])
                , "lon": float(value["lon"])
                , "label": value.get("label", f"{float(value['lat']):.6f},{float(value['lon']):.6f}")
            }

        # 3) dict structured address
        if isinstance(value, dict):
            street      = value.get("street")
            housenumber = value.get("housenumber")
            locality    = value.get("locality")
            region      = value.get("region")
            postalcode  = value.get("postalcode")
            country     = value.get("country") or self.cfg.default_country
            if any([street, housenumber, locality, region, postalcode]):
                hits = self.geocode_structured(
                      street=street
                    , housenumber=housenumber
                    , locality=locality
                    , region=region
                    , postalcode=postalcode
                    , country=country
                    , size=1
                )
                if hits:
                    h = hits[0]
                    return {
                          "lat": h["lat"]
                        , "lon": h["lon"]
                        , "label": h.get("label") or "structured"
                    }
                raise ValueError(f"Structured geocoding yielded no results for: {value}")

        # 4) string: "lat,lon"?
        if isinstance(value, str):
            maybe = self._is_latlon_str(value)
            if maybe:
                lat, lon = maybe
                return {
                      "lat": lat
                    , "lon": lon
                    , "label": f"{lat:.6f},{lon:.6f}"
                }

            # 5) string: CEP?
            cep = self._is_cep(value)
            if cep:
                hits = self.geocode_structured(postalcode=cep, country=self.cfg.default_country, size=1)
                if not hits:
                    # fallback to free text (some CEPs resolve via text index)
                    hits = self.geocode_text(value, size=1, country=self.cfg.default_country)
                if hits:
                    h = hits[0]
                    return {
                          "lat": h["lat"]
                        , "lon": h["lon"]
                        , "label": h.get("label") or cep
                    }
                raise ValueError(f"CEP geocoding yielded no results for: {value}")

            # 6) string: free text (address/city/place)
            hits = self.geocode_text(value, size=1, country=self.cfg.default_country)
            if hits:
                h = hits[0]
                return {
                      "lat": h["lat"]
                    , "lon": h["lon"]
                    , "label": h.get("label") or value
                }
            raise ValueError(f"Geocoding yielded no results for: {value}")

        raise TypeError(
            "Unsupported point type. Use address/CEP/city string, {'lat','lon'} dict, "
            "structured address dict, or (lat, lon)."
        )

    # ─────────────────── directions (single route)
    def route_road(
          self
        , origin: _Any
        , destination: _Any
        , profile: str | None = None
        , geometry: bool = False
        , extra_info: _Optional[_List[str]] = None
        , avoid_features: _Optional[_List[str]] = None
    ) -> _Dict[str, _Any]:
        """
        Compute one road route (distance meters, duration seconds).
        Inputs 'origin' and 'destination' may be address/CEP/city/coords as documented.
        """
        o = self._resolve_point(origin)
        d = self._resolve_point(destination)

        prof = (profile or self.cfg.default_profile)

        payload: _Dict[str, _Any] = {
              "coordinates": [
                    [o["lon"], o["lat"]]
                ,   [d["lon"], d["lat"]]
              ]
            , "units": "m"
            , "preference": "fastest"
        }
        if extra_info:
            payload["extra_info"] = list(extra_info)
        if avoid_features:
            payload["options"] = { "avoid_features": list(avoid_features) }

        try:
            data = self._post(f"/v2/directions/{prof}", payload)
        except Exception:
            # If ORS can’t snap one of the points (common with CEPs near ports/campuses),
            # pre-snap to the road network and retry once.
            snapped = self._snap_to_road(payload["coordinates"], prof, radius_m=2000)
            # Only retry if snapping actually changed something
            if snapped != payload["coordinates"]:
                payload["coordinates"] = snapped
                data = self._post(f"/v2/directions/{prof}", payload)
            else:
                raise

        route = data["routes"][0]
        summ  = route["summary"]

        resp: _Dict[str, _Any] = {
              "distance_m": float(summ["distance"])
            , "duration_s": float(summ["duration"])
            , "origin": o
            , "destination": d
        }
        if geometry:
            resp["geometry"] = route.get("geometry")
        if "segments" in route:
            resp["segments"] = route["segments"]  # per-step detail
        if "extras" in route:
            resp["extras"] = route["extras"]      # surfaces/waytypes/etc when requested
        return resp

    # ─────────────────── matrix (batch OD)
    def matrix_road(
          self
        , origins: _List[_Any]
        , destinations: _List[_Any]
        , profile: str | None = None
    ) -> _Dict[str, _Any]:
        """
        Compute a distance/time matrix for heterogeneous origin/destination inputs.
        """
        os_ = [self._resolve_point(x) for x in origins]
        ds_ = [self._resolve_point(x) for x in destinations]

        coords = [
            [p["lon"], p["lat"]]
            for p in (os_ + ds_)
        ]
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
        data = self._post(f"/v2/matrix/{prof}", payload)
        return {
              "origins": os_
            , "destinations": ds_
            , "distances_m": data.get("distances")  # 2D list [len(origins) x len(destinations)]
            , "durations_s": data.get("durations")
        }
