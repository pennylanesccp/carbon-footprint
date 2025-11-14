# modules/road/ors_mixins.py
# -*- coding: utf-8 -*-
"""
Reusable mixins for the ORS HTTP client:
- GeocodingMixin: text & structured geocoding helpers
- RoutingMixin: snap-to-road, directions, and matrix helpers

Expectations for the concrete client class that inherits these mixins:
- Attributes:
    self.cfg                 : ORSConfig (see modules.road.ors_common)
- Methods:
    self._get(endpoint, params=None, headers=None)   -> dict
    self._post(endpoint, json=None, headers=None)    -> dict

Notes
-----
• All logs use the standardized logger (modules.functions.logging).
• These helpers log both inputs (sanitized) and outputs (summaries).
• They raise domain exceptions from ors_common for clearer upstream handling.
"""

from __future__ import annotations

from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Optional
import requests as _req

from modules.infra.logging import get_logger
from .ors_common import (
    _short,
    _extract_error_text,
    RateLimited,
    NoRoute,
    GeocodeNotFound,
)

_log = get_logger(__name__)


# ────────────────────────────────────────────────────────────────────────────────
# Geocoding
# ────────────────────────────────────────────────────────────────────────────────

class GeocodingMixin:
    """
    Text and structured geocoding wrappers over ORS endpoints.

    Requires concrete client to provide:
      - self._get(...)
      - self.cfg.default_country
    """

    def geocode_text(
        self,
        text: str,
        size: int = 1,
        country: str | None = None,
    ):
        """
        Perform Pelias-style free-text geocoding.

        Parameters
        ----------
        text : str
            Query like "Rua X 123, São Paulo".
        size : int
            Max number of features to retrieve.
        country : str | None
            ISO2 country hint (defaults to cfg.default_country).

        Returns
        -------
        list[dict]    # Pelias Feature-like objects

        Raises
        ------
        GeocodeNotFound
            If no features are returned.
        """
        country = (country or self.cfg.default_country)
        _log.info("GEOCODE text=%s country=%s size=%s", _short(text), country, size)

        raw = self._get(
            "/geocode/search",
            {
                "text": text,
                "size": size,
                "boundary.country": country,
            },
        )

        feats = (raw or {}).get("features") or []
        _log.debug("GEOCODE text got %s features", len(feats))
        if not feats:
            raise GeocodeNotFound(f"No geocode results for: {text}")
        return feats

    def geocode_structured(
        self,
        street: str | None = None,
        housenumber: str | None = None,
        locality: str | None = None,
        region: str | None = None,
        postalcode: str | None = None,
        country: str | None = None,
        size: int = 1,
    ) -> _Dict[str, _Any]:
        """
        Perform structured geocoding (Pelias /geocode/search/structured).

        Returns raw JSON (dict) as provided by the API.
        """
        address = None
        if street and housenumber:
            address = f"{housenumber} {street}"
        elif street:
            address = street

        params: _Dict[str, _Any] = {
            "size": size,
            "country": (country or self.cfg.default_country),
        }
        if address:
            params["address"] = address
        if locality:
            params["locality"] = locality
        if region:
            params["region"] = region
        if postalcode:
            params["postalcode"] = postalcode

        _log.info("GEOCODE structured params=%s", _short(params))
        out = self._get("/geocode/search/structured", params)
        _log.debug("GEOCODE structured raw_keys=%s", list(out.keys()) if isinstance(out, dict) else type(out).__name__)
        return out


# ────────────────────────────────────────────────────────────────────────────────
# Routing
# ────────────────────────────────────────────────────────────────────────────────

class RoutingMixin:
    """
    Routing helpers (snap-to-road, directions, matrix).

    Requires concrete client to provide:
      - self._get(...), self._post(...)
      - self.cfg.default_profile, self.cfg.snap_radius_m, self.cfg.snap_retry_on_404
    """

    def _snap_to_road(self, coords_lonlat, profile: str, *, radius_m: int | None = None):
        """
        Try to snap given coordinates (lon,lat) to the selected profile.
        Falls back to 'driving-car' when some profiles don't support SNAP.

        Returns a new list of snapped [lon,lat] pairs or the original list.
        """
        rad = int(radius_m or getattr(self.cfg, "snap_radius_m", 2500) or 2500)
        payload = {
            "locations": coords_lonlat,
            "radius": rad,
        }
        _log.info("SNAP %s radius=%s coords=%s", profile, rad, _short(coords_lonlat))

        try:
            data = self._post(f"/v2/snap/{profile}", json=payload)
        except _req.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None) if resp is not None else None
            if code in (400, 404):
                _log.warning(
                    "SNAP %s unavailable (status=%s) — falling back to driving-car",
                    profile, code
                )
                data = self._post("/v2/snap/driving-car", json=payload)
            else:
                raise

        snapped = []
        for item in (data or {}).get("locations", []):
            if isinstance(item, dict):
                loc = item.get("location") or []
                if isinstance(loc, list) and len(loc) == 2:
                    snapped.append([float(loc[0]), float(loc[1])])

        if snapped:
            _log.debug("SNAP produced %s points", len(snapped))
            return snapped

        _log.debug("SNAP returned no changes; using original coordinates")
        return coords_lonlat

    def route(self, profile: str, coords: _List[_List[float]], **kwargs):
        """
        Low-level directions call.

        Parameters
        ----------
        profile : str
            'driving-hgv', 'driving-car', etc.
        coords : list[[lon,lat], ...]
        kwargs : dict
            Extra ORS parameters (e.g., geometry='polyline').

        Returns
        -------
        dict : raw ORS response
        """
        body = {"coordinates": coords, **kwargs}
        _log.info("ROUTE raw %s coords=%s", profile, _short(coords))
        try:
            data = self._post(f"/v2/directions/{profile}", json=body)
            _log.debug("ROUTE raw ok keys=%s", list(data.keys()) if isinstance(data, dict) else type(data).__name__)
            return data
        except RateLimited:
            raise
        except NoRoute:
            raise

    def route_road(
        self,
        origin: _Any,
        destination: _Any,
        profile: str | None = None,
        geometry: bool = False,
        extra_info: _Optional[_List[str]] = None,
        avoid_features: _Optional[_List[str]] = None,
    ) -> _Dict[str, _Any]:
        """
        High-level "route by points" helper that:
        - resolves origin/destination via resolver
        - does one request to /v2/directions/{profile}
        - can retry once with SNAP when 404 and snap_retry_on_404=True

        Returns a normalized dict:
            {
              "distance_m": float | None,
              "duration_s": float | None,
              "origin": {...resolved...},
              "destination": {...resolved...},
              ["geometry"]: <geojson/linestring>  # if geometry=True
            }
        """
        from modules.addressing.resolver import resolve_point

        o = resolve_point(origin, ors=self)
        d = resolve_point(destination, ors=self)
        prof = (profile or self.cfg.default_profile)

        payload: _Dict[str, _Any] = {
            "coordinates": [[o["lon"], o["lat"]], [d["lon"], d["lat"]]],
            "units": "m",
            "preference": "fastest",
        }
        if extra_info:
            payload["extra_info"] = list(extra_info)
        if avoid_features:
            payload["options"] = {"avoid_features": list(avoid_features)}

        _log.info("ROUTE try1 %s coords=%s", prof, payload["coordinates"])
        try:
            data = self._post(f"/v2/directions/{prof}", json=payload)
        except _req.HTTPError as e:
            resp = getattr(e, "response", None)
            code = getattr(resp, "status_code", None)
            body = _extract_error_text(resp) if resp is not None else str(e)
            _log.error("ROUTE try1 failed status=%s msg=%s", code, body)

            if getattr(self.cfg, "snap_retry_on_404", True) and code == 404:
                snapped = self._snap_to_road(payload["coordinates"], prof, radius_m=self.cfg.snap_radius_m)
                if snapped != payload["coordinates"]:
                    payload["coordinates"] = snapped
                    _log.info("ROUTE try2 after SNAP %s coords=%s", prof, payload["coordinates"])
                    data = self._post(f"/v2/directions/{prof}", json=payload)
                else:
                    _log.warning("ROUTE SNAP made no change; re-raising")
                    raise
            else:
                raise

        # ORS may return {"routes":[...]} or GeoJSON {"features":[...]}
        route = (data.get("routes") or data.get("features") or [None])[0]
        if route is None:
            raise RuntimeError("Directions response missing 'routes' or 'features'.")

        summ = route.get("summary")
        if not summ and isinstance(route, dict):
            props = route.get("properties", {})
            summ = props.get("summary")

        resp: _Dict[str, _Any] = {
            "distance_m": float(summ["distance"]) if summ else None,
            "duration_s": float(summ["duration"]) if summ else None,
            "origin": o,
            "destination": d,
        }
        if geometry:
            resp["geometry"] = route.get("geometry")

        _log.info(
            "ROUTE ok %s dist=%.1fm dur=%.0fs",
            prof,
            (resp["distance_m"] or 0.0),
            (resp["duration_s"] or 0.0),
        )
        return resp

    def matrix_road(
        self,
        origins: _List[_Any],
        destinations: _List[_Any],
        profile: str | None = None,
    ) -> _Dict[str, _Any]:
        """
        Build a distance/duration matrix between origins and destinations.

        Returns
        -------
        dict: {
          "origins":      [resolved origins],
          "destinations": [resolved destinations],
          "distances_m":  [[...]],
          "durations_s":  [[...]],
        }
        """
        from modules.addressing.resolver import resolve_point

        os_ = [resolve_point(x, ors=self) for x in origins]
        ds_ = [resolve_point(x, ors=self) for x in destinations]

        coords = [[p["lon"], p["lat"]] for p in (os_ + ds_)]
        n_o = len(os_)
        sources = list(range(0, n_o))
        destinations_idx = list(range(n_o, n_o + len(ds_)))
        prof = (profile or self.cfg.default_profile)

        payload = {
            "locations": coords,
            "sources": sources,
            "destinations": destinations_idx,
            "metrics": ["distance", "duration"],
            "units": "m",
        }
        _log.info("MATRIX %s n_origins=%s n_destinations=%s", prof, len(os_), len(ds_))
        data = self._post(f"/v2/matrix/{prof}", json=payload)
        _log.debug(
            "MATRIX ok shapes: dist=%sx%s dur=%sx%s",
            len(data.get("distances", []) or []),
            len((data.get("distances") or [[]])[0] or []),
            len(data.get("durations", []) or []),
            len((data.get("durations") or [[]])[0] or []),
        )
        return {
            "origins": os_,
            "destinations": ds_,
            "distances_m": data.get("distances"),
            "durations_s": data.get("durations"),
        }


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
