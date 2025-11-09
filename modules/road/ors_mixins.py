# modules/road/ors_mixins.py
from __future__ import annotations

from typing import Any as _Any, Dict as _Dict, List as _List, Optional as _Optional
import requests as _req

from .ors_common import (
      _log
    , _short
    , _extract_error_text
    , RateLimited, NoRoute, GeocodeNotFound
)

class GeocodingMixin:
    # Requires: self._get, self.cfg
    def geocode_text(self, text: str, size: int = 1, country: str | None = None):
        country = (country or self.cfg.default_country)
        _log.info(f"GEOCODE text={_short(text)} country={country} size={size}")
        raw = self._get(
              "/geocode/search"
            , {"text": text, "size": size, "boundary.country": country}
        )
        feats = (raw or {}).get("features") or []
        if not feats:
            raise GeocodeNotFound(f"No geocode results for: {text}")
        return feats

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


class RoutingMixin:
    # Requires: self._get, self._post, self.cfg
    def _snap_to_road(self, coords_lonlat, profile: str, *, radius_m: int | None = None):
        rad = int(radius_m or self.cfg.snap_radius_m or 2500)
        payload = {
              "locations": coords_lonlat
            , "radius": rad
        }
        _log.info(f"SNAP {profile} radius={rad} coords={_short(coords_lonlat)}")

        try:
            data = self._post(f"/v2/snap/{profile}", json=payload)
        except _req.HTTPError as e:
            resp = getattr(e, "response", None)
            if resp is not None and getattr(resp, "status_code", None) in (400, 404):
                _log.warning(
                    "SNAP %s unavailable (status=%s) — falling back to driving-car",
                    profile, getattr(resp, "status_code", "?")
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
        return snapped or coords_lonlat

    def route(self, profile: str, coords: _List[_List[float]], **kwargs):
        body = {"coordinates": coords, **kwargs}
        try:
            return self._post(f"/v2/directions/{profile}", json=body)
        except RateLimited:
            raise
        except NoRoute:
            raise

    def route_road(
          self
        , origin: _Any
        , destination: _Any
        , profile: str | None = None
        , geometry: bool = False
        , extra_info: _Optional[_List[str]] = None
        , avoid_features: _Optional[_List[str]] = None
    ) -> _Dict[str, _Any]:
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
            payload["options"] = {"avoid_features": list(avoid_features)}

        _log.info(f"ROUTE try1 {prof} coords={payload['coordinates']}")
        try:
            data = self._post(f"/v2/directions/{prof}", json=payload)
        except _req.HTTPError as e:
            code = getattr(getattr(e, "response", None), "status_code", None)
            body = _extract_error_text(getattr(e, "response", None)) if getattr(e, "response", None) else str(e)
            _log.error(f"ROUTE try1 failed status={code} msg={body}")
            if self.cfg.snap_retry_on_404 and code == 404:
                snapped = self._snap_to_road(payload["coordinates"], prof, radius_m=self.cfg.snap_radius_m)
                if snapped != payload["coordinates"]:
                    payload["coordinates"] = snapped
                    _log.info(f"ROUTE try2 after SNAP {prof} coords={payload['coordinates']}")
                    data = self._post(f"/v2/directions/{prof}", json=payload)
                else:
                    _log.warning("ROUTE SNAP made no change; re-raising.")
                    raise
            else:
                raise

        route = (data.get("routes") or data.get("features") or [None])[0]
        if route is None:
            raise RuntimeError("Directions response missing 'routes' or 'features'.")

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
        data = self._post(f"/v2/matrix/{prof}", json=payload)
        return {
              "origins": os_
            , "destinations": ds_
            , "distances_m": data.get("distances")
            , "durations_s": data.get("durations")
        }
