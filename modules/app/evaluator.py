# modules/app/evaluator.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# TOP OF FILE — replace the wrong import block
# from modules.road.ors_common import ORSClient, ORSConfig
from modules.road.ors_common import ORSConfig
from modules.road.ors_client  import ORSClient
from modules.addressing.resolver import resolve_point
from modules.cabotage.ports_index import load_ports
from modules.cabotage.ports_nearest import find_nearest_port
from modules.cabotage.sea_matrix import SeaMatrix
from modules.road.emissions import estimate_road_trip, TRUCK_SPECS
from modules.cabotage import accounting as acc

# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────

_log = logging.getLogger("cabosupernet.app.evaluator")


# ────────────────────────────────────────────────────────────────────────────────
# Constants / Defaults
# ────────────────────────────────────────────────────────────────────────────────

DIESEL_DENSITY_KG_PER_L: float = 0.84

DEFAULT_SEA_K_KG_PER_TKM: float = 0.0027
DEFAULT_K_PORT_KG_PER_T: float = 0.48

DEFAULT_MGO_PRICE_BRL_PER_T: float = 3200.0

EF_TTW_MGO_KG_PER_T = dict(
      CO2=3206.0
    , CH4=0.0
    , N2O=0.0
)
GWP100 = dict(CH4=29.8, N2O=273.0)


# ────────────────────────────────────────────────────────────────────────────────
# Dependency & path carriers
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class Dependencies:
    ors: Optional[ORSClient] = None
    ports: Optional[List[Dict[str, Any]]] = None
    sea_mx: Optional[SeaMatrix] = None


@dataclass
class DataPaths:
    ports_json: Path = Path("modules/cabotage/_data/ports_br.json")
    sea_matrix_json: Path = Path("modules/cabotage/_data/sea_matrix.json")
    hotel_json: Path = Path("modules/cabotage/_data/hotel.json")


# ────────────────────────────────────────────────────────────────────────────────
# Small helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_gate_point(p: Dict[str, Any]) -> Dict[str, float]:
    g = p.get("gate")
    if isinstance(g, dict) and "lat" in g and "lon" in g:
        return dict(
              lat=float(g["lat"])
            , lon=float(g["lon"])
            , label=f"{p.get('name', 'port')} gate"
        )
    return dict(
          lat=float(p["lat"])
        , lon=float(p["lon"])
        , label=p.get("name", "port")
    )


def _route_km(ors: ORSClient, src: Dict[str, Any], dst: Dict[str, Any], profile: str) -> float:
    data = ors.route_road(src, dst, profile=profile)
    return float(data["distance_m"]) / 1000.0


def _route_km_with_fallback(
      ors: ORSClient
    , src: Dict[str, Any]
    , dst: Dict[str, Any]
    , *
    , primary_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
) -> Tuple[float, str]:
    try:
        km = _route_km(ors, src, dst, primary_profile)
        return km, primary_profile
    except Exception as e_primary:
        if fallback_to_car and primary_profile != "driving-car":
            _log.warning(
                  "Primary profile '%s' failed (%s). Falling back to 'driving-car'."
                , primary_profile
                , e_primary
            )
            km = _route_km(ors, src, dst, "driving-car")
            return km, "driving-car"
        raise


def _road_totals_for_distance(
      *
    , distance_km: float
    , cargo_t: float
    , diesel_price_brl_per_l: float
    , truck_key: str
    , empty_backhaul_share: float
) -> Tuple[float, float, float]:
    spec = TRUCK_SPECS[truck_key]
    est = estimate_road_trip(
          distance_km=distance_km
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , spec=spec
        , empty_backhaul_share=empty_backhaul_share
    )
    liters_total = float(est["fuel"]["liters_total"])
    fuel_kg      = liters_total * DIESEL_DENSITY_KG_PER_L
    co2e_kg      = float(est["emissions"]["co2e_total_kg"])
    cost_brl     = float(est["cost"]["fuel_cost_brl"])
    return fuel_kg, co2e_kg, cost_brl


def _emissions_co2e_from_fuel(
      *
    , fuel_kg: float
    , ef_ttw_per_tonne_fuel: Dict[str, float] = EF_TTW_MGO_KG_PER_T
    , gwp100: Optional[Dict[str, float]] = GWP100
) -> float:
    res = acc.emissions_ttw(
          fuel_kg=fuel_kg
        , ef_ttw_per_tonne_fuel=ef_ttw_per_tonne_fuel
        , gwp100=gwp100
    )
    return float(res.get("CO2e", 0.0))


def _sea_fuel_for_leg(
      *
    , sea_km: float
    , cargo_t: float
    , K_kg_per_tkm: float
) -> float:
    return float(K_kg_per_tkm) * float(cargo_t) * float(sea_km)


def _port_and_hotel_fuel(
      *
    , origin_port: Dict[str, Any]
    , dest_port: Dict[str, Any]
    , cargo_t: float
    , hotel_json_path: Path
    , K_port_kg_per_t: float = DEFAULT_K_PORT_KG_PER_T
    , default_hotel_kg_per_t: float = 0.0
) -> Tuple[float, Dict[str, float]]:
    f_port = (2.0 * float(cargo_t) * float(K_port_kg_per_t))

    hotel = acc.load_hotel_entries(path=str(hotel_json_path))
    idx   = acc.build_hotel_factor_index(hotel_data=hotel)

    k_o = float(idx.get(str(origin_port.get("city", "")).strip(), default_hotel_kg_per_t))
    k_d = float(idx.get(str(dest_port.get("city", "")).strip(),  default_hotel_kg_per_t))

    f_hotel = float(cargo_t) * (k_o + k_d)

    return f_port + f_hotel, dict(
          port_handling_kg=f_port
        , hotel_o_kg=float(cargo_t) * k_o
        , hotel_d_kg=float(cargo_t) * k_d
    )


# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────

def _evaluate(
      *
    , deps: Optional[Dependencies] = None
    , paths: Optional[DataPaths] = None
    , origin: Any
    , destiny: Any
    , cargo_t: float
    , truck_key: str = "semi_27t"
    , diesel_price_brl_per_l: float = 6.0
    , empty_backhaul_share: float = 0.0
    , K_sea_kg_per_tkm: float = DEFAULT_SEA_K_KG_PER_TKM
    , mgo_price_brl_per_t: float = DEFAULT_MGO_PRICE_BRL_PER_T
    , ors_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
    , include_geo: bool = False
) -> Dict[str, Any]:
    """
    Compute ROAD vs CABOTAGE for a single destiny and return a structured dict.
    """

    deps  = deps  or Dependencies()
    paths = paths or DataPaths()

    if deps.ors is None:
        cfg = ORSConfig()
        ors = ORSClient(cfg=cfg)
    else:
        ors = deps.ors

    ports = deps.ports if deps.ports is not None else load_ports(path=str(paths.ports_json))
    sea_mx = deps.sea_mx if deps.sea_mx is not None else SeaMatrix.from_json_path(paths.sea_matrix_json)
    hotel_json_path = paths.hotel_json

    # Resolve endpoints
    o = resolve_point(origin,  ors=ors)
    d = resolve_point(destiny, ors=ors)

    # ROAD (direct)
    road_km, used_prof_road = _route_km_with_fallback(
          ors
        , o
        , d
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
    )
    road_fuel_kg, road_co2e_kg, road_cost_brl = _road_totals_for_distance(
          distance_km=road_km
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )

    # CABOTAGE: ports + gates
    p_o = find_nearest_port(float(o["lat"]), float(o["lon"]), ports)
    p_d = find_nearest_port(float(d["lat"]), float(d["lon"]), ports)

    gate_o = _get_gate_point(p_o)
    gate_d = _get_gate_point(p_d)

    # O → Po
    km1, used_prof_km1 = _route_km_with_fallback(
          ors
        , o
        , gate_o
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
    )
    f1_kg, e1_kg, c1_brl = _road_totals_for_distance(
          distance_km=km1
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )

    # Sea (Po ↔ Pd)
    sea_km = float(sea_mx.km(
          dict(name=p_o["name"], lat=float(p_o["lat"]), lon=float(p_o["lon"]))
        , dict(name=p_d["name"], lat=float(p_d["lat"]), lon=float(p_d["lon"]))
    ))
    fuel_sea_kg  = _sea_fuel_for_leg(
          sea_km=sea_km
        , cargo_t=cargo_t
        , K_kg_per_tkm=K_sea_kg_per_tkm
    )
    emis_sea_kg  = _emissions_co2e_from_fuel(fuel_kg=fuel_sea_kg)
    cost_sea_brl = (fuel_sea_kg / 1000.0) * mgo_price_brl_per_t

    # Port ops + hotel
    fuel_ops_hotel_kg, ops_break = _port_and_hotel_fuel(
          origin_port=p_o
        , dest_port=p_d
        , cargo_t=cargo_t
        , hotel_json_path=hotel_json_path
        , K_port_kg_per_t=DEFAULT_K_PORT_KG_PER_T
    )
    emis_ops_hotel_kg  = _emissions_co2e_from_fuel(fuel_kg=fuel_ops_hotel_kg)
    cost_ops_hotel_brl = (fuel_ops_hotel_kg / 1000.0) * mgo_price_brl_per_t

    # Pd → D
    km3, used_prof_km3 = _route_km_with_fallback(
          ors
        , gate_d
        , d
        , primary_profile=ors_profile
        , fallback_to_car=fallback_to_car
    )
    f3_kg, e3_kg, c3_brl = _road_totals_for_distance(
          distance_km=km3
        , cargo_t=cargo_t
        , diesel_price_brl_per_l=diesel_price_brl_per_l
        , truck_key=truck_key
        , empty_backhaul_share=empty_backhaul_share
    )

    # Totals (cabotage)
    cab_fuel_kg  = f1_kg + fuel_sea_kg + fuel_ops_hotel_kg + f3_kg
    cab_co2e_kg  = e1_kg + emis_sea_kg + emis_ops_hotel_kg + e3_kg
    cab_cost_brl = c1_brl + cost_sea_brl + cost_ops_hotel_brl + c3_brl

    # Deltas (cabotage − road)
    delta_fuel_kg  = cab_fuel_kg  - road_fuel_kg
    delta_co2e_kg  = cab_co2e_kg  - road_co2e_kg
    delta_cost_brl = cab_cost_brl - road_cost_brl

    out: Dict[str, Any] = dict(
          input=dict(
              origin=str(o.get("label") or origin)
            , destiny=str(d.get("label") or destiny)
            , cargo_t=float(cargo_t)
            , truck_key=str(truck_key)
            , diesel_brl_l=float(diesel_price_brl_per_l)
            , empty_backhaul_share=float(empty_backhaul_share)
            , ors_profile=str(ors_profile)
            , fallback_to_car=bool(fallback_to_car)
            , sea_K_kg_per_tkm=float(K_sea_kg_per_tkm)
            , mgo_price_brl_per_t=float(mgo_price_brl_per_t)
            , k_port_kg_per_t=float(DEFAULT_K_PORT_KG_PER_T)
        )
        , selection=dict(
              port_origin=dict(
                  name=p_o.get("name")
                , city=p_o.get("city")
                , lat=float(p_o["lat"])
                , lon=float(p_o["lon"])
            )
            , port_destiny=dict(
                  name=p_d.get("name")
                , city=p_d.get("city")
                , lat=float(p_d["lat"])
                , lon=float(p_d["lon"])
            )
            , profiles_used=dict(
                  road_direct=used_prof_road
                , o_to_po=used_prof_km1
                , pd_to_d=used_prof_km3
            )
        )
        , road_only=dict(
              distance_km=road_km
            , fuel_kg=road_fuel_kg
            , co2e_kg=road_co2e_kg
            , cost_brl=road_cost_brl
        )
        , cabotage=dict(
              o_to_po=dict(
                  distance_km=km1
                , fuel_kg=f1_kg
                , co2e_kg=e1_kg
                , cost_brl=c1_brl
                , origin_gate=_get_gate_point(p_o)
            )
            , sea=dict(
                  sea_km=sea_km
                , fuel_kg=fuel_sea_kg
                , co2e_kg=emis_sea_kg
                , cost_brl=cost_sea_brl
            )
            , ops_hotel=dict(
                  fuel_kg=fuel_ops_hotel_kg
                , co2e_kg=emis_ops_hotel_kg
                , cost_brl=cost_ops_hotel_brl
                , breakdown=ops_break
            )
            , pd_to_d=dict(
                  distance_km=km3
                , fuel_kg=f3_kg
                , co2e_kg=e3_kg
                , cost_brl=c3_brl
                , dest_gate=_get_gate_point(p_d)
            )
            , totals=dict(
                  fuel_kg=cab_fuel_kg
                , co2e_kg=cab_co2e_kg
                , cost_brl=cab_cost_brl
            )
        )
        , deltas_cabotage_minus_road=dict(
              fuel_kg=delta_fuel_kg
            , co2e_kg=delta_co2e_kg
            , cost_brl=delta_cost_brl
        )
    )

    if include_geo:
        out["input"].update(dict(
              origin_lat=float(o["lat"])
            , origin_lon=float(o["lon"])
            , destiny_lat=float(d["lat"])
            , destiny_lon=float(d["lon"])
        ))

    return out
