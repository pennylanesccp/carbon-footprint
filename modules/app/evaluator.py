# modules/app/evaluator.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Correct import split (Config and Client come from different modules)
from modules.road.ors_common import ORSConfig
from modules.road.ors_client  import ORSClient

from modules.addressing.resolver      import resolve_point
from modules.cabotage.ports_index     import load_ports
from modules.cabotage.ports_nearest   import find_nearest_port
from modules.cabotage.sea_matrix      import SeaMatrix
from modules.road.emissions           import estimate_road_trip, TRUCK_SPECS
from modules.cabotage                 import accounting as acc

# NEW: diesel price loader (UF -> price)
from modules.road.fuel_model          import load_diesel_prices


# ────────────────────────────────────────────────────────────────────────────────
# Logging
# ────────────────────────────────────────────────────────────────────────────────

_log = logging.getLogger("cabosupernet.app.evaluator")


# ────────────────────────────────────────────────────────────────────────────────
# Constants / Defaults
# ────────────────────────────────────────────────────────────────────────────────

DIESEL_DENSITY_KG_PER_L: float = 0.84

DEFAULT_SEA_K_KG_PER_TKM: float = 0.0027
DEFAULT_K_PORT_KG_PER_T: float  = 0.48

DEFAULT_MGO_PRICE_BRL_PER_T: float = 3200.0

# constants (either in evaluator.py or your accounting module)
EF_TTW_DIESEL_KG_PER_T = {"CO2": 3206.0, "CH4": 0.0, "N2O": 0.0}
EF_TTW_MGO_KG_PER_T    = {"CO2": 3206.0, "CH4": 0.0, "N2O": 0.0}

# GWPs stay as dimensionless multipliers (do NOT put them in EF)
GWP100 = {"CH4": 29.8, "N2O": 273.0}



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
    # NEW: default location for the diesel prices CSV (UF, price)
    diesel_prices_csv: Path = Path("modules/road/_data/latest_diesel_prices.csv")


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
    , axles_override: Optional[int] = None
) -> Tuple[float, float, float]:
    spec = TRUCK_SPECS[truck_key].copy()
    if axles_override is not None:
        spec["axles"] = int(axles_override)

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
# UF extraction + CSV average helpers
# ────────────────────────────────────────────────────────────────────────────────

_UF_SET = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
    "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
}

_STATE_NAME_TO_UF = {
      "acre":"AC" , "alagoas":"AL", "amapá":"AP", "amapa":"AP", "amazonas":"AM"
    , "bahia":"BA", "ceará":"CE", "ceara":"CE", "distrito federal":"DF"
    , "espírito santo":"ES", "espirito santo":"ES", "goiás":"GO", "goias":"GO"
    , "maranhão":"MA", "maranhao":"MA", "mato grosso":"MT"
    , "mato grosso do sul":"MS" , "minas gerais":"MG"
    , "pará":"PA", "para":"PA", "paraíba":"PB", "paraiba":"PB"
    , "paraná":"PR", "parana":"PR", "pernambuco":"PE"
    , "piauí":"PI", "piaui":"PI", "rio de janeiro":"RJ"
    , "rio grande do norte":"RN", "rio grande do sul":"RS"
    , "rondônia":"RO", "rondonia":"RO", "roraima":"RR"
    , "santa catarina":"SC", "são paulo":"SP", "sao paulo":"SP"
    , "sergipe":"SE", "tocantins":"TO"
}

def _extract_uf(point: Dict[str, Any], fallback_text: str = "") -> Optional[str]:
    """
    Best-effort UF extraction from a resolved point.
    Looks at direct fields, two-letter tokens, and state names.
    """
    # direct fields that may carry a UF/UF-like code
    for k in ("uf","state_code","state","region_code","admin1_code"):
        v = point.get(k)
        if isinstance(v, str):
            v2 = v.strip().upper()[:2]
            if v2 in _UF_SET:
                return v2

    # combine label/city/state/etc. to search tokens
    text = " ".join([
          str(point.get("label", ""))
        , str(point.get("city", ""))
        , str(point.get("state", ""))
        , str(fallback_text or "")
    ])

    # two-letter tokens
    for tok in re.findall(r"\b[A-Za-z]{2}\b", text.upper()):
        if tok in _UF_SET:
            return tok

    # full state names
    low = text.lower()
    for name, uf in _STATE_NAME_TO_UF.items():
        if name in low:
            return uf

    return None


def _avg_diesel_price_for_endpoints(
      *
    , origin_point: Dict[str, Any]
    , destiny_point: Dict[str, Any]
    , csv_path: Path
    , fallback_price: float = 6.0
) -> Tuple[float, Dict[str, Any]]:
    """
    Average diesel price for (UF_origin, UF_destiny) using CSV.
    If only one UF is found, use that one. If none found or UF not in CSV, fallback.
    Returns (price_brl_per_l, meta_dict).
    """
    price_idx = load_diesel_prices(str(csv_path) if csv_path else None)

    uf_o = _extract_uf(origin_point)
    uf_d = _extract_uf(destiny_point)

    p_o = price_idx.get(uf_o) if uf_o else None
    p_d = price_idx.get(uf_d) if uf_d else None

    candidates = [p for p in (p_o, p_d) if isinstance(p, (int, float))]
    if candidates:
        avg = sum(candidates) / len(candidates)
        return float(avg), dict(
              uf_origin=uf_o
            , uf_destiny=uf_d
            , price_origin=p_o
            , price_destiny=p_d
            , source_csv=str(csv_path)
            , fallback_used=False
        )

    return float(fallback_price), dict(
          uf_origin=uf_o
        , uf_destiny=uf_d
        , price_origin=p_o
        , price_destiny=p_d
        , source_csv=str(csv_path)
        , fallback_used=True
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
    , diesel_price_brl_per_l: float | None = None   # ← None ⇒ use CSV average
    , empty_backhaul_share: float = 0.0
    , K_sea_kg_per_tkm: float = DEFAULT_SEA_K_KG_PER_TKM
    , mgo_price_brl_per_t: float = DEFAULT_MGO_PRICE_BRL_PER_T
    , ors_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
    , include_geo: bool = False
    , diesel_prices_csv: Optional[Path] = None       # ← NEW, added at the end to avoid breaking positional calls
) -> Dict[str, Any]:
    """
    Compute ROAD vs CABOTAGE for a single destiny and return a structured dict.
    """
    # Decide axle strategy (manual vs. auto-by-weight)
    # If user passes truck_key == "auto_by_weight", infer from cargo_t
    if truck_key in ("auto", "auto_by_weight"):
        from modules.road.fuel_model import infer_axles_for_payload
        axles_eff = infer_axles_for_payload(cargo_t)
    else:
        axles_eff = None  # use the preset's axles

    deps  = deps  or Dependencies()
    paths = paths or DataPaths()

    if deps.ors is None:
        cfg = ORSConfig()
        ors = ORSClient(cfg=cfg)
    else:
        ors = deps.ors

    ports        = deps.ports  if deps.ports  is not None else load_ports(path=str(paths.ports_json))
    sea_mx       = deps.sea_mx if deps.sea_mx is not None else SeaMatrix.from_json_path(paths.sea_matrix_json)
    hotel_json_path = paths.hotel_json

    # Resolve endpoints
    o = resolve_point(origin,  ors=ors)
    d = resolve_point(destiny, ors=ors)

    # Determine diesel price
    if diesel_price_brl_per_l is None:
        csv_path = Path(diesel_prices_csv) if diesel_prices_csv else paths.diesel_prices_csv
        diesel_price_brl_per_l, diesel_meta = _avg_diesel_price_for_endpoints(
              origin_point=o
            , destiny_point=d
            , csv_path=csv_path
            , fallback_price=6.0
        )
    else:
        diesel_meta = dict(
              uf_origin=_extract_uf(o)
            , uf_destiny=_extract_uf(d)
            , price_origin=None
            , price_destiny=None
            , source_csv=None
            , fallback_used=False
            , override_cli=True
        )

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
            , diesel_source=diesel_meta                # ← transparency on CSV/UF/fallback
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
            , vehicle=dict(
                  truck_key=str(truck_key)
                , axles_used=int(axles_eff) if axles_eff is not None else int(TRUCK_SPECS[truck_key]["axles"])
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
