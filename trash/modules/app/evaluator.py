# modules/app/evaluator.py
# -*- coding: utf-8 -*-
"""
Single-route evaluator (Road × Cabotage)

- Resolves origin/destiny (accepts dicts with lat/lon to skip geocoding)
- Picks nearest ports (gate-aware), routes O→Po and Pd→D (ORS)
- Computes sea distance via SeaMatrix (matrix first, haversine fallback)
- ROAD totals via standardized road fuel/emissions model
- CABOTAGE totals = sea K [kg/t·km] + port ops + hotel (all in MGO EF family)
- Diesel price from CSV (avg UF_origin/UF_destiny) with clear metadata
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from modules.core import Path, Any, Dict, List, Optional, Tuple

from modules.infra.logging import get_logger
from modules.road.ors_common import ORSConfig
from modules.road.ors_client import ORSClient

from modules.addressing.resolver    import resolve_point
from modules.ports.ports_index   import load_ports
from modules.ports.ports_nearest import find_nearest_port
from modules.cabotage.sea_matrix    import SeaMatrix
from modules.fuel               import accounting as acc

from modules.emissions.emissions   import estimate_road_trip
from modules.fuel.truck_specs import get_truck_spec
from modules.costs.diesel_prices import load_latest_diesel_price, avg_price_for_ufs

_log = get_logger(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Constants / Defaults
# ────────────────────────────────────────────────────────────────────────────────

DIESEL_DENSITY_KG_PER_L: float = 0.84

DEFAULT_SEA_K_KG_PER_TKM: float = 0.0027   # [kg fuel / (t·km)]
DEFAULT_K_PORT_KG_PER_T: float  = 0.48     # [kg fuel / t handled]
DEFAULT_MGO_PRICE_BRL_PER_T: float = 3200.0

# MGO TTW EF (keep diesel separate, only compare CO2e totals)
EF_TTW_MGO_KG_PER_T = {"CO2": 3206.0, "CH4": 0.0, "N2O": 0.0}
GWP100 = {"CH4": 29.8, "N2O": 273.0}

# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class Dependencies:
    ors: Optional[ORSClient] = None
    ports: Optional[List[Dict[str, Any]]] = None
    sea_mx: Optional[SeaMatrix] = None


@dataclass
class DataPaths:
    ports_json: Path = Path("data/cabotage_data/ports_br.json")
    sea_matrix_json: Path = Path("data/cabotage_data/sea_matrix.json")
    hotel_json: Path = Path("data/cabotage_data/hotel.json")
    diesel_prices_csv: Path = Path("data/road_data/latest_diesel_prices.csv")

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _get_gate_point(p: Dict[str, Any]) -> Dict[str, float]:
    g = p.get("gate")
    if isinstance(g, dict) and "lat" in g and "lon" in g:
        return {"lat": float(g["lat"]), "lon": float(g["lon"]), "label": f"{p.get('name','port')} gate"}
    return {"lat": float(p["lat"]), "lon": float(p["lon"]), "label": p.get("name", "port")}

def _route_km(ors: ORSClient, src: Dict[str, Any], dst: Dict[str, Any], profile: str) -> float:
    data = ors.route_road(src, dst, profile=profile)
    km = float(data["distance_m"]) / 1000.0
    _log.debug("route_km: %s → %.3f km (profile=%s)", src.get("label"), km, profile)
    return km

def _route_km_with_fallback(
      ors: ORSClient
    , src: Dict[str, Any]
    , dst: Dict[str, Any]
    , *
    , primary_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
) -> Tuple[float, str]:
    try:
        return _route_km(ors, src, dst, primary_profile), primary_profile
    except Exception as e_primary:
        if fallback_to_car and primary_profile != "driving-car":
            _log.warning("Primary '%s' failed (%s).", primary_profile, e_primary)
            _log.warning("Falling back to 'driving-car'.")
            return _route_km(ors, src, dst, "driving-car"), "driving-car"
        _log.error("ORS routing failed for profile '%s': %s", primary_profile, e_primary)
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
    """Returns (fuel_kg, co2e_kg, cost_brl) for one road leg."""
    try:
        spec = get_truck_spec(truck_key).copy()
    except Exception:
        spec = get_truck_spec("semi_27t").copy()
    if axles_override is not None:
        spec["axles"] = int(axles_override)

    est = estimate_road_trip(
          distance_km=distance_km
        , cargo_t=cargo_t
        , truck_key=truck_key
        , truck_spec=spec
        , empty_backhaul_share=empty_backhaul_share
        , diesel_price_brl_l=diesel_price_brl_per_l
    )
    liters_total = float(est["fuel"]["liters_total"])
    fuel_kg      = liters_total * DIESEL_DENSITY_KG_PER_L
    co2e_kg      = float(est["emissions"]["co2e_total_kg"])
    cost_brl     = float(est["cost"]["fuel_cost_brl"])
    _log.info("road_leg: km=%.3f liters=%.3f fuel_kg=%.3f co2e=%.3f cost=%.2f",
              distance_km, liters_total, fuel_kg, co2e_kg, cost_brl)
    return fuel_kg, co2e_kg, cost_brl

def _emissions_co2e_from_fuel(*, fuel_kg: float,
                              ef_ttw_per_tonne_fuel: Dict[str, float] = EF_TTW_MGO_KG_PER_T,
                              gwp100: Optional[Dict[str, float]] = GWP100) -> float:
    res = acc.emissions_ttw(fuel_kg=fuel_kg, ef_ttw_per_tonne_fuel=ef_ttw_per_tonne_fuel,
                            gwp100=(gwp100 or {"CH4": 0.0, "N2O": 0.0}))
    return float(res.get("CO2e", 0.0))

def _sea_fuel_for_leg(*, sea_km: float, cargo_t: float, K_kg_per_tkm: float) -> float:
    return float(K_kg_per_tkm) * float(cargo_t) * float(sea_km)

def _port_and_hotel_fuel(
      *
    , origin_port: Dict[str, Any]
    , dest_port: Dict[str, Any]
    , cargo_t: float
    , hotel_json_path: Path
    , K_port_kg_per_t: float = DEFAULT_K_PORT_KG_PER_T
    , default_kg_per_t: float = 0.0
) -> Tuple[float, Dict[str, float]]:
    f_port = 2.0 * float(cargo_t) * float(K_port_kg_per_t)
    hotel = acc.load_hotel_entries(path=str(hotel_json_path))
    idx   = acc.build_hotel_factor_index(hotel_data=hotel)
    k_o = float(idx.get(str(origin_port.get("city", "")).strip(), default_kg_per_t))
    k_d = float(idx.get(str(dest_port.get("city", "")).strip(),  default_kg_per_t))
    f_hotel = float(cargo_t) * (k_o + k_d)
    total = f_port + f_hotel
    _log.info("ops+hotel: port=%.3f kg hotel_o=%.3f kg hotel_d=%.3f kg → total=%.3f kg",
              f_port, float(cargo_t)*k_o, float(cargo_t)*k_d, total)
    return total, {"port_handling_kg": f_port, "hotel_o_kg": float(cargo_t)*k_o, "hotel_d_kg": float(cargo_t)*k_d}

# ────────────────────────────────────────────────────────────────────────────────
# UF extraction + diesel CSV averaging
# ────────────────────────────────────────────────────────────────────────────────

_UF_SET = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS",
    "MG","PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
}
_STATE_NAME_TO_UF = {
      "acre":"AC","alagoas":"AL","amapá":"AP","amapa":"AP","amazonas":"AM","bahia":"BA",
      "ceará":"CE","ceara":"CE","distrito federal":"DF","espírito santo":"ES","espirito santo":"ES",
      "goiás":"GO","goias":"GO","maranhão":"MA","maranhao":"MA","mato grosso":"MT",
      "mato grosso do sul":"MS","minas gerais":"MG","pará":"PA","para":"PA","paraíba":"PB","paraiba":"PB",
      "paraná":"PR","parana":"PR","pernambuco":"PE","piauí":"PI","piaui":"PI","rio de janeiro":"RJ",
      "rio grande do norte":"RN","rio grande do sul":"RS","rondônia":"RO","rondonia":"RO",
      "roraima":"RR","santa catarina":"SC","são paulo":"SP","sao paulo":"SP","sergipe":"SE","tocantins":"TO"
}

def _extract_uf(point: Dict[str, Any], fallback_text: str = "") -> Optional[str]:
    for k in ("uf","state_code","state","region_code","admin1_code"):
        v = point.get(k)
        if isinstance(v, str):
            v2 = v.strip().upper()[:2]
            if v2 in _UF_SET:
                return v2
    text = " ".join([str(point.get("label","")), str(point.get("city","")),
                     str(point.get("state","")), str(fallback_text or "")])
    for tok in re.findall(r"\b[A-Za-z]{2}\b", text.upper()):
        if tok in _UF_SET:
            return tok
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
) -> Tuple[float, Dict[str, Any]]:
    tbl = load_latest_diesel_price(str(csv_path) if csv_path else None)
    uf_o = _extract_uf(origin_point)
    uf_d = _extract_uf(destiny_point)
    avg, ctx = avg_price_for_ufs(uf_o, uf_d, tbl)
    ctx["source_csv"] = str(csv_path)
    return float(avg), ctx

# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────

def evaluate(
      *
    , deps: Optional[Dependencies] = None
    , paths: Optional[DataPaths] = None
    , origin: Any
    , destiny: Any
    , cargo_t: float
    , truck_key: str = "semi_27t"
    , diesel_price_brl_per_l: float | None = None
    , empty_backhaul_share: float = 0.0
    , K_sea_kg_per_tkm: float = DEFAULT_SEA_K_KG_PER_TKM
    , mgo_price_brl_per_t: float = DEFAULT_MGO_PRICE_BRL_PER_T
    , ors_profile: str = "driving-hgv"
    , fallback_to_car: bool = True
    , include_geo: bool = False
    , diesel_prices_csv: Optional[Path] = None
) -> Dict[str, Any]:
    # axle strategy
    if truck_key in ("auto", "auto_by_weight"):
        from modules.fuel.road_fuel_model import infer_axles_for_payload
        axles_eff = infer_axles_for_payload(cargo_t)
        _log.info("Axles resolved via payload=%.3f t → %d (truck_key='%s').", cargo_t, axles_eff, truck_key)
    else:
        axles_eff = None

    deps  = deps  or Dependencies()
    paths = paths or DataPaths()

    # ORS
    if deps.ors is None:
        ors = ORSClient(cfg=ORSConfig())
        _log.info("ORSClient: created.")
    else:
        ors = deps.ors
        _log.info("ORSClient: injected dependency.")

    # Ports & SeaMatrix
    ports  = deps.ports  if deps.ports  is not None else load_ports(path=str(paths.ports_json))
    sea_mx = deps.sea_mx if deps.sea_mx is not None else SeaMatrix.from_json_path(paths.sea_matrix_json)

    # Resolve endpoints
    o = resolve_point(origin,  ors=ors)
    d = resolve_point(destiny, ors=ors)
    _log.info("Resolved origin='%s' destiny='%s'.", str(o.get("label") or origin), str(d.get("label") or destiny))

    # Diesel price
    if diesel_price_brl_per_l is None:
        csv_path = Path(diesel_prices_csv) if diesel_prices_csv else paths.diesel_prices_csv
        diesel_price_brl_per_l, diesel_meta = _avg_diesel_price_for_endpoints(
            origin_point=o
            , destiny_point=d
            , csv_path=csv_path
        )
        _log.info("Diesel price (avg UF_o/UF_d): %.4f BRL/L (fallback_used=%s).",
                  diesel_price_brl_per_l, diesel_meta.get("fallback_used"))
    else:
        diesel_meta = {
            "uf_origin": _extract_uf(o), "uf_destiny": _extract_uf(d),
            "price_origin": None, "price_destiny": None,
            "source_csv": None, "fallback_used": False, "override_cli": True
        }
        _log.info("Diesel price overridden via CLI: %.4f BRL/L.", diesel_price_brl_per_l)

    # ROAD direct
    road_km, used_prof_road = _route_km_with_fallback(ors, o, d, primary_profile=ors_profile, fallback_to_car=fallback_to_car)
    road_fuel_kg, road_co2e_kg, road_cost_brl = _road_totals_for_distance(
        distance_km=road_km, cargo_t=cargo_t, diesel_price_brl_per_l=diesel_price_brl_per_l,
        truck_key=truck_key, empty_backhaul_share=empty_backhaul_share, axles_override=axles_eff
    )

    # CABOTAGE — nearest ports
    p_o = find_nearest_port(float(o["lat"]), float(o["lon"]), ports)
    p_d = find_nearest_port(float(d["lat"]), float(d["lon"]), ports)
    _log.info("Nearest ports: origin='%s' (%.3f km), destiny='%s' (%.3f km).",
              p_o["name"], p_o["distance_km"], p_d["name"], p_d["distance_km"])
    gate_o = _get_gate_point(p_o)
    gate_d = _get_gate_point(p_d)

    # O → Po (road)
    km1, used_prof_km1 = _route_km_with_fallback(ors, o, gate_o, primary_profile=ors_profile, fallback_to_car=fallback_to_car)
    f1_kg, e1_kg, c1_brl = _road_totals_for_distance(
        distance_km=km1, cargo_t=cargo_t, diesel_price_brl_per_l=diesel_price_brl_per_l,
        truck_key=truck_key, empty_backhaul_share=empty_backhaul_share, axles_override=axles_eff
    )

    # Sea (Po ↔ Pd)
    sea_km = float(sea_mx.km(
        {"name": p_o["name"], "lat": float(p_o["lat"]), "lon": float(p_o["lon"])},
        {"name": p_d["name"], "lat": float(p_d["lat"]), "lon": float(p_d["lon"])}
    ))
    fuel_sea_kg  = _sea_fuel_for_leg(sea_km=sea_km, cargo_t=cargo_t, K_kg_per_tkm=K_sea_kg_per_tkm)
    emis_sea_kg  = _emissions_co2e_from_fuel(fuel_kg=fuel_sea_kg, ef_ttw_per_tonne_fuel=EF_TTW_MGO_KG_PER_T)
    cost_sea_brl = (fuel_sea_kg / 1000.0) * mgo_price_brl_per_t
    _log.info("Sea leg: km=%.3f fuel=%.3f kg CO2e=%.3f cost=%.2f.", sea_km, fuel_sea_kg, emis_sea_kg, cost_sea_brl)

    # Port ops + hotel (MGO family)
    fuel_ops_hotel_kg, ops_break = _port_and_hotel_fuel(
        origin_port=p_o, dest_port=p_d, cargo_t=cargo_t, hotel_json_path=paths.hotel_json,
        K_port_kg_per_t=DEFAULT_K_PORT_KG_PER_T
    )
    emis_ops_hotel_kg  = _emissions_co2e_from_fuel(fuel_kg=fuel_ops_hotel_kg, ef_ttw_per_tonne_fuel=EF_TTW_MGO_KG_PER_T)
    cost_ops_hotel_brl = (fuel_ops_hotel_kg / 1000.0) * mgo_price_brl_per_t

    # Pd → D (road)
    km3, used_prof_km3 = _route_km_with_fallback(ors, gate_d, d, primary_profile=ors_profile, fallback_to_car=fallback_to_car)
    f3_kg, e3_kg, c3_brl = _road_totals_for_distance(
        distance_km=km3, cargo_t=cargo_t, diesel_price_brl_per_l=diesel_price_brl_per_l,
        truck_key=truck_key, empty_backhaul_share=empty_backhaul_share, axles_override=axles_eff
    )

    # Totals & deltas
    cab_fuel_kg  = f1_kg + fuel_sea_kg + fuel_ops_hotel_kg + f3_kg
    cab_co2e_kg  = e1_kg + emis_sea_kg + emis_ops_hotel_kg + e3_kg
    cab_cost_brl = c1_brl + cost_sea_brl + cost_ops_hotel_brl + c3_brl

    delta_fuel_kg  = cab_fuel_kg  - road_fuel_kg
    delta_co2e_kg  = cab_co2e_kg  - road_co2e_kg
    delta_cost_brl = cab_cost_brl - road_cost_brl

    # vehicle meta (axles used)
    try:
        baseline_ax = int(get_truck_spec(truck_key).get("axles"))
    except Exception:
        baseline_ax = None
    axles_used = int(axles_eff) if axles_eff is not None else (baseline_ax if baseline_ax is not None else 5)

    out: Dict[str, Any] = dict(
          input=dict(
              origin=str(o.get("label") or origin)
            , destiny=str(d.get("label") or destiny)
            , cargo_t=float(cargo_t)
            , truck_key=str(truck_key)
            , diesel_brl_l=float(diesel_price_brl_per_l)
            , diesel_source=diesel_meta
            , empty_backhaul_share=float(empty_backhaul_share)
            , ors_profile=str(ors_profile)
            , fallback_to_car=bool(fallback_to_car)
            , sea_K_kg_per_tkm=float(K_sea_kg_per_tkm)
            , mgo_price_brl_per_t=float(mgo_price_brl_per_t)
            , k_port_kg_per_t=float(DEFAULT_K_PORT_KG_PER_T)
        )
        , selection=dict(
              port_origin=dict(name=p_o.get("name"), city=p_o.get("city"),
                               lat=float(p_o["lat"]), lon=float(p_o["lon"]))
            , port_destiny=dict(name=p_d.get("name"), city=p_d.get("city"),
                                lat=float(p_d["lat"]), lon=float(p_d["lon"]))
            , profiles_used=dict(road_direct=used_prof_road, o_to_po=used_prof_km1, pd_to_d=used_prof_km3)
            , vehicle=dict(truck_key=str(truck_key), axles_used=axles_used)
        )
        , road_only=dict(distance_km=road_km, fuel_kg=road_fuel_kg, co2e_kg=road_co2e_kg, cost_brl=road_cost_brl)
        , cabotage=dict(
              o_to_po=dict(distance_km=km1, fuel_kg=f1_kg, co2e_kg=e1_kg, cost_brl=c1_brl, origin_gate=_get_gate_point(p_o))
            , sea=dict(sea_km=sea_km, fuel_kg=fuel_sea_kg, co2e_kg=emis_sea_kg, cost_brl=cost_sea_brl)
            , ops_hotel=dict(fuel_kg=fuel_ops_hotel_kg, co2e_kg=emis_ops_hotel_kg, cost_brl=cost_ops_hotel_brl, breakdown=ops_break)
            , pd_to_d=dict(distance_km=km3, fuel_kg=f3_kg, co2e_kg=e3_kg, cost_brl=c3_brl, dest_gate=_get_gate_point(p_d))
            , totals=dict(fuel_kg=cab_fuel_kg, co2e_kg=cab_co2e_kg, cost_brl=cab_cost_brl)
        )
        , deltas_cabotage_minus_road=dict(fuel_kg=delta_fuel_kg, co2e_kg=delta_co2e_kg, cost_brl=delta_cost_brl)
    )

    if include_geo:
        out["input"].update(dict(
            origin_lat=float(o["lat"]), origin_lon=float(o["lon"]),
            destiny_lat=float(d["lat"]), destiny_lon=float(d["lon"])
        ))

    _log.info("Evaluation done: ROAD CO2e=%.3f kg, CABOTAGE CO2e=%.3f kg (Δ=%.3f kg).",
              road_co2e_kg, cab_co2e_kg, delta_co2e_kg)
    return out


"""
Quick logging smoke test (PowerShell)
python -c `
"from modules.functions.logging import init_logging; `
from modules.app.evaluator import evaluate, Dependencies, DataPaths; `
from modules.cabotage.sea_matrix import SeaMatrix; `
import types; `
init_logging(level='INFO', force=True, write_output=False); `
# --- Fake ORS (no network)
def _route_road(src, dst, profile='driving-hgv'):
    s = (src.get('label','') or '').lower(); d = (dst.get('label','') or '').lower()
    if 'gate' in d: return {'distance_m': 55_000.0}   # O -> Po
    if 'gate' in s: return {'distance_m': 30_000.0}   # Pd -> D
    return {'distance_m': 432_000.0}                  # direct O -> D
ors = types.SimpleNamespace(route_road=_route_road); `
# --- Minimal ports + sea matrix
ports = [
  {'name':'Santos (SP)','city':'Santos','state':'SP','lat':-23.952,'lon':-46.328,
   'gates':[{'label':'Ponta da Praia','lat':-23.986,'lon':-46.296}]},
  {'name':'Rio de Janeiro (RJ)','city':'Rio de Janeiro','state':'RJ','lat':-22.903,'lon':-43.172}
]; `
sm = SeaMatrix.from_json_dict({'matrix': {'Santos (SP)': {'Rio de Janeiro (RJ)': 430.0}}, 'coastline_factor': 1.10}); `
deps = Dependencies(ors=ors, ports=ports, sea_mx=sm); `
O = {'lat':-23.55,'lon':-46.63,'label':'São Paulo, SP','state':'SP'}; `
D = {'lat':-22.90,'lon':-43.17,'label':'Rio de Janeiro, RJ','state':'RJ'}; `
out = evaluate(deps=deps, paths=DataPaths(), origin=O, destiny=D, cargo_t=20.0, `
  truck_key='auto_by_weight', diesel_price_brl_per_l=6.10, empty_backhaul_share=0.25, include_geo=True); `
print('OK; road_km=', round(out['road_only']['distance_km'],1), ' sea_km=', out['cabotage']['sea']['sea_km']); "
"""
