from __future__ import annotations
from pathlib import Path
import os

# ── package data paths ──────────────────────────────────────────────────────────
_DATA_DIR  = os.path.join("data", "cabotage_data")
PORTS_JSON = _DATA_DIR / "ports_br.json"
K_JSON     = _DATA_DIR / "k.json"

def get_ports_path() -> str:
    return str(PORTS_JSON)

def get_k_path() -> str:
    return str(K_JSON)

# ── ports utilities ─────────────────────────────────────────────────────────────
from .ports_index import load_ports, load_cts
from .ports_nearest import find_nearest_port, port_distance_km, haversine_km

# ── accounting (public API) ─────────────────────────────────────────────────────
from .accounting import (
      Leg
    , Shipment
    , compute_tonne_km
    , calibrate_K_from_observation
    , predict_fuel_from_K
    , allocate_fuel_by_leg_and_shipment
    , allocate_costs_emissions
    , allocate_port_fuel_to_shipments
    , port_fuel_from_handled_mass
    , fuel_cost
    , fuel_cost_by_leg
    , emissions_ttw
    , emissions_ttw_by_leg
    , load_k_entries
    , summarize_Ks
    , choose_K
    , load_hotel_entries
    , build_hotel_factor_index
    , allocate_hotel_fuel_from_json
)

__all__ = [
    # paths
      "get_ports_path", "get_k_path",
    # ports
      "load_ports", "load_cts", "find_nearest_port", "port_distance_km", "haversine_km",
    # accounting
      "Leg", "Shipment", "compute_tonne_km",
      "calibrate_K_from_observation", "predict_fuel_from_K",
      "allocate_fuel_by_leg_and_shipment",
      "allocate_costs_emissions", 
      "fuel_cost", "fuel_cost_by_leg",
      "emissions_ttw", "emissions_ttw_by_leg",
      "load_k_entries", "summarize_Ks", "choose_K",
      "load_hotel_entries", "build_hotel_factor_index", "allocate_hotel_fuel_from_json",
      "allocate_port_fuel_to_shipments", "port_fuel_from_handled_mass"
]
