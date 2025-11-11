# BIBLIOGRAPHY.md

This document lists the primary data sources, academic papers, and engineering benchmarks used by the model.

---

## 1) Primary data sources (raw)

- **ANTAQ — `2025Atracacao.txt`**  
  Public port-call records for 2025. Used by `calcs/hotel.py` to compute **average time at berth** per port (Cabotagem only), which is transformed into per-tonne hotel factors saved to `_data/hotel.json`.

- **OpenRouteService (ORS) API**  
  Geocoding and routing for road legs (profile `driving-hgv`; SNAP-to-road retry on 404).

---

## 2) Pre-processed / generated files

- **`ports_br.json`**  
  Canonical list of Brazilian ports with coordinates, aliases, and **truck gate** positions. Used for port normalization and gate-aware nearest-port selection.

- **`sea_matrix.json`**  
  Precomputed **port-to-port** distances (Haversine-based), with metadata including a **`coastline_factor = 1.18`**. Used directly by the cabotage leg; runtime fallback also uses Haversine × coastline factor.

- **`hotel.json`**  
  **Per-port hotel factors** in **kg of fuel per tonne** (`kg/t`) derived from ANTAQ time-at-berth and an auxiliary power/SFOC benchmark. Loaded by the cabotage logic.

- **`emissions.py` (road)**  
  Contains `TRUCK_SPECS` (axles, payload, reference weight, empty-gain) and the **per-trip** estimator used by the evaluator. Baseline km/L by axle is embedded here.

---

## 3) Public benchmark references

- **ICCT (Jul/2022). _Brazilian coastal shipping: New prospects for growth with decarbonization._**  
  Policy and context frame for cabotage; supports modeling port effects and operational factors.

- **IMO / engineering references for auxiliaries (SFOC)**  
  Benchmarks used in preprocessing for per-port hotel factors: **~600 kW** average auxiliary demand and **~225 g/kWh** SFOC, implying about **135 kg of fuel per hour** at berth.

- **ANTT (Brazil) axle-based fuel economy**  
  Source of **baseline km/L by axle count** used in road calculations (e.g., 5-axle ≈ 2.3 km/L; 6–7 axles ≈ 2.0 km/L).

- **OpenRouteService documentation**  
  API semantics for **geocoding**, **directions**, and **SNAP-to-road** behavior.

---

## 4) Methodological papers cited by approach

- **Leenders et al. (2017). _Emissions allocation in transportation routes._**  
  Motivation for treating multi-stop allocation carefully (future work path; current code uses per-tonne constants at port).

- **Cooperative game theory (Shapley value)** — e.g., Arroyo et al. (2024)  
  “Gold-standard” fairness concept for common-cost sharing (not yet implemented; referenced as extension path).

---

## 5) Emission factors & constants used in code

- **Road (diesel, TTW)**  
  `DIESEL_DENSITY_KG_PER_L = 0.84`, `EF_DIESEL_CO2_KG_PER_L = 2.68`.

- **Maritime (MGO, TTW)**  
  `EF_TTW_MGO_KG_PER_T = {"CO2": 3206.0, "CH4": 0.0, "N2O": 0.0}` → **3.206 tCO₂ per t fuel**.  
  Monetary cost uses `MGO_PRICE_BRL_PER_T = 3200.0`.  
  Port handling also applies **fixed dwell hours** and a **fixed cost per call** in totals.
