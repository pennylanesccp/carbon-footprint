# METHODOLOGY.md

This document details the methodology, core assumptions, and engineering simplifications used in the carbon footprint calculator. The model is an **activity-based** system that breaks a journey into constituent legs (road, sea, port) and applies specific factors to each.

---

## 1) Road model (`modules/road/`)

### 1.1 Vehicle presets & trips (`emissions.py`)

- The tool uses **truck presets** (e.g., `semi_27t`) exposed via `TRUCK_SPECS`, containing at least:
  - `axles` (for baseline efficiency),
  - `payload_t` (planning payload per trip),
  - `ref_weight_t` (reference loaded weight tied to the baseline),
  - `empty_efficiency_gain` (km/L improvement when empty).
- **Trips** are simplified to `ceil(cargo_t / payload_t)`. Only mass is considered (no volume constraints). All per-trip liters/emissions/cost are scaled by this integer number of trips.

### 1.2 Fuel model (baseline & backhaul; implemented in `emissions.py`)

- **Baseline efficiency (km/L) by axles — ANTT (planning values used in code)**

  | Axles | Baseline km/L |
  |------:|---------------:|
  | 2     | 4.0            |
  | 3     | 3.0            |
  | 4     | 2.7            |
  | 5     | 2.3            |
  | 6     | 2.0            |
  | 7     | 2.0            |
  | ≥9    | 1.7            |

  The preset’s `axles` selects the baseline.

- **Loaded vs. empty**  
  The baseline km/L is treated as **loaded**. Empty returns are modeled by applying the preset’s `empty_efficiency_gain` to km/L.

- **Backhaul share**  
  Instead of modeling an explicit return itinerary, the function accepts `empty_backhaul_share ∈ [0,1]` and blends loaded/empty consumption accordingly for the given distance.

### 1.3 Road emissions & cost (`emissions.py`)

- **Diesel density**: `DIESEL_DENSITY_KG_PER_L = 0.84`.
- **CO₂ factor (TTW)**: `EF_DIESEL_CO2_KG_PER_L = 2.68` kg CO₂ per liter.
- **CO₂e**: road currently treats **CO₂ as CO₂e** (CH₄ and N₂O are not added in this function).
- **Cost**: `liters × diesel_price_brl_per_l` (price is supplied at runtime via CLI/config and applied to all road legs in the evaluation).

---

## 2) Maritime model (`modules/cabotage/`)

Maritime impact is split into **At Sea** (main engines) and **At Port** (hotel + handling).

### 2.1 At-sea (propulsion)

- **Linear model**:  
  `Fuel_kg = κ_sea × distance_km × cargo_t`
- **Intensity factor**: default `κ_sea = 0.0027 kg/(t·km)` (constant used by the evaluator).  
  This factor covers **propulsion only** (main engines), not port hotel load.
- **Emissions (TTW)** use marine gas oil (MGO) constants below (see §2.3).

### 2.2 At port (stopped)

Two components are accounted for:

**A) Hotel load (ship-side, time-based)**  

- Modeled as a **per-port factor** expressed in **kg of fuel per tonne of cargo** (`kg/t`).  
- Factors per port are produced offline by `calcs/hotel.py`, which:
  1) derives **average time at berth** from ANTAQ’s port-call records (Cabotagem only),  
  2) applies an engineering benchmark for auxiliary demand (`~600 kW`) and SFOC (`~225 g/kWh`),  
  3) converts this to a per-tonne allocation for that port.  
- The app loads these factors from `_data/hotel.json` and applies:
  `Fuel_hotel_kg = cargo_t × (k_port_origin + k_port_destiny)`.

**B) Port handling dwell & cost (land-side)**  

- The cabotage leg **adds fixed handling dwell** to the schedule and a **fixed handling cost** to monetary totals:  
  - Hours: `+ 2 × PORT_HANDLING_HOURS` (load + discharge).  
  - Cost:  `+ 2 × PORT_HANDLING_COST`.  
- No extra fuel is added here beyond the **hotel** term above.

### 2.3 Maritime emissions & cost

- **Fuel basis**: MGO (marine gas oil), using fuel **mass** (kg) directly.
- **CO₂ factors (TTW) used in code**:  
  `EF_TTW_MGO_KG_PER_T = {"CO2": 3206.0, "CH4": 0.0, "N2O": 0.0}` → **3.206 tCO₂ per tonne of fuel**.  
  CH₄ and N₂O are set to zero in the current TTW calculation, so **CO₂e = CO₂** for maritime legs.
- **Cost**: marine fuel cost is computed from mass using a constant default price:  
  `MGO_PRICE_BRL_PER_T = 3200.0`, applied to both **at-sea** and **at-port** fuel.  
  The **fixed port handling cost** above is **added on top** of fuel cost.

---

## 3) Routing, distances & addressing

### 3.1 Road routing (`modules/road/ors_client.py`)

- Routes are requested from **OpenRouteService (ORS)** using **`driving-hgv`**.
- If directions return **404**, the client performs a **SNAP-to-road** and retries the same profile; the SNAP step may internally use `driving-car` only for snapping if the chosen profile isn’t supported for SNAP. The **directions profile remains `driving-hgv`** in this flow.

### 3.2 Address resolution (`modules/addressing/resolver.py`)

- Inputs can be free-text addresses, **CEP**, `"lat,lon"` strings, or `"City, State"`.
- Geocoding uses ORS (biased to **BR**, `size=1`), returning `{lat, lon, label}` for downstream routing.

### 3.3 Sea distances (`_data/sea_matrix.json`)

- Port-to-port distances come from a **precomputed matrix** (looked up by normalized port label).  
- Matrix entries were generated with **great-circle (Haversine)** distances between port centroids; when needed, a **coastline factor** is applied in generation and/or fallback (`coastline_factor = 1.18` in the JSON metadata).  
- If a pair is missing from the matrix at runtime, the router **falls back** to Haversine × coastline factor.

### 3.4 Nearest ports (gate-aware) & label normalization

- The “nearest port” to O and D is selected **gate-aware**: the search uses each port’s **truck gate** coordinates (if present) to measure proximity and to pull drayage routes from ORS.
- Port labels are normalized and **aliased** so that project labels match matrix labels (e.g., *Pecém* ↔ *São Gonçalo do Amarante*, *Suape* ↔ *Ipojuca*, *Vitória/TVV* ↔ *Vila Velha*, etc.).

---

## 4) Output structure highlights

The evaluation output (single-route run) includes:

- `input`: cargo, truck key, **`diesel_brl_l`**, `empty_backhaul_share`, profile flags, and κ/price constants used for maritime legs.
- `selection`: picked ports (with lat/lon) and the routing profiles actually used (`road_direct`, `o_to_po`, `pd_to_d`).
- `road_only` and `cabotage` (with per-leg and totals for **distance_km**, **fuel_kg**, **co2e_kg**, **cost_brl**).
- `deltas_cabotage_minus_road` for **fuel**, **CO₂e**, and **cost**.
- Sea leg provenance in `legs[i].extras.distance_source ∈ {"matrix","haversine"}`.

---

## 5) Model boundaries and simplifications

- **TTW only** (tank-to-wake) for both road and maritime; **CO₂e ≡ CO₂** in current code paths.
- **Sea matrix** is static; routing at sea is not simulated.
- **Hotel factors** are applied as per-tonne port constants from preprocessing.
- **Port handling** adds **fixed dwell hours** and a **fixed monetary cost**; no extra fuel beyond hotel load.
- **Diesel price** is supplied at runtime (CLI/config) and applied uniformly to road legs in a run.
- **No speed/weather effects, no transshipment modeling**, and no volumetric constraints (mass-only).
