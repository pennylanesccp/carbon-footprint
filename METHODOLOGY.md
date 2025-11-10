# `METHODOLOGY.md`

This document details the methodology, core assumptions, and engineering simplifications used in the carbon footprint calculator. The model is an **activity-based** system that breaks a journey into its constituent legs (road, sea, port) and applies specific factors to each.

## 1. Road Model (`modules/road/`)

The road model is based on the axle-based fuel economy standards from ANTT (Agência Nacional de Transportes Terrestres).

### 1.1. Fuel Consumption (`fuel_model.py`)

* **Baseline Efficiency:** Fuel efficiency (`km/L`) is determined by the truck's axle count, based on the `_ANTT_KM_PER_L_BASELINE` dictionary (e.g., 5 axles = 2.3 km/L). This is a planning value, not a specific vehicle's telemetry.
* **Weight Adjustment:** The baseline efficiency is adjusted using a linear elasticity model (`adjust_km_per_liter`). It is assumed that a truck's fuel economy decreases linearly as its cargo weight increases relative to a `ref_weight_t`.
* **Backhaul Simplification:** The model does not simulate a full return journey. Instead, it assumes a `empty_backhaul_share` (e.g., 1.0 = 100%) and calculates the fuel for the empty return trip using a higher `kmL_empty` (based on an `empty_efficiency_gain` factor).
* **Trips:** The number of truck trips is simplified to `ceil(cargo_t / payload_t)`. It assumes cargo can be perfectly split and does not model volume constraints, only weight.

### 1.2. Fuel Pricing (`calcs/diesel_price_updater.py`)

* **Data Source:** Diesel prices are dynamic and sourced from the official ANP (Agência Nacional do Petróleo) weekly price survey (`semanal-estados-desde-2013.xlsx`).
* **Data Pipeline:** The `calcs/diesel_price_updater.py` script is a "data pipeline" script, run manually/offline, to pre-process the large ANP Excel file into a simple `latest_diesel_prices.csv`.
* **Price Selection:** The "app pipeline" (`evaluator.py`) loads this small CSV.
* **Geographic Simplification:** It is assumed that the diesel price for *all* road legs in a single evaluation (e.g., O->Po and Pd->D) is the price from the **origin state (UF)** (e.g., "SP").

### 1.3. Emissions (`emissions.py`)

* **Static Factors:** The model uses static, industry-standard conversion factors:
  * **Diesel Density:** `DIESEL_DENSITY_KG_PER_L` is fixed at **0.84 kg/L**.
  * **CO₂ Factor:** `EF_DIESEL_CO2_KG_PER_L` (Tank-to-Wake) is fixed at **2.68 kg CO₂ per liter** of diesel.
* **CO₂e Simplification:** The model currently assumes `CO₂e ≈ CO₂`. CH₄ and N₂O emissions, while included in the `Emissions` dataclass, are not calculated in the `estimate_road_trip` function.

---

## 2. Maritime Model (`modules/cabotage/`)

This is the core of the thesis, breaking the maritime journey into two distinct components: "At Sea" and "At Port".

### 2.1. At-Sea Legs (`κ_sea`)

This factor represents the fuel burned by the ship's **main engines** for propulsion.

* **Model:** The at-sea fuel consumption is a linear model:
    `Fuel_kg = κ * distance_km * cargo_t`
* **Intensity Factor (`κ_sea`):** The factor `κ` (Kappa) is **not** for a specific vessel. It is a **proxy value** taken from `_data/k.json`, which represents a **trimmed mean from a literature review** of Brazilian cabotage studies.
* **Model Boundary:** This `κ` factor *only* accounts for fuel used for propulsion (main engines) and explicitly *excludes* fuel for auxiliary engines at port ("hotel load").

### 2.2. Port Operations (The "Bus Stop" Problem)

This is the fuel burned while the ship is stopped. We model this as two separate components: ship-side (time-based) and land-side (work-based).

#### A. Ship-Side "Hotel Load" (`τ_hotel`)

This represents the fuel burned by the ship's **auxiliary engines** (generators) to power lights, computers, crew facilities, and refrigerated containers while at the dock.

* **Model:** This is a **time-dependent** cost: `Fuel_kg = R_hotel × T_berth`
* **Consumption Rate (`R_hotel`):** This is a constant engineering benchmark set at **135 kg of fuel per hour**.
  * **Proxy Model:** This rate is itself a model, derived from international benchmarks:
    * `~600 kW` (average auxiliary power demand for a container ship at berth)
    * `~225 g/kWh` (Specific Fuel Oil Consumption for auxiliary engines)
* **Time at Berth (`T_berth`):** This is the key variable. It is **calculated empirically** for each port using the `calcs/hotel.py` script, which parses the `_data/2025Atracacao.txt` file from ANTAQ.
  * The script calculates the average duration between `Data Atracação` and `Data Desatracação` for all "Cabotagem" vessels at that port.
  * The final per-port *total fuel* values (not the rate) are stored in `_data/hotel.json`.
* **Allocation:** This is treated as a **fixed cost** for the port stop. In `accounting.py`, this total fuel cost is allocated *pro-rata* (by weight) to **all shipments on board the vessel** at that time, regardless of whether they are being loaded or discharged.

#### B. Land-Side Cargo Handling (`μ_cargo`)

This represents the fuel burned by the **port's own equipment** (cranes, tractors) to move containers.

* **Model:** This is a **work-dependent** (variable) cost: `Fuel_kg = μ_cargo × tonnes_handled`
* **Handling Factor (`μ_cargo`):** This is a constant benchmark set at **0.48 kg of fuel per tonne** of cargo handled.
* **Proxy Model:** This factor is *also* a model, based on international benchmarks for *diesel-powered* equipment:
  * `~0.18 kg/t` (for the Quay Crane lift)
  * `~0.085 kg/t` (for the RTG/Gantry Crane lift)
  * `~0.21 kg/t` (for the Terminal Tractor move)
* **Simplification:** The tractor component (`0.21 kg/t`) implicitly assumes an average travel distance `d` of **~1.0 km** from the quay to the container stack. This is a major assumption.
* **Allocation:** This is treated as a **variable cost**. In `accounting.py`, it is applied *only* to the specific shipments being **loaded or discharged** at that port.

---

## 3. Routing & Distances

* **Road Distances (`modules/road/ors_client.py`):**
  * All road routing (O->D, O->Po, Pd->D) is performed by **OpenRouteService (ORS)**.
  * It specifically uses the `driving-hgv` (Heavy Goods Vehicle) profile, which is assumed to be the most realistic proxy for a heavy truck.
* **Sea Distances (`modules/cabotage/_data/sea_matrix.json`):**
  * Sea distances are **not** from a live routing engine.
  * They are pre-calculated using the **Haversine (great-circle)** formula between port coordinates.
  * A **+15% circuity factor** is added to the straight-line distance to approximate real-world navigation channels and coastal detours. This is a significant simplification.
* **Nearest Port (`modules/cabotage/ports_nearest.py`):**
  * The *initial* search for the "nearest port" to an address is done using simple Haversine (as-the-crow-flies) distance.
  * The `cabotage/router.py` then *validates* this by requesting the *actual road route* from ORS to get the true drayage distance.
