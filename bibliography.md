# Bibliography

## 1. User-Provided Data Files

* **`ports_br.json`**  
  **What it is:** JSON for 28 Brazilian ports with coordinates, aliases, and (optionally) truck gates.  
  **Use in code:** Master index for normalizing names and selecting nearest ports (`ports_index.py`, `ports_nearest.py`). Gate coordinates are used for the road legs.

* **`2025Atracacao.txt` (ANTAQ)**  
  **What it is:** Raw public port-call records for 2025.  
  **Use in code/analysis:** Basis for computing average **time at berth** per port (`T_avg_berth`) and for deriving **hotel factors** per city; filtered to `Tipo de Navegação == Cabotagem`.

## 2. Public Data & Benchmark Reports

* **ANTAQ – Estatístico Aquaviário & Metodologia de Indicadores**  
  Clarifies timestamp semantics and standard indicators. Used to validate `T_avg_berth` and discuss possible port-to-port tonnage for building a denominator to calibrate `K_sea`.

* **ICCT (Jul/2022). “Brazilian coastal shipping: New prospects for growth with decarbonization.”**  
  Frames the policy context (BR do Mar) and highlights potential emissions growth without operational efficiency—motivating a bottom-up estimator for routes and port effects.

* **IMO & engineering references (auxiliary power at berth, SFOC ranges)**  
  Benchmarks for **hotel load**: typical auxiliary demand around **~600 kW** and **SFOC ~225 g/kWh** for medium-speed auxiliaries. Combined, they motivate a rule-of-thumb **fuel ~135 kg/h** at berth, later adapted into **kg/t·call** factors per city in `hotel.json`.

* **CETESB, CNT, PNLT, ICCT (truck fuel studies)**  
  Empirical ranges for heavy-duty fuel consumption in Brazil; support the choice of ANTT axle-based baselines and linear payload sensitivity used in the road model.

* **OpenRouteService (ORS) documentation**  
  API semantics for **geocoding** and **directions** (profiles like `driving-hgv` and `driving-car`) and the retry/`Retry-After` behavior. Informs our session setup, retries, and the profile fallback strategy.

## 3. Key Academic Papers & Methodologies

* **Leenders et al. (2017). “Emissions allocation in transportation routes.”**  
  Establishes that allocating emissions on multi-stop, shared routes is non-trivial and that naïve per-distance splits can bias results—motivating allocation methods (see next item).

* **Cooperative Game Theory (Shapley Value)** — e.g., Arroyo et al. (2024) applications to transport cost allocation  
  Underpins fair sharing of **common costs** (sea leg fuel, hotel fuel) among multiple shipments. This is referenced as a “gold-standard” path to extend the current constant-K approach.

* **Container port equipment fuel benchmarks (academic/technical papers)**  
  Provide per-move consumption for quay cranes, RTGs, tractors, etc. These inform the **land-side factor** embodied in **`K_port_kg_per_t` ≈ 0.48 kg/t** used in the current MVP.

* **Naval engineering principles (propeller law, power–speed)**  
  First-principles route to derive sea fuel from AIS speed and hull/engine parameters. We note this as an extension path beyond the constant **`K_sea`** factor.

## 4. Brazilian Regulations & Limits (Road)

* **CONTRAN resolutions on weights and dimensions** (e.g., 12/1998, 68/1998, 184/2005, 189/2005, and later updates; plus consolidated tables widely reproduced)  
  Define **axle-group weight limits** and **maximum gross combination mass** by configuration. These norms bound the **payload** per truck composition and thus the **number of trips** for a given cargo mass.

* **ANTT Portaria SUROC/ANTT/MI nº 17/2020 (km/L baselines by axle count for containerized cargo)**  
  Source of **baseline fuel efficiencies** per **axle class** used in the earlier MVP variant and for sensitivity checks (e.g., 5-axle ≈ **2.3 km/L** loaded).

## 5. How the sources map to code

* **ORS docs** → `ors_common.py`, `ors_client.py`, `ors_mixins.py` (timeouts, retries, headers, endpoints, fallback).

* **Ports & ANTAQ** → `ports_index.py`, `ports_nearest.py`, `sea_matrix.py`, `accounting.py` (hotel indices).

* **Truck fuel studies & ANTT baselines** → `road/emissions.py` (per-trip estimator), plus future split into `vehicles.py` and `efficiency.py` for axle/payload logic.

* **IMO/engineering** → constants and structure for the **hotel** and **sea** factors (used in `evaluator.py` through `accounting.emissions_ttw` and `SeaMatrix`).

> **Note:** Exact numeric constants in this MVP are intentionally conservative and easily replaceable. As you incorporate more granular telemetry (truck km/L, port equipment fuel logs, AIS), cite those new sources here and update the corresponding modules and defaults.
