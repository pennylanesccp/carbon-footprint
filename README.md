# Carbon Footprint: Cabotage vs. Road (MVP)

This project is the **MVP** of the graduation thesis *“Carbon Footprint: Cabotage vs. Road”*.
The goal is to estimate **fuel consumption**, **cost** (optional), and **CO₂ emissions** for container transport between **São Paulo (fixed origin)** and a set of **Brazilian capitals**, comparing:

* **Pure road** (São Paulo → destination), and
* **Cabotage**: road (São Paulo → Port of Santos) + **maritime** (Santos → destination port) + road (destination port → city) + **port operations**.

The system also renders **heatmaps** showing where cabotage is more advantageous than road (by **cost** and **CO₂**).

---

## ✨ Destinations in the MVP

* Rio de Janeiro (RJ)
* Brasília (DF)
* Fortaleza (CE)
* Salvador (BA)
* Belo Horizonte (MG)
* Manaus (AM)
* Curitiba (PR)
* Recife (PE)
* Goiânia (GO)
* Porto Alegre (RS)

---

## ⚙️ MVP Structure

1. **Input (configuration)**

* Origin: **São Paulo, SP**
* Ports: **Santos** (origin port) + **nearest** destination port (simple rule for MVP)
* Distances: simplified tables (**road + maritime**; APIs can come later)
* Parameters:

  * Truck fuel efficiency (empty/loaded baseline),
  * Ship fuel per container-km (or per leg),
  * Port operations fuel per move,
  * Fuel prices,
  * Emission factors (e.g., diesel CO₂ factor).

2. **Per-destination calculations**

* **Road-only**: São Paulo → destination → km → liters → (cost) + CO₂
* **Cabotage**:

  * SP → Port of Santos (road),
  * Santos → destination port (maritime),
  * Destination port → city (road),
  * Port operations (origin + destination),
  * Sum → liters → (cost) + CO₂
* **Comparison**: deltas of **cost** and **CO₂** (cabotage − road)

3. **Outputs**

* `results.csv` → one row per destination (all metrics)
* `heatmap_cost.html` → interactive map (cab. vs. road by cost)
* `heatmap_co2.html` → interactive map (cab. vs. road by CO₂)

---

## 🛠️ Tech Stack

* **Python 3.12+**
* **Pandas** (data wrangling)
* **Folium** (interactive maps)
* **GeoPandas** (optional, for larger meshes/shapefiles later)
* **JSON** config (distances and parameters)

---

## 📐 Fuel Methodology (Road)

We use **ANTT**’s official baseline **km/L** by **number of axles** for *containerized cargo* (loaded), from **Portaria SUROC/ANTT/MI nº 17/2020**.
Because real efficiency depends on payload, we apply a simple **linear correction** around a **reference weight**.

### 1) Baseline km/L (ANTT, containerized cargo)

| Axles | km/L |
| ----: | :--: |
|     2 |  4.0 |
|     3 |  3.0 |
|     4 |  2.7 |
|     5 |  2.3 |
|     6 |  2.0 |
|     7 |  2.0 |
|   ≥ 9 |  1.7 |

> We generalize to **1.7 km/L for 9 or more axles**.

**Typical mapping (Brazil):**

* 3 axles → “toco / truck”
* 5 axles → “carreta LS”
* 7 or 9+ axles → “bitrens / rodotrens”

### 2) Weight adjustment (linear around a reference)

Let:

* `km_l_baseline`: ANTT baseline for your axle class (loaded),
* `cargo_weight` (*t*): actual payload,
* `ref_weight` (*t*): “typical loaded” payload (e.g., **20 t** for a 40’ container, **10 t** for 20’),
* `elasticity` (default **1.0**): 10% weight change ⇒ 10% efficiency change.

Formula:
[
\text{kmL}(p) = \text{kmL}*{baseline} \times \left(1 - \text{elasticity} \cdot \frac{p - p*{ref}}{p_{ref}}\right)
]

* Lighter than `ref_weight` ⇒ **higher km/L**
* Heavier than `ref_weight` ⇒ **lower km/L**

> Rationale: empirical sources (CETESB, CNT, PNLT, ICCT) consistently show ~15–25% difference between empty vs. loaded operations, compatible with a first-order linear sensitivity for planning/MVP use.

---

## 📦 Port & Maritime (MVP)

* **Port operations:** use a fixed **liters per container per move** and the number of moves (gate-in/out, lifts) at origin/destination ports.
* **Maritime:** for MVP, use **distance (port-to-port)** × **liters per container-km** (or a per-leg average for your target ship size).

  * Later you can refine with service schedules, vessel classes, or specific line factors.

---

## 🔢 From km/L to liters and CO₂

* **Liters (road)** = `distance_km / kmL`
* **CO₂ (kg)** = `liters × 2.68` *(commonly used IPCC factor for diesel; replace if your inventory uses another factor)*

For cabotage totals, sum:

* Road (SP→Santos) + Maritime (Santos→dest_port) + Road (dest_port→city) + Port ops

---

## 🧩 Helper Functions (ready to paste)

### Baseline by axles (ANTT)

```python
def get_km_l_baseline(axles: int) -> float:
    """
    Returns the baseline fuel efficiency (km/L) from ANTT Portaria 17/2020
    for containerized cargo, based on the number of axles.

    Parameters
    ----------
    axles : int
        Number of axles in the truck composition.

    Returns
    -------
    float
        Baseline fuel efficiency (km/L).
    """

    antt_km_l = {
        2: 4.0,
        3: 3.0,
        4: 2.7,
        5: 2.3,
        6: 2.0,
        7: 2.0,
        # 9+ handled below
    }

    if axles >= 9:
        return 1.7
    if axles in antt_km_l:
        return antt_km_l[axles]
    raise KeyError(f"No ANTT baseline for {axles} axles.")
```

### Weight-adjusted km/L

```python
def adjust_km_per_liter(km_l_baseline: float
    , cargo_weight: float
    , ref_weight: float
    , elasticity: float = 1.0) -> float:
    """
    Adjusts truck fuel efficiency (km/L) as a function of cargo weight,
    using ANTT baseline values (average efficiency for a loaded container).

    Parameters
    ----------
    km_l_baseline : float
        Baseline efficiency (km/L) from ANTT for a loaded container.
    cargo_weight : float
        Actual cargo weight in metric tons (t).
    ref_weight : float
        Reference weight representing the baseline (e.g., 20 t for 40' container).
    elasticity : float
        Sensitivity factor. 1.0 = linear (10% more weight ⇒ 10% less efficiency).

    Returns
    -------
    float
        Adjusted efficiency (km/L).
    """
    delta = (cargo_weight - ref_weight) / ref_weight
    return km_l_baseline * (1 - elasticity * delta)
```

### End-to-end example (Santos leg included)

```python
# 1) Baseline by axles
kmL0 = get_km_l_baseline(axles=5)  # 2.3 km/L (e.g., carreta LS)

# 2) Adjust by actual weight (assume 40' ref_weight = 20 t)
kmL_sp_santos = adjust_km_per_liter(kmL0, cargo_weight=18, ref_weight=20)

# 3) Liters for São Paulo → Port of Santos (road)
distance_sp_santos_km = 82.0  # SAI corridor order of magnitude
liters_sp_santos = distance_sp_santos_km / kmL_sp_santos

# 4) Maritime + destination road + port ops (MVP parameters)
liters_maritime =  # distance_nm_to_km * liters_per_container_km  (fill your value)
liters_dest_road = # distance_km * (1 / kmL_at_destination)      (compute similarly)
liters_ports =     # moves * liters_per_move

# 5) Totals
liters_cabotage = liters_sp_santos + liters_maritime + liters_dest_road + liters_ports
liters_road_only = # distance_sp_to_city_km / kmL_for_that_leg

EF_DIESEL = 2.68
co2_cabotage_kg = liters_cabotage * EF_DIESEL
co2_road_only_kg = liters_road_only * EF_DIESEL
```

---

## 🚀 Next Steps

* [ ] Implement stepwise calculators (road, maritime, port ops).
* [ ] Build minimal base tables for **road** and **maritime** distances.
* [ ] Generate `results.csv` for the 10 destinations.
* [ ] Render `heatmap_cost.html` and `heatmap_co2.html`.

---

## 📌 MVP Limitations

* **Distances** are approximate (static tables; no routing API yet).
* **Parameters** (truck/ship, port ops, emission factors) are simplified averages.
* Origin is fixed (**São Paulo**), and origin port is fixed (**Santos**).
* ANTT **km/L** are **averages for loaded containerized cargo**; we adjust for weight linearly (first-order approximation).
* Directional effects (e.g., **Santos→São Paulo uphill**) are not yet modeled; you can add multiplicative factors later.

---

## 📚 Sources & Rationale

* **ANTT — Portaria SUROC/ANTT/MI nº 17, de 21/01/2020.**
  Baseline **km/L by number of axles** for **containerized cargo** (used by `get_km_l_baseline`).

* **CETESB — Road Transport Emission Inventories (2013, 2018).**
  Empirical evidence of the gap between empty and loaded operation and sensitivity to payload.

* **CNT — Operational Costs of Road Freight (2019, 2021).**
  Field-based averages for Brazilian truck fuel consumption.

* **PNLT (2018) — National Logistics Plan** & **ICCT (2019)**.
  Technical coefficients and international framing supporting near-linear load vs. consumption relationships for heavy-duty vehicles.

> These references justify using composition-based ANTT baselines plus a **simple linear weight adjustment** for MVP-level estimation and comparative mapping.

---

## 🔧 Extending the Model

* **Directional grade** factors (uphill/downhill multipliers for SAI corridor).
* **Vehicle profiles** (Euro V/VI, aero kits, tire class).
* **Calibrated elasticity** per fleet or corridor (via telemetry or fuel-card data).
* **Confidence bands** (±10–20%) to show uncertainty ranges on the heatmap.

---
