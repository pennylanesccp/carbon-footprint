# Carbon Footprint — **Single Route Evaluator (Road × Cabotage)**

This repo evaluates **one origin–destination** movement and returns a structured JSON comparing:

- **Road only** (direct O→D), and  
- **Cabotage** (O→Origin Port **[road]** → **sea** → Destination Port→D **[road]**) + **port ops & hotelling**.

The same core function can be looped by your heatmap script.

---

## 📦 Repo pieces that matter

```

modules/
addressing/
resolver.py             # flexible inputs: address, CEP, "lat,lon", city/state
app/
evaluator.py            # ← orchestrates everything (_evaluate)
cabotage/
ports_index.py          # load ports list from JSON
ports_nearest.py        # nearest-port lookup (gate-aware)
sea_matrix.py           # port↔port sea distances (km)
accounting.py           # emissions from fuel, hotel factor index, etc.
road/
ors_common.py           # config + session setup for ORS
ors_client.py           # low-level HTTP + request/response handling
ors_mixins.py           # geocoding + directions helpers
emissions.py            # road fuel/emissions/cost estimator (per trip)
scripts/
single_evaluation.py      # CLI wrapper for one (origin, destiny) evaluation

````

---

## 🔧 How it works (step-by-step)

For a single `origin` and `destiny`:

1. **Resolve points**  
   `modules.addressing.resolver.resolve_point()` accepts free text, CEP, city/state, or `"lat,lon"`.  
   Under the hood it calls **OpenRouteService (ORS)** geocoding.

2. **Road-only (direct O→D)**  
   - Calls ORS **directions** (profile defaults to `driving-hgv`, with optional **fallback to `driving-car`**).  
   - Converts **distance (km)** to a **per-trip** estimate via `modules.road.emissions.estimate_road_trip()` →  
     fuel (L), fuel mass (kg via diesel density), **CO₂e (kg)**, and **fuel cost (R$)**.

3. **Cabotage**
   - **Pick ports**: finds the **nearest origin port** and **nearest destination port** (`ports_nearest.py`).  
     If a **truck gate** exists in your `ports_br.json`, it uses that gate for the road legs.
   - **Road legs (O→Po, Pd→D)**: same ORS call + same road estimator as the direct route.
   - **Sea leg (Po↔Pd)**: uses `SeaMatrix` (km). Fuel is **proportional**:  
     `fuel_kg = K_sea_kg_per_tkm × cargo_t × sea_km`.
   - **Port ops + hotelling (both ports)**:  
     - Handling: `2 × cargo_t × K_port_kg_per_t` (load + discharge).  
     - Hotel: city-specific factor (kg/t·call) loaded from `hotel.json` (origin + destination).
   - **Totals**: sums all **fuel_kg / CO₂e_kg / cost_brl** for cabotage.

4. **Deltas**  
   Returns `cabotage − road_only` for fuel, CO₂e, and cost.

---

## 📥 Inputs & Defaults

- **Env:** `ORS_API_KEY` must be set.
- **CLI (PowerShell example):**
  ```powershell
  # from repo root (venv active)
  $env:ORS_API_KEY = '<your-ors-key>'

  python .\scripts\single_evaluation.py `
    --origin  "avenida luciano gualberto, 380" `
    --destiny "Curitiba, PR" `
    --amount-tons 26 `
    --pretty

**Defaults (tune in CLI or code if needed):**

| Parameter             | Default                                                     | Where                        |
| --------------------- | ----------------------------------------------------------- | ---------------------------- |
| Truck preset          | `semi_27t`                                                  | `road.emissions.TRUCK_SPECS` |
| ORS profile           | `driving-hgv`                                               | CLI `--ors-profile`          |
| Fallback to car       | `True`                                                      | CLI `--fallback-to-car`      |
| Diesel price [BRL/L]  | `6.0`                                                       | CLI                          |
| Diesel density [kg/L] | `0.84`                                                      | `evaluator.py`               |
| Sea K [kg/t·km]       | `0.0027`                                                    | CLI `--sea-K`                |
| MGO price [BRL/t]     | `3200`                                                      | CLI `--mgo-price`            |
| Port ops K [kg/t]     | `0.48` (per call; handled as load+discharge = 2× cargo × K) | `evaluator.py`               |
| Hotel factors         | from `hotel.json` (per city)                                | `cabotage.accounting`        |

**Default data paths:**
`modules/cabotage/_data/ports_br.json`, `modules/cabotage/_data/sea_matrix.json`, `modules/cabotage/_data/hotel.json`.

---

## 📤 Output structure (JSON)

```json
{
  "input": { "...keys..." },
  "selection": {
    "port_origin": { "..."},
    "port_destiny": { "..."},
    "profiles_used": {
      "road_direct": "...",
      "o_to_po": "...",
      "pd_to_d": "..."
    }
  },
  "road_only": {
    "distance_km": 0.0,
    "fuel_kg": 0.0,
    "co2e_kg": 0.0,
    "cost_brl": 0.0
  },
  "cabotage": {
    "o_to_po": { "distance_km": 0.0, "fuel_kg": 0.0, "co2e_kg": 0.0, "cost_brl": 0.0 },
    "sea":      { "sea_km": 0.0, "fuel_kg": 0.0, "co2e_kg": 0.0, "cost_brl": 0.0 },
    "ops_hotel":{ "fuel_kg": 0.0, "co2e_kg": 0.0, "cost_brl": 0.0, "breakdown": { "...": 0.0 } },
    "pd_to_d":  { "distance_km": 0.0, "fuel_kg": 0.0, "co2e_kg": 0.0, "cost_brl": 0.0 },
    "totals":   { "fuel_kg": 0.0, "co2e_kg": 0.0, "cost_brl": 0.0 }
  },
  "deltas_cabotage_minus_road": {
    "fuel_kg": 0.0,
    "co2e_kg": 0.0,
    "cost_brl": 0.0
  }
}
```

This is the object that your heatmap builder will **loop** over multiple destinations.

---

## 🧠 Method notes & key concepts

* **Profiles & fallback.** Some corridors or profiles can fail (e.g., restrictions with `driving-hgv`). The evaluator retries with `driving-car` if `--fallback-to-car` is enabled.
* **Per-trip road model.** `estimate_road_trip()` is the single-trip calculator (fuel/emissions/cost). Total results scale with **number of trips** when your cargo exceeds the truck payload (your truck preset controls payload).
* **Sea intensity `K`.** Currently a constant (kg/t·km). Replace later with a vessel/TEU-based model if desired.
* **Port ops.** We apply **load + discharge** for the shipment, plus **hotel at berth** at **both ports**, city-specific.
* **CO₂e.** Uses your tailpipe factors via `accounting.emissions_ttw()` (CO₂ plus CH₄/N₂O × GWP100).
* **Sensitivity.** Most impactful knobs: **road efficiency**, **diesel price**, **ops/hotel factors**. Sea K is smaller weight on short sea legs.

---

## ✅ Quick validation recipe

* Run one lane with `--pretty` and check:

  * Distances vs reality (SP↔Curitiba ~405 km; SP→Santos ~90 km; Paranaguá→Curitiba ~95 km; Santos↔Paranaguá ~330 km).
  * Relative shares in cabotage totals: road legs often dominate on short voyages; ops/hotel can be material; sea share grows for long legs.
* For a quick sensitivity pass, add a temporary CLI option (e.g., `--road-kml-override`) and sweep 1.5→2.5 km/L to see how the delta flips.

---

## 🧪 Example result (26 t, SP→Curitiba)

* **Road-only:** 405.5 km; ~**271 L**; **726.9 kg CO₂e**; **R$ 1,627**
* **Cabotage total:** ~**231 L**; **619.9 kg CO₂e**; **R$ 1,029**
* **Delta (cab − road):** **−15% fuel**, **−15% CO₂e**, **−37% cost**

*Interpretation:* With current assumptions, cabotage is favorable on this lane. Pushing road efficiency up toward 2.3–2.5 km/L narrows or reverses the gap—useful for calibration against external apps.

```
::contentReference[oaicite:0]{index=0}
```
