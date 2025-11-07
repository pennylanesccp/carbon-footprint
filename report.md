# (Working Title) — Fixed-Origin Assessment: São Paulo → Port of Santos

## Front Matter

- Title Page
- Acknowledgments (optional)
- Abstract (EN) + Resumo (PT) + Keywords
- Lists of Figures, Tables, Acronyms, and Symbols

---

## 1. Introduction

Brazil’s container logistics remain heavily road-dependent, even on corridors where short-sea shipping (cabotage) can reduce total cost and greenhouse-gas (GHG) emissions. Many comparisons compress port operations into coarse “port time” add-ons, masking a meaningful share of energy use—especially auxiliary-engine fuel during hotelling and the electricity/diesel consumed by quay and yard equipment. Without opening that black box, it is difficult to explain where and why cabotage actually outperforms trucking.

This TF1-stage deliverable of the Graduation Project (*Trabalho de Formatura*) focuses on a practical, auditable case: **container flows originating in São Paulo (SP), trucked to the Port of Santos, shipped by cabotage, and delivered to Brazilian capitals**. The core contribution is a **first-principles port-energy module** that separates (i) vessel at berth (auxiliaries/boiler) and (ii) terminal handling (STS cranes and yard equipment), integrated with established models for the road legs (first/last mile) and the maritime leg (slot/bunker; speed–consumption). Realistic road distances and times are obtained from **OpenRouteService**, and all results are computed on a consistent **functional unit** (per TEU or per 40’ container) as **total logistics cost, WTW GHG emissions, and transit time**.

**Key questions addressed:**

1) **Competitiveness:** Under which conditions (distance, carbon price, bunker/diesel costs, terminal productivity) does cabotage beat direct road in **total cost** for SP-origin flows?  
2) **Carbon:** What is the **marginal contribution** of port operations to end-to-end **WTW CO₂-equivalent** per container, and how sensitive is it to operational/energy parameters (e.g., STS kWh/move, grid factor, auxiliary load at berth)?  
3) **Decision support:** Can **map-based evidence** (ΔCost and ΔCO₂), with leg-level decomposition and a breakeven curve versus distance, provide reproducible and auditable inputs for policy and planning?

**TF1 contributions:**

- Transparent, modular **port-energy model** (hotelling and terminal handling) with per-container allocation.  
- Integrated **road–port–sea–port–road** pipeline in Python with ORS routing and cached, versioned inputs.  
- **Fixed-origin maps** (São Paulo → capitals) for ΔCost (R$/TEU) and ΔCO₂ (kgCO₂/TEU), plus leg-level decomposition and breakeven analysis.  
- Compact **validation and sensitivity** set (e.g., carbon price, bunker price, STS kWh/move) to bound uncertainty.

**Next steps (TF2).** The framework will be **generalized to any origin–destination pair in Brazil**, with time-expanded service schedules and capacity constraints, and broader scenarios (e.g., alternative fuels such as HVO), while maintaining the same reproducible toolchain.

---

## 2. Objectives

### 2.1 General Objective

Develop a transparent, reproducible framework to compare **road-only** versus **road–sea–road (cabotage)** chains for São Paulo–origin container flows, with explicit accounting of **port-operation energy use**, delivering comparable metrics of **total logistics cost**, **WTW GHG emissions**, and **transit time** per container.

### 2.2 Specific Objectives

- Formulate a **port-energy model** separating (i) vessel-at-berth (auxiliary/boiler hotelling) and (ii) terminal handling (STS cranes, yard equipment), with per-container allocation.
- Parameterize the model for **Port of Santos** using literature-derived ranges (SFC, kWh/move, L/h, grid factors) and document assumptions and defaults.
- Implement a **Python pipeline** integrating:
  - Road legs via **OpenRouteService (ORS)** distances/times with local caching,
  - Road cost/CO₂/inventory model,
  - Maritime service model (slot/bunker, speed–consumption),
  - Port-energy module (hotelling + terminal).
- Produce **fixed-origin maps** (São Paulo → capitals) of **ΔCost (R$/TEU)** and **ΔCO₂ (kgCO₂/TEU)**, with leg-level decomposition and a breakeven curve versus distance.
- Establish a **validation plan** (reference OD pairs, error metrics, tolerances) and report consistency checks.
- Run **sensitivity analyses** (1D) on key drivers (carbon price, bunker/diesel price, STS kWh/move, grid factor).
- Ensure **reproducibility** (versioned inputs, cached routes, seeds, requirements) and provide a minimal CLI/notebook to regenerate results.

### 2.3 Deliverables (TF1)

- Methodology and parameter tables (defaults and ranges).
- Python modules and tests; cached ORS artifacts.
- ΔCost and ΔCO₂ maps (PNG/SVG) and breakeven plot.
- Validation summary and sensitivity results.

### 2.4 Outlook to TF2

Generalize the framework to **any origin–destination pair in Brazil**, incorporate **time-expanded service schedules and capacity constraints**, and extend scenario analysis (e.g., **alternative fuels** such as HVO).

---

## 3. Scope and Boundaries

### 3.1 System Boundary

### 3.2 Functional Unit (per TEU / per 40’ container)

---

## 4. Literature Review (concise)

### 4.1 Road Costs and Inventory Cost

### 4.2 Port Operations (hotelling; STS/RTG/yard tractors; shore power)

### 4.3 Maritime Leg (slot/bunker; speed–consumption relationship)

### 4.4 WTW Emissions and Carbon Pricing

---

## 5. Data and Sources

### 5.1 Project XLS and Economic/Operational Parameters

### 5.2 ORS Routes (distance/time)

### 5.3 Emission Factors (diesel, MDO/VLSFO, grid electricity)

### 5.4 Port Charges (THC) and Assumed Productivities

---

## 6. Methodology

### 6.1 Road Model: Cost, CO₂, Inventory

### 6.2 Port — Vessel at Berth (P_AE, load factor, berth time, SFC → fuel)

### 6.3 Port — Terminal (STS kWh/move; RTG/tractors L/h or kWh → cost/CO₂)

### 6.4 Maritime Service (slot, k₁V³+k₂, MDO/VLSFO, EF)

### 6.5 Objective Function & Decision Criteria (cost, CO₂, time; tie handling)

### 6.6 Assumptions & Parameter Table (defaults and ranges)

---

## 7. Implementation (Python)

### 7.1 Code Architecture and ORS Route Caching

### 7.2 Testing and Reproducibility (seeds, versions, requirements)

---

## 8. Scenarios

### 8.1 Base Case

### 8.2 One-Dimensional Sensitivities (e.g., carbon price, bunker price, STS kWh/move)

---

## 9. Validation Plan

### 9.1 Reference OD Pairs

### 9.2 Error Metrics and Tolerances

---

## 10. Results — TF1 (Fixed Origin: São Paulo)

### 10.1 ΔCost Map (R$/TEU)

### 10.2 ΔCO₂ Map (kgCO₂/TEU)

### 10.3 Decomposition by Leg (road→port, port, sea, port→destination)

### 10.4 Breakeven Curve (ΔCost vs. distance)

---

## 11. Sensitivity Analysis (brief)

---

## 12. Discussion

### 12.1 Implications

### 12.2 Limitations and Data/Ethics Notes

---

## 13. Conclusions and Next Steps (bridge to TF2)

---

## References

---

## Appendices

- **A.** Parameter Tables (SFC, kWh/move, L/h, WTW factors)
- **B.** Full Equations
- **C.** Test Cases and Logs (validation)
- **D.** Execution Guide (CLI/conda)
- **E.** Block Diagram of SP → Santos → capitals flow
