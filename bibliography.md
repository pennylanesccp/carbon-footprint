# Bibliography

## 1. User-Provided Data Files

* **`ports_br.json`**
  * **What it is:** Your JSON file listing 28 Brazilian ports, their coordinates, and known aliases (e.g., "Santos", "Porto de Santos", "BRSSZ").
  * **What it was used for:** This was the **master key** for the analysis. We used it to create an "alias map" to filter the raw ANTAQ data. This allowed us to group all vessel calls (which use different naming conventions) under the correct, single port name (e.g., "Santos").

* **`2025Atracacao.txt`**
  * **What it is:** Your raw data file of port call records from ANTAQ for 2025.
  * **What it was used for:** This was the **primary data source** for your port fuel model. We wrote code to parse this file and, for each port, calculate the **`T_avg_berth` (Average Time at Berth)**. We did this by filtering for `Tipo de Navegação da Atracação == 'Cabotagem'` and then calculating the average duration between the `Data Atracação` and `Data Desatracação` timestamps.

## 2. Public Data & Benchmark Reports

* **ANTAQ (Agência Nacional de Transportes Aquaviários) - "Estatístico Aquaviário"**
  * **What it is:** The official public database for all Brazilian waterway statistics (which is the source of your `.txt` file).
  * **What it was used for:** We used its "Metodologia de Cálculo dos Indicadores" (Indicator Calculation Methodology) manual to confirm the exact definitions of the timestamp columns, validating our approach for calculating `T_avg_berth`. We also discussed using its cargo flow data (port-to-port tonnage) to build the `K_sea` denominator.

* **International Council on Clean Transportation (ICCT) Report**
  * **What it is:** Specifically, the July 2022 report: "Brazilian coastal shipping: New prospects for growth with decarbonization".
  * **What it was used for:** This report provided the **core problem statement** for your thesis. It gave us the key insight that the "BR do Mar" program is projected to *increase* cabotage-sector emissions by 28%, proving that a detailed emissions model (like yours) is necessary.

* **International Maritime Organization (IMO) & Sustainable Ships**
  * **What it is:** Engineering and regulatory benchmark sources for maritime operations.
  * **What it was used for:** These sources provided the two crucial engineering constants for your "hotel load" model.
        1.  **~600 kW:** The average auxiliary engine power demand for a container ship at berth.
        2.  **~225 g/kWh:** The Specific Fuel Oil Consumption (SFOC) for those auxiliary engines.
  * **Result:** We multiplied these to create your constant **`R_hotel` = 135 kg/hour**.

## 3. Key Academic Papers & Methodologies

* **"Emissions allocation in transportation routes" (Leenders et al., 2017)**
  * **What it is:** The key academic paper that first validated your thesis question.
  * **What it was used for:** It confirmed that allocating emissions on multi-stop, "bus-like" routes is a complex problem and that standard GHG Protocol methods are insufficient. This established the "academic gap" your thesis aims to fill.

* **"Estimating of CO2 emissions in a container port..." (Academic Paper)**
  * **What it is:** A paper detailing fuel consumption of diesel-powered port equipment.
  * **What it was used for:** This was the source for your **`μ_cargo` (land-side) factor**. It provided the specific benchmarks (e.g., 2.77 liters/move for a Quay Crane, 1.32 liters/move for an RTG) that we used to calculate your final estimate of **0.48 kg/tonne** (assuming a ~1km tractor distance).

* **Cooperative Game Theory (Shapley Value)**
  * **What it is:** A concept from economics and mathematics (supported by papers like Arroyo et al., 2024).
  * **What it was used for:** We identified this as the "gold standard" academic method for fairly allocating *shared* costs (like hotel fuel or the fuel on a shared sea leg) among all shipments on the vessel.

* **Naval Engineering Principles (Propeller Cube Law, etc.)**
  * **What it is:** Standard engineering formulas for calculating ship propulsion.
  * **What it was used for:** We discussed this as the "first principles" method to build your `K_sea` factor from scratch, by modeling a ship's speed (from AIS data) and engine power. You ultimately used a `K` factor from a literature review (in your `k.json`), which is a valid alternative to this bottom-up build.
