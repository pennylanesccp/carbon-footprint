# --- Build a port-to-port sea-distance matrix (km)
# Save as calcs/build_distance_matrix.py (or a notebook cell)

import os, sys, json, time, math, itertools as it
from pathlib import Path
import pandas as pd
import requests

# 1) Repo root & ports loader --------------------------------------------------
def _repo_root():
    cand = os.getcwd()
    for _ in range(8):
        if os.path.isdir(os.path.join(cand, "modules")):
            return cand
        cand = os.path.dirname(cand)
    env = os.getenv("PROJECT_ROOT")
    if env and os.path.isdir(os.path.join(env, "modules")):
        return env
    raise SystemExit("❌ Could not locate repo root.")

ROOT = _repo_root()
sys.path.insert(0, ROOT)

from modules.cabotage import load_ports  # you already have this
PORTS_PATH = os.path.join(ROOT, "data", "cabotage_data", "ports_br.json")
ports = load_ports(PORTS_PATH)

# Keep a stable list of (label, lat, lon)
port_list = [
      (p["name"], float(p["lat"]), float(p["lon"]))
    for p in ports
]

# 2) Utilities ----------------------------------------------------------------
def haversine_km(a_lat, a_lon, b_lat, b_lon) -> float:
    R = 6371.0088
    ar1, br1 = math.radians(a_lat), math.radians(a_lon)
    ar2, br2 = math.radians(b_lat), math.radians(b_lon)
    da, db = ar2 - ar1, br2 - br1
    s = (math.sin(da/2)**2 + math.cos(ar1) * math.cos(ar2) * math.sin(db/2)**2)
    return 2 * R * math.atan2(math.sqrt(s), math.sqrt(1 - s))

def nm_to_km(nm: float) -> float:
    return nm * 1.852

# 3) Plug your API here --------------------------------------------------------
# Choose ONE provider and implement the call inside fetch_distance_nm().
# Fallback is Haversine×coastline_factor (≈1.15–1.25) to approximate sailing lanes.

COASTLINE_FACTOR = 1.18   # tweak if you later compare against API values

# EXAMPLE (VesselFinder) — requires API key:
# DOCS: https://api.vesselfinder.com/docs/distance.html
VF_KEY = os.getenv("VF_API_KEY", "")   # set in your environment if you have it

def fetch_distance_nm(a_name, a_lat, a_lon, b_name, b_lat, b_lon) -> float | None:
    # If you have a key, uncomment to use VesselFinder's shortest sea route:
    # if VF_KEY:
    #     url = "https://api.vesselfinder.com/distance"
    #     params = {
    #           "userkey": VF_KEY
    #         , "from": f"{a_lon},{a_lat}"
    #         , "to":   f"{b_lon},{b_lat}"
    #     }
    #     r = requests.get(url, params=params, timeout=30)
    #     r.raise_for_status()
    #     data = r.json()
    #     return float(data["distance_nm"])

    # EXAMPLE (SeaRates) — see: https://www.searates.com/services/distances-time
    # (their API/endpoint details depend on account plan; adapt similarly)

    # Fallback: Haversine × coastline factor:
    hv_km = haversine_km(a_lat, a_lon, b_lat, b_lon)
    return hv_km / 1.852 * COASTLINE_FACTOR  # return NM

# 4) Build the matrix ----------------------------------------------------------
names = [n for (n, _, _) in port_list]
N = len(names)
matrix_km = pd.DataFrame(0.0, index=names, columns=names)

# cache so symmetric queries only run once
cache_nm: dict[tuple[str, str], float] = {}

for (i, (na, la, loa)) in enumerate(port_list):
    for (j, (nb, lb, lob)) in enumerate(port_list):
        if i == j:
            continue
        key = tuple(sorted((na, nb)))
        if key in cache_nm:
            nm = cache_nm[key]
        else:
            nm = fetch_distance_nm(na, la, loa, nb, lb, lob) or 0.0
            cache_nm[key] = nm
            # be kind to APIs:
            time.sleep(0.2)
        matrix_km.at[na, nb] = nm_to_km(nm)

# 5) Persist outputs -----------------------------------------------------------
outdir = Path(ROOT) / "data" / "cabotage_data"
outdir.mkdir(parents=True, exist_ok=True)

csv_path  = outdir / "sea_matrix.csv"
json_path = outdir / "sea_matrix.json"

# matrix_km.to_csv(csv_path, float_format="%.3f", encoding="utf-8")
with open(json_path, "w", encoding="utf-8") as f:
    json.dump({
          "unit": "km"
        , "method": "API_or_Haversine"
        , "coastline_factor": COASTLINE_FACTOR
        , "note": "Off-diagonal entries are sea distances (km) between port centroids; symmetric."
        , "matrix": {
              r: {c: float(matrix_km.at[r, c]) for c in names}
              for r in names
          }
    }, f, ensure_ascii=False, indent=2)

# print(f"✓ Saved {csv_path}")
print(f"✓ Saved {json_path}")
