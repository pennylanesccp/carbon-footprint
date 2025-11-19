import json
import subprocess
import re
import math
import sqlite3
from pathlib import Path

# 1. Setup Paths
project_root = Path.cwd()
ports_path = project_root / "data" / "processed" / "cabotage_data" / "ports_br.json"
db_path = project_root / "data" / "processed" / "database" / "carbon_footprint.sqlite"

# 2. Load Ports
with open(ports_path, "r", encoding="utf-8") as f:
    ports_data = json.load(f)

print(f"📦 Loaded {len(ports_data)} ports.\n")

# 3. Helper to Clear Cache
def clear_cache_for_origin(db_path, origin_str):
    """Deletes rows where origin matches the input string exactly."""
    try:
        with sqlite3.connect(db_path) as conn:
            # We delete by origin name matching our input string
            # because router.py checks the cache using the raw input string first.
            conn.execute("DELETE FROM routes WHERE origin = ?", (origin_str,))
    except Exception as e:
        print(f"⚠️ DB Warning: {e}")

# 4. The Validation Loop
success_count = 0
failure_count = 0

for port in ports_data:
    port_name = port["name"]
    
    # --- A. Determine target coordinates ---
    if port.get("gates"):
        target_gate = port["gates"][0]
        target_lat = target_gate["lat"]
        target_lon = target_gate["lon"]
        coord_source = f"Gate: {target_gate.get('label', 'unnamed')}"
    else:
        target_lat = port["lat"]
        target_lon = port["lon"]
        coord_source = "Centroid"

    # Input format: "lat, lon"
    origin_input = f"{target_lat}, {target_lon}"
    destiny_input = "Campinas, SP" 

    # --- B. Force Cache Clear (The Fix) ---
    # We assume the router saved it previously with the label "lat, lon" or the resolved label.
    # To be safe, we rely on the router's first step: checking raw input. 
    # But the router might have saved a resolved label (e.g. "-23.960800, -46.333600").
    # Let's try to clear the exact input string.
    clear_cache_for_origin(db_path, origin_input)

    # --- C. Run Router (No --overwrite flag) ---
    cmd = [
        "python", "-m", "modules.road.router",
        "--origin", origin_input,
        "--destiny", destiny_input,
        "--log-level", "INFO", 
        "--pretty"
    ]
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=project_root
    )
    
    log_content = result.stderr

    # --- D. Verify Output ---
    # 1. Check if we hit cache despite our delete attempt
    if "Cache hit" in log_content:
        # This happens if the router saved the route under a slightly different 
        # label (e.g. spaces or float formatting) than our input string.
        # We'll treat this as a warning but try to proceed if possible.
        status = "⚠️ CACHED"
    else:
        status = "❌ FAIL"

    # 2. Look for the API call coordinates
    # Log line format: "ROUTE ... coords=[[lon, lat], ...]"
    match = re.search(r"coords=\[\[(.*?), (.*?)],", log_content)
    used_lat, used_lon = 0.0, 0.0
    
    if match:
        try:
            used_lon = float(match.group(1))
            used_lat = float(match.group(2))
            
            if math.isclose(used_lat, target_lat, rel_tol=1e-4) and \
               math.isclose(used_lon, target_lon, rel_tol=1e-4):
                status = "✅ PASS"
                success_count += 1
            else:
                failure_count += 1
                status = "❌ WRONG COORDS"
        except ValueError:
            failure_count += 1
    elif status != "⚠️ CACHED":
        # Only count as fail if not cached
        if result.returncode != 0:
            status = "💥 CRASH"
        failure_count += 1

    # --- E. Report ---
    print(f"{status:<12} | {port_name:<30} | {coord_source:<20} | In: {target_lat:.4f}, {target_lon:.4f} -> API: {used_lat:.4f}, {used_lon:.4f}")
    
    if status == "💥 CRASH":
        print(f"    └─ STDERR: {log_content[-200:].replace(chr(10), ' ')}...")

print(f"\n🏁 Summary: {success_count} Passed, {failure_count} Failed.")