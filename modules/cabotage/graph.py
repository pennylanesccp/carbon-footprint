# modules/cabotage/graph.py
# Minimal placeholders for a super-network graph (you can expand later).

from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List

@dataclass(frozen=True)
class Node:
    key: str
    kind: str   # CITY | CT | BT | DEPOT

@dataclass
class Arc:
    u: str
    v: str
    mode: str   # road | sea | barge
    distance_km: float
    hours: float
    cost_brl: float
    co2eq_t: float

@dataclass
class Graph:
    nodes: Dict[str, Node]
    arcs: List[Arc]
