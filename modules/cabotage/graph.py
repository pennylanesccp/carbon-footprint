# modules/cabotage/graph.py
# -*- coding: utf-8 -*-
"""
Super-network graph (minimal, production-friendly)
==================================================

Purpose
-------
Lightweight structures to represent a multi-modal network:
- **Node**: city / container terminal / barge terminal / depot (string `kind`)
- **Arc** : directed edge with mode ('road' | 'sea' | 'barge') and metrics

Backwards compatibility
-----------------------
- Keep dataclasses and field names:
  • Node(key, kind)
  • Arc(u, v, mode, distance_km, hours, cost_brl, co2eq_t)
  • Graph(nodes: Dict[str, Node], arcs: List[Arc])

Enhancements
------------
- Standardized logging.
- Validation helpers and safe constructors.
- Convenience queries: neighbors, arcs_from, arcs_to, stats.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Iterable, Tuple, Optional

from modules.functions._logging import get_logger

_log = get_logger(__name__)

__all__ = ["Node", "Arc", "Graph"]

# Allowed values are not enforced hard (to keep compatibility),
# but we log warnings when they differ.
_ALLOWED_NODE_KINDS = {"CITY", "CT", "BT", "DEPOT"}
_ALLOWED_ARC_MODES = {"road", "sea", "barge"}


# ────────────────────────────────────────────────────────────────────────────────
# Core data
# ────────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Node:
    key: str
    kind: str  # CITY | CT | BT | DEPOT


@dataclass(frozen=True)
class Arc:
    u: str
    v: str
    mode: str  # road | sea | barge
    distance_km: float
    hours: float
    cost_brl: float
    co2eq_t: float  # CO2e in tonnes for this arc (not auto-computed here)


@dataclass
class Graph:
    nodes: Dict[str, Node] = field(default_factory=dict)
    arcs: List[Arc] = field(default_factory=list)

    # ── lifecycle ───────────────────────────────────────────────────────────────
    def __post_init__(self):
        """
        Normalize node keys and run a quick validation pass.
        - Ensures dict keys mirror Node.key (repair if needed).
        - Logs counts and any obvious inconsistencies.
        """
        if not isinstance(self.nodes, dict):
            raise TypeError("Graph.nodes must be a dict[str, Node].")
        if not isinstance(self.arcs, list):
            raise TypeError("Graph.arcs must be a list[Arc].")

        # Re-key nodes by their own `key` (source of truth).
        fixed: Dict[str, Node] = {}
        for k, n in list(self.nodes.items()):
            if not isinstance(n, Node):
                _log.warning("Graph.__post_init__: skipping non-Node value under key=%r: %r", k, n)
                continue
            if n.key != k:
                _log.info("Graph.__post_init__: re-keying node '%s' under key '%s'.", n.key, k)
            fixed[n.key] = n
        self.nodes = fixed

        # Soft validation
        issues = self._validate(log_warnings=True)

        _log.info(
            "Graph initialized with %d nodes and %d arcs (issues=%d).",
            len(self.nodes),
            len(self.arcs),
            issues,
        )

    # ── mutation helpers (safe) ────────────────────────────────────────────────
    def add_node(self, node: Node, *, overwrite: bool = False) -> None:
        """Insert a node; optionally overwrite an existing key."""
        if not isinstance(node, Node):
            raise TypeError("add_node expects a Node.")
        if not overwrite and node.key in self.nodes:
            _log.debug("add_node: node '%s' already exists (overwrite=False) -> no-op.", node.key)
            return
        if node.kind not in _ALLOWED_NODE_KINDS:
            _log.warning("add_node: unexpected node kind '%s' for key '%s'.", node.kind, node.key)
        self.nodes[node.key] = node
        _log.info("add_node: added '%s' (kind=%s).", node.key, node.kind)

    def add_arc(self, arc: Arc, *, validate: bool = True, bidir: bool = False) -> None:
        """
        Insert a directed arc. If bidir=True, also inserts v→u with same metrics.
        """
        if not isinstance(arc, Arc):
            raise TypeError("add_arc expects an Arc.")
        self.arcs.append(arc)
        _log.info("add_arc: %s %s→%s (%.3f km).", arc.mode, arc.u, arc.v, arc.distance_km)

        if bidir:
            rev = Arc(
                u=arc.v,
                v=arc.u,
                mode=arc.mode,
                distance_km=arc.distance_km,
                hours=arc.hours,
                cost_brl=arc.cost_brl,
                co2eq_t=arc.co2eq_t,
            )
            self.arcs.append(rev)
            _log.info("add_arc: (bidir) %s %s→%s (%.3f km).", rev.mode, rev.u, rev.v, rev.distance_km)

        if validate:
            self._validate_arcs([arc])

    # ── queries ────────────────────────────────────────────────────────────────
    def get_node(self, key: str) -> Optional[Node]:
        """Return node by key (or None)."""
        return self.nodes.get(key)

    def neighbors(self, key: str) -> List[Tuple[str, Arc]]:
        """
        Outgoing neighbors of `key`.
        Returns list of (neighbor_key, arc).
        """
        out: List[Tuple[str, Arc]] = []
        for a in self.arcs:
            if a.u == key:
                out.append((a.v, a))
        _log.debug("neighbors('%s'): %d items.", key, len(out))
        return out

    def arcs_from(self, key: str) -> List[Arc]:
        """All arcs with u == key."""
        lst = [a for a in self.arcs if a.u == key]
        _log.debug("arcs_from('%s'): %d arcs.", key, len(lst))
        return lst

    def arcs_to(self, key: str) -> List[Arc]:
        """All arcs with v == key."""
        lst = [a for a in self.arcs if a.v == key]
        _log.debug("arcs_to('%s'): %d arcs.", key, len(lst))
        return lst

    def stats(self) -> Dict[str, int]:
        """Quick counts of nodes/arcs by type."""
        mode_counts: Dict[str, int] = {}
        for a in self.arcs:
            mode_counts[a.mode] = mode_counts.get(a.mode, 0) + 1
        kind_counts: Dict[str, int] = {}
        for n in self.nodes.values():
            kind_counts[n.kind] = kind_counts.get(n.kind, 0) + 1
        out = {
            "nodes": len(self.nodes),
            "arcs": len(self.arcs),
            **{f"mode_{k}": v for k, v in mode_counts.items()},
            **{f"kind_{k}": v for k, v in kind_counts.items()},
        }
        _log.debug("stats: %r", out)
        return out

    # ── validation ─────────────────────────────────────────────────────────────
    def _validate(self, *, log_warnings: bool = True) -> int:
        """
        Validate nodes and arcs; returns count of issues detected.
        Does not raise (soft validation).
        """
        issues = 0

        # Node kinds
        for n in self.nodes.values():
            if n.kind not in _ALLOWED_NODE_KINDS:
                issues += 1
                if log_warnings:
                    _log.warning("validate: node '%s' has unexpected kind '%s'.", n.key, n.kind)

        # Arcs
        issues += self._validate_arcs(self.arcs, log_warnings=log_warnings)

        return issues

    def _validate_arcs(self, arcs: Iterable[Arc], *, log_warnings: bool = True) -> int:
        """
        Validate a batch of arcs (soft). Returns number of issues.
        """
        issues = 0
        for a in arcs:
            if a.u not in self.nodes:
                issues += 1
                if log_warnings:
                    _log.warning("validate: arc u='%s' not found among nodes.", a.u)
            if a.v not in self.nodes:
                issues += 1
                if log_warnings:
                    _log.warning("validate: arc v='%s' not found among nodes.", a.v)
            if a.mode not in _ALLOWED_ARC_MODES:
                issues += 1
                if log_warnings:
                    _log.warning("validate: arc '%s→%s' has unexpected mode '%s'.", a.u, a.v, a.mode)
            if a.distance_km < 0 or a.hours < 0 or a.cost_brl < 0 or a.co2eq_t < 0:
                issues += 1
                if log_warnings:
                    _log.warning(
                        "validate: negative metric in arc %s→%s (dist=%.3f, hours=%.3f, cost=%.2f, co2eq_t=%.4f).",
                        a.u, a.v, a.distance_km, a.hours, a.cost_brl, a.co2eq_t
                    )
        return issues
