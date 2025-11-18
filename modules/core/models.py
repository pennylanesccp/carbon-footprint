# modules/core/models.py
# -*- coding: utf-8 -*-

"""
Core domain models (pure dataclasses).

These are small, shared structures used across the project:
    - GeoPoint: a labelled geographic coordinate
    - RoadLeg: a single road leg (O→D) with distance/profile info
    - CabotageLegs: road legs connecting origin/destiny to ports
    - RouteRun: full routing result for an origin/destiny pair

This module deliberately has:
    - no database imports
    - no HTTP / ORS imports
    - no plotting, fuel or emissions logic

It is safe to import from anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ────────────────────────────────────────────────────────────────────────────────
# Basic geographic point
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class GeoPoint:
    """
    A labelled geographic point.

    Attributes
    ----------
    lat : float
        Latitude in decimal degrees.
    lon : float
        Longitude in decimal degrees.
    label : str
        Human-readable label (city name, address, port name, etc.).
    """

    lat: float
    lon: float
    uf: Optional[str]
    label: str


# ────────────────────────────────────────────────────────────────────────────────
# Road-only leg
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class RoadLeg:
    """
    A single road leg between two geographic points.

    Attributes
    ----------
    origin : GeoPoint
        Origin point (resolved).
    destiny : GeoPoint
        Destiny point (resolved).
    distance_km : Optional[float]
        Distance in kilometers for this leg. May be None if routing failed.
    profile_used : Optional[str]
        ORS profile actually used (e.g. 'driving-hgv', 'driving-car'),
        or None if no route was found.
    """

    origin: GeoPoint
    destiny: GeoPoint
    distance_km: Optional[float]
    profile_used: Optional[str]


# ────────────────────────────────────────────────────────────────────────────────
# Cabotage road legs (O→Po and Pd→D)
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class CabotageLegs:
    """
    Road components of a cabotage alternative.

    Attributes
    ----------
    port_origin : Optional[GeoPoint]
        Nearest port to the origin, or None if not found.
    port_destiny : Optional[GeoPoint]
        Nearest port to the destiny, or None if not found.
    road_o_to_po_km : Optional[float]
        Road distance from origin to origin-port, in km. None if not computed.
    road_pd_to_d_km : Optional[float]
        Road distance from destiny-port to destiny, in km. None if not computed.
    """

    port_origin: Optional[GeoPoint]
    port_destiny: Optional[GeoPoint]
    road_o_to_po_km: Optional[float]
    road_pd_to_d_km: Optional[float]


# ────────────────────────────────────────────────────────────────────────────────
# Full routing run (raw inputs + resolved points + legs)
# ────────────────────────────────────────────────────────────────────────────────

@dataclass
class RouteRun:
    """
    Full routing result for a single origin/destiny pair.

    This is a high-level container that can be passed around to:
        - fuel/emissions modules
        - plotting functions
        - comparison/report generators

    Attributes
    ----------
    origin_raw : str
        Raw origin string as given by the user/CLI.
    destiny_raw : str
        Raw destiny string as given by the user/CLI.
    origin_resolved : Optional[GeoPoint]
        Resolved origin point (after geocoding), or None on failure.
    destiny_resolved : Optional[GeoPoint]
        Resolved destiny point (after geocoding), or None on failure.
    road_only : Optional[RoadLeg]
        Main road leg from origin_resolved to destiny_resolved.
        May be None if geocoding or routing failed.
    cabotage : Optional[CabotageLegs]
        Cabotage-related road legs (O→Po and Pd→D). Optional so that
        pure road-only contexts do not need to populate it.
    is_hgv : Optional[bool]
        Flag representing whether the *final* route profile used is HGV:
            True  → heavy vehicle (e.g. 'driving-hgv')
            False → non-HGV (e.g. 'driving-car')
            None  → unknown / no route
    """

    origin_raw: str
    destiny_raw: str

    origin_resolved: Optional[GeoPoint]
    destiny_resolved: Optional[GeoPoint]

    road_only: Optional[RoadLeg]
    cabotage: Optional[CabotageLegs]

    is_hgv: Optional[bool]
