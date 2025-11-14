# modules/core/types.py
# -*- coding: utf-8 -*-

"""
Shared type aliases and lightweight protocols.

This module centralizes common typing helpers so they can be imported
everywhere without creating circular dependencies.

Contents
--------
- StrPath: str or pathlib.Path
- JSON* aliases: JSONScalar, JSONValue, JSONList, JSONDict
- CoordinatePair: (lat, lon) as a tuple
- HasLatLon, HasLabel: small Protocols for duck-typed objects
"""

from __future__ import annotations

from pathlib import Path
from typing import (
      Any
    , Dict
    , Iterable
    , List
    , Mapping
    , MutableMapping
    , Protocol
    , Sequence
    , Tuple
    , Union
    , runtime_checkable
)


# ────────────────────────────────────────────────────────────────────────────────
# Path-like
# ────────────────────────────────────────────────────────────────────────────────

StrPath = Union[str, Path]
"""Path representation accepted by most IO helpers (string or Path)."""


# ────────────────────────────────────────────────────────────────────────────────
# JSON-like structures
# ────────────────────────────────────────────────────────────────────────────────

JSONScalar = Union[str, int, float, bool, None]
"""Scalar values allowed inside JSON structures."""

JSONValue = Union["JSONScalar", "JSONList", "JSONDict"]
"""Recursive JSON value type."""

JSONList = List[JSONValue]
"""List of JSON values."""

JSONDict = Dict[str, JSONValue]
"""Dictionary with string keys and JSON values."""


# More generic mapping aliases (handy for ORS responses, configs, etc.)
AnyMapping = Mapping[str, Any]
AnyMutableMapping = MutableMapping[str, Any]


# ────────────────────────────────────────────────────────────────────────────────
# Geographic + numeric helpers
# ────────────────────────────────────────────────────────────────────────────────

CoordinatePair = Tuple[float, float]
"""Simple (lat, lon) pair in decimal degrees."""

Number = Union[int, float]
"""Numeric value (int or float)."""


# ────────────────────────────────────────────────────────────────────────────────
# Lightweight protocols
# ────────────────────────────────────────────────────────────────────────────────

@runtime_checkable
class HasLatLon(Protocol):
    """
    Protocol for objects that expose `lat` and `lon` attributes.

    Useful for functions that can accept either a GeoPoint or any other
    structure with lat/lon, without depending on a specific class.
    """

    lat: float
    lon: float


@runtime_checkable
class HasLabel(Protocol):
    """
    Protocol for objects that expose a `label` attribute.
    """

    label: str


# Generic collections (for convenience)

SequenceOfStr = Sequence[str]
IterableOfStr = Iterable[str]
