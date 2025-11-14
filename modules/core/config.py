# modules/core/config.py
# -*- coding: utf-8 -*-

"""
Core configuration models and globals.

This module centralizes *pure* configuration structures that are
independent of any specific infrastructure (DB, HTTP client, etc.).

It is meant to be safe to import from anywhere.

Current contents
----------------
- ProjectConfig: high-level defaults for the whole project
- RoutingDefaults: generic routing-related defaults (country, profile)
"""

from __future__ import annotations

from dataclasses import dataclass


# ────────────────────────────────────────────────────────────────────────────────
# High-level project configuration
# ────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ProjectConfig:
    """
    Global project configuration.

    Attributes
    ----------
    default_country : str
        ISO 3166-1 alpha-2 country code used when none is provided.
    default_language : str
        Language/locale tag used for labels, logs, etc.
    """

    default_country: str = "BR"
    default_language: str = "pt-BR"


# ────────────────────────────────────────────────────────────────────────────────
# Routing defaults (kept generic, not tied to ORS directly)
# ────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RoutingDefaults:
    """
    Generic routing-related defaults.

    These values can be used by road/cabotage modules as *defaults*
    without hardcoding literals all over the codebase.

    Attributes
    ----------
    primary_profile : str
        Default routing profile mnemonic (e.g. 'driving-hgv').
    fallback_profile : str
        Fallback profile when primary fails (e.g. 'driving-car').
    enable_fallback : bool
        Whether the system should try the fallback profile by default.
    """

    primary_profile: str = "driving-hgv"
    fallback_profile: str = "driving-car"
    enable_fallback: bool = True


# ────────────────────────────────────────────────────────────────────────────────
# Singleton-style instances
# ────────────────────────────────────────────────────────────────────────────────

# Global, immutable configuration objects used as defaults.
PROJECT_CONFIG = ProjectConfig()
ROUTING_DEFAULTS = RoutingDefaults()


def get_project_config() -> ProjectConfig:
    """
    Return the global project configuration.

    Provided as a function in case this ever needs to become dynamic
    (e.g. loaded from a file or environment variables) without changing
    call sites.
    """
    return PROJECT_CONFIG


def get_routing_defaults() -> RoutingDefaults:
    """
    Return the global routing defaults.
    """
    return ROUTING_DEFAULTS
