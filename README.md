# Multimodal Freight Assessment: A Computational Framework for Cost and Carbon Footprint Analysis of Road vs. Cabotage in Brazil

> **Codebase for the Graduation Thesis (*Trabalho de Formatura*) evaluating the environmental and economic trade-offs between Road and Cabotage freight in Brazil.**

This repository implements a transparent, reproducible computational framework to compare **Road-only** versus **Multimodal (Road–Sea–Road)** logistics chains. Developed as part of the Engineering Graduation Project, it aims to open the "black box" of port operations often found in logistics comparisons.

## Core Contributions

* **First-Principles Port Energy Model:** Explicitly models energy consumption during **vessel hotelling** (auxiliary engines/boilers) and **terminal handling** (STS cranes, yard equipment), rather than using generic time-based averages.
* **Integrated Routing:** Automates the retrieval of realistic road distances and times via **OpenRouteService (ORS)** for first- and last-mile legs.
* **Holistic Metrics:** Computes **Total Logistics Cost**, **Well-to-Wheel (WTW) GHG Emissions**, and **Transit Time** on a per-container (TEU) basis.
* **Reproducibility:** Features a modular Python architecture with SQLite caching, ensuring that scenarios (e.g., varying carbon tax, bunker prices, or terminal efficiency) are auditable and repeatable.

The current release focuses on the **Fixed-Origin Assessment** (São Paulo → Port of Santos → Brazilian Capitals), establishing the methodology for the broader national analysis.
