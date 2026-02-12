# Quickbase → ArcGIS Automated Sync Pipeline

Automated Python pipeline that reads validated records from Quickbase and updates ArcGIS feature layers using controlled field mapping, batch-safe edits, and cross-system verification logic.

This solution is designed for scheduled, unattended execution using **Power Automate Cloud + Power Automate Desktop**, with structured logging and run summaries posted to Microsoft Teams.

---

## 🚀 Overview

This project implements an enterprise-style integration pipeline between **Quickbase** and **ArcGIS Enterprise** feature services.

The script:

- Queries Quickbase tables via REST API
- Normalizes and validates record values
- Cross-references ArcGIS features by unique ID
- Applies controlled attribute updates only where needed
- Performs batch-safe edits to ArcGIS layers
- Produces structured run metrics for automation reporting

It supports multiple entity types (FDH + MDU in this implementation) with separate mappings and validation rules.

---

## 🧩 Architecture

Quickbase → Python Sync Script → ArcGIS Feature Layers  
                     ↑  
Power Automate Cloud (schedule trigger)  
                     ↓  
Power Automate Desktop (runs script + posts Teams log)

Flow:

1. **Power Automate Cloud** triggers on schedule
2. Launches **Power Automate Desktop**
3. Desktop flow runs Python sync script
4. Script authenticates to:
   - Quickbase API
   - ArcGIS Portal / Enterprise
5. Records are fetched, cleaned, and validated
6. Minimal ArcGIS update payloads are generated
7. Batch edits are applied with fallback handling
8. JSON run summary is emitted
9. Desktop flow posts summary to **Microsoft Teams**

---

## 🔐 Authentication

Credentials are provided via:

### ArcGIS
- Environment variables or CLI args:
  - `ARCGIS_PORTAL_URL`
  - `OMNI_GIS_USER`
  - `OMNI_GIS_PASS`

### Quickbase
- User token via:
  - Environment variable `QB_TOKEN`
  - or `--qb-token` argument

No credentials are stored in code.

---

## 🗺 Supported Syncs

### FDH Sync
- Matches on unique FDH ID
- Field mapping from Quickbase → ArcGIS
- Date parsing and normalization
- Null / sentinel value cleanup
- Selective field updates only

### MDU Sync
- Separate table + layer mapping
- Checkbox normalization (Y/N)
- Date parsing with safe fallback
- Type-aware numeric coercion

---

## 🧠 Key Technical Features

- ✅ Quickbase REST API query integration
- ✅ ArcGIS Python API feature edits
- ✅ Minimal update payload design (OBJECTID + mapped fields only)
- ✅ Batch editing with per-record fallback on failure
- ✅ Schema-aware type coercion
- ✅ Date parsing with multi-format support
- ✅ Null and sentinel value normalization
- ✅ Chunked ArcGIS queries for scale
- ✅ Structured metrics collection
- ✅ Machine-readable JSON run summary output
- ✅ Automation-friendly logging

---

## 📊 Automation Metrics Output

At completion, the script emits a single-line JSON summary:

Includes:

- Run timing
- Records processed
- Updates prepared
- Successful updates
- Failed updates
- Date parse skips
- Validation skips
- Error counts

Designed for easy parsing by Power Automate Desktop and posting to Teams.

---

## ⚙️ Usage

### Command Line

```bash
python QB_ArcGIS_Sync_Automated.py \
  --portal-url https://yourportal.domain/portal \
  --gis-user USERNAME \
  --gis-pass PASSWORD \
  --qb-token QB_TOKEN
