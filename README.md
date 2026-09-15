# Olist Data Production Pipeline

[![Python Version](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![Status](https://img.shields.io/badge/status-specification--ready-success.svg)]()
[![Architecture](https://img.shields.io/badge/architecture-Medallion%20Lakehouse-orange.svg)]()

Production-grade analytical data pipeline engineered to ingest, cleanse, model, and serve Olist's multi-source operational e-commerce data into a centralized analytical lakehouse/data warehouse.

---

## 📖 Primary Documentation

The comprehensive developer requirements, data architecture, dimensional modeling (SCD Type 2), KPI definitions, data quality contracts, and scalability blueprints are documented in:

👉 **[DATA_PIPELINE_SPECIFICATION.md](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DATA_PIPELINE_SPECIFICATION.md)**

---

## 🚀 Quick Overview

### Source Data Landscape
- **Operational PostgreSQL:** Live transactions, order items, payments, customer updates.
- **Kaggle Datasets:** Historical e-commerce baseline archives.
- **BigQuery Public Datasets:** Geospatial coordinates and regional macroeconomic indicators.
- **Public APIs:** Currency exchange rates and postal address verification.
- **Government Data:** Brazilian IBGE demographic, municipal, and tax code datasets.

### Target Architecture (Medallion Pattern)
- **Bronze (Raw Zone):** Immutable, partitioned Parquet storage of raw extracts with ingestion metadata.
- **Silver (Conformed & Cleansed):** Schema-validated, deduplicated, standardized entities with **Slowly Changing Dimensions (SCD Type 2)** for customers and products.
- **Gold (Curated Business Marts):** Star schema dimensional marts for Sales, Marketing, Operations, Finance, and Executive decision-making.
- **Quarantine / DLQ:** Strict capture of invalid records ensuring no bad data silently disappears.

### SLAs & Freshness
- **Daily Batch:** Orchestrated off-peak run ensuring data is verified and available by **07:00 AM** every day.
- **Strict Idempotency:** Any pipeline failure is safely restartable without duplicating rows or corrupting analytical state.
- **Scalability:** Designed for high throughput (starting at ~590k records/day and scaling $8\times$ over 4 years to billions of rows annually).

---

## 📂 Repository Structure

```text
Data-Production-Pipeline/
├── DATA_PIPELINE_SPECIFICATION.md   # Complete technical & architecture specification
├── Business Requirements.txt        # Original raw business requirements
├── pyproject.toml                   # UV / Python package configuration
├── src/
│   └── data_production_pipeline/   # Pipeline source code packages
└── README.md                        # Project introduction
```

For full details on business rules (BR-01 through BR-07), KPI calculations, data quality rules, and system constraints, please refer to [DATA_PIPELINE_SPECIFICATION.md](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DATA_PIPELINE_SPECIFICATION.md).
