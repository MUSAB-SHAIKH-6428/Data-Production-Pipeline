# Olist Data Production Pipeline

[![Python Version](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/)
[![Status](https://img.shields.io/badge/status-specification--ready-success.svg)]()
[![Architecture](https://img.shields.io/badge/architecture-Medallion%20Lakehouse-orange.svg)]()

Production-grade analytical data pipeline engineered to ingest, cleanse, model, and serve Olist's multi-source operational e-commerce data into a centralized analytical lakehouse and data warehouse.

---

## 📚 Project Documentation

The documentation is organized into modular, domain-specific guides under the [`DOCUMENTATION/`](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION) directory:

| # | Document | Focus Area |
| :---: | :--- | :--- |
| **01** | [**Business Requirements & Scope**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/01_BUSINESS_REQUIREMENTS.md) | Business problem, BR-01 to BR-07, stakeholder personas, questions catalog, 07:00 AM SLA. |
| **02** | [**Data Sources & Ingestion Strategy**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/02_DATA_SOURCES_AND_INGESTION.md) | 5 source systems, off-peak PostgreSQL protection, read-only constraints, S3 raw landing. |
| **03** | [**Data Modeling & SCD Strategy**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/03_DATA_MODELING_AND_SCD.md) | Star schema ER diagram, fact table grains, surrogate keys, Slowly Changing Dimensions (SCD-2). |
| **04** | [**KPIs & Business Metrics Dictionary**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/04_KPIS_AND_METRICS.md) | Standardized mathematical formulas, grains, and definitions for all corporate metrics. |
| **05** | [**Data Quality, Governance & Observability**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/05_DATA_QUALITY_AND_GOVERNANCE.md) | Data contract validation matrix, Dead Letter Queue (DLQ), idempotency, and audit logging. |
| **06** | [**System Architecture & Roadmap**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/06_ARCHITECTURE_AND_ROADMAP.md) | Medallion Lakehouse architecture, 10-phase roadmap, Airflow DAG flow, repository blueprint. |
| **07** | [**Scale, Throughput & Growth Projections**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/07_SCALE_AND_GROWTH.md) | Baseline volume (590k/day), 4-year scaling ($1\times \rightarrow 8\times$), anti-laptop architecture rules. |
| **08** | [**DWH Architecture & Physical Design**](file:///C:/Users/MUSAB/Desktop/Data-Production-Pipeline/DOCUMENTATION/08_DWH_DESIGN.md) | Physical layout of `ecommerce_dwh`, staging tables, DDLs, indexes, constraints, and ELT lineage. |

---

## 🚀 Architecture Snapshot

- **Source Systems:** Operational PostgreSQL (Read-replica/CDC), Kaggle baseline files, BigQuery Public Data, Public REST APIs, Government (IBGE) Data.
- **Storage & Compute:** AWS S3 raw landing $\rightarrow$ Databricks (PySpark / Delta Lake) Medallion Architecture (Bronze, Silver, Gold) $\rightarrow$ PostgreSQL Analytical Marts $\rightarrow$ Power BI.
- **Orchestration:** Apache Airflow with automated daily batch execution by **07:00 AM SLA**.
- **Quality & Resilience:** Dead Letter Queue (DLQ) quarantine pattern, strict idempotency, and Slowly Changing Dimensions (SCD Type 2).

---

## 📂 Repository Structure

```text
Data-Production-Pipeline/
├── architecture_design/                 # Architectural & dimensional schema diagrams
│   ├── Data_Model.png                   # Kaggle source relational model
│   ├── DETAILED_DIM_MODEL.png           # Enterprise dimensional bus architecture
│   └── DIM_MODEL.png                    # Target dimensional star schema
├── DOCUMENTATION/
│   ├── 01_BUSINESS_REQUIREMENTS.md
│   ├── 02_DATA_SOURCES_AND_INGESTION.md
│   ├── 03_DATA_MODELING_AND_SCD.md
│   ├── 04_KPIS_AND_METRICS.md
│   ├── 05_DATA_QUALITY_AND_GOVERNANCE.md
│   ├── 06_ARCHITECTURE_AND_ROADMAP.md
│   ├── 07_SCALE_AND_GROWTH.md
│   └── 08_DWH_DESIGN.md
├── Kaggle_dataset/                      # Baseline Brazilian E-Commerce dataset
├── Business Requirements.txt            # Original business requirements
├── DECISION.md                          # Design decisions & model specifications
├── dwh_design.md                        # Data warehouse physical design specification
├── pyproject.toml                       # UV / Python package configuration
├── src/
│   └── data_production_pipeline/       # Pipeline source code packages
└── README.md                            # Project entry point
```
