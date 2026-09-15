# 02. Data Sources & Ingestion Strategy

> **Focus:** Source inventory, access protocols, extraction strategies, and source constraints.

---

## 1. Source Systems Inventory

The pipeline ingests data across five distinct external and internal sources:

| Source System | Data Domain | Ingestion Frequency | Ingestion Mechanism | Target Format |
| :--- | :--- | :--- | :--- | :--- |
| **Operational PostgreSQL** | Orders, order items, payments, customer master, seller profiles | Daily batch (incremental) | Read-Replica JDBC / WAL CDC | Parquet / JSON |
| **Kaggle Datasets** | Historical baseline e-commerce dataset (reviews, orders, customer logs) | One-time bootstrap & backfills | Cloud Storage Sync / Download | Parquet |
| **BigQuery Public Data** | Geospatial reference tables, Brazilian postal coordinates, macro indices | Periodic batch (Monthly) | BQ Storage Read API | Parquet |
| **Public REST APIs** | Currency exchange rates, logistics tracking status, geocoding validation | Daily incremental snapshots | Python HTTP Client (Rate-limited) | JSON |
| **Government Data (IBGE)** | Brazilian census, municipal codes, holiday calendars, regional tax codes | Static / Quarterly refresh | Scheduled HTTP / SFTP fetch | Parquet / CSV |

---

## 2. Source-Side Constraints & Safeguards

These two operational constraints are non-negotiable:

### Constraint 1: Operational PostgreSQL Database Protection
- **The Issue:** The operational database is a live production OLTP system processing customer checkout orders. Heavy analytical queries or full-table scans during business hours will exhaust connection pools and lock transactional rows.
- **Rules:**
  1. **Zero analytical queries** against the operational primary database.
  2. Ingestion queries must connect exclusively to a **dedicated Read Replica** or read transactional logs via **Change Data Capture (CDC)**.
  3. Batch extraction jobs must run strictly during the **off-peak window (01:00 AM – 04:00 AM)**.
  4. Extractions must use indexed watermark queries (`WHERE updated_at >= :last_sync AND updated_at < :current_sync`) with cursor pagination.

### Constraint 2: Source Systems Cannot Be Modified
- **The Issue:** Data engineers do not have administrative access to alter production source schemas.
- **Rules:**
  1. You **cannot** add indexes, columns, triggers, or stored procedures to source databases.
  2. The ingestion client must be strictly read-only (`SELECT` permissions only).
  3. All data cleansing, deduplication, surrogate key generation, and transformations occur downstream in the Lakehouse / Data Warehouse compute layer.

---

## 3. Raw Landing Zone Architecture (AWS S3)

Extracted data is stored immutably in an AWS S3 landing bucket before any transformations take place:

```text
s3://<project-lakehouse-bucket>/
├── raw/
│   ├── orders/
│   │   └── ingestion_date=YYYY-MM-DD/
│   │       ├── orders_part_001.parquet
│   │       └── _metadata.json
│   ├── order_items/
│   │   └── ingestion_date=YYYY-MM-DD/
│   ├── payments/
│   │   └── ingestion_date=YYYY-MM-DD/
│   ├── customers/
│   │   └── ingestion_date=YYYY-MM-DD/
│   ├── products/
│   │   └── ingestion_date=YYYY-MM-DD/
│   └── external_api/
│       └── rate_date=YYYY-MM-DD/
└── archive/
```

### Ingestion Metadata Captured Per File:
- `_ingestion_timestamp`: UTC timestamp when extraction was written.
- `_source_system`: Name of the originating system (e.g., `postgres_replica`, `api_rates`).
- `_batch_id`: Unique Airflow DAG execution run identifier.
- `_record_count`: Number of records written in this partition.
