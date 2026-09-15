# OPTIONAL

# 07. Scale, Throughput & Growth Projections

> **Focus:** Baseline volume, 4-year capacity growth model ($1\times \rightarrow 8\times$), and scaling engineering principles.

---

## 1. Current Daily Ingestion Profile

The pipeline is architected for enterprise-scale transaction throughput:

| Entity / Stream | Daily Records | Monthly Records | Annual Records |
| :--- | :--- | :--- | :--- |
| **Orders** | 100,000 | 3,000,000 | 36,500,000 |
| **Order Items** | 250,000 | 7,500,000 | 91,250,000 |
| **Payments** | 120,000 | 3,600,000 | 43,800,000 |
| **Customers (Active / Updates)** | 20,000 | 600,000 | 7,300,000 |
| **Product Updates** | 10,000 | 300,000 | 3,650,000 |
| **Shipments & Logistics Events** | 90,000 | 2,700,000 | 32,850,000 |
| **Total Daily Ingestion** | **590,000** | **17,700,000** | **215,350,000** |

---

## 2. Four-Year Growth Projections ($1\times \rightarrow 8\times$)

Management projects substantial multi-year business growth:

| Horizon | Scale Factor | Daily Orders | Daily Order Items | Annual Fact Rows | Est. Compressed Parquet / Year |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Year 1 (Baseline)** | **$1\times$** | 100,000 | 250,000 | $\approx 215\text{ Million}$ | $\approx 20\text{ GB}$ |
| **Year 2** | **$2\times$** | 200,000 | 500,000 | $\approx 430\text{ Million}$ | $\approx 40\text{ GB}$ |
| **Year 3** | **$4\times$** | 400,000 | 1,000,000 | $\approx 860\text{ Million}$ | $\approx 80\text{ GB}$ |
| **Year 4** | **$8\times$** | 800,000 | 2,000,000 | $\approx 1.72\text{ Billion}$ | $\approx 160\text{ GB}$ |

---

## 3. Scale Architectural Principles (Constraint 5)

> **Core Architecture Mandate:** The pipeline architecture must **never** assume *"the data fits comfortably on my laptop"*.

### Architectural Rules for Scale:
1. **No In-Memory Single-Node Processing:** Avoid pulling entire datasets into local Pandas DataFrames or running single-threaded in-memory merges. Memory errors (OOM) will occur within months.
2. **Distributed & Chunked Compute:** Use **PySpark on Databricks** or chunked streaming engines that distribute memory pressure across cluster nodes.
3. **Partitioning & File Compaction:**
   - Partition raw and fact tables by calendar date: `order_date (YYYY-MM)`.
   - Cluster high-cardinality keys: `customer_id` and `product_id`.
   - Schedule regular file compaction (`OPTIMIZE` / `VACUUM` in Delta Lake) to eliminate small file problems.
4. **Columnar Compressed Storage:** Store all intermediate and analytical datasets in **Snappy/ZSTD-compressed Apache Parquet / Delta Lake**, delivering up to $5\times$ compression and massive I/O savings via predicate pushdown and column pruning.

---

## 4. Dual-Mode Processing (Constraint 4)

The pipeline engine must operate seamlessly in two distinct execution modes:

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DUAL-MODE EXECUTION ENGINE                         │
├──────────────────────────────────────┬──────────────────────────────────────┤
│ 1. Historical Backfill Mode          │ 2. Daily Incremental Mode            │
├──────────────────────────────────────┼──────────────────────────────────────┤
│ • Bootstrap years of Kaggle drops.   │ • Runs daily off-peak (02:00 AM).    │
│ • Bulk-loads partitioned tables.     │ • Watermark CDC via updated_at.      │
│ • Skips external API lookups.        │ • Active SCD Type 2 change tracking. │
│ • High parallel cluster sizing.      │ • Idempotent atomic MERGE into Gold. │
└──────────────────────────────────────┴──────────────────────────────────────┘
```
