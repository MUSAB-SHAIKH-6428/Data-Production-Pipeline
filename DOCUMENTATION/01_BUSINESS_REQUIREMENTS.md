# 01. Business Requirements & Analytics Scope

> **Domain:** Olist E-Commerce Analytics Platform  
> **Status:** Approved Baseline  
> **Freshness SLA:** Daily batch, verified and available by **07:00 AM**

---

## 1. Business Problem
Olist operates across multiple transactional databases, external logistics providers, payment gateways, and public registries. Currently, operational data is fragmented across these disconnected systems.

Business users face:
- Manual, error-prone data extraction and reconciliation in spreadsheets.
- Inability to perform cross-domain analytics (e.g., impact of delivery delays on customer churn and seller ratings).
- Analytical queries impacting production database performance.

**Objective:** Deliver a single, trusted, automated analytical data platform that consolidates all domain data to empower data-driven decisions across the organization.

---

## 2. Business Stakeholders & Personas

| Stakeholder Group | Primary Analytics Focus |
| :--- | :--- |
| **Sales** | Daily/monthly revenue, top-selling products, category performance, discount loss |
| **Marketing** | Customer acquisition rate, cohort retention, repeat purchase behavior, LTV |
| **Operations** | Courier delivery delays, logistics bottlenecks, warehouse shipping costs |
| **Finance** | Gross vs. net revenue, payment settlement, cancellation and return losses |
| **Management** | High-level executive scorecards, overall business health, market expansion |
| **Data Analysts / BI** | Self-serve SQL querying, dimensional star schemas, ad-hoc analysis |

---

## 3. Core Business Requirements

### BR-01 — Centralized Data Platform
The company requires a single source of truth (SSOT) instead of manually combining source extracts. All analytical dashboards and queries must read from this platform rather than operational transactional databases.

### BR-02 — Automated Daily Processing
The end-to-end pipeline must execute automatically on a daily schedule without manual intervention, completing before the **07:00 AM** business SLA.

### BR-03 — Incremental Processing
The pipeline must process only newly created or modified transactional data, avoiding expensive and redundant full-table re-scans.

### BR-04 — Historical Data Preservation (SCD Type 2)
Historical customer, product, and order changes must be preserved where business changes matter:
- **Example:** A customer moves from *Maharashtra* to *Karnataka*. Historical reporting must reflect the state applicable at the time an order was placed, while current marketing targets the new state.
- Product attributes (category, brand, list price) must retain historical point-in-time validity.

### BR-05 — Data Quality & Non-Silent Quarantine
The pipeline must validate every record against strict schema and business rules. Missing mandatory fields, duplicates, negative amounts, invalid dates, and broken foreign keys must be captured.
> **Critical Rule:** Bad records must **never silently disappear**. They must be routed to a dedicated quarantine / Dead Letter Queue (DLQ).

### BR-06 — Recoverability & Idempotency
If any pipeline task fails, the process must be safely restartable from the failure point without corrupting previously processed data, producing duplicates, or requiring manual cleanup.

### BR-07 — Auditability & Observability
Every execution run must record operational metadata:
- Ingestion start and completion timestamps.
- Record counts: received, successfully processed, and quarantined.
- Failure logs, error stack traces, and SLA adherence status.

---

## 4. Business Questions Catalog

The analytical platform must answer these specific operational and strategic questions:

### Sales Analytics
- What is total revenue by day, week, month, and year?
- What are the top-selling products by volume and Gross Merchandise Value (GMV)?
- Which product categories generate the highest revenue?
- What is the overall and segmented Average Order Value (AOV)?
- How much revenue is lost through discounts and promo vouchers?

### Customer Analytics
- How many new customers are acquired each month?
- Which customer cohorts possess the highest Lifetime Value (CLV)?
- What percentage of customers make repeat purchases (30/60/90-day windows)?
- How does purchasing behavior vary across geographic states and cities?

### Seller Analytics
- Which sellers generate the highest revenue and order volume?
- Which sellers have substandard fulfillment performance or high cancellation rates?

### Operations & Logistics
- What percentage of total customer orders are delivered late?
- Which delivery couriers / 3PL partners have the highest delay rates?
- Which warehouses and origin routes incur the highest shipping costs?

### Product Performance
- Which products generate the highest cumulative revenue?
- Which products show declining sales velocity over the last 90 days?
- How do product review ratings correlate with order volume and returns?
- Which brands maintain the best balance of sales volume and low return rates?

