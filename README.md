# Databricks ETL Project – Audit‑Driven Lakehouse Pipeline

## 1. Overview
This project implements a **production‑style Databricks Lakehouse ETL framework** using **PySpark, Delta Lake, and SQL**, designed to handle **dimension full loads** and **fact incremental loads** with strong governance.

Key characteristics:
- Metadata‑driven processing via **Control Table**
- End‑to‑end **audit logging** and **error logging**
- Reusable **utility framework** for validation, watermarking, and upserts
- Separation of **system**, **data**, and **processing logic**

The project mimics a real enterprise ingestion pipeline from **raw files (CSV/JSON)** into **Silver‑layer Delta tables**.

---

## 2. High‑Level Architecture

```
Raw Layer (ADLS / Files)
   ├── customers.csv
   ├── products.csv
   ├── orders.csv
   └── events.json
          ↓
   Data Validation & Transformation (PySpark)
          ↓
Delta Lake Tables (Silver)
   ├── Dim Tables (Full Load)
   └── Fact Tables (Incremental / Upsert)
          ↓
System Tables
   ├── Audit Table
   ├── Control Table
   └── Error Table
```

---

## 3. Project Structure

```
project-root/
│
├── utils/
│   ├── audit_logger.py
│   ├── control_table_manager.py
│   ├── data_process_functions.py
│   ├── data_validation_rule.py
│   ├── error_handler.py
│   └── environment_variables_config.py
│
├── data_process/
│   ├── dim_full_load.py
│   └── fact_delta_load.py
│
├── ddl/
│   ├── create_system_tables.py
│   └── create_data_tables.py
│
├── raw_data/
│   ├── customers.csv
│   ├── products.csv
│   ├── orders.csv
│   └── events.json
│
├── sql/
│   └── scripts.sql
│
└── README.md
```

---

## 4. System Tables Design

### 4.1 Audit Table (`tbl_sys_audit`)
Tracks **each job execution**.

**Why this design?**
- Enables operational monitoring
- Supports reconciliation (records read vs written)
- Provides traceability at notebook & job‑run level

Key fields:
- `job_id`, `job_run_id`
- `source_layer`, `target_table`
- `records_inserted`, `records_updated`
- `status`, `start_time`, `end_time`

---

### 4.2 Control Table (`tbl_sys_control`)
Drives the **entire pipeline dynamically**.

**Responsibilities:**
- Determines which tables are active
- Stores load type (Full / Incremental)
- Maintains last watermark
- Prevents duplicate daily processing

**Design decision:**
> Processing logic never hard‑codes source or target tables. Everything is metadata‑driven.

---

### 4.3 Error Table (`tbl_sys_error`)
Centralized error tracking with stack traces.

**Why separate error logging?**
- Keeps audit table clean
- Enables alerting & root‑cause analysis

---

## 5. Utility Layer Design

### 5.1 Audit Logger
Encapsulates **start_audit** and **end_audit**.

**Design choice:**
- Audit handling is isolated to avoid repetition
- Ensures audit closes even on failure

---

### 5.2 Control Table Manager
Provides APIs to:
- Check if table is active
- Fetch last watermark
- Update run status & metrics

**Why?**
- Central point of truth
- Reduces SQL duplication

---

### 5.3 Data Validation
Supports:
- Schema validation (fields only or fields + types)
- Composite uniqueness checks
- Null validations

**Design philosophy:**
> Fail fast before writing bad data to Delta tables.

---

### 5.4 Data Process Functions
Handles:
- Full reads
- Incremental reads
- Delta MERGE (upsert)
- Append loads
- Watermark calculation

**Why Delta MERGE?**
- Ensures idempotency
- Supports late‑arriving data

---

## 6. Data Processing Logic

### 6.1 Dimension Tables – Full Load
Implemented in `dim_full_load.py`.

Flow:
1. Fetch dimension targets from control table
2. Truncate target table
3. Read full source data
4. Validate schema & nulls
5. Append into Delta table
6. Update audit & control tables

**Why Full Load for Dimensions?**
- Smaller data volume
- Simpler logic
- Avoids complex SCD handling for demo

---

### 6.2 Fact Tables – Incremental Load
Implemented in `fact_delta_load.py`.

Flow:
1. Read last watermark from control table
2. Filter source data
3. Validate schema, uniqueness & nulls
4. MERGE into Delta table
5. Update watermark
6. Optimize & vacuum tables

**Why watermark‑based loads?**
- Scales well
- Supports re‑runs
- Avoids reprocessing full data

---

## 7. SQL Scripts

`scripts.sql` contains:
- Validation queries
- Sample analytics queries
- Debugging helpers

SQL is intentionally kept separate to:
- Support BI use cases
- Allow ad‑hoc analysis without touching PySpark jobs

---

## 8. Setup Instructions

### 8.1 Prerequisites
- Azure Databricks workspace
- Unity Catalog enabled
- ADLS / DBFS access

### 8.2 Deployment Steps
1. Run `create_system_tables.py`
2. Run `create_data_tables.py`
3. Upload raw datasets to configured paths
4. Validate control table entries
5. Execute:
   - `dim_full_load.py`
   - `fact_delta_load.py`

---

## 9. Key Design Decisions Summary

| Area | Decision | Reason |
|-----|--------|-------|
| Metadata | Control table driven | Scalability & flexibility |
| Auditing | Central audit table | Compliance & observability |
| Validation | Pre‑write checks | Data quality |
| Storage | Delta Lake | ACID + performance |
| Loads | Full (Dim), Incremental (Fact) | Optimal trade‑off |

---

## 10. Future Enhancements
- SCD Type‑2 dimensions
- Automated schema evolution
- Data quality scorecards
- Job‑level alerting
- Bronze → Silver → Gold layering

---

## 11. Conclusion
This project demonstrates a **real‑world Databricks ETL framework** with production‑grade patterns: auditing, control metadata, validation, and scalable Delta Lake processing. It is intentionally modular to support extension into larger enterprise data platforms.

