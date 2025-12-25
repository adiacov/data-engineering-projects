# **Project P1 — Local File-Based Batch Ingestion**

## 1. Business Problem & Real-World Context

A public transportation authority publishes monthly datasets describing **road traffic accidents**, including location, severity, weather conditions, and time of occurrence.

Data analysts and policy makers want this data **consistently ingested into an analytical database** so they can:

* Track accident trends over time
* Identify high-risk areas
* Build dashboards and reports

However, the raw data is:

* Delivered as flat files (CSV)
* Inconsistent across columns
* Prone to missing values
* Re-downloaded every month (risk of duplicates)

Your task is to build a **reliable batch ingestion pipeline** that turns raw files into a **clean, queryable analytical dataset**.

This project mirrors a very common **entry-level data engineering responsibility**.

---

## 2. Dataset (Real, Public, Ready-to-Use)

**Dataset:** UK Road Safety – Traffic Accidents

**Source:** UK Department for Transport (Open Data)

**Format:** CSV

**Access:** Public

You will use the **Accidents** dataset only.

Example download (exact link may change; document it in your code):

```
https://data.dft.gov.uk/road-accidents-safety-data/Accidents_2019.csv
```

You may choose **one year only** to keep scope controlled.

---

## 3. High-Level System Design

```
+-------------------+
|  Raw CSV File(s)  |
| (Downloaded Data) |
+---------+---------+
          |
          v
+-------------------+
| Ingestion Script  |
|  - Read CSV       |
|  - Validate       |
|  - Normalize      |
+---------+---------+
          |
          v
+-------------------+
| Analytical Store  |
| (SQLite or DuckDB)|
+-------------------+
```

---

## 4. Architecture Explanation

### Components

1. **Raw Data Layer**

   * Original CSV file(s)
   * Stored exactly as downloaded
   * Never modified

2. **Ingestion & Validation Layer (Python)**

   * Reads raw CSV
   * Enforces schema
   * Handles missing values
   * Deduplicates records
   * Logs failures

3. **Analytical Storage**

   * Local analytical database (SQLite or DuckDB)
   * One clean, structured table
   * Queryable via SQL

---

## 5. Step-by-Step Milestones

### **Milestone 1 — Project Setup**

* Create a Git repository
* Recommended structure:

  ```
  project_p1/
    ├── data/
    │   ├── raw/
    │   └── processed/
    ├── src/
    │   ├── ingest.py
    │   └── schema.py
    ├── README.md
    └── requirements.txt
  ```

---

### **Milestone 2 — Raw Data Acquisition**

* Download the dataset manually or via script
* Store it under `data/raw/`
* Do **not** modify the file

---

### **Milestone 3 — Schema Definition**

* Explicitly define:

  * Column names
  * Data types
  * Required vs optional fields
* Reject or log rows that violate critical constraints

---

### **Milestone 4 — Ingestion Logic**

Your ingestion script must:

* Read CSV in a memory-safe way
* Apply schema casting
* Normalize column names
* Convert dates/timestamps
* Handle missing values deterministically
* Deduplicate records using a logical primary key

---

### **Milestone 5 — Analytical Storage**

* Create a table programmatically
* Load clean data into SQLite or DuckDB
* Ensure **idempotency**:

  * Re-running the pipeline must not create duplicates

---

### **Milestone 6 — Basic Validation Queries**

Write SQL queries to confirm:

* Row counts
* No nulls in critical fields
* No duplicate primary keys

---

## 6. Core Data Engineering Principles Involved

* Batch ingestion patterns
* Raw vs processed data separation
* Schema-on-write
* Idempotent pipelines
* Deterministic transformations
* Data quality validation

---

## 7. Open-Source Tools Used (and Why)

* **Python**

  * Industry standard for DE scripting
* **pandas / csv module**

  * Controlled ingestion and validation
* **SQLite or DuckDB**

  * Lightweight analytical storage
  * No server required
* **SQL**

  * Universal analytical interface

(No Airflow, Spark, or dbt yet — intentionally.)

---

## 8. Evaluation Checklist (Must-Pass)

You pass Project P1 only if **all** items below are true:

### **Data Ingestion**

* [ ] Raw data stored unchanged
* [ ] Script can be re-run without errors
* [ ] No duplicate records after multiple runs

### **Schema & Quality**

* [ ] Explicit schema defined in code
* [ ] Critical fields enforced
* [ ] Missing values handled intentionally

### **Storage**

* [ ] Analytical table created via code
* [ ] Queryable using SQL
* [ ] Row counts match expectations

### **Engineering Discipline**

* [ ] Clear project structure
* [ ] Reproducible setup
* [ ] README accurately reflects implementation

---

## 9. Optional Extensions (Locked Until Passing)

Only attempt these **after** you pass:

* Multiple years ingestion
* Incremental loads
* Partitioned storage
* CLI arguments for ingestion
