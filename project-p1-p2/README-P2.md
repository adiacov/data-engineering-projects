Below is the **complete `README.md` for Project 2**.
This project **builds directly on P1**. You are expected to reuse your ingestion output and extend it, not start from scratch.

---

# **Project P2 — Batch Transformations & Data Quality**

## 1. Business Problem & Real-World Context

The traffic authority now has **raw collision data ingested** into an analytical database, but analysts report that:

* Columns are inconsistently named and typed
* Business concepts (severity, time of day, location) are hard to query
* Data quality issues are discovered late, during analysis
* There is no clear separation between *raw ingested data* and *analytics-ready data*

Your task is to design and implement a **multi-layer batch transformation pipeline** that converts ingested raw data into **clean, curated, analytics-ready tables**, while enforcing **explicit, business-driven data quality rules**.

This project mirrors the daily work of data engineers maintaining **data marts** and **analytical layers**.

---

## 2. Dataset

You must continue using:

* **UK Road Safety – Traffic Accidents (2023)**
* Source: UK Department for Transport (Open Data)
* Input data must come from **your P1 analytical table**, not directly from CSV

No new datasets.

---

## 3. High-Level System Design

```
+--------------------+
|  Raw Analytical    |
|  Table (from P1)   |
+----------+---------+
           |
           v
+--------------------+
| Transform Layer    |
|  - Cleaning        |
|  - Normalization   |
|  - Enrichment      |
+----------+---------+
           |
           v
+--------------------+
| Curated Tables     |
|  - Analytics-ready |
|  - Business rules  |
+--------------------+
```

---

## 4. Architecture Explanation

### Data Layers

1. **Raw Layer**

   * Output table from P1
   * Schema enforced but minimally transformed

2. **Clean Layer**

   * Renamed columns
   * Normalized categorical values
   * Corrected data types
   * Nulls handled explicitly

3. **Curated Layer**

   * Business-level fields
   * Derived columns
   * Ready for BI / analytics use

Each layer must be **materialized** (stored), not virtual.

---

## 5. Step-by-Step Milestones

### **Milestone 1 — Project Structure**

Extend your existing project:

```
project_p1_p2/
  ├── src/
  │   ├── ingest/
  │   ├── transform/
  │   │   ├── clean.py
  │   │   ├── curate.py
  │   │   └── rules.py
  ├── data/
  ├── README.md
```

---

### **Milestone 2 — Define Business Rules**

Create explicit rules, for example:

* Valid accident severity values
* Valid latitude/longitude ranges
* Logical timestamp bounds
* Categorical normalization (e.g. “Severe” vs “Severe ”)

Rules must be **codified**, not informal.

---

### **Milestone 3 — Clean Layer Transformations**

Implement transformations that:

* Rename columns consistently
* Normalize categorical values
* Cast data types explicitly
* Handle nulls column-by-column
* Reject or quarantine invalid rows

Materialize as a **clean table**.

---

### **Milestone 4 — Curated Layer Transformations**

Create derived fields such as:

* `accident_hour`
* `day_of_week`
* `is_weekend`
* `severity_group`
* `geo_bucket` (simple lat/long binning)

Materialize as a **curated table**.

---

### **Milestone 5 — Data Quality Reporting**

Produce metrics such as:

* Rows dropped per rule
* Percentage of invalid records
* Distribution of severity categories

These may be logged or stored in a separate table.

---

## 6. Core Data Engineering Principles Involved

* Layered batch architecture
* Deterministic transformations
* Business-driven data quality
* Data observability basics
* Reusability of transformation logic

---

## 7. Open-Source Tools Used (and Why)

* **Python**
* **pandas**
* **Pandera** (extended use)
* **SQLite or DuckDB**
* **SQL**

No orchestration yet. No Spark yet.

---

## 8. Evaluation Checklist (Must-Pass)

You pass Project P2 only if **all** are true:

### **Architecture**

* [ ] Clear raw → clean → curated layers
* [ ] Each layer is materialized
* [ ] No transformations done directly on raw tables

### **Transformations**

* [ ] Column renaming and typing is explicit
* [ ] Derived fields are deterministic
* [ ] Business rules are codified in code

### **Data Quality**

* [ ] Invalid records are detected
* [ ] Handling strategy is explicit (drop, quarantine, fix)
* [ ] Metrics are produced

### **Engineering Discipline**

* [ ] Transform code is modular
* [ ] Rules are reusable
* [ ] README reflects actual behavior

---

## 9. Optional Extensions (Locked Until Passing)

* Slowly changing dimensions
* Incremental transformations
* Multiple curated tables
* Unit tests for rules
