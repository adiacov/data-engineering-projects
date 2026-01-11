# **Project P3 — Analytical Data Modeling**

## 1. Business Problem & Real-World Context

The traffic authority wants to enable **fast, correct analytical queries** for:

* Monthly accident trends
* Severity analysis
* Time-based patterns (hour, weekday, weekend)
* Geographic breakdowns

The current **curated table** is clean but still **not analytically optimal**:

* Repeated categorical values
* No explicit grain definition
* Metrics are easy to miscalculate
* Hard to extend for BI tools

Your task is to design and implement a **proper analytical data model** using **fact and dimension tables**, following data warehousing best practices.

This mirrors real work done for **data marts** and **analytics platforms**.

---

## 2. Input Data

You must use:

* **`collisions_curated`** table produced in P2
* No direct access to raw or clean layers

The curated table is your **single source of truth** for modeling.

---

## 3. Modeling Goals

You must:

* Define a **clear grain** for the fact table
* Separate facts from dimensions
* Prevent double-counting
* Enable intuitive SQL queries
* Materialize all tables locally

---

## 4. High-Level System Design

```
+---------------------+
| collisions_curated  |
+----------+----------+
           |
           v
+---------------------+
| Fact & Dimensions   |
|  - fact_collisions  |
|  - dim_date         |
|  - dim_location     |
|  - dim_severity     |
|  - dim_time         |
+---------------------+
```

---

## 5. Architecture Explanation

### **Fact Table**

* Represents **events**
* One row per collision
* Contains foreign keys + numeric metrics

### **Dimension Tables**

* Descriptive attributes
* Used for filtering, grouping, slicing
* Joined to the fact table

All tables must be **materialized** and queryable.

---

## 6. Step-by-Step Milestones

### **Milestone 1 — Define Grain**

Explicitly state:

* What one row in the fact table represents
* Why this grain is correct
* What would violate it

---

### **Milestone 2 — Identify Dimensions**

At minimum, create:

* **Date dimension**
* **Time dimension**
* **Severity dimension**
* **Location dimension**

Each must:

* Have a primary key
* Contain descriptive attributes
* Be deduplicated

---

### **Milestone 3 — Fact Table Design**

Create `fact_collisions` with:

* Surrogate key
* Foreign keys to dimensions
* Additive metrics (e.g. collision_count = 1)

No descriptive text fields allowed here.

---

### **Milestone 4 — Key Management**

* Generate surrogate keys deterministically
* Ensure referential integrity
* No NULL foreign keys

---

### **Milestone 5 — Analytical Queries**

Write SQL queries answering:

* Collisions per month
* Severity distribution
* Weekend vs weekday collisions
* Peak collision hours

---

## 7. Core Data Engineering Principles Involved

* Dimensional modeling
* Grain definition
* Star schema design
* Metric correctness
* Join performance basics

---

## 8. Open-Source Tools Used

* **Python**
* **pandas**
* **SQLite or DuckDB**
* **SQL**

No dbt yet — modeling logic is explicit.

---

## 9. Evaluation Checklist (Must-Pass)

### **Modeling Correctness**

* [ ] Fact table has a single, explicit grain
* [ ] No duplicated facts
* [ ] Metrics are additive

### **Dimensions**

* [ ] Each dimension has a primary key
* [ ] No duplicate rows
* [ ] Descriptive attributes only

### **Keys & Integrity**

* [ ] Surrogate keys used
* [ ] All foreign keys valid
* [ ] No NULL FKs in fact table

### **Analytics**

* [ ] Queries return correct results
* [ ] No double counting
* [ ] SQL is readable and logical

### **Engineering Discipline**

* [ ] Clear table naming
* [ ] Deterministic builds
* [ ] README reflects implementation

---

## 10. Optional Extensions (Locked Until Passing)

* Slowly changing dimensions (Type 1)
* Snowflake schema
* Aggregate fact tables
* BI tool connection
