# **Project P5 — Distributed Batch Processing with Apache Spark**

## 1. Business Problem & Real-World Context

The transportation authority expands the dataset to include:

* Multiple years of collision data (e.g., 2018–2023)
* Additional related datasets (vehicles, casualties)

The dataset now reaches **millions of rows**, and pandas-based processing becomes:

* Slow
* Memory-bound
* Difficult to scale

Your task is to **rebuild key parts of the pipeline using Apache Spark (local mode)** and apply distributed processing principles.

This mirrors real-world evolution:

> Script-based batch → Distributed data platform

---

## 2. Input Data

Use:

* UK Road Safety dataset
* Multiple years (at least 3 years recommended)

You may optionally include:

* Vehicles dataset
* Casualties dataset

All data must be local files (no cloud).

---

## 3. High-Level System Design

```
+------------------------+
|  Multi-Year Raw CSVs   |
+-----------+------------+
            |
            v
+------------------------+
|  Spark Ingestion       |
|  (Distributed Read)    |
+-----------+------------+
            |
            v
+------------------------+
|  Spark Transformations |
|  - Filtering           |
|  - Enrichment          |
|  - Joins               |
+-----------+------------+
            |
            v
+------------------------+
|  Parquet Storage       |
|  Partitioned by Year   |
+------------------------+
            |
            v
+------------------------+
|  Spark SQL Analytics   |
+------------------------+
```

---

## 4. Architecture Explanation

### Components

1. **SparkSession (Local Mode)**

   * Simulates distributed execution
   * Uses multiple cores

2. **DataFrame API**

   * No pandas allowed for transformations
   * Use Spark DataFrames only

3. **Partitioned Parquet Storage**

   * Store output as Parquet
   * Partition by `year`

4. **Spark SQL**

   * Analytical queries on distributed data

---

## 5. Step-by-Step Milestones

---

### **Milestone 1 — Spark Setup**

* Install PySpark
* Initialize SparkSession
* Configure:

  * Local master (e.g., `local[*]`)
  * Shuffle partitions explicitly

Demonstrate:

* `spark.sparkContext.defaultParallelism`

---

### **Milestone 2 — Distributed Ingestion**

* Read multiple CSV files using wildcard
* Infer schema OR define schema explicitly
* Count total records across years
* Validate partition count

You must show:

* How many partitions the DataFrame has
* How to change partition count

---

### **Milestone 3 — Transformations in Spark**

Reimplement P2 logic using Spark:

* Normalize categorical columns
* Enforce latitude/longitude bounds
* Filter invalid records
* Derive:

  * collision_hour
  * day_of_week
  * severity_group
  * year

No `.toPandas()` allowed.

---

### **Milestone 4 — Join Additional Dataset (Required)**

Join:

* Collisions with Vehicles OR Casualties dataset

Purpose:

* Introduce one-to-many joins
* Prevent grain explosion
* Aggregate safely before modeling

You must:

* Explain join strategy
* Show pre- and post-join counts
* Avoid duplicate inflation

---

### **Milestone 5 — Partitioned Storage**

Write output as:

* Parquet
* Partitioned by `year`

Example directory structure:

```
output/
  year=2019/
  year=2020/
  year=2021/
```

Demonstrate:

* Predicate pushdown via filtering by year
* Reduced file scan

---

### **Milestone 6 — Performance Reasoning**

Show:

* Execution plan (`.explain()`)
* Shuffle stage presence
* When wide vs narrow transformations occur

Explain:

* Why groupBy causes shuffle
* Why filter does not

---

## 6. Core Distributed Engineering Concepts

* Lazy evaluation
* Transformations vs actions
* Wide vs narrow dependencies
* Shuffle mechanics
* Partitioning strategy
* Data locality
* One-to-many join explosion risk

---

## 7. Open-Source Tools Used

* **Apache Spark (PySpark)**
* **Parquet**
* **Python**
* **Spark SQL**

No pandas for transformation logic.

---

## 8. Evaluation Checklist (Must-Pass)**

### Distributed Execution

* [ ] SparkSession configured correctly
* [ ] Multiple partitions used
* [ ] No `.toPandas()` in main pipeline

### Transformations

* [ ] Business rules reimplemented in Spark
* [ ] Derived fields created
* [ ] Invalid rows filtered correctly

### Joins

* [ ] Join logic explained
* [ ] No accidental row explosion
* [ ] Aggregations preserve grain

### Storage

* [ ] Output written as Parquet
* [ ] Partitioned by year
* [ ] Filtering by year reduces scanned files

### Performance Understanding

* [ ] `.explain()` interpreted
* [ ] Shuffle behavior understood
* [ ] Partition reasoning explained

### Engineering Discipline

* [ ] Clean project structure
* [ ] Deterministic outputs
* [ ] README matches implementation

---

## 9. Optional Extensions (Locked Until Passing)

* Bucketing
* Broadcast joins
* Incremental loads
* Delta Lake (if desired later)
* Airflow + Spark integration
